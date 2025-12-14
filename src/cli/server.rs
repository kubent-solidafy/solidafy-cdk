//! HTTP server mode for REST API access to connector operations

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::auth::{AuthConfig, Authenticator};
use crate::connectors::{self, is_database_connector, list_builtin_info};
use crate::database::DbEngine;
use crate::decode::{CsvDecoder, JsonDecoder, JsonlDecoder, RecordDecoder};
use crate::engine::{Message, SyncConfig, SyncEngine};
use crate::error::{Error, Result};
use crate::http::{HttpClient, HttpClientConfig, RateLimiterConfig, RequestConfig};
use crate::loader::{
    load_connector, AuthDefinition, ConnectorDefinition, DatabaseConnectionDef,
    DatabaseEngine as DbType, DatabaseStreamDefinition, DecoderDefinition, PaginationDefinition,
    PartitionDefinition, StopConditionDefinition,
};
use crate::output::{build_partitioned_dir, build_partitioned_path, CloudDestination};
use crate::pagination::{
    CursorPaginator, LinkHeaderPaginator, NextUrlPaginator, NoPaginator, OffsetPaginator,
    PageNumberPaginator, Paginator, StopCondition,
};
use crate::partition::{ListRouter, PartitionRouter};
use crate::schema::SchemaInferrer;
use crate::state::StateManager;
use crate::template::{self, TemplateContext};
use base64::Engine as _;

/// Server configuration
#[derive(Clone)]
pub struct ServerConfig {
    /// Directory containing connector YAML files
    pub connectors_dir: PathBuf,
}

/// App state shared across handlers
#[derive(Clone)]
struct AppState {
    config: ServerConfig,
}

/// Request body for check/discover endpoints
#[derive(Debug, Deserialize)]
struct ConnectorRequest {
    /// Connector name (without .yaml extension)
    connector: String,
    /// Configuration values (api_key, etc.)
    config: Value,
}

/// Request body for discover with sampling
#[derive(Debug, Deserialize)]
struct DiscoverRequest {
    /// Connector name (without .yaml extension)
    connector: String,
    /// Configuration values (api_key, etc.)
    config: Value,
    /// Number of records to sample for schema inference (0 = static schema)
    #[serde(default)]
    sample: usize,
}

/// Request body for sync endpoint
#[derive(Debug, Deserialize)]
struct SyncRequest {
    /// Connector name (without .yaml extension)
    connector: String,
    /// Configuration values (api_key, etc.)
    config: Value,
    /// Streams to sync (comma-separated or array). If empty, syncs all streams.
    #[serde(default)]
    streams: Option<Vec<String>>,
    /// Output destination (local path or cloud URL: s3://, r2://, gs://, az://)
    #[serde(default)]
    output: Option<String>,
    /// Output format: "json" or "parquet" (default: "json")
    #[serde(default = "default_format")]
    format: String,
    /// State from previous sync for incremental sync
    #[serde(default)]
    state: Option<Value>,
    /// Maximum records to sync per stream
    #[serde(default)]
    max_records: Option<usize>,
    /// Cursor field configuration for incremental database sync
    /// Maps stream/table name to cursor field name: {"public.users": "updated_at"}
    #[serde(default)]
    cursor_fields: Option<HashMap<String, String>>,
}

fn default_format() -> String {
    "json".to_string()
}

/// Response wrapper
#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(msg: impl Into<String>) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

/// Start the HTTP server
pub async fn serve(config: ServerConfig, port: u16) -> Result<()> {
    let state = AppState { config };

    // Build CORS layer - allow all origins for development
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Build router
    let app = Router::new()
        .route("/health", get(health))
        .route("/connectors", get(list_connectors))
        .route("/connectors/:name/streams", get(get_streams))
        .route("/check", post(check_connection))
        .route("/streams", post(get_streams_post))
        .route("/discover", post(discover))
        .route("/sync", post(sync_data))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Starting HTTP server on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::config(format!("Failed to bind to port {port}: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::config(format!("Server error: {e}")))?;

    Ok(())
}

/// Health check endpoint
async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// List available connectors (returns built-in connectors)
async fn list_connectors(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // Return built-in connectors with metadata and config schema
    let connectors: Vec<Value> = list_builtin_info()
        .into_iter()
        .map(|info| {
            let config_fields: Vec<Value> = info
                .config_schema
                .iter()
                .map(|field| {
                    json!({
                        "name": field.name,
                        "type": field.field_type,
                        "required": field.required,
                        "secret": field.secret,
                        "description": field.description,
                        "default": field.default
                    })
                })
                .collect();

            // Check if this is a database connector (streams are dynamic)
            let is_db = is_database_connector(info.name);

            json!({
                "name": info.name,
                "description": info.description,
                "category": info.category,
                "aliases": info.aliases,
                "config_schema": config_fields,
                "streams": if is_db { json!(null) } else { json!(info.streams) },
                "streams_dynamic": is_db,
                "streams_hint": if is_db {
                    Some("Call POST /streams with connection config to discover tables")
                } else {
                    None
                }
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(ApiResponse::success(json!({
            "type": "CONNECTORS",
            "connectors": connectors
        }))),
    )
        .into_response()
}

/// Get streams for a connector (GET - no config needed)
/// First checks built-in connectors, then falls back to file
async fn get_streams(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    // Try built-in connector first, then fall back to file
    let connector = if let Some(yaml) = connectors::get_builtin(&name) {
        crate::loader::load_connector_from_str(yaml)
    } else {
        let path = state.config.connectors_dir.join(format!("{name}.yaml"));
        load_connector(&path)
    };

    match connector {
        Ok(connector) => {
            let streams: Vec<&str> = connector.streams.iter().map(|s| s.name.as_str()).collect();

            (
                StatusCode::OK,
                Json(ApiResponse::success(json!({
                    "type": "STREAMS",
                    "connector": connector.name,
                    "streams": streams
                }))),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<()>::error(format!(
                "Connector not found: {e}"
            ))),
        )
            .into_response(),
    }
}

/// Get streams for a connector (POST - same as GET but accepts body)
async fn get_streams_post(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ConnectorRequest>,
) -> impl IntoResponse {
    // Check if this is a database connector
    if is_database_connector(&req.connector) {
        return get_database_streams(&req);
    }

    let connector = resolve_connector(&req.connector, &state.config.connectors_dir);

    match connector {
        Ok(connector) => {
            let streams: Vec<&str> = connector.streams.iter().map(|s| s.name.as_str()).collect();

            (
                StatusCode::OK,
                Json(ApiResponse::success(json!({
                    "type": "STREAMS",
                    "connector": connector.name,
                    "streams": streams
                }))),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::<()>::error(format!(
                "Connector not found: {e}"
            ))),
        )
            .into_response(),
    }
}

/// Helper to resolve connector by name (built-in first, then file)
fn resolve_connector(name: &str, connectors_dir: &std::path::Path) -> Result<ConnectorDefinition> {
    // Try built-in connector first
    if let Some(yaml) = connectors::get_builtin(name) {
        return crate::loader::load_connector_from_str(yaml);
    }
    // Fall back to file
    let path = connectors_dir.join(format!("{name}.yaml"));
    load_connector(&path)
}

/// Check connection to API
async fn check_connection(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ConnectorRequest>,
) -> impl IntoResponse {
    // Check if this is a database connector
    if is_database_connector(&req.connector) {
        return check_database_connection(&req);
    }

    let connector = match resolve_connector(&req.connector, &state.config.connectors_dir) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<()>::error(format!(
                    "Connector not found: {e}"
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config);

    // Render base URL
    let base_url = match template::render(&connector.base_url, &context) {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!("Template error: {e}"))),
            )
                .into_response();
        }
    };

    // Build auth headers
    let auth_headers = match build_auth_headers_async(&connector.auth, &context).await {
        Ok(h) => h,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!("Auth error: {e}"))),
            )
                .into_response();
        }
    };

    // Build HTTP client
    let http_config = build_http_config(&connector, &base_url);
    let client = HttpClient::with_config(http_config);

    // Build check URL
    let test_url = if let Some(check_def) = &connector.check {
        let path = match template::render(&check_def.path, &context) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::<()>::error(format!("Template error: {e}"))),
                )
                    .into_response();
            }
        };
        let mut url = format!("{}{}", base_url.trim_end_matches('/'), path);

        // Add query params if any
        if !check_def.params.is_empty() {
            let params: Vec<String> = check_def
                .params
                .iter()
                .filter_map(|(k, v)| {
                    template::render(v, &context)
                        .ok()
                        .map(|rendered_v| format!("{k}={rendered_v}"))
                })
                .collect();
            if !params.is_empty() {
                url = format!("{}?{}", url, params.join("&"));
            }
        }
        url
    } else if let Some(first_stream) = connector.streams.first() {
        let path = match template::render(&first_stream.request.path, &context) {
            Ok(p) => p,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::<()>::error(format!("Template error: {e}"))),
                )
                    .into_response();
            }
        };
        format!("{}{}", base_url.trim_end_matches('/'), path)
    } else {
        format!("{}/", base_url.trim_end_matches('/'))
    };

    // Merge headers
    let mut headers = auth_headers;
    headers.extend(connector.headers.clone());

    let mut request_config = RequestConfig::new();
    request_config.headers = headers;

    // Make request
    match client.get_with_config(&test_url, request_config).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse::success(json!({
                "type": "CONNECTION_STATUS",
                "connectionStatus": {
                    "status": "SUCCEEDED",
                    "message": "Connection successful"
                }
            }))),
        )
            .into_response(),
        Err(e) => (
            StatusCode::OK,
            Json(ApiResponse::success(json!({
                "type": "CONNECTION_STATUS",
                "connectionStatus": {
                    "status": "FAILED",
                    "message": format!("Connection failed: {}", e)
                }
            }))),
        )
            .into_response(),
    }
}

/// Discover streams with schemas
async fn discover(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DiscoverRequest>,
) -> impl IntoResponse {
    let connector = match resolve_connector(&req.connector, &state.config.connectors_dir) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<()>::error(format!(
                    "Connector not found: {e}"
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config.clone());

    // Sample for schema inference if requested
    let inferred_schemas = if req.sample > 0 {
        match sample_streams_for_schema(&connector, &context, req.sample).await {
            Ok(schemas) => schemas,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::<()>::error(format!("Sampling failed: {e}"))),
                )
                    .into_response();
            }
        }
    } else {
        HashMap::new()
    };

    // Build catalog
    let mut streams = Vec::new();
    for stream_def in &connector.streams {
        let json_schema = if let Some(schema) = inferred_schemas.get(&stream_def.name) {
            schema.to_json()
        } else {
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": true
            })
        };

        let primary_key: Vec<Vec<String>> = stream_def
            .primary_key
            .iter()
            .map(|k| vec![k.clone()])
            .collect();

        streams.push(json!({
            "name": stream_def.name,
            "json_schema": json_schema,
            "supported_sync_modes": ["full_refresh", "incremental"],
            "source_defined_cursor": stream_def.cursor_field.is_some(),
            "default_cursor_field": stream_def.cursor_field.as_ref().map(|f| vec![f.clone()]),
            "source_defined_primary_key": primary_key
        }));
    }

    (
        StatusCode::OK,
        Json(ApiResponse::success(json!({
            "type": "CATALOG",
            "catalog": {
                "streams": streams
            }
        }))),
    )
        .into_response()
}

// Helper functions (duplicated from runner.rs for now - could be refactored)

fn build_http_config(connector: &ConnectorDefinition, base_url: &str) -> HttpClientConfig {
    let mut builder = HttpClientConfig::builder()
        .base_url(base_url)
        .timeout(std::time::Duration::from_secs(connector.http.timeout_secs))
        .max_retries(connector.http.max_retries);

    if let Some(rps) = connector.http.rate_limit_rps {
        builder = builder.rate_limit(RateLimiterConfig::new(rps, rps));
    } else {
        builder = builder.no_rate_limit();
    }

    if let Some(ua) = &connector.http.user_agent {
        builder = builder.user_agent(ua);
    }

    builder.build()
}

async fn build_auth_headers_async(
    auth: &Option<AuthDefinition>,
    context: &TemplateContext,
) -> Result<HashMap<String, String>> {
    use base64::Engine as _;

    let mut headers = HashMap::new();

    let Some(auth_def) = auth else {
        return Ok(headers);
    };

    match auth_def {
        AuthDefinition::ApiKey {
            key,
            value,
            location,
        } => {
            let rendered_value = template::render(value, context)?;
            if location == "header" {
                headers.insert(key.clone(), rendered_value);
            }
        }
        AuthDefinition::Bearer { token } => {
            let rendered_token = template::render(token, context)?;
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {rendered_token}"),
            );
        }
        AuthDefinition::Basic { username, password } => {
            let rendered_username = template::render(username, context)?;
            let rendered_password = template::render(password, context)?;
            let credentials = base64::engine::general_purpose::STANDARD
                .encode(format!("{rendered_username}:{rendered_password}"));
            headers.insert("Authorization".to_string(), format!("Basic {credentials}"));
        }
        AuthDefinition::None => {}
        // OAuth2 types would need the full authenticator - for now just return error
        AuthDefinition::OAuth2ClientCredentials { .. }
        | AuthDefinition::OAuth2RefreshToken { .. }
        | AuthDefinition::SessionToken { .. } => {
            return Err(Error::auth(
                "OAuth2/Session auth not yet supported in HTTP mode",
            ));
        }
    }

    Ok(headers)
}

async fn sample_streams_for_schema(
    connector: &ConnectorDefinition,
    context: &TemplateContext,
    sample_count: usize,
) -> Result<HashMap<String, crate::schema::JsonSchema>> {
    let mut schemas = HashMap::new();

    let base_url = template::render(&connector.base_url, context)?;
    let auth_headers = build_auth_headers_async(&connector.auth, context).await?;
    let http_config = build_http_config(connector, &base_url);
    let client = HttpClient::with_config(http_config);

    for stream_def in &connector.streams {
        // Skip streams with partitions
        if stream_def.partition.is_some() {
            continue;
        }

        // Skip streams with partition-dependent params
        let has_partition_params = stream_def
            .request
            .params
            .values()
            .any(|v| v.contains("{{ partition."));
        if has_partition_params {
            continue;
        }

        // Build request URL
        let path = template::render(&stream_def.request.path, context)?;
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);

        // Build query params
        let mut params = HashMap::new();
        for (key, value) in &stream_def.request.params {
            if let Ok(rendered) = template::render(value, context) {
                params.insert(key.clone(), rendered);
            }
        }

        // Merge headers
        let mut headers = auth_headers.clone();
        headers.extend(connector.headers.clone());
        headers.extend(stream_def.headers.clone());

        let mut request_config = RequestConfig::new();
        request_config.headers = headers;
        request_config.query = params;

        // Make request
        if let Ok(response) = client.get_with_config(&url, request_config).await {
            if let Ok(body) = response.text().await {
                if let Ok(json_value) = serde_json::from_str::<Value>(&body) {
                    let records = extract_records(&json_value, &stream_def.decoder);
                    let sample_records: Vec<_> = records.into_iter().take(sample_count).collect();

                    if !sample_records.is_empty() {
                        let mut inferrer = SchemaInferrer::new();
                        let schema = inferrer.infer_from_records(&sample_records);
                        schemas.insert(stream_def.name.clone(), schema);
                    }
                }
            }
        }
    }

    Ok(schemas)
}

fn extract_records(value: &Value, decoder: &DecoderDefinition) -> Vec<Value> {
    match decoder {
        DecoderDefinition::Json { records_path } => {
            if let Some(path) = records_path {
                let mut current = value;
                for part in path.split('.') {
                    if let Some(obj) = current.as_object() {
                        if let Some(next) = obj.get(part) {
                            current = next;
                        } else {
                            return vec![];
                        }
                    } else {
                        return vec![];
                    }
                }
                if let Some(arr) = current.as_array() {
                    arr.clone()
                } else {
                    vec![current.clone()]
                }
            } else if let Some(arr) = value.as_array() {
                arr.clone()
            } else {
                vec![value.clone()]
            }
        }
        DecoderDefinition::Jsonl => {
            if let Some(arr) = value.as_array() {
                arr.clone()
            } else {
                vec![value.clone()]
            }
        }
        _ => vec![value.clone()],
    }
}

/// Sync data from a connector
async fn sync_data(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SyncRequest>,
) -> impl IntoResponse {
    // Check if this is a database connector
    if is_database_connector(&req.connector) {
        return sync_database_data(&req);
    }

    let connector = match resolve_connector(&req.connector, &state.config.connectors_dir) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<()>::error(format!(
                    "Connector not found: {e}"
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config.clone());

    // Render base URL
    let base_url = match template::render(&connector.base_url, &context) {
        Ok(url) => url,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!("Template error: {e}"))),
            )
                .into_response();
        }
    };

    // Build auth
    let authenticator = match create_authenticator(&connector.auth, &context) {
        Ok(a) => a,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!("Auth error: {e}"))),
            )
                .into_response();
        }
    };

    let auth_headers =
        match build_auth_headers_full(&connector.auth, &context, authenticator.as_ref()).await {
            Ok(h) => h,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::<()>::error(format!("Auth error: {e}"))),
                )
                    .into_response();
            }
        };

    // Parse output destination
    let destination = match req
        .output
        .as_ref()
        .map(|s| CloudDestination::parse(s))
        .transpose()
    {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!("Invalid output: {e}"))),
            )
                .into_response();
        }
    };

    // Initialize state manager
    let state_manager = if let Some(state_value) = &req.state {
        match StateManager::from_json(&state_value.to_string()) {
            Ok(s) => s,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::<()>::error(format!("Invalid state: {e}"))),
                )
                    .into_response();
            }
        }
    } else {
        StateManager::in_memory()
    };

    // Determine which streams to sync
    let stream_filter: Option<Vec<String>> = req.streams;

    // Build HTTP client
    let http_config = build_http_config(&connector, &base_url);
    let client = HttpClient::with_config(http_config);

    // Build sync config
    let mut sync_config = SyncConfig::new();
    if let Some(max) = req.max_records {
        sync_config = sync_config.with_max_records(max);
    }

    let mut engine = SyncEngine::new(client, state_manager).with_config(sync_config);

    // Track results
    let mut stream_results: Vec<Value> = Vec::new();
    let mut all_records: Vec<Value> = Vec::new();
    let mut total_records = 0usize;
    let sync_start = std::time::Instant::now();
    let is_parquet = req.format == "parquet";

    // Process each stream
    for stream_def in &connector.streams {
        // Check filter
        if let Some(ref filter) = stream_filter {
            if !filter.contains(&stream_def.name) {
                continue;
            }
        }

        let stream_start = std::time::Instant::now();
        let records_before = engine.stats().records_synced;

        // Build decoder
        let decoder: Box<dyn RecordDecoder> = build_decoder(&stream_def.decoder);

        // Build paginator
        let paginator: Box<dyn Paginator> = build_paginator(stream_def.pagination.as_ref());

        // Merge headers
        let mut headers = auth_headers.clone();
        headers.extend(connector.headers.clone());
        headers.extend(stream_def.headers.clone());

        // Sync stream
        let sync_result = if let Some(partition_def) = &stream_def.partition {
            let router: Box<dyn PartitionRouter> = build_router(partition_def);
            engine
                .sync_partitioned_stream(
                    &stream_def.name,
                    &base_url,
                    &stream_def.request.path,
                    &stream_def.request.params,
                    &headers,
                    decoder.as_ref(),
                    paginator.as_ref(),
                    router.as_ref(),
                    &context,
                )
                .await
        } else {
            engine
                .sync_stream(
                    &stream_def.name,
                    &base_url,
                    &stream_def.request.path,
                    &stream_def.request.params,
                    &headers,
                    decoder.as_ref(),
                    paginator.as_ref(),
                    &context,
                    stream_def.cursor_field.as_deref(),
                )
                .await
        };

        let stream_duration_ms = stream_start.elapsed().as_millis() as u64;
        let records_after = engine.stats().records_synced;
        let stream_records = records_after - records_before;

        match sync_result {
            Ok(messages) => {
                // Process messages
                for msg in messages {
                    if let Message::Record { stream, batch } = &msg {
                        if is_parquet {
                            // Write to cloud storage if destination specified
                            if let Some(dest) = destination.as_ref() {
                                if let Ok(parquet_bytes) = batch_to_parquet_bytes(batch) {
                                    let _ = dest.write_parquet(stream, parquet_bytes).await;
                                }
                            }
                        } else {
                            // Collect records as JSON
                            if let Ok(records) = crate::output::arrow_to_json(batch) {
                                let emitted_at = chrono::Utc::now().timestamp_millis();
                                for record in records {
                                    all_records.push(json!({
                                        "stream": stream,
                                        "data": record,
                                        "emitted_at": emitted_at
                                    }));
                                }
                            }
                        }
                    }
                }

                total_records += stream_records;

                let mut stream_result = json!({
                    "stream": stream_def.name,
                    "status": "SUCCESS",
                    "records_synced": stream_records,
                    "duration_ms": stream_duration_ms
                });

                if is_parquet {
                    if let Some(dest) = destination.as_ref() {
                        // Use Hive-style partitioned path for output file
                        let partitioned = build_partitioned_path(&stream_def.name, "parquet");
                        stream_result["output_file"] =
                            json!(format!("{}://{}", dest.scheme(), partitioned));
                    }
                }

                stream_results.push(stream_result);
            }
            Err(e) => {
                stream_results.push(json!({
                    "stream": stream_def.name,
                    "status": "FAILED",
                    "error": e.to_string(),
                    "records_synced": stream_records,
                    "duration_ms": stream_duration_ms
                }));
            }
        }
    }

    // Get final state
    let final_state = engine
        .state()
        .to_json()
        .await
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or(json!({}));

    let total_duration_ms = sync_start.elapsed().as_millis() as u64;
    let successful_streams = stream_results
        .iter()
        .filter(|r| r["status"] == "SUCCESS")
        .count();
    let failed_streams = stream_results
        .iter()
        .filter(|r| r["status"] == "FAILED")
        .count();

    let status = if failed_streams == 0 {
        "SUCCEEDED"
    } else if successful_streams == 0 {
        "FAILED"
    } else {
        "PARTIAL"
    };

    let response = json!({
        "type": "SYNC_RESULT",
        "result": {
            "status": status,
            "connector": connector.name,
            "total_records": total_records,
            "total_streams": stream_results.len(),
            "successful_streams": successful_streams,
            "failed_streams": failed_streams,
            "duration_ms": total_duration_ms,
            "output": {
                "format": req.format,
                "destination": req.output
            },
            "streams": stream_results,
            "state": final_state,
            "records": if is_parquet { json!(null) } else { json!(all_records) }
        }
    });

    (StatusCode::OK, Json(ApiResponse::success(response))).into_response()
}

/// Build auth headers with full OAuth2 support
async fn build_auth_headers_full(
    auth: &Option<AuthDefinition>,
    context: &TemplateContext,
    authenticator: Option<&Arc<Authenticator>>,
) -> Result<HashMap<String, String>> {
    let mut headers = HashMap::new();

    let Some(auth_def) = auth else {
        return Ok(headers);
    };

    match auth_def {
        AuthDefinition::ApiKey {
            key,
            value,
            location,
        } => {
            let rendered_value = template::render(value, context)?;
            if location == "header" {
                headers.insert(key.clone(), rendered_value);
            }
        }
        AuthDefinition::Bearer { token } => {
            let rendered_token = template::render(token, context)?;
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {rendered_token}"),
            );
        }
        AuthDefinition::Basic { username, password } => {
            let rendered_username = template::render(username, context)?;
            let rendered_password = template::render(password, context)?;
            let credentials = base64::engine::general_purpose::STANDARD
                .encode(format!("{rendered_username}:{rendered_password}"));
            headers.insert("Authorization".to_string(), format!("Basic {credentials}"));
        }
        AuthDefinition::None => {}
        AuthDefinition::OAuth2ClientCredentials { .. }
        | AuthDefinition::OAuth2RefreshToken { .. }
        | AuthDefinition::SessionToken { .. } => {
            if let Some(auth) = authenticator {
                let req = reqwest::Client::new().get("http://dummy");
                let req = auth.apply(req).await?;
                if let Some(auth_header) = req
                    .build()
                    .ok()
                    .and_then(|r| r.headers().get("Authorization").cloned())
                {
                    headers.insert(
                        "Authorization".to_string(),
                        auth_header.to_str().unwrap_or_default().to_string(),
                    );
                }
            } else {
                return Err(Error::auth("OAuth2/Session auth requires authenticator"));
            }
        }
    }

    Ok(headers)
}

/// Create authenticator for OAuth2 types
fn create_authenticator(
    auth: &Option<AuthDefinition>,
    context: &TemplateContext,
) -> Result<Option<Arc<Authenticator>>> {
    let Some(auth_def) = auth else {
        return Ok(None);
    };

    match auth_def {
        AuthDefinition::OAuth2ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scopes,
        } => {
            let config = AuthConfig::Oauth2ClientCredentials {
                token_url: template::render(token_url, context)?,
                client_id: template::render(client_id, context)?,
                client_secret: template::render(client_secret, context)?,
                scopes: scopes.clone(),
                token_body: HashMap::new(),
            };
            Ok(Some(Arc::new(Authenticator::new(config))))
        }
        AuthDefinition::OAuth2RefreshToken {
            token_url,
            client_id,
            client_secret,
            refresh_token,
        } => {
            let config = AuthConfig::Oauth2Refresh {
                token_url: template::render(token_url, context)?,
                client_id: template::render(client_id, context)?,
                client_secret: template::render(client_secret, context)?,
                refresh_token: template::render(refresh_token, context)?,
            };
            Ok(Some(Arc::new(Authenticator::new(config))))
        }
        AuthDefinition::SessionToken {
            login_url,
            body,
            token_path,
            header_name,
            header_prefix,
        } => {
            let rendered_body = template::render(body, context)?;
            let login_body: HashMap<String, String> =
                serde_json::from_str(&rendered_body).unwrap_or_default();
            let config = AuthConfig::Session {
                login_url: template::render(login_url, context)?,
                login_method: reqwest::Method::POST,
                login_body,
                token_path: token_path.clone(),
                token_header: header_name.clone(),
                token_prefix: if header_prefix.is_empty() {
                    None
                } else {
                    Some(header_prefix.clone())
                },
                expires_in_path: None,
            };
            Ok(Some(Arc::new(Authenticator::new(config))))
        }
        _ => Ok(None),
    }
}

/// Build decoder from definition
fn build_decoder(def: &DecoderDefinition) -> Box<dyn RecordDecoder> {
    match def {
        DecoderDefinition::Json { records_path } => {
            if let Some(path) = records_path {
                Box::new(JsonDecoder::with_path(path))
            } else {
                Box::new(JsonDecoder::new())
            }
        }
        DecoderDefinition::Jsonl => Box::new(JsonlDecoder::new()),
        DecoderDefinition::Csv {
            delimiter,
            has_header,
        } => Box::new(CsvDecoder::with_options(*delimiter, *has_header)),
        DecoderDefinition::Xml { .. } => Box::new(JsonDecoder::new()),
    }
}

/// Build paginator from definition
fn build_paginator(def: Option<&PaginationDefinition>) -> Box<dyn Paginator> {
    match def {
        None | Some(PaginationDefinition::None) => Box::new(NoPaginator),
        Some(PaginationDefinition::Offset {
            offset_param,
            limit_param,
            limit,
            stop,
        }) => {
            let stop_condition = build_stop_condition(stop);
            Box::new(OffsetPaginator::new(
                offset_param,
                limit_param,
                *limit,
                stop_condition,
            ))
        }
        Some(PaginationDefinition::PageNumber {
            page_param,
            start_page,
            page_size_param,
            page_size,
            stop,
        }) => {
            let stop_condition = build_stop_condition(stop);
            let mut pag = PageNumberPaginator::new(page_param, *start_page)
                .with_stop_condition(stop_condition);
            if let (Some(param), Some(size)) = (page_size_param, page_size) {
                pag = pag.with_page_size(param, *size);
            }
            Box::new(pag)
        }
        Some(PaginationDefinition::Cursor {
            cursor_param,
            cursor_path,
            ..
        }) => Box::new(CursorPaginator::new(
            cursor_param,
            cursor_path,
            StopCondition::EmptyPage,
        )),
        Some(PaginationDefinition::LinkHeader { rel }) => Box::new(LinkHeaderPaginator::new(rel)),
        Some(PaginationDefinition::NextUrl { next_url_path }) => {
            Box::new(NextUrlPaginator::new(next_url_path))
        }
    }
}

/// Build stop condition from definition
fn build_stop_condition(def: &StopConditionDefinition) -> StopCondition {
    match def {
        StopConditionDefinition::EmptyPage => StopCondition::EmptyPage,
        StopConditionDefinition::TotalCount { path } => StopCondition::total_count(path),
        StopConditionDefinition::TotalPages { path } => StopCondition::total_pages(path),
        StopConditionDefinition::Field { path, value } => StopCondition::field(path, value.clone()),
    }
}

/// Build partition router from definition
fn build_router(def: &PartitionDefinition) -> Box<dyn PartitionRouter> {
    match def {
        PartitionDefinition::List { field, values } => {
            Box::new(ListRouter::new(values.clone(), field))
        }
        PartitionDefinition::Parent { .. } => Box::new(ListRouter::new(vec![], "parent_id")),
        PartitionDefinition::DateRange { .. } => Box::new(ListRouter::new(vec![], "date")),
        PartitionDefinition::AsyncJob { .. } => Box::new(ListRouter::new(vec![], "job_id")),
    }
}

/// Convert Arrow batch to Parquet bytes
fn batch_to_parquet_bytes(batch: &arrow::record_batch::RecordBatch) -> Result<bytes::Bytes> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .map_err(|e| Error::io(format!("Failed to create parquet writer: {e}")))?;

    writer
        .write(batch)
        .map_err(|e| Error::io(format!("Failed to write parquet: {e}")))?;

    writer
        .close()
        .map_err(|e| Error::io(format!("Failed to close parquet writer: {e}")))?;

    Ok(bytes::Bytes::from(buf))
}

/// Sync data from a database connector (postgres, mysql, sqlite)
fn sync_database_data(req: &SyncRequest) -> axum::response::Response {
    let sync_start = std::time::Instant::now();

    // Determine database type
    let db_type = match req.connector.to_lowercase().as_str() {
        "postgres" | "postgresql" => DbType::Postgres,
        "mysql" | "mariadb" => DbType::Mysql,
        "sqlite" => DbType::Sqlite,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!(
                    "Unknown database connector: {}",
                    req.connector
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config.clone());

    // Build connection from config
    let connection = DatabaseConnectionDef {
        connection_string: req
            .config
            .get("connection_string")
            .and_then(|v| v.as_str())
            .map(String::from),
        host: req
            .config
            .get("host")
            .and_then(|v| v.as_str())
            .map(String::from),
        port: req
            .config
            .get("port")
            .and_then(serde_json::Value::as_u64)
            .map(|p| p as u16),
        database: req
            .config
            .get("database")
            .and_then(|v| v.as_str())
            .map(String::from),
        user: req
            .config
            .get("user")
            .and_then(|v| v.as_str())
            .map(String::from),
        password: req
            .config
            .get("password")
            .and_then(|v| v.as_str())
            .map(String::from),
        ssl_mode: req
            .config
            .get("ssl_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("prefer")
            .to_string(),
    };

    // Create database engine
    let engine = match DbEngine::new(db_type, &connection, &context) {
        Ok(e) => e,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!(
                    "Failed to connect to database: {e}"
                ))),
            )
                .into_response();
        }
    };

    // Configure cloud storage if output is cloud URL
    if let Some(ref output) = req.output {
        if output.starts_with("s3://")
            || output.starts_with("r2://")
            || output.starts_with("gs://")
            || output.starts_with("az://")
        {
            if let Err(e) = engine.configure_cloud_storage() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::<()>::error(format!(
                        "Failed to configure cloud storage: {e}"
                    ))),
                )
                    .into_response();
            }
        }
    }

    // Get list of tables if no streams specified
    let tables_to_sync: Vec<String> = if let Some(ref streams) = req.streams {
        streams.clone()
    } else {
        match engine.list_tables() {
            Ok(tables) => tables,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::<()>::error(format!(
                        "Failed to list tables: {e}"
                    ))),
                )
                    .into_response();
            }
        }
    };

    let is_parquet = req.format == "parquet";
    let mut stream_results: Vec<Value> = Vec::new();
    let mut all_records: Vec<Value> = Vec::new();
    let mut total_records = 0usize;

    // Process each table/stream
    for table in &tables_to_sync {
        let stream_start = std::time::Instant::now();

        // Get cursor field from cursor_fields config if provided
        let cursor_field = req
            .cursor_fields
            .as_ref()
            .and_then(|cf| cf.get(table))
            .cloned();

        // Build stream definition
        let stream_def = DatabaseStreamDefinition {
            name: table.clone(),
            table: Some(table.clone()),
            query: None,
            primary_key: vec![],
            cursor_field: cursor_field.clone(),
            batch_size: req.max_records.unwrap_or(10000) as u32,
        };

        // Get cursor value from state if available (only if cursor_field is defined)
        let cursor_value = if cursor_field.is_some() {
            req.state
                .as_ref()
                .and_then(|s| s.get(table))
                .and_then(|v| v.get("cursor"))
                .and_then(|v| v.as_str())
        } else {
            None
        };

        let sync_result = if is_parquet {
            if let Some(ref output) = req.output {
                // Use Hive-style partitioning: {stream}/dt={YYYY-MM-DD}/data.parquet
                let output_dir = build_partitioned_dir(output, table);
                if let Err(e) = std::fs::create_dir_all(&output_dir) {
                    Err(Error::io(format!(
                        "Failed to create directory {output_dir}: {e}"
                    )))
                } else {
                    let output_path = format!("{output_dir}/data.parquet");
                    engine.sync_to_parquet(&stream_def, &output_path, cursor_value)
                }
            } else {
                // Parquet requires output path
                Err(Error::config("Parquet format requires output path"))
            }
        } else if let Some(ref output) = req.output {
            // Use Hive-style partitioning: {stream}/dt={YYYY-MM-DD}/data.json
            let output_dir = build_partitioned_dir(output, table);
            if let Err(e) = std::fs::create_dir_all(&output_dir) {
                Err(Error::io(format!(
                    "Failed to create directory {output_dir}: {e}"
                )))
            } else {
                let output_path = format!("{output_dir}/data.json");
                engine.sync_to_json_file(&stream_def, &output_path, cursor_value)
            }
        } else {
            // JSON in memory
            engine.sync_to_json(&stream_def, cursor_value)
        };

        let stream_duration_ms = stream_start.elapsed().as_millis() as u64;

        match sync_result {
            Ok(result) => {
                total_records += result.record_count;

                // Collect records if JSON and no output path
                if let Some(records) = result.records {
                    let emitted_at = chrono::Utc::now().timestamp_millis();
                    for record in records {
                        all_records.push(json!({
                            "stream": table,
                            "data": record,
                            "emitted_at": emitted_at
                        }));
                    }
                }

                let mut stream_result = json!({
                    "stream": table,
                    "status": "SUCCESS",
                    "records_synced": result.record_count,
                    "duration_ms": stream_duration_ms
                });

                if let Some(output_path) = result.output_path {
                    stream_result["output_file"] = json!(output_path);
                }

                if let Some(cursor) = result.cursor_value {
                    stream_result["cursor_value"] = json!(cursor);
                }

                stream_results.push(stream_result);
            }
            Err(e) => {
                stream_results.push(json!({
                    "stream": table,
                    "status": "FAILED",
                    "error": e.to_string(),
                    "records_synced": 0,
                    "duration_ms": stream_duration_ms
                }));
            }
        }
    }

    let total_duration_ms = sync_start.elapsed().as_millis() as u64;
    let successful_streams = stream_results
        .iter()
        .filter(|r| r["status"] == "SUCCESS")
        .count();
    let failed_streams = stream_results
        .iter()
        .filter(|r| r["status"] == "FAILED")
        .count();

    let status = if failed_streams == 0 {
        "SUCCEEDED"
    } else if successful_streams == 0 {
        "FAILED"
    } else {
        "PARTIAL"
    };

    // Build state from cursor values
    let mut final_state = json!({});
    for result in &stream_results {
        if let Some(cursor) = result.get("cursor_value") {
            if let Some(stream) = result.get("stream").and_then(|s| s.as_str()) {
                final_state[stream] = json!({ "cursor": cursor });
            }
        }
    }

    let response = json!({
        "type": "SYNC_RESULT",
        "result": {
            "status": status,
            "connector": req.connector,
            "connector_type": "database",
            "total_records": total_records,
            "total_streams": stream_results.len(),
            "successful_streams": successful_streams,
            "failed_streams": failed_streams,
            "duration_ms": total_duration_ms,
            "output": {
                "format": req.format,
                "destination": req.output
            },
            "streams": stream_results,
            "state": final_state,
            "records": if is_parquet || req.output.is_some() { json!(null) } else { json!(all_records) }
        }
    });

    (StatusCode::OK, Json(ApiResponse::success(response))).into_response()
}

/// Check connection to a database
fn check_database_connection(req: &ConnectorRequest) -> axum::response::Response {
    // Determine database type
    let db_type = match req.connector.to_lowercase().as_str() {
        "postgres" | "postgresql" => DbType::Postgres,
        "mysql" | "mariadb" => DbType::Mysql,
        "sqlite" => DbType::Sqlite,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!(
                    "Unknown database connector: {}",
                    req.connector
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config.clone());

    // Build connection from config
    let connection = DatabaseConnectionDef {
        connection_string: req
            .config
            .get("connection_string")
            .and_then(|v| v.as_str())
            .map(String::from),
        host: req
            .config
            .get("host")
            .and_then(|v| v.as_str())
            .map(String::from),
        port: req
            .config
            .get("port")
            .and_then(serde_json::Value::as_u64)
            .map(|p| p as u16),
        database: req
            .config
            .get("database")
            .and_then(|v| v.as_str())
            .map(String::from),
        user: req
            .config
            .get("user")
            .and_then(|v| v.as_str())
            .map(String::from),
        password: req
            .config
            .get("password")
            .and_then(|v| v.as_str())
            .map(String::from),
        ssl_mode: req
            .config
            .get("ssl_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("prefer")
            .to_string(),
    };

    // Create database engine and check connection
    match DbEngine::new(db_type, &connection, &context) {
        Ok(engine) => {
            match engine.check_connection() {
                Ok(()) => {
                    // Get table count for info
                    let table_count = engine.list_tables().map(|t| t.len()).unwrap_or(0);
                    (StatusCode::OK, Json(ApiResponse::success(json!({
                        "type": "CONNECTION_STATUS",
                        "connectionStatus": {
                            "status": "SUCCEEDED",
                            "message": format!("Connection successful. Found {} tables.", table_count)
                        }
                    })))).into_response()
                }
                Err(e) => (
                    StatusCode::OK,
                    Json(ApiResponse::success(json!({
                        "type": "CONNECTION_STATUS",
                        "connectionStatus": {
                            "status": "FAILED",
                            "message": format!("Connection check failed: {}", e)
                        }
                    }))),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::OK,
            Json(ApiResponse::success(json!({
                "type": "CONNECTION_STATUS",
                "connectionStatus": {
                    "status": "FAILED",
                    "message": format!("Failed to connect: {}", e)
                }
            }))),
        )
            .into_response(),
    }
}

/// Get streams (tables) from a database
fn get_database_streams(req: &ConnectorRequest) -> axum::response::Response {
    // Determine database type
    let db_type = match req.connector.to_lowercase().as_str() {
        "postgres" | "postgresql" => DbType::Postgres,
        "mysql" | "mariadb" => DbType::Mysql,
        "sqlite" => DbType::Sqlite,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error(format!(
                    "Unknown database connector: {}",
                    req.connector
                ))),
            )
                .into_response();
        }
    };

    // Build template context
    let mut context = TemplateContext::new();
    context.set_config(req.config.clone());

    // Build connection from config
    let connection = DatabaseConnectionDef {
        connection_string: req
            .config
            .get("connection_string")
            .and_then(|v| v.as_str())
            .map(String::from),
        host: req
            .config
            .get("host")
            .and_then(|v| v.as_str())
            .map(String::from),
        port: req
            .config
            .get("port")
            .and_then(serde_json::Value::as_u64)
            .map(|p| p as u16),
        database: req
            .config
            .get("database")
            .and_then(|v| v.as_str())
            .map(String::from),
        user: req
            .config
            .get("user")
            .and_then(|v| v.as_str())
            .map(String::from),
        password: req
            .config
            .get("password")
            .and_then(|v| v.as_str())
            .map(String::from),
        ssl_mode: req
            .config
            .get("ssl_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("prefer")
            .to_string(),
    };

    // Create database engine and list tables
    match DbEngine::new(db_type, &connection, &context) {
        Ok(engine) => match engine.list_tables() {
            Ok(tables) => (
                StatusCode::OK,
                Json(ApiResponse::success(json!({
                    "type": "STREAMS",
                    "connector": req.connector,
                    "connector_type": "database",
                    "streams": tables
                }))),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<()>::error(format!(
                    "Failed to list tables: {e}"
                ))),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(format!("Failed to connect: {e}"))),
        )
            .into_response(),
    }
}
