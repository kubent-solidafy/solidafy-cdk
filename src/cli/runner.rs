//! CLI runner - executes commands

use crate::auth::{AuthConfig, Authenticator};
use crate::cli::commands::{Cli, Commands, OutputFormat};
use crate::connectors::is_database_connector;
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
use crate::output::{
    arrow_to_json, build_partitioned_dir, build_partitioned_path, ParquetWriter,
    ParquetWriterConfig,
};
use crate::pagination::{
    CursorPaginator, LinkHeaderPaginator, NextUrlPaginator, NoPaginator, OffsetPaginator,
    PageNumberPaginator, Paginator, StopCondition,
};
use crate::partition::{ListRouter, PartitionRouter};
use crate::state::StateManager;
use crate::template::{self, TemplateContext};
use base64::Engine as _;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// CLI runner
pub struct Runner {
    cli: Cli,
}

impl Runner {
    /// Create a new runner
    pub fn new(cli: Cli) -> Self {
        Self { cli }
    }

    /// Run the CLI command
    pub async fn run(&self) -> Result<()> {
        match &self.cli.command {
            Commands::Check { config_json } => self.check(config_json.as_deref()).await,
            Commands::Discover {
                config_json,
                sample,
            } => self.discover(config_json.as_deref(), *sample).await,
            Commands::Read {
                streams,
                config_json,
                output,
                max_records,
                state_per_page,
            } => {
                self.read(
                    streams.as_deref(),
                    config_json.as_deref(),
                    output.as_deref(),
                    *max_records,
                    *state_per_page,
                )
                .await
            }
            Commands::Spec => self.spec(),
            Commands::Validate => self.validate(),
            Commands::Streams { config_json } => self.streams(config_json.as_deref()),
            Commands::List => self.list_connectors(),
            Commands::Serve {
                port,
                connectors_dir,
            } => {
                let config = crate::cli::ServerConfig {
                    connectors_dir: connectors_dir.clone(),
                };
                crate::cli::serve(config, *port).await
            }
        }
    }

    /// Load connector definition
    fn load_connector(&self) -> Result<ConnectorDefinition> {
        let path = self
            .cli
            .connector
            .as_ref()
            .ok_or_else(|| Error::config("Connector file not specified (use -c flag)"))?;
        load_connector(path)
    }

    /// Check if the connector is a database connector
    fn is_database_connector(&self) -> bool {
        self.cli
            .connector
            .as_ref()
            .is_some_and(|c| is_database_connector(&c.to_string_lossy()))
    }

    /// Get database type from connector name
    fn get_database_type(&self) -> Result<DbType> {
        let connector = self
            .cli
            .connector
            .as_ref()
            .ok_or_else(|| Error::config("Connector not specified"))?;

        let connector_str = connector.to_string_lossy();
        match connector_str.to_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(DbType::Postgres),
            "mysql" | "mariadb" => Ok(DbType::Mysql),
            "sqlite" => Ok(DbType::Sqlite),
            _ => Err(Error::config(format!(
                "Unknown database connector: {connector_str}"
            ))),
        }
    }

    /// Get connector name as string
    fn get_connector_name(&self) -> String {
        self.cli
            .connector
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    }

    /// Build database connection from config
    fn build_database_connection(&self, config: &Value) -> DatabaseConnectionDef {
        DatabaseConnectionDef {
            connection_string: config
                .get("connection_string")
                .and_then(|v| v.as_str())
                .map(String::from),
            host: config
                .get("host")
                .and_then(|v| v.as_str())
                .map(String::from),
            port: config
                .get("port")
                .and_then(serde_json::Value::as_u64)
                .map(|p| p as u16),
            database: config
                .get("database")
                .and_then(|v| v.as_str())
                .map(String::from),
            user: config
                .get("user")
                .and_then(|v| v.as_str())
                .map(String::from),
            password: config
                .get("password")
                .and_then(|v| v.as_str())
                .map(String::from),
            ssl_mode: config
                .get("ssl_mode")
                .and_then(|v| v.as_str())
                .unwrap_or("prefer")
                .to_string(),
        }
    }

    /// Load configuration
    fn load_config(&self, inline: Option<&str>) -> Result<Value> {
        // Inline config takes precedence
        if let Some(json_str) = inline {
            return serde_json::from_str(json_str)
                .map_err(|e| Error::config(format!("Invalid config JSON: {e}")));
        }

        // Load from file
        if let Some(path) = &self.cli.config {
            let content = fs::read_to_string(path)
                .map_err(|e| Error::config(format!("Failed to read config file: {e}")))?;
            return serde_json::from_str(&content)
                .map_err(|e| Error::config(format!("Invalid config JSON: {e}")));
        }

        // Default empty config
        Ok(json!({}))
    }

    /// Load state
    fn load_state(&self) -> Result<StateManager> {
        // Inline state takes precedence
        if let Some(state_json) = &self.cli.state_json {
            StateManager::from_json(state_json)
        } else if let Some(path) = &self.cli.state {
            StateManager::from_file(path)
        } else {
            Ok(StateManager::in_memory())
        }
    }

    /// Check connection
    async fn check(&self, config_json: Option<&str>) -> Result<()> {
        // Handle database connectors
        if self.is_database_connector() {
            return self.check_database(config_json);
        }

        let connector = self.load_connector()?;
        let config = self.load_config(config_json)?;

        // Build template context
        let mut context = TemplateContext::new();
        context.set_config(config);

        // Render base URL template
        let base_url = template::render(&connector.base_url, &context)?;

        self.output_message(&json!({
            "type": "LOG",
            "log": {
                "level": "INFO",
                "message": format!("Checking connection to {}", connector.name)
            }
        }));

        // Build auth headers (use async version for OAuth2)
        let authenticator = Self::create_authenticator(&connector.auth, &context)?;
        let auth_headers =
            Self::build_auth_headers_async(&connector.auth, &context, authenticator.as_ref())
                .await?;

        // Build HTTP client with rendered base URL
        let http_config = Self::build_http_config_with_url(&connector, &base_url);
        let client = HttpClient::with_config(http_config);

        // Build check URL - use check.path if defined, otherwise try first stream
        let test_url = if let Some(check_def) = &connector.check {
            let path = template::render(&check_def.path, &context)?;
            let mut url = format!("{}{}", base_url.trim_end_matches('/'), path);

            // Add query params if any
            if !check_def.params.is_empty() {
                let params: Vec<String> = check_def
                    .params
                    .iter()
                    .map(|(k, v)| {
                        let rendered_v =
                            template::render(v, &context).unwrap_or_else(|_| v.clone());
                        format!("{k}={rendered_v}")
                    })
                    .collect();
                url = format!("{}?{}", url, params.join("&"));
            }
            url
        } else if let Some(first_stream) = connector.streams.first() {
            // Fallback: use first stream's path
            let path = template::render(&first_stream.request.path, &context)?;
            format!("{}{}", base_url.trim_end_matches('/'), path)
        } else {
            // Last resort: try base URL root
            format!("{}/", base_url.trim_end_matches('/'))
        };

        // Merge auth headers with connector headers
        let mut headers = auth_headers;
        headers.extend(connector.headers.clone());

        let mut request_config = RequestConfig::new();
        request_config.headers = headers;

        match client.get_with_config(&test_url, request_config).await {
            Ok(_) => {
                self.output_message(&json!({
                    "type": "CONNECTION_STATUS",
                    "connectionStatus": {
                        "status": "SUCCEEDED",
                        "message": "Connection successful"
                    }
                }));
            }
            Err(e) => {
                self.output_message(&json!({
                    "type": "CONNECTION_STATUS",
                    "connectionStatus": {
                        "status": "FAILED",
                        "message": format!("Connection failed: {e}")
                    }
                }));
            }
        }

        Ok(())
    }

    /// Discover streams
    async fn discover(&self, config_json: Option<&str>, sample_count: usize) -> Result<()> {
        let connector = self.load_connector()?;
        let config = self.load_config(config_json)?;

        // Build template context for sampling
        let mut context = TemplateContext::new();
        context.set_config(config.clone());

        // Optionally sample data for schema inference
        let inferred_schemas = if sample_count > 0 {
            self.sample_streams_for_schema(&connector, &context, sample_count)
                .await?
        } else {
            HashMap::new()
        };

        let mut streams = Vec::new();

        for stream_def in &connector.streams {
            // Use inferred schema if available, otherwise use default
            let json_schema = if let Some(schema) = inferred_schemas.get(&stream_def.name) {
                schema.to_json()
            } else {
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": true
                })
            };

            // Add primary key
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

        self.output_message(&json!({
            "type": "CATALOG",
            "catalog": {
                "streams": streams
            }
        }));

        Ok(())
    }

    /// Sample streams to infer schemas
    async fn sample_streams_for_schema(
        &self,
        connector: &ConnectorDefinition,
        context: &TemplateContext,
        sample_count: usize,
    ) -> Result<HashMap<String, crate::schema::JsonSchema>> {
        use crate::schema::SchemaInferrer;

        let mut schemas = HashMap::new();

        // Render base URL
        let base_url = template::render(&connector.base_url, context)?;

        // Build auth headers (use async version for OAuth2)
        let authenticator = Self::create_authenticator(&connector.auth, context)?;
        let auth_headers =
            Self::build_auth_headers_async(&connector.auth, context, authenticator.as_ref())
                .await?;

        // Build HTTP client
        let http_config = Self::build_http_config_with_url(connector, &base_url);
        let client = HttpClient::with_config(http_config);

        for stream_def in &connector.streams {
            // Skip streams with partitions for now (require parent data)
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
                self.output_message(&json!({
                    "type": "LOG",
                    "log": {
                        "level": "DEBUG",
                        "message": format!("Skipping {} (requires partition data)", stream_def.name)
                    }
                }));
                continue;
            }

            self.output_message(&json!({
                "type": "LOG",
                "log": {
                    "level": "DEBUG",
                    "message": format!("Sampling {} records from stream: {}", sample_count, stream_def.name)
                }
            }));

            // Build request URL
            let path = template::render(&stream_def.request.path, context)?;
            let url = format!("{}{}", base_url.trim_end_matches('/'), path);

            // Build query params
            let mut params = HashMap::new();
            for (key, value) in &stream_def.request.params {
                let rendered = template::render(value, context)?;
                params.insert(key.clone(), rendered);
            }

            // Merge headers
            let mut headers = auth_headers.clone();
            headers.extend(connector.headers.clone());
            headers.extend(stream_def.headers.clone());

            // Build request config
            let mut request_config = RequestConfig::new();
            request_config.headers = headers;
            request_config.query = params;

            // Make request
            match client.get_with_config(&url, request_config).await {
                Ok(response) => {
                    // Parse response as JSON
                    if let Ok(body) = response.text().await {
                        if let Ok(json_value) = serde_json::from_str::<Value>(&body) {
                            // Extract records using decoder path
                            let records = self.extract_records(&json_value, &stream_def.decoder);

                            // Limit to sample_count
                            let sample_records: Vec<_> =
                                records.into_iter().take(sample_count).collect();

                            if !sample_records.is_empty() {
                                // Infer schema
                                let mut inferrer = SchemaInferrer::new();
                                let schema = inferrer.infer_from_records(&sample_records);
                                schemas.insert(stream_def.name.clone(), schema);

                                self.output_message(&json!({
                                    "type": "LOG",
                                    "log": {
                                        "level": "DEBUG",
                                        "message": format!(
                                            "Inferred schema for {} from {} records",
                                            stream_def.name,
                                            sample_records.len()
                                        )
                                    }
                                }));
                            }
                        }
                    }
                }
                Err(e) => {
                    self.output_message(&json!({
                        "type": "LOG",
                        "log": {
                            "level": "WARN",
                            "message": format!("Failed to sample {}: {}", stream_def.name, e)
                        }
                    }));
                }
            }
        }

        Ok(schemas)
    }

    /// Extract records from JSON response using decoder definition
    fn extract_records(&self, value: &Value, decoder: &DecoderDefinition) -> Vec<Value> {
        match decoder {
            DecoderDefinition::Json { records_path } => {
                if let Some(path) = records_path {
                    // Navigate to records path
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
                    // Extract array
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
                // JSONL would be line-by-line, but we have full response
                if let Some(arr) = value.as_array() {
                    arr.clone()
                } else {
                    vec![value.clone()]
                }
            }
            _ => {
                // CSV/XML - just return the value as-is
                vec![value.clone()]
            }
        }
    }

    /// Read data
    async fn read(
        &self,
        streams: Option<&str>,
        config_json: Option<&str>,
        output: Option<&str>,
        max_records: Option<usize>,
        state_per_page: bool,
    ) -> Result<()> {
        // Handle database connectors
        if self.is_database_connector() {
            return self
                .read_database(streams, config_json, output, max_records)
                .await;
        }

        use crate::output::CloudDestination;

        let sync_start = Instant::now();
        let connector = self.load_connector()?;
        let config = self.load_config(config_json)?;
        let state = self.load_state()?;

        // Parse output destination (local or cloud)
        let destination = output.map(CloudDestination::parse).transpose()?;

        // Build template context
        let mut context = TemplateContext::new();
        context.set_config(config.clone());

        // Render base URL template
        let base_url = template::render(&connector.base_url, &context)?;

        // Build auth headers (use async version for OAuth2)
        let authenticator = Self::create_authenticator(&connector.auth, &context)?;
        let auth_headers =
            Self::build_auth_headers_async(&connector.auth, &context, authenticator.as_ref())
                .await?;

        // Parse streams filter
        let stream_filter: Option<Vec<&str>> = streams.map(|s| s.split(',').collect());

        // Build HTTP client with rendered base URL
        let http_config = Self::build_http_config_with_url(&connector, &base_url);
        let client = HttpClient::with_config(http_config);

        // Build sync config
        let mut sync_config = SyncConfig::new();
        if let Some(max) = max_records {
            sync_config = sync_config.with_max_records(max);
        }
        if state_per_page {
            sync_config = sync_config.with_state_per_page(true);
        }

        let mut engine = SyncEngine::new(client, state).with_config(sync_config);

        // Track per-stream statistics
        let mut stream_results: Vec<Value> = Vec::new();
        let mut total_records = 0usize;

        // Process each stream
        for stream_def in &connector.streams {
            // Check filter
            if let Some(ref filter) = stream_filter {
                if !filter.contains(&stream_def.name.as_str()) {
                    continue;
                }
            }

            let stream_start = Instant::now();
            let records_before = engine.stats().records_synced;

            self.output_message(&json!({
                "type": "LOG",
                "log": {
                    "level": "INFO",
                    "message": format!("Starting sync for stream: {}", stream_def.name)
                }
            }));

            // Build decoder
            let decoder: Box<dyn RecordDecoder> = Self::build_decoder(&stream_def.decoder);

            // Build paginator
            let paginator: Box<dyn Paginator> =
                Self::build_paginator(stream_def.pagination.as_ref());

            // Merge headers: auth headers + connector headers + stream headers
            let mut headers = auth_headers.clone();
            headers.extend(connector.headers.clone());
            headers.extend(stream_def.headers.clone());

            // Sync stream
            let sync_result = if let Some(partition_def) = &stream_def.partition {
                // Build partition router
                let router: Box<dyn PartitionRouter> = Self::build_router(partition_def);

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
                    // Output messages
                    for msg in messages {
                        self.output_engine_message_async(&msg, destination.as_ref())
                            .await?;
                    }

                    total_records += stream_records;

                    // Build stream result with optional output file path
                    let mut stream_result = json!({
                        "stream": stream_def.name,
                        "status": "SUCCESS",
                        "records_synced": stream_records,
                        "duration_ms": stream_duration_ms
                    });

                    // Add output file path if parquet format with output
                    if matches!(self.cli.format, OutputFormat::Parquet) {
                        if let Some(dest) = destination.as_ref() {
                            // Use Hive-style partitioned path for output file
                            let partitioned = build_partitioned_path(&stream_def.name, "parquet");
                            let file_path = format!("{}://{}", dest.scheme(), partitioned);
                            stream_result["output_file"] = json!(file_path);
                        }
                    }

                    stream_results.push(stream_result);
                }
                Err(e) => {
                    self.output_message(&json!({
                        "type": "LOG",
                        "log": {
                            "level": "ERROR",
                            "message": format!("Error syncing stream {}: {}", stream_def.name, e)
                        }
                    }));

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

        // Output final state
        let state_file_path: Option<String> = if let Some(state_path) = &self.cli.state {
            engine.state().save_to_file(state_path).await?;
            Some(state_path.to_string_lossy().to_string())
        } else {
            None
        };

        // Always emit final state to stdout so caller can capture it
        let final_state = engine.state().to_json().await?;
        self.output_message(&json!({
            "type": "STATE",
            "state": serde_json::from_str::<serde_json::Value>(&final_state).unwrap_or_default()
        }));

        // Emit sync summary for programmatic consumption
        let total_duration_ms = sync_start.elapsed().as_millis() as u64;
        let successful_streams = stream_results
            .iter()
            .filter(|r| r["status"] == "SUCCESS")
            .count();
        let failed_streams = stream_results
            .iter()
            .filter(|r| r["status"] == "FAILED")
            .count();

        // Build output files info
        let output_dir: Option<String> = output.map(std::string::ToString::to_string);

        self.output_message(&json!({
            "type": "SYNC_SUMMARY",
            "summary": {
                "status": if failed_streams == 0 { "SUCCEEDED" } else if successful_streams == 0 { "FAILED" } else { "PARTIAL" },
                "connector": connector.name,
                "total_records": total_records,
                "total_streams": stream_results.len(),
                "successful_streams": successful_streams,
                "failed_streams": failed_streams,
                "duration_ms": total_duration_ms,
                "output": {
                    "format": match self.cli.format {
                        OutputFormat::Json => "json",
                        OutputFormat::Pretty => "pretty",
                        OutputFormat::Parquet => "parquet",
                    },
                    "directory": output_dir,
                    "state_file": state_file_path
                },
                "streams": stream_results
            }
        }));

        Ok(())
    }

    /// Show spec
    fn spec(&self) -> Result<()> {
        let connector = self.load_connector()?;

        self.output_message(&json!({
            "type": "SPEC",
            "spec": {
                "documentationUrl": "https://github.com/solidafy/solidafy-cdk",
                "connectionSpecification": {
                    "type": "object",
                    "title": connector.name,
                    "properties": {},
                    "required": []
                }
            }
        }));

        Ok(())
    }

    /// Validate connector definition
    fn validate(&self) -> Result<()> {
        let connector = self.load_connector()?;

        self.output_message(&json!({
            "type": "LOG",
            "log": {
                "level": "INFO",
                "message": format!(
                    "Connector '{}' v{} is valid with {} streams",
                    connector.name,
                    connector.version,
                    connector.streams.len()
                )
            }
        }));

        Ok(())
    }

    /// Check database connection
    fn check_database(&self, config_json: Option<&str>) -> Result<()> {
        let db_type = self.get_database_type()?;
        let config = self.load_config(config_json)?;
        let connection = self.build_database_connection(&config);

        let connector_name = self.get_connector_name();

        self.output_message(&json!({
            "type": "LOG",
            "log": {
                "level": "INFO",
                "message": format!("Checking connection to {} database", connector_name)
            }
        }));

        let context = TemplateContext::new();
        match DbEngine::new(db_type, &connection, &context) {
            Ok(engine) => match engine.check_connection() {
                Ok(()) => {
                    let table_count = engine.list_tables().map(|t| t.len()).unwrap_or(0);
                    self.output_message(&json!({
                        "type": "CONNECTION_STATUS",
                        "connectionStatus": {
                            "status": "SUCCEEDED",
                            "message": format!("Connection successful. Found {} tables.", table_count)
                        }
                    }));
                }
                Err(e) => {
                    self.output_message(&json!({
                        "type": "CONNECTION_STATUS",
                        "connectionStatus": {
                            "status": "FAILED",
                            "message": format!("Connection check failed: {}", e)
                        }
                    }));
                }
            },
            Err(e) => {
                self.output_message(&json!({
                    "type": "CONNECTION_STATUS",
                    "connectionStatus": {
                        "status": "FAILED",
                        "message": format!("Failed to connect: {}", e)
                    }
                }));
            }
        }

        Ok(())
    }

    /// List database tables as streams
    fn streams_database(&self, config_json: Option<&str>) -> Result<()> {
        let db_type = self.get_database_type()?;
        let config = self.load_config(config_json)?;
        let connection = self.build_database_connection(&config);
        let connector_name = self.get_connector_name();

        let context = TemplateContext::new();
        let engine = DbEngine::new(db_type, &connection, &context)?;
        let tables = engine.list_tables()?;

        self.output_message(&json!({
            "type": "STREAMS",
            "streams": tables,
            "connector": connector_name,
            "connector_type": "database"
        }));

        Ok(())
    }

    /// Read data from database
    async fn read_database(
        &self,
        streams: Option<&str>,
        config_json: Option<&str>,
        output: Option<&str>,
        max_records: Option<usize>,
    ) -> Result<()> {
        let sync_start = Instant::now();
        let db_type = self.get_database_type()?;
        let config = self.load_config(config_json)?;
        let connection = self.build_database_connection(&config);
        let connector_name = self.get_connector_name();

        let context = TemplateContext::new();
        let engine = DbEngine::new(db_type, &connection, &context)?;

        // Get cursor_field from config if provided
        let cursor_field = config
            .get("cursor_field")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Get tables to sync
        let tables_to_sync: Vec<String> = if let Some(stream_list) = streams {
            stream_list
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        } else {
            engine.list_tables()?
        };

        // Determine output format
        let is_parquet = matches!(self.cli.format, OutputFormat::Parquet);
        let batch_size = max_records.unwrap_or(10000) as u32;

        let mut stream_results: Vec<Value> = Vec::new();
        let mut total_records = 0usize;
        let mut all_states: HashMap<String, Value> = HashMap::new();

        self.output_message(&json!({
            "type": "LOG",
            "log": {
                "level": "INFO",
                "message": format!("Starting sync for {} tables from {}", tables_to_sync.len(), connector_name)
            }
        }));

        for table in &tables_to_sync {
            let stream_start = Instant::now();

            self.output_message(&json!({
                "type": "LOG",
                "log": {
                    "level": "INFO",
                    "message": format!("Starting sync for table: {}", table)
                }
            }));

            let stream_def = DatabaseStreamDefinition {
                name: table.clone(),
                table: Some(table.clone()),
                query: None,
                primary_key: vec![],
                cursor_field: cursor_field.clone(),
                batch_size,
            };

            // Get cursor value from state if available
            let state_manager = self.load_state()?;
            let cursor_value = state_manager.get_cursor(table).await;

            let sync_result = if is_parquet {
                if let Some(out_path) = output {
                    // Use Hive-style partitioning: {stream}/dt={YYYY-MM-DD}/data.parquet
                    let output_dir = build_partitioned_dir(out_path, table);
                    std::fs::create_dir_all(&output_dir).map_err(|e| {
                        Error::io(format!("Failed to create directory {output_dir}: {e}"))
                    })?;
                    let output_path = format!("{output_dir}/data.parquet");
                    engine.sync_to_parquet(&stream_def, &output_path, cursor_value.as_deref())
                } else {
                    Err(Error::config("Parquet format requires --output path"))
                }
            } else if let Some(out_path) = output {
                // Use Hive-style partitioning: {stream}/dt={YYYY-MM-DD}/data.json
                let output_dir = build_partitioned_dir(out_path, table);
                std::fs::create_dir_all(&output_dir).map_err(|e| {
                    Error::io(format!("Failed to create directory {output_dir}: {e}"))
                })?;
                let output_path = format!("{output_dir}/data.json");
                engine.sync_to_json_file(&stream_def, &output_path, cursor_value.as_deref())
            } else {
                engine.sync_to_json(&stream_def, cursor_value.as_deref())
            };

            let stream_duration_ms = stream_start.elapsed().as_millis() as u64;

            match sync_result {
                Ok(result) => {
                    total_records += result.record_count;

                    // Output records if JSON and no output path
                    if let Some(records) = &result.records {
                        let emitted_at = chrono::Utc::now().timestamp_millis();
                        for record in records {
                            self.output_message(&json!({
                                "type": "RECORD",
                                "record": {
                                    "stream": table,
                                    "data": record,
                                    "emitted_at": emitted_at
                                }
                            }));
                        }
                    }

                    // Save cursor state
                    if let Some(cursor) = &result.cursor_value {
                        all_states.insert(table.clone(), json!({ "cursor": cursor }));
                    }

                    let mut stream_result = json!({
                        "stream": table,
                        "status": "SUCCESS",
                        "records_synced": result.record_count,
                        "duration_ms": stream_duration_ms
                    });

                    if let Some(output_path) = &result.output_path {
                        stream_result["output_file"] = json!(output_path);
                    }

                    if let Some(cursor) = &result.cursor_value {
                        stream_result["cursor_value"] = json!(cursor);
                    }

                    stream_results.push(stream_result);

                    self.output_message(&json!({
                        "type": "LOG",
                        "log": {
                            "level": "INFO",
                            "message": format!("Completed sync for {}: {} records", table, result.record_count)
                        }
                    }));
                }
                Err(e) => {
                    stream_results.push(json!({
                        "stream": table,
                        "status": "FAILED",
                        "error": e.to_string(),
                        "records_synced": 0,
                        "duration_ms": stream_duration_ms
                    }));

                    self.output_message(&json!({
                        "type": "LOG",
                        "log": {
                            "level": "ERROR",
                            "message": format!("Failed to sync {}: {}", table, e)
                        }
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

        // Output final state
        if !all_states.is_empty() {
            self.output_message(&json!({
                "type": "STATE",
                "state": {
                    "streams": all_states
                }
            }));
        }

        // Output sync summary
        self.output_message(&json!({
            "type": "SYNC_SUMMARY",
            "summary": {
                "status": status,
                "connector": connector_name,
                "connector_type": "database",
                "total_records": total_records,
                "total_streams": stream_results.len(),
                "successful_streams": successful_streams,
                "failed_streams": failed_streams,
                "duration_ms": total_duration_ms,
                "output": {
                    "format": if is_parquet { "parquet" } else { "json" },
                    "directory": output,
                    "state_file": self.cli.state.as_ref().map(|p| p.to_string_lossy().to_string())
                },
                "streams": stream_results
            }
        }));

        Ok(())
    }

    /// List available streams (lightweight, no schemas)
    fn streams(&self, config_json: Option<&str>) -> Result<()> {
        // Handle database connectors - requires config for connection
        if self.is_database_connector() {
            return self.streams_database(config_json);
        }

        let connector = self.load_connector()?;

        let stream_names: Vec<&str> = connector.streams.iter().map(|s| s.name.as_str()).collect();

        self.output_message(&json!({
            "type": "STREAMS",
            "streams": stream_names,
            "connector": connector.name
        }));

        Ok(())
    }

    /// List built-in connectors
    fn list_connectors(&self) -> Result<()> {
        use crate::connectors::list_builtin_info;

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

                json!({
                    "name": info.name,
                    "description": info.description,
                    "category": info.category,
                    "aliases": info.aliases,
                    "config_schema": config_fields,
                    "streams": info.streams
                })
            })
            .collect();

        self.output_message(&json!({
            "type": "CONNECTORS",
            "connectors": connectors
        }));

        Ok(())
    }

    /// Build HTTP client config with rendered base URL
    fn build_http_config_with_url(
        connector: &ConnectorDefinition,
        base_url: &str,
    ) -> HttpClientConfig {
        let mut builder = HttpClientConfig::builder()
            .base_url(base_url)
            .timeout(Duration::from_secs(connector.http.timeout_secs))
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
            DecoderDefinition::Xml { records_path: _ } => {
                // Fall back to JSON decoder for now
                Box::new(JsonDecoder::new())
            }
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
                let stop_condition = Self::build_stop_condition(stop);
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
                let stop_condition = Self::build_stop_condition(stop);
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
                location: _,
            }) => Box::new(CursorPaginator::new(
                cursor_param,
                cursor_path,
                StopCondition::EmptyPage,
            )),
            Some(PaginationDefinition::LinkHeader { rel }) => {
                Box::new(LinkHeaderPaginator::new(rel))
            }
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
            StopConditionDefinition::Field { path, value } => {
                StopCondition::field(path, value.clone())
            }
        }
    }

    /// Build partition router from definition
    fn build_router(def: &PartitionDefinition) -> Box<dyn PartitionRouter> {
        match def {
            PartitionDefinition::List { field, values } => {
                Box::new(ListRouter::new(values.clone(), field))
            }
            PartitionDefinition::Parent { .. } => {
                // Parent router requires parent stream records
                // For now, use empty list
                Box::new(ListRouter::new(vec![], "parent_id"))
            }
            PartitionDefinition::DateRange { .. } => {
                // Date range router would need to be implemented
                Box::new(ListRouter::new(vec![], "date"))
            }
            PartitionDefinition::AsyncJob { .. } => {
                // Async job router is handled separately in sync_async_job_stream
                // Return empty list as placeholder
                Box::new(ListRouter::new(vec![], "job_id"))
            }
        }
    }

    /// Build auth headers from auth definition (handles all auth types including OAuth2)
    async fn build_auth_headers_async(
        auth: &Option<AuthDefinition>,
        context: &TemplateContext,
        authenticator: Option<&Arc<Authenticator>>,
    ) -> Result<HashMap<String, String>> {
        let mut headers = HashMap::new();

        let Some(auth_def) = auth else {
            return Ok(headers);
        };

        match auth_def {
            // Simple auth types - handle synchronously
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

            // OAuth2 types - use authenticator for token management
            AuthDefinition::OAuth2ClientCredentials { .. }
            | AuthDefinition::OAuth2RefreshToken { .. }
            | AuthDefinition::SessionToken { .. } => {
                if let Some(auth) = authenticator {
                    // Get token from authenticator (handles caching and refresh)
                    let req = reqwest::Client::new().get("http://dummy");
                    let req = auth.apply(req).await?;
                    // Extract Authorization header from the built request
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

    /// Convert AuthDefinition (from YAML) to AuthConfig (runtime)
    fn build_auth_config(
        auth: &Option<AuthDefinition>,
        context: &TemplateContext,
    ) -> Result<AuthConfig> {
        let Some(auth_def) = auth else {
            return Ok(AuthConfig::None);
        };

        match auth_def {
            AuthDefinition::None => Ok(AuthConfig::None),

            AuthDefinition::ApiKey {
                key,
                value,
                location,
            } => {
                let rendered_value = template::render(value, context)?;
                Ok(AuthConfig::ApiKey {
                    location: if location == "query" {
                        crate::auth::Location::Query
                    } else {
                        crate::auth::Location::Header
                    },
                    header_name: Some(key.clone()),
                    query_param: Some(key.clone()),
                    prefix: None,
                    value: rendered_value,
                })
            }

            AuthDefinition::Bearer { token } => {
                let rendered_token = template::render(token, context)?;
                Ok(AuthConfig::Bearer {
                    token: rendered_token,
                })
            }

            AuthDefinition::Basic { username, password } => {
                let rendered_username = template::render(username, context)?;
                let rendered_password = template::render(password, context)?;
                Ok(AuthConfig::Basic {
                    username: rendered_username,
                    password: rendered_password,
                })
            }

            AuthDefinition::OAuth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scopes,
            } => {
                let rendered_token_url = template::render(token_url, context)?;
                let rendered_client_id = template::render(client_id, context)?;
                let rendered_client_secret = template::render(client_secret, context)?;
                Ok(AuthConfig::Oauth2ClientCredentials {
                    token_url: rendered_token_url,
                    client_id: rendered_client_id,
                    client_secret: rendered_client_secret,
                    scopes: scopes.clone(),
                    token_body: HashMap::new(),
                })
            }

            AuthDefinition::OAuth2RefreshToken {
                token_url,
                client_id,
                client_secret,
                refresh_token,
            } => {
                let rendered_token_url = template::render(token_url, context)?;
                let rendered_client_id = template::render(client_id, context)?;
                let rendered_client_secret = template::render(client_secret, context)?;
                let rendered_refresh_token = template::render(refresh_token, context)?;
                Ok(AuthConfig::Oauth2Refresh {
                    token_url: rendered_token_url,
                    client_id: rendered_client_id,
                    client_secret: rendered_client_secret,
                    refresh_token: rendered_refresh_token,
                })
            }

            AuthDefinition::SessionToken {
                login_url,
                body,
                token_path,
                header_name,
                header_prefix,
            } => {
                let rendered_login_url = template::render(login_url, context)?;
                let rendered_body = template::render(body, context)?;
                // Parse the body as JSON
                let login_body: HashMap<String, String> =
                    serde_json::from_str(&rendered_body).unwrap_or_default();
                Ok(AuthConfig::Session {
                    login_url: rendered_login_url,
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
                })
            }
        }
    }

    /// Check if auth type requires async token fetch
    fn is_oauth2_auth(auth: &Option<AuthDefinition>) -> bool {
        matches!(
            auth,
            Some(
                AuthDefinition::OAuth2ClientCredentials { .. }
                    | AuthDefinition::OAuth2RefreshToken { .. }
                    | AuthDefinition::SessionToken { .. }
            )
        )
    }

    /// Create an authenticator for OAuth2 auth types
    fn create_authenticator(
        auth: &Option<AuthDefinition>,
        context: &TemplateContext,
    ) -> Result<Option<Arc<Authenticator>>> {
        if !Self::is_oauth2_auth(auth) {
            return Ok(None);
        }
        let auth_config = Self::build_auth_config(auth, context)?;
        Ok(Some(Arc::new(Authenticator::new(auth_config))))
    }

    /// Output a message
    fn output_message(&self, msg: &Value) {
        match self.cli.format {
            OutputFormat::Json | OutputFormat::Parquet => {
                println!("{}", serde_json::to_string(msg).unwrap_or_default());
            }
            OutputFormat::Pretty => {
                println!("{}", serde_json::to_string_pretty(msg).unwrap_or_default());
            }
        }
    }

    /// Output an engine message (async version supporting cloud storage)
    async fn output_engine_message_async(
        &self,
        msg: &Message,
        destination: Option<&crate::output::CloudDestination>,
    ) -> Result<()> {
        match msg {
            Message::Record { stream, batch } => {
                match self.cli.format {
                    OutputFormat::Parquet => {
                        // Parquet format: write to destination only, no stdout output for records
                        let dest = destination.ok_or_else(|| {
                            Error::config("Parquet format requires --output destination")
                        })?;

                        // Write parquet to memory buffer then upload
                        let parquet_bytes = Self::batch_to_parquet_bytes(batch)?;
                        dest.write_parquet(stream, parquet_bytes).await?;
                    }
                    OutputFormat::Json | OutputFormat::Pretty => {
                        // JSON format: output records to stdout
                        // Also write parquet if destination specified
                        if let Some(dest) = destination {
                            let parquet_bytes = Self::batch_to_parquet_bytes(batch)?;
                            dest.write_parquet(stream, parquet_bytes).await?;
                        }

                        // Convert Arrow batch to JSON records
                        let records = arrow_to_json(batch)?;
                        let emitted_at = chrono::Utc::now().timestamp_millis();

                        // Output each record as a separate message
                        for record in records {
                            self.output_message(&json!({
                                "type": "RECORD",
                                "record": {
                                    "stream": stream,
                                    "data": record,
                                    "emitted_at": emitted_at
                                }
                            }));
                        }
                    }
                }
            }
            Message::State { stream, data } => {
                self.output_message(&json!({
                    "type": "STATE",
                    "state": {
                        "type": "STREAM",
                        "stream": {
                            "stream_descriptor": {
                                "name": stream
                            },
                            "stream_state": data
                        }
                    }
                }));
            }
            Message::Log { level, message } => {
                let level_str = match level {
                    crate::engine::LogLevel::Debug => "DEBUG",
                    crate::engine::LogLevel::Info => "INFO",
                    crate::engine::LogLevel::Warn => "WARN",
                    crate::engine::LogLevel::Error => "ERROR",
                };
                self.output_message(&json!({
                    "type": "LOG",
                    "log": {
                        "level": level_str,
                        "message": message
                    }
                }));
            }
        }
        Ok(())
    }

    /// Convert Arrow batch to Parquet bytes in memory
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

    /// Output an engine message (legacy sync version - kept for compatibility)
    #[allow(dead_code)]
    fn output_engine_message(&self, msg: &Message, output: Option<&Path>) -> Result<()> {
        match msg {
            Message::Record { stream, batch } => {
                match self.cli.format {
                    OutputFormat::Parquet => {
                        // Parquet format: write to file only, no stdout output for records
                        let dir = output.ok_or_else(|| {
                            Error::config("Parquet format requires --output directory")
                        })?;
                        let path = dir.join(format!("{stream}.parquet"));
                        let config = ParquetWriterConfig::default();
                        let mut writer =
                            ParquetWriter::new(&path, batch.schema().as_ref(), &config)?;
                        writer.write(batch)?;
                        writer.close()?;
                    }
                    OutputFormat::Json | OutputFormat::Pretty => {
                        // JSON format: output records to stdout
                        // Also write parquet if output directory specified
                        if let Some(dir) = output {
                            let path = dir.join(format!("{stream}.parquet"));
                            let config = ParquetWriterConfig::default();
                            let mut writer =
                                ParquetWriter::new(&path, batch.schema().as_ref(), &config)?;
                            writer.write(batch)?;
                            writer.close()?;
                        }

                        // Convert Arrow batch to JSON records
                        let records = arrow_to_json(batch)?;
                        let emitted_at = chrono::Utc::now().timestamp_millis();

                        // Output each record as a separate message
                        for record in records {
                            self.output_message(&json!({
                                "type": "RECORD",
                                "record": {
                                    "stream": stream,
                                    "data": record,
                                    "emitted_at": emitted_at
                                }
                            }));
                        }
                    }
                }
            }
            Message::State { stream, data } => {
                self.output_message(&json!({
                    "type": "STATE",
                    "state": {
                        "type": "STREAM",
                        "stream": {
                            "stream_descriptor": {
                                "name": stream
                            },
                            "stream_state": data
                        }
                    }
                }));
            }
            Message::Log { level, message } => {
                let level_str = match level {
                    crate::engine::LogLevel::Debug => "DEBUG",
                    crate::engine::LogLevel::Info => "INFO",
                    crate::engine::LogLevel::Warn => "WARN",
                    crate::engine::LogLevel::Error => "ERROR",
                };
                self.output_message(&json!({
                    "type": "LOG",
                    "log": {
                        "level": level_str,
                        "message": message
                    }
                }));
            }
        }
        Ok(())
    }
}
