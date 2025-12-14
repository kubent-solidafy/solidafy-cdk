//! Configuration types for connector definitions
//!
//! This module contains all the configuration structures used to define
//! connectors in YAML format.

use crate::types::{
    BackoffType, CursorFormat, DestinationSyncMode, ErrorStrategy, Method, PropertyType, SyncMode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Top-Level Connector Config
// ============================================================================

/// Complete connector configuration loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Kind of config (always "connector")
    #[serde(default = "default_kind")]
    pub kind: String,

    /// Config version
    #[serde(default = "default_version")]
    pub version: String,

    /// Connector metadata
    pub metadata: ConnectorMetadata,

    /// Configuration specification (for UI/validation)
    pub spec: SpecConfig,

    /// Connection check configuration
    #[serde(default)]
    pub check: CheckConfig,

    /// Base URL for API requests
    pub base_url: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfigDef,

    /// HTTP client configuration
    #[serde(default)]
    pub http: HttpConfig,

    /// Default request settings
    #[serde(default)]
    pub request_defaults: RequestDefaults,

    /// Stream definitions
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
}

fn default_kind() -> String {
    "connector".to_string()
}

fn default_version() -> String {
    "1.0".to_string()
}

// ============================================================================
// Metadata
// ============================================================================

/// Connector metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMetadata {
    /// Unique connector name (e.g., "stripe")
    pub name: String,

    /// Human-readable title (e.g., "Stripe")
    #[serde(default)]
    pub title: Option<String>,

    /// Description of the connector
    #[serde(default)]
    pub description: Option<String>,

    /// Connector icon URL
    #[serde(default)]
    pub icon: Option<String>,
}

// ============================================================================
// Spec Config (for UI)
// ============================================================================

/// Configuration specification for connector setup
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SpecConfig {
    /// Configuration properties
    #[serde(default)]
    pub properties: HashMap<String, PropertyConfig>,
}

/// Configuration property definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyConfig {
    /// Property type
    #[serde(rename = "type", default)]
    pub property_type: PropertyType,

    /// Human-readable title
    #[serde(default)]
    pub title: Option<String>,

    /// Property description
    #[serde(default)]
    pub description: Option<String>,

    /// Whether this is a secret (should be masked)
    #[serde(default)]
    pub secret: bool,

    /// Whether this property is required
    #[serde(default)]
    pub required: bool,

    /// Default value
    #[serde(default)]
    pub default: Option<serde_json::Value>,

    /// Format hint (e.g., "date", "uri")
    #[serde(default)]
    pub format: Option<String>,

    /// Enum of allowed values
    #[serde(rename = "enum", default)]
    pub allowed_values: Option<Vec<serde_json::Value>>,

    /// For arrays: item type
    #[serde(default)]
    pub items: Option<Box<PropertyConfig>>,

    /// For objects: nested properties
    #[serde(default)]
    pub properties: Option<HashMap<String, PropertyConfig>>,
}

// ============================================================================
// Check Config
// ============================================================================

/// Configuration for connection validation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckConfig {
    /// Endpoint to call for check
    #[serde(default)]
    pub endpoint: Option<String>,

    /// HTTP method for check
    #[serde(default)]
    pub method: Method,

    /// Query parameters
    #[serde(default)]
    pub params: HashMap<String, String>,

    /// Expected status code
    #[serde(default = "default_expect_status")]
    pub expect_status: u16,
}

fn default_expect_status() -> u16 {
    200
}

// ============================================================================
// Auth Config Definition (in YAML)
// ============================================================================

/// Authentication configuration from YAML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfigDef {
    /// No authentication
    #[default]
    None,

    /// API Key authentication
    ApiKey {
        /// Where to put the key
        location: AuthLocation,
        /// Header name (for header location)
        #[serde(default)]
        header_name: Option<String>,
        /// Query parameter name (for query location)
        #[serde(default)]
        query_param: Option<String>,
        /// Prefix to add before the value
        #[serde(default)]
        prefix: Option<String>,
        /// The API key value (usually a template)
        value: String,
    },

    /// Basic authentication
    Basic {
        /// Username (usually a template)
        username: String,
        /// Password (usually a template)
        password: String,
    },

    /// Bearer token authentication
    Bearer {
        /// The token value (usually a template)
        token: String,
    },

    /// OAuth2 Client Credentials flow
    Oauth2ClientCredentials {
        /// Token endpoint URL
        token_url: String,
        /// Client ID (usually a template)
        client_id: String,
        /// Client secret (usually a template)
        client_secret: String,
        /// Requested scopes
        #[serde(default)]
        scopes: Vec<String>,
        /// Additional token request body parameters
        #[serde(default)]
        token_body: HashMap<String, String>,
    },

    /// OAuth2 Refresh Token flow
    Oauth2Refresh {
        /// Token endpoint URL
        token_url: String,
        /// Client ID (usually a template)
        client_id: String,
        /// Client secret (usually a template)
        client_secret: String,
        /// Refresh token (usually a template)
        refresh_token: String,
    },

    /// Session-based authentication (login endpoint)
    Session {
        /// Login endpoint URL
        login_url: String,
        /// HTTP method for login
        #[serde(default)]
        login_method: Method,
        /// Login request body
        #[serde(default)]
        login_body: HashMap<String, String>,
        /// JSONPath to extract token from response
        token_path: String,
        /// Header name to use for token
        token_header: String,
        /// Prefix for token value
        #[serde(default)]
        token_prefix: Option<String>,
        /// JSONPath to extract expiration
        #[serde(default)]
        expires_in_path: Option<String>,
    },

    /// JWT authentication (service account style)
    Jwt {
        /// Token issuer
        issuer: String,
        /// Token subject (optional)
        #[serde(default)]
        subject: Option<String>,
        /// Token audience
        audience: String,
        /// Private key for signing (usually a template)
        private_key: String,
        /// Signing algorithm
        #[serde(default)]
        algorithm: crate::types::JwtAlgorithm,
        /// Token lifetime in seconds
        #[serde(default = "default_token_lifetime")]
        token_lifetime_seconds: u64,
        /// Additional claims
        #[serde(default)]
        claims: HashMap<String, String>,
        /// Optional token endpoint for token exchange
        #[serde(default)]
        token_url: Option<String>,
    },

    /// Custom headers
    CustomHeaders {
        /// Headers to add
        headers: HashMap<String, String>,
    },
}

fn default_token_lifetime() -> u64 {
    3600
}

/// Location for API key
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthLocation {
    #[default]
    Header,
    Query,
}

// ============================================================================
// HTTP Config
// ============================================================================

/// HTTP client configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,

    /// Maximum number of retries
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// HTTP status codes to retry on
    #[serde(default = "default_retry_statuses")]
    pub retry_statuses: Vec<u16>,

    /// Retry backoff configuration
    #[serde(default)]
    pub retry_backoff: BackoffConfig,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: default_timeout(),
            connect_timeout_seconds: default_connect_timeout(),
            max_retries: default_max_retries(),
            retry_statuses: default_retry_statuses(),
            retry_backoff: BackoffConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

fn default_timeout() -> u64 {
    30
}

fn default_connect_timeout() -> u64 {
    10
}

fn default_max_retries() -> u32 {
    5
}

fn default_retry_statuses() -> Vec<u16> {
    vec![429, 500, 502, 503, 504]
}

/// Backoff configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    /// Type of backoff
    #[serde(rename = "type", default)]
    pub backoff_type: BackoffType,

    /// Initial delay in milliseconds
    #[serde(default = "default_initial_ms")]
    pub initial_ms: u64,

    /// Maximum delay in milliseconds
    #[serde(default = "default_max_ms")]
    pub max_ms: u64,

    /// Multiplier for exponential backoff
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            backoff_type: BackoffType::Exponential,
            initial_ms: default_initial_ms(),
            max_ms: default_max_ms(),
            multiplier: default_multiplier(),
        }
    }
}

fn default_initial_ms() -> u64 {
    100
}

fn default_max_ms() -> u64 {
    60000
}

fn default_multiplier() -> f64 {
    2.0
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Requests per second limit
    #[serde(default = "default_rps")]
    pub requests_per_second: f64,

    /// Whether to respect rate limit headers from responses
    #[serde(default = "default_true")]
    pub respect_headers: bool,

    /// Header name for remaining requests
    #[serde(default = "default_remaining_header")]
    pub remaining_header: String,

    /// Header name for rate limit reset time
    #[serde(default = "default_reset_header")]
    pub reset_header: String,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: default_rps(),
            respect_headers: default_true(),
            remaining_header: default_remaining_header(),
            reset_header: default_reset_header(),
        }
    }
}

fn default_rps() -> f64 {
    10.0
}

fn default_true() -> bool {
    true
}

fn default_remaining_header() -> String {
    "X-RateLimit-Remaining".to_string()
}

fn default_reset_header() -> String {
    "X-RateLimit-Reset".to_string()
}

/// Default request settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestDefaults {
    /// Default headers for all requests
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Default query parameters
    #[serde(default)]
    pub params: HashMap<String, String>,
}

// ============================================================================
// Stream Config
// ============================================================================

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Unique stream name
    pub name: String,

    /// API endpoint path
    pub endpoint: String,

    /// HTTP method
    #[serde(default)]
    pub method: Method,

    /// Request body configuration
    #[serde(default)]
    pub body: Option<RequestBodyConfig>,

    /// Query parameters
    #[serde(default)]
    pub params: HashMap<String, String>,

    /// Additional headers
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// JSONPath to extract records
    pub record_path: String,

    /// Primary key fields
    #[serde(default)]
    pub primary_key: Vec<String>,

    /// Response format
    #[serde(default)]
    pub response_format: ResponseFormatConfig,

    /// Pagination configuration
    #[serde(default)]
    pub pagination: PaginationConfigDef,

    /// Cursor field for incremental sync
    #[serde(default)]
    pub cursor_field: Option<String>,

    /// Incremental sync configuration
    #[serde(default)]
    pub incremental: Option<IncrementalConfig>,

    /// Partition configuration
    #[serde(default)]
    pub partition: PartitionConfigDef,

    /// Error handling configuration
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

/// Request body configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestBodyConfig {
    /// Body type
    #[serde(rename = "type", default)]
    pub body_type: BodyType,

    /// Body content
    #[serde(default)]
    pub content: serde_json::Value,
}

/// Body content type
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyType {
    #[default]
    Json,
    Form,
}

/// Response format configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormatConfig {
    #[default]
    Json,
    Jsonl,
    Csv,
    Xml {
        record_element: String,
    },
}

// ============================================================================
// Pagination Config
// ============================================================================

/// Pagination configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfigDef {
    #[default]
    None,

    Cursor {
        cursor_param: String,
        cursor_path: String,
        #[serde(default)]
        stop_condition: StopConditionConfig,
    },

    Offset {
        offset_param: String,
        limit_param: String,
        limit_value: u32,
        #[serde(default)]
        stop_condition: StopConditionConfig,
    },

    PageNumber {
        page_param: String,
        #[serde(default = "default_start_page")]
        start_page: u32,
        #[serde(default)]
        page_size_param: Option<String>,
        #[serde(default)]
        page_size: Option<u32>,
        #[serde(default)]
        stop_condition: StopConditionConfig,
    },

    LinkHeader {
        #[serde(default = "default_rel")]
        rel: String,
    },

    NextUrl {
        path: String,
    },
}

fn default_start_page() -> u32 {
    1
}

fn default_rel() -> String {
    "next".to_string()
}

/// Stop condition for pagination
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StopConditionConfig {
    #[default]
    EmptyPage,

    Field {
        path: String,
        value: serde_json::Value,
    },

    TotalCount {
        path: String,
    },

    TotalPages {
        path: String,
    },
}

// ============================================================================
// Incremental Config
// ============================================================================

/// Incremental sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalConfig {
    /// Field to use as cursor
    pub cursor_field: String,

    /// Query parameter name for cursor
    pub cursor_param: String,

    /// Format for cursor values
    #[serde(default)]
    pub cursor_format: CursorFormat,

    /// Lookback window in seconds
    #[serde(default)]
    pub lookback_seconds: Option<u64>,
}

// ============================================================================
// Partition Config
// ============================================================================

/// Partition configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionConfigDef {
    #[default]
    None,

    Parent {
        parent_stream: String,
        parent_key: String,
        partition_field: String,
    },

    List {
        values: Vec<String>,
        partition_field: String,
    },

    Datetime {
        start: String,
        end: String,
        step: String,
        format: String,
        start_param: String,
        end_param: String,
    },

    AsyncJob {
        create: AsyncJobCreateConfig,
        poll: AsyncJobPollConfig,
        download: AsyncJobDownloadConfig,
    },
}

/// Async job creation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncJobCreateConfig {
    pub endpoint: String,
    #[serde(default)]
    pub method: Method,
    #[serde(default)]
    pub body: serde_json::Value,
    pub job_id_path: String,
}

/// Async job polling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncJobPollConfig {
    pub endpoint: String,
    #[serde(default = "default_poll_interval")]
    pub interval_seconds: u64,
    #[serde(default = "default_max_poll")]
    pub max_attempts: u32,
    pub completed_condition: ConditionConfig,
    #[serde(default)]
    pub failed_condition: Option<ConditionConfig>,
}

fn default_poll_interval() -> u64 {
    10
}

fn default_max_poll() -> u32 {
    60
}

/// Async job download configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncJobDownloadConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub url_path: Option<String>,
}

/// Condition configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionConfig {
    pub path: String,
    pub value: serde_json::Value,
}

// ============================================================================
// Error Handling Config
// ============================================================================

/// Error handling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHandlingConfig {
    /// Error handling strategy
    #[serde(default)]
    pub strategy: ErrorStrategy,

    /// Maximum errors before failing
    #[serde(default = "default_max_errors")]
    pub max_errors: u32,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            strategy: ErrorStrategy::Fail,
            max_errors: default_max_errors(),
        }
    }
}

fn default_max_errors() -> u32 {
    100
}

// ============================================================================
// Catalog Types
// ============================================================================

/// Discovered catalog (available streams)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Catalog {
    /// Available streams
    pub streams: Vec<CatalogStream>,
}

/// Stream in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogStream {
    /// Stream name
    pub name: String,

    /// JSON schema for the stream
    #[serde(default)]
    pub json_schema: serde_json::Value,

    /// Supported sync modes
    #[serde(default)]
    pub supported_sync_modes: Vec<SyncMode>,

    /// Default cursor field
    #[serde(default)]
    pub default_cursor_field: Option<Vec<String>>,

    /// Source-defined primary key
    #[serde(default)]
    pub source_defined_primary_key: Option<Vec<Vec<String>>>,
}

/// Configured catalog (selected streams for sync)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfiguredCatalog {
    /// Selected streams
    pub streams: Vec<ConfiguredStream>,
}

/// Configured stream for sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfiguredStream {
    /// Stream reference
    pub stream: CatalogStream,

    /// Selected sync mode
    #[serde(default)]
    pub sync_mode: SyncMode,

    /// Destination sync mode
    #[serde(default)]
    pub destination_sync_mode: DestinationSyncMode,

    /// Cursor field to use
    #[serde(default)]
    pub cursor_field: Option<Vec<String>>,

    /// Primary key to use
    #[serde(default)]
    pub primary_key: Option<Vec<Vec<String>>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_connector() {
        let yaml = r#"
kind: connector
version: "1.0"
metadata:
  name: test
spec:
  properties: {}
base_url: "https://api.example.com"
streams: []
"#;

        let config: ConnectorConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.metadata.name, "test");
        assert_eq!(config.base_url, "https://api.example.com");
    }

    #[test]
    fn test_parse_auth_api_key() {
        let yaml = r#"
type: api_key
location: header
header_name: "Authorization"
prefix: "Bearer "
value: "{{ config.api_key }}"
"#;

        let auth: AuthConfigDef = serde_yaml::from_str(yaml).unwrap();
        match auth {
            AuthConfigDef::ApiKey {
                location,
                header_name,
                prefix,
                value,
                ..
            } => {
                assert!(matches!(location, AuthLocation::Header));
                assert_eq!(header_name, Some("Authorization".to_string()));
                assert_eq!(prefix, Some("Bearer ".to_string()));
                assert_eq!(value, "{{ config.api_key }}");
            }
            _ => panic!("Expected ApiKey auth"),
        }
    }

    #[test]
    fn test_parse_pagination_cursor() {
        let yaml = r#"
type: cursor
cursor_param: "starting_after"
cursor_path: "$.data[-1:].id"
stop_condition:
  type: field
  path: "$.has_more"
  value: false
"#;

        let pagination: PaginationConfigDef = serde_yaml::from_str(yaml).unwrap();
        match pagination {
            PaginationConfigDef::Cursor {
                cursor_param,
                cursor_path,
                stop_condition,
            } => {
                assert_eq!(cursor_param, "starting_after");
                assert_eq!(cursor_path, "$.data[-1:].id");
                match stop_condition {
                    StopConditionConfig::Field { path, value } => {
                        assert_eq!(path, "$.has_more");
                        assert_eq!(value, serde_json::Value::Bool(false));
                    }
                    _ => panic!("Expected Field stop condition"),
                }
            }
            _ => panic!("Expected Cursor pagination"),
        }
    }

    #[test]
    fn test_parse_stream_config() {
        let yaml = r#"
name: customers
endpoint: "/v1/customers"
primary_key: [id]
record_path: "$.data[*]"
pagination:
  type: cursor
  cursor_param: "starting_after"
  cursor_path: "$.data[-1:].id"
  stop_condition:
    type: field
    path: "$.has_more"
    value: false
"#;

        let stream: StreamConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(stream.name, "customers");
        assert_eq!(stream.endpoint, "/v1/customers");
        assert_eq!(stream.primary_key, vec!["id"]);
        assert_eq!(stream.record_path, "$.data[*]");
    }

    #[test]
    fn test_default_http_config() {
        let config = HttpConfig::default();
        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_statuses, vec![429, 500, 502, 503, 504]);
    }
}
