//! Loader types
//!
//! Declarative connector definition types for YAML parsing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Connector Definition
// ============================================================================

/// Top-level connector definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConnectorDefinition {
    /// Connector name
    pub name: String,
    /// Connector version
    #[serde(default = "default_version")]
    pub version: String,
    /// Base URL for all requests
    pub base_url: String,
    /// Authentication configuration
    #[serde(default)]
    pub auth: Option<AuthDefinition>,
    /// HTTP client configuration
    #[serde(default)]
    pub http: HttpDefinition,
    /// Connection check configuration
    #[serde(default)]
    pub check: Option<CheckDefinition>,
    /// Stream definitions
    pub streams: Vec<StreamDefinition>,
    /// Global headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// Connection check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CheckDefinition {
    /// URL path for check endpoint
    pub path: String,
    /// Query parameters
    #[serde(default)]
    pub params: HashMap<String, String>,
}

fn default_version() -> String {
    "0.1.0".to_string()
}

// ============================================================================
// Auth Definition
// ============================================================================

/// Authentication definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuthDefinition {
    /// API key authentication
    #[serde(rename = "api_key")]
    ApiKey {
        /// Header or query param name
        key: String,
        /// Value (usually a template like `{{ config.api_key }}`)
        value: String,
        /// Location: header or query
        #[serde(default = "default_auth_location")]
        location: String,
    },
    /// Bearer token authentication
    #[serde(rename = "bearer")]
    Bearer {
        /// Token value (template)
        token: String,
    },
    /// Basic authentication
    #[serde(rename = "basic")]
    Basic {
        /// Username (template)
        username: String,
        /// Password (template)
        password: String,
    },
    /// OAuth2 client credentials
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials {
        /// Token URL
        token_url: String,
        /// Client ID (template)
        client_id: String,
        /// Client secret (template)
        client_secret: String,
        /// Scopes
        #[serde(default)]
        scopes: Vec<String>,
    },
    /// OAuth2 refresh token
    #[serde(rename = "oauth2_refresh_token")]
    OAuth2RefreshToken {
        /// Token URL
        token_url: String,
        /// Client ID (template)
        client_id: String,
        /// Client secret (template)
        client_secret: String,
        /// Refresh token (template)
        refresh_token: String,
    },
    /// Session token authentication
    #[serde(rename = "session_token")]
    SessionToken {
        /// Login URL
        login_url: String,
        /// Login body (template)
        body: String,
        /// Path to extract token from response
        token_path: String,
        /// Header name for token
        header_name: String,
        /// Header prefix (e.g., "Bearer ")
        #[serde(default)]
        header_prefix: String,
    },
    /// No authentication
    #[serde(rename = "none")]
    None,
}

fn default_auth_location() -> String {
    "header".to_string()
}

// ============================================================================
// HTTP Definition
// ============================================================================

/// HTTP client configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct HttpDefinition {
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Maximum retries
    #[serde(default = "default_retries")]
    pub max_retries: u32,
    /// Rate limit (requests per second)
    #[serde(default)]
    pub rate_limit_rps: Option<u32>,
    /// User agent
    #[serde(default)]
    pub user_agent: Option<String>,
}

impl Default for HttpDefinition {
    fn default() -> Self {
        Self {
            timeout_secs: default_timeout(),
            max_retries: default_retries(),
            rate_limit_rps: None,
            user_agent: None,
        }
    }
}

fn default_timeout() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

// ============================================================================
// Stream Definition
// ============================================================================

/// Stream definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct StreamDefinition {
    /// Stream name
    pub name: String,
    /// Request configuration
    pub request: RequestDefinition,
    /// Response decoder
    #[serde(default)]
    pub decoder: DecoderDefinition,
    /// Pagination configuration
    #[serde(default)]
    pub pagination: Option<PaginationDefinition>,
    /// Partition router
    #[serde(default)]
    pub partition: Option<PartitionDefinition>,
    /// Primary key fields
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// Cursor field for incremental sync
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Stream-specific headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

// ============================================================================
// Request Definition
// ============================================================================

/// Request configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RequestDefinition {
    /// HTTP method
    #[serde(default = "default_method")]
    pub method: String,
    /// URL path (can contain templates)
    pub path: String,
    /// Query parameters
    #[serde(default)]
    pub params: HashMap<String, String>,
    /// Request body (for POST/PUT)
    #[serde(default)]
    pub body: Option<String>,
    /// Content type
    #[serde(default)]
    pub content_type: Option<String>,
}

fn default_method() -> String {
    "GET".to_string()
}

// ============================================================================
// Decoder Definition
// ============================================================================

/// Response decoder configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecoderDefinition {
    /// JSON decoder
    Json {
        /// JSON path to records array
        #[serde(default)]
        records_path: Option<String>,
    },
    /// JSONL (newline-delimited JSON) decoder
    Jsonl,
    /// CSV decoder
    Csv {
        /// Delimiter character
        #[serde(default = "default_csv_delimiter")]
        delimiter: char,
        /// Whether first row is header
        #[serde(default = "default_true")]
        has_header: bool,
    },
    /// XML decoder
    Xml {
        /// XPath to records
        records_path: String,
    },
}

impl Default for DecoderDefinition {
    fn default() -> Self {
        Self::Json { records_path: None }
    }
}

fn default_csv_delimiter() -> char {
    ','
}

fn default_true() -> bool {
    true
}

// ============================================================================
// Pagination Definition
// ============================================================================

/// Pagination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationDefinition {
    /// No pagination
    None,
    /// Offset-based pagination
    Offset {
        /// Offset parameter name
        offset_param: String,
        /// Limit parameter name
        limit_param: String,
        /// Page size
        limit: u32,
        /// Stop condition
        #[serde(default)]
        stop: StopConditionDefinition,
    },
    /// Page number pagination
    PageNumber {
        /// Page parameter name
        page_param: String,
        /// Start page (usually 0 or 1)
        #[serde(default = "default_start_page")]
        start_page: u32,
        /// Page size parameter name
        #[serde(default)]
        page_size_param: Option<String>,
        /// Page size
        #[serde(default)]
        page_size: Option<u32>,
        /// Stop condition
        #[serde(default)]
        stop: StopConditionDefinition,
    },
    /// Cursor-based pagination
    Cursor {
        /// Cursor parameter name
        cursor_param: String,
        /// Path to next cursor in response
        cursor_path: String,
        /// Location: query or header
        #[serde(default = "default_cursor_location")]
        location: String,
    },
    /// Link header pagination (RFC 5988)
    LinkHeader {
        /// Relation to follow (usually "next")
        #[serde(default = "default_link_rel")]
        rel: String,
    },
    /// JSON pointer to next URL
    NextUrl {
        /// JSON path to next URL
        next_url_path: String,
    },
}

fn default_start_page() -> u32 {
    1
}

fn default_cursor_location() -> String {
    "query".to_string()
}

fn default_link_rel() -> String {
    "next".to_string()
}

/// Stop condition for pagination
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StopConditionDefinition {
    /// Stop when page is empty
    #[default]
    EmptyPage,
    /// Stop when total count is reached
    TotalCount {
        /// JSON path to total count
        path: String,
    },
    /// Stop when total pages reached
    TotalPages {
        /// JSON path to total pages
        path: String,
    },
    /// Stop when field equals value
    Field {
        /// JSON path to field
        path: String,
        /// Value to match
        value: serde_json::Value,
    },
}

// ============================================================================
// Partition Definition
// ============================================================================

/// Partition router configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionDefinition {
    /// List of static values
    List {
        /// Field name for partition value
        field: String,
        /// List of values
        values: Vec<String>,
    },
    /// Parent stream partition
    Parent {
        /// Parent stream name
        stream: String,
        /// Field to extract from parent records
        parent_field: String,
        /// Field name in partition context
        partition_field: String,
    },
    /// Date/time range partition
    DateRange {
        /// Start date (template)
        start: String,
        /// End date (template)
        end: String,
        /// Step (e.g., "1d", "1h", "1w")
        step: String,
        /// Field name for start date
        start_field: String,
        /// Field name for end date
        end_field: String,
    },
    /// Async job-based partition (create → poll → download)
    AsyncJob {
        /// Job creation configuration
        create: AsyncJobCreateDef,
        /// Job polling configuration
        poll: AsyncJobPollDef,
        /// Results download configuration
        download: AsyncJobDownloadDef,
    },
}

/// Async job creation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AsyncJobCreateDef {
    /// HTTP method (POST, PUT)
    #[serde(default = "default_post_method")]
    pub method: String,
    /// Endpoint path (can include templates)
    pub path: String,
    /// Request body (JSON template)
    #[serde(default)]
    pub body: Option<String>,
    /// JSONPath to extract job ID from response
    #[serde(default = "default_job_id_path")]
    pub job_id_path: String,
}

fn default_post_method() -> String {
    "POST".to_string()
}

fn default_job_id_path() -> String {
    "id".to_string()
}

/// Async job polling definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AsyncJobPollDef {
    /// Endpoint path (must include {{ job_id }})
    pub path: String,
    /// Polling interval in seconds
    #[serde(default = "default_poll_interval")]
    pub interval_secs: u64,
    /// Maximum poll attempts
    #[serde(default = "default_poll_max_attempts")]
    pub max_attempts: u64,
    /// JSONPath to check job status
    #[serde(default = "default_status_path")]
    pub status_path: String,
    /// Value indicating job completion
    pub completed_value: String,
    /// Values indicating job failure
    #[serde(default)]
    pub failed_values: Vec<String>,
}

fn default_poll_interval() -> u64 {
    5
}

fn default_poll_max_attempts() -> u64 {
    120
}

fn default_status_path() -> String {
    "state".to_string()
}

/// Async job download definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct AsyncJobDownloadDef {
    /// Endpoint path (must include {{ job_id }})
    pub path: String,
    /// JSONPath to extract records (if JSON response)
    #[serde(default)]
    pub records_path: Option<String>,
}

// ============================================================================
// Database Connector Definition
// ============================================================================

/// Database connector definition (for SQL databases via DuckDB)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DatabaseConnectorDefinition {
    /// Connector name
    pub name: String,
    /// Connector version
    #[serde(default = "default_version")]
    pub version: String,
    /// Database type (postgres, mysql, sqlite, etc.)
    pub engine: DatabaseEngine,
    /// Connection configuration
    pub connection: DatabaseConnectionDef,
    /// Stream definitions (tables/queries)
    pub streams: Vec<DatabaseStreamDefinition>,
}

/// Supported database engines
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseEngine {
    /// PostgreSQL
    Postgres,
    /// MySQL
    Mysql,
    /// SQLite
    Sqlite,
    /// DuckDB (native)
    Duckdb,
}

impl std::fmt::Display for DatabaseEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseEngine::Postgres => write!(f, "postgres"),
            DatabaseEngine::Mysql => write!(f, "mysql"),
            DatabaseEngine::Sqlite => write!(f, "sqlite"),
            DatabaseEngine::Duckdb => write!(f, "duckdb"),
        }
    }
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DatabaseConnectionDef {
    /// Connection string (template, e.g., "{{ config.connection_string }}")
    /// For postgres: postgresql://user:pass@host:port/database
    /// For mysql: mysql://user:pass@host:port/database
    /// For sqlite: path/to/database.db
    #[serde(default)]
    pub connection_string: Option<String>,
    /// Individual connection parameters (alternative to connection_string)
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// SSL mode (disable, prefer, require)
    #[serde(default = "default_ssl_mode")]
    pub ssl_mode: String,
}

fn default_ssl_mode() -> String {
    "prefer".to_string()
}

/// Database stream definition (table or query)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DatabaseStreamDefinition {
    /// Stream name
    pub name: String,
    /// Table name (schema.table format supported)
    #[serde(default)]
    pub table: Option<String>,
    /// Custom SQL query (alternative to table)
    #[serde(default)]
    pub query: Option<String>,
    /// Primary key columns
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// Cursor field for incremental sync (e.g., updated_at)
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Batch size for pagination
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

fn default_batch_size() -> u32 {
    10000
}

// ============================================================================
// Unified Connector Type
// ============================================================================

/// Unified connector that can be either REST API or Database
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UnifiedConnectorDefinition {
    /// REST API connector (default, existing behavior)
    #[serde(rename = "api")]
    Api(ConnectorDefinition),
    /// Database connector (new)
    #[serde(rename = "database")]
    Database(DatabaseConnectorDefinition),
}
