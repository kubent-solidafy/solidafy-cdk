//! Error types for Solidafy CDK
//!
//! This module defines the error hierarchy for the entire CDK.
//! All public APIs return `Result<T, Error>` where Error is defined here.

use thiserror::Error;

/// The main error type for Solidafy CDK
#[derive(Error, Debug)]
pub enum Error {
    // ============================================================================
    // Configuration Errors
    // ============================================================================
    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("Missing required config field: {field}")]
    MissingConfigField { field: String },

    #[error("Invalid config value for '{field}': {message}")]
    InvalidConfigValue { field: String, message: String },

    #[error("Failed to parse YAML: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("Failed to parse JSON: {0}")]
    JsonParse(#[from] serde_json::Error),

    // ============================================================================
    // Authentication Errors
    // ============================================================================
    #[error("Authentication failed: {message}")]
    Auth { message: String },

    #[error("Token refresh failed: {message}")]
    TokenRefresh { message: String },

    #[error("JWT generation failed: {message}")]
    JwtGeneration { message: String },

    #[error("OAuth2 error: {message}")]
    OAuth2 { message: String },

    // ============================================================================
    // HTTP Errors
    // ============================================================================
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },

    #[error("Rate limited, retry after {retry_after_seconds}s")]
    RateLimited { retry_after_seconds: u64 },

    #[error("Request timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Max retries ({max_retries}) exceeded")]
    MaxRetriesExceeded { max_retries: u32 },

    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    // ============================================================================
    // Data Processing Errors
    // ============================================================================
    #[error("JSONPath error: {message}")]
    JsonPath { message: String },

    #[error("Failed to extract records from path '{path}': {message}")]
    RecordExtraction { path: String, message: String },

    #[error("Failed to decode response: {message}")]
    Decode { message: String },

    #[error("CSV parsing error: {message}")]
    CsvParse { message: String },

    #[error("XML parsing error: {message}")]
    XmlParse { message: String },

    // ============================================================================
    // Arrow/Parquet Errors
    // ============================================================================
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Schema inference failed: {message}")]
    SchemaInference { message: String },

    #[error("Output error: {message}")]
    Output { message: String },

    // ============================================================================
    // State Errors
    // ============================================================================
    #[error("State error: {message}")]
    State { message: String },

    #[error("Checkpoint failed: {message}")]
    Checkpoint { message: String },

    // ============================================================================
    // Connector Errors
    // ============================================================================
    #[error("Connection check failed: {message}")]
    ConnectionCheck { message: String },

    #[error("Stream '{stream}' not found in catalog")]
    StreamNotFound { stream: String },

    #[error("Partition error for stream '{stream}': {message}")]
    Partition { stream: String, message: String },

    // ============================================================================
    // Template Errors
    // ============================================================================
    #[error("Template error: {message}")]
    Template { message: String },

    #[error("Undefined variable in template: {variable}")]
    UndefinedVariable { variable: String },

    // ============================================================================
    // I/O Errors
    // ============================================================================
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("File not found: {path}")]
    FileNotFound { path: String },

    // ============================================================================
    // Generic Errors
    // ============================================================================
    #[error("{0}")]
    Other(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl Error {
    /// Create a config error
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Create a missing field error
    pub fn missing_field(field: impl Into<String>) -> Self {
        Self::MissingConfigField {
            field: field.into(),
        }
    }

    /// Create an auth error
    pub fn auth(message: impl Into<String>) -> Self {
        Self::Auth {
            message: message.into(),
        }
    }

    /// Create an HTTP status error
    pub fn http_status(status: u16, body: impl Into<String>) -> Self {
        Self::HttpStatus {
            status,
            body: body.into(),
        }
    }

    /// Create a JSONPath error
    pub fn json_path(message: impl Into<String>) -> Self {
        Self::JsonPath {
            message: message.into(),
        }
    }

    /// Create a decode error
    pub fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: message.into(),
        }
    }

    /// Create a template error
    pub fn template(message: impl Into<String>) -> Self {
        Self::Template {
            message: message.into(),
        }
    }

    /// Create an undefined variable error
    pub fn undefined_var(variable: impl Into<String>) -> Self {
        Self::UndefinedVariable {
            variable: variable.into(),
        }
    }

    /// Create a state error
    pub fn state(message: impl Into<String>) -> Self {
        Self::State {
            message: message.into(),
        }
    }

    /// Create a partition error
    pub fn partition(stream: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Partition {
            stream: stream.into(),
            message: message.into(),
        }
    }

    /// Create an output error
    pub fn output(message: impl Into<String>) -> Self {
        Self::Output {
            message: message.into(),
        }
    }

    /// Create an IO error from a message
    pub fn io(message: impl Into<String>) -> Self {
        Self::Output {
            message: message.into(),
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Http(_) | Error::RateLimited { .. } | Error::Timeout { .. } => true,
            Error::HttpStatus { status, .. } => is_retryable_status(*status),
            _ => false,
        }
    }
}

/// Check if an HTTP status code is retryable
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Result type alias for Solidafy CDK
pub type Result<T> = std::result::Result<T, Error>;

/// Extension trait for adding context to errors
pub trait ResultExt<T> {
    /// Add context to an error
    fn context(self, message: impl Into<String>) -> Result<T>;

    /// Add context with a closure (lazy evaluation)
    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T>;
}

impl<T, E: Into<Error>> ResultExt<T> for std::result::Result<T, E> {
    fn context(self, message: impl Into<String>) -> Result<T> {
        self.map_err(|e| {
            let inner = e.into();
            Error::Other(format!("{}: {}", message.into(), inner))
        })
    }

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T> {
        self.map_err(|e| {
            let inner = e.into();
            Error::Other(format!("{}: {}", f(), inner))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::config("test message");
        assert_eq!(err.to_string(), "Configuration error: test message");

        let err = Error::missing_field("api_key");
        assert_eq!(err.to_string(), "Missing required config field: api_key");

        let err = Error::http_status(404, "Not found");
        assert_eq!(err.to_string(), "HTTP 404: Not found");
    }

    #[test]
    fn test_is_retryable() {
        assert!(Error::RateLimited {
            retry_after_seconds: 60
        }
        .is_retryable());
        assert!(Error::Timeout { timeout_ms: 1000 }.is_retryable());
        assert!(Error::http_status(429, "").is_retryable());
        assert!(Error::http_status(500, "").is_retryable());
        assert!(Error::http_status(503, "").is_retryable());

        assert!(!Error::http_status(400, "").is_retryable());
        assert!(!Error::http_status(401, "").is_retryable());
        assert!(!Error::http_status(404, "").is_retryable());
        assert!(!Error::config("test").is_retryable());
    }

    #[test]
    fn test_result_context() {
        let result: Result<()> = Err(Error::config("inner"));
        let with_context = result.context("outer");
        assert!(with_context
            .unwrap_err()
            .to_string()
            .contains("outer: Configuration error: inner"));
    }
}
