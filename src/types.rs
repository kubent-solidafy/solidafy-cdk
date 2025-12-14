//! Common types used throughout Solidafy CDK
//!
//! This module contains shared type definitions, type aliases,
//! and utility types used across multiple modules.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Type Aliases
// ============================================================================

/// JSON value type (re-exported from serde_json)
pub type JsonValue = serde_json::Value;

/// JSON object type
pub type JsonObject = serde_json::Map<String, JsonValue>;

/// Generic key-value map with string keys and values
pub type StringMap = HashMap<String, String>;

/// Generic key-value map with string keys and JSON values
pub type ValueMap = HashMap<String, JsonValue>;

// ============================================================================
// HTTP Types
// ============================================================================

/// HTTP method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    #[default]
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
}

impl From<Method> for reqwest::Method {
    fn from(method: Method) -> Self {
        match method {
            Method::GET => reqwest::Method::GET,
            Method::POST => reqwest::Method::POST,
            Method::PUT => reqwest::Method::PUT,
            Method::PATCH => reqwest::Method::PATCH,
            Method::DELETE => reqwest::Method::DELETE,
        }
    }
}

// ============================================================================
// Sync Mode
// ============================================================================

/// Synchronization mode for streams
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Full refresh - fetch all data every time
    #[default]
    FullRefresh,
    /// Incremental - only fetch new/updated data
    Incremental,
}

// ============================================================================
// Destination Sync Mode
// ============================================================================

/// How data should be written to the destination
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationSyncMode {
    /// Append new records
    #[default]
    Append,
    /// Overwrite existing data
    Overwrite,
    /// Append with deduplication
    AppendDedup,
}

// ============================================================================
// Log Level
// ============================================================================

/// Log level for connector messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

// ============================================================================
// Property Type (for spec)
// ============================================================================

/// Property type for configuration schema
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyType {
    #[default]
    String,
    Integer,
    Number,
    Boolean,
    Array,
    Object,
}

// ============================================================================
// Cursor Format
// ============================================================================

/// Format for cursor values in incremental sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorFormat {
    /// ISO 8601 datetime string
    #[default]
    Iso8601,
    /// Unix timestamp (seconds)
    Unix,
    /// Unix timestamp (milliseconds)
    UnixMs,
    /// Plain string (no conversion)
    String,
}

// ============================================================================
// Error Handling Strategy
// ============================================================================

/// Strategy for handling errors during sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    /// Stop on first error
    #[default]
    Fail,
    /// Skip erroring records, continue
    Skip,
    /// Retry with backoff
    Retry,
}

// ============================================================================
// Backoff Type
// ============================================================================

/// Type of backoff for retries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffType {
    /// Constant delay between retries
    Constant,
    /// Linear increase in delay
    Linear,
    /// Exponential increase in delay
    #[default]
    Exponential,
}

// ============================================================================
// JWT Algorithm
// ============================================================================

/// JWT signing algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    /// HMAC using SHA-256
    HS256,
    /// HMAC using SHA-384
    HS384,
    /// HMAC using SHA-512
    HS512,
    /// RSA using SHA-256
    #[default]
    RS256,
    /// RSA using SHA-384
    RS384,
    /// RSA using SHA-512
    RS512,
    /// ECDSA using P-256 and SHA-256
    ES256,
    /// ECDSA using P-384 and SHA-384
    ES384,
}

impl From<JwtAlgorithm> for jsonwebtoken::Algorithm {
    fn from(alg: JwtAlgorithm) -> Self {
        match alg {
            JwtAlgorithm::HS256 => jsonwebtoken::Algorithm::HS256,
            JwtAlgorithm::HS384 => jsonwebtoken::Algorithm::HS384,
            JwtAlgorithm::HS512 => jsonwebtoken::Algorithm::HS512,
            JwtAlgorithm::RS256 => jsonwebtoken::Algorithm::RS256,
            JwtAlgorithm::RS384 => jsonwebtoken::Algorithm::RS384,
            JwtAlgorithm::RS512 => jsonwebtoken::Algorithm::RS512,
            JwtAlgorithm::ES256 => jsonwebtoken::Algorithm::ES256,
            JwtAlgorithm::ES384 => jsonwebtoken::Algorithm::ES384,
        }
    }
}

// ============================================================================
// Utilities
// ============================================================================

/// Extension trait for Option<String> to handle empty strings
pub trait OptionStringExt {
    /// Returns None if the string is empty
    fn none_if_empty(self) -> Option<String>;
}

impl OptionStringExt for Option<String> {
    fn none_if_empty(self) -> Option<String> {
        self.filter(|s| !s.is_empty())
    }
}

impl OptionStringExt for String {
    fn none_if_empty(self) -> Option<String> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_method_conversion() {
        let get: reqwest::Method = Method::GET.into();
        assert_eq!(reqwest::Method::GET, get);
        let post: reqwest::Method = Method::POST.into();
        assert_eq!(reqwest::Method::POST, post);
    }

    #[test]
    fn test_method_default() {
        assert_eq!(Method::default(), Method::GET);
    }

    #[test]
    fn test_sync_mode_serde() {
        let mode: SyncMode = serde_json::from_str("\"incremental\"").unwrap();
        assert_eq!(mode, SyncMode::Incremental);

        let json = serde_json::to_string(&SyncMode::FullRefresh).unwrap();
        assert_eq!(json, "\"full_refresh\"");
    }

    #[test]
    fn test_jwt_algorithm_conversion() {
        assert_eq!(jsonwebtoken::Algorithm::RS256, JwtAlgorithm::RS256.into());
        assert_eq!(jsonwebtoken::Algorithm::HS256, JwtAlgorithm::HS256.into());
    }

    #[test]
    fn test_option_string_none_if_empty() {
        assert_eq!(
            Some("test".to_string()).none_if_empty(),
            Some("test".to_string())
        );
        assert_eq!(Some("".to_string()).none_if_empty(), None);
        assert_eq!(None::<String>.none_if_empty(), None);
        assert_eq!("test".to_string().none_if_empty(), Some("test".to_string()));
        assert_eq!("".to_string().none_if_empty(), None);
    }
}
