//! Decoder types and traits
//!
//! Defines the core decoder abstractions.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Format of the response body
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecoderFormat {
    /// JSON format (default)
    #[default]
    Json,
    /// JSON Lines format (one JSON object per line)
    Jsonl,
    /// CSV format
    Csv,
    /// XML format
    Xml,
}

/// Configuration for decoding responses
#[derive(Debug, Clone, Default)]
pub struct DecoderConfig {
    /// Response format
    pub format: DecoderFormat,
    /// JSONPath to extract records from response (for JSON/XML)
    pub record_path: Option<String>,
    /// CSV delimiter (default: comma)
    pub csv_delimiter: Option<char>,
    /// Whether CSV has a header row
    pub csv_has_header: bool,
    /// XML element name for records
    pub xml_record_element: Option<String>,
}

impl DecoderConfig {
    /// Create a JSON decoder config
    pub fn json() -> Self {
        Self {
            format: DecoderFormat::Json,
            ..Default::default()
        }
    }

    /// Create a JSON decoder config with a record path
    pub fn json_with_path(path: impl Into<String>) -> Self {
        Self {
            format: DecoderFormat::Json,
            record_path: Some(path.into()),
            ..Default::default()
        }
    }

    /// Create a JSONL decoder config
    pub fn jsonl() -> Self {
        Self {
            format: DecoderFormat::Jsonl,
            ..Default::default()
        }
    }

    /// Create a CSV decoder config
    pub fn csv() -> Self {
        Self {
            format: DecoderFormat::Csv,
            csv_delimiter: Some(','),
            csv_has_header: true,
            ..Default::default()
        }
    }

    /// Create a CSV decoder config with custom delimiter
    pub fn csv_with_delimiter(delimiter: char, has_header: bool) -> Self {
        Self {
            format: DecoderFormat::Csv,
            csv_delimiter: Some(delimiter),
            csv_has_header: has_header,
            ..Default::default()
        }
    }

    /// Create an XML decoder config
    pub fn xml(record_element: impl Into<String>) -> Self {
        Self {
            format: DecoderFormat::Xml,
            xml_record_element: Some(record_element.into()),
            ..Default::default()
        }
    }

    /// Set the record path
    #[must_use]
    pub fn with_record_path(mut self, path: impl Into<String>) -> Self {
        self.record_path = Some(path.into());
        self
    }
}

/// Trait for decoding response bodies into records
pub trait RecordDecoder: Send + Sync {
    /// Decode the response body into a list of records
    fn decode(&self, body: &str) -> Result<Vec<Value>>;

    /// Decode the response body into a single JSON value (full response)
    fn decode_raw(&self, body: &str) -> Result<Value>;
}
