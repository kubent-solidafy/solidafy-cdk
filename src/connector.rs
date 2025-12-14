//! Connector trait and YAML loader
//!
//! Defines the core Connector trait that all connectors implement,
//! and provides functions to load connectors from YAML definitions.

use crate::config::{Catalog, ConfiguredCatalog, ConnectorConfig, SpecConfig};
use crate::error::{Error, Result};
use crate::state::State;
use crate::types::LogLevel;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::pin::Pin;

// ============================================================================
// Connector Spec (for UI)
// ============================================================================

/// Connector specification returned by spec()
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Connector name
    pub name: String,

    /// Human-readable title
    pub title: String,

    /// Description
    pub description: Option<String>,

    /// Configuration specification
    pub spec: SpecConfig,

    /// Icon URL
    pub icon: Option<String>,
}

// ============================================================================
// Check Result
// ============================================================================

/// Result of a connection check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// Whether the check succeeded
    pub success: bool,

    /// Error message if failed
    pub message: Option<String>,
}

impl CheckResult {
    /// Create a successful check result
    pub fn success() -> Self {
        Self {
            success: true,
            message: None,
        }
    }

    /// Create a failed check result
    pub fn failure(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
        }
    }
}

// ============================================================================
// Messages
// ============================================================================

/// Messages emitted during read operations
#[derive(Debug, Clone)]
pub enum Message {
    /// A batch of records
    Record {
        /// Stream name
        stream: String,
        /// Record data as Arrow RecordBatch
        data: RecordBatch,
        /// Timestamp when records were emitted
        emitted_at: DateTime<Utc>,
    },

    /// State checkpoint
    State(State),

    /// Log message
    Log {
        /// Log level
        level: LogLevel,
        /// Log message
        message: String,
    },
}

impl Message {
    /// Create a record message
    pub fn record(stream: impl Into<String>, data: RecordBatch) -> Self {
        Self::Record {
            stream: stream.into(),
            data,
            emitted_at: Utc::now(),
        }
    }

    /// Create a state message
    pub fn state(state: State) -> Self {
        Self::State(state)
    }

    /// Create a log message
    pub fn log(level: LogLevel, message: impl Into<String>) -> Self {
        Self::Log {
            level,
            message: message.into(),
        }
    }

    /// Create an info log message
    pub fn info(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Info, message)
    }

    /// Create a warning log message
    pub fn warn(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Warn, message)
    }

    /// Create an error log message
    pub fn error(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Error, message)
    }
}

// ============================================================================
// Connector Trait
// ============================================================================

/// Type alias for the message stream returned by read()
pub type MessageStream = Pin<Box<dyn Stream<Item = Result<Message>> + Send>>;

/// Core trait that all connectors implement
#[async_trait]
pub trait Connector: Send + Sync {
    /// Returns the connector specification (for UI/validation)
    fn spec(&self) -> ConnectorSpec;

    /// Tests if credentials and configuration are valid
    async fn check(&self, config: &Value) -> Result<CheckResult>;

    /// Lists available streams from the source
    async fn discover(&self, config: &Value) -> Result<Catalog>;

    /// Reads data from selected streams
    ///
    /// Returns a stream of messages (records, state checkpoints, logs)
    async fn read(
        &self,
        config: &Value,
        catalog: &ConfiguredCatalog,
        state: Option<&State>,
    ) -> Result<MessageStream>;
}

// ============================================================================
// YAML Connector
// ============================================================================

/// A connector loaded from a YAML definition
pub struct YamlConnector {
    /// The parsed configuration
    pub config: ConnectorConfig,
}

impl YamlConnector {
    /// Create a new YAML connector from a configuration
    pub fn new(config: ConnectorConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Connector for YamlConnector {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec {
            name: self.config.metadata.name.clone(),
            title: self
                .config
                .metadata
                .title
                .clone()
                .unwrap_or_else(|| self.config.metadata.name.clone()),
            description: self.config.metadata.description.clone(),
            spec: self.config.spec.clone(),
            icon: self.config.metadata.icon.clone(),
        }
    }

    async fn check(&self, _config: &Value) -> Result<CheckResult> {
        // Will be fully implemented with HTTP client
        Ok(CheckResult::success())
    }

    async fn discover(&self, _config: &Value) -> Result<Catalog> {
        // Will be fully implemented with engine
        use crate::config::CatalogStream;
        use crate::types::SyncMode;

        let streams = self
            .config
            .streams
            .iter()
            .map(|s| CatalogStream {
                name: s.name.clone(),
                json_schema: Value::Object(serde_json::Map::new()),
                supported_sync_modes: if s.incremental.is_some() {
                    vec![SyncMode::FullRefresh, SyncMode::Incremental]
                } else {
                    vec![SyncMode::FullRefresh]
                },
                default_cursor_field: s.cursor_field.clone().map(|f| vec![f]),
                source_defined_primary_key: if s.primary_key.is_empty() {
                    None
                } else {
                    Some(s.primary_key.iter().map(|k| vec![k.clone()]).collect())
                },
            })
            .collect();

        Ok(Catalog { streams })
    }

    async fn read(
        &self,
        _config: &Value,
        _catalog: &ConfiguredCatalog,
        _state: Option<&State>,
    ) -> Result<MessageStream> {
        // Will be fully implemented with engine
        Ok(Box::pin(futures::stream::empty()))
    }
}

// ============================================================================
// Loader Functions
// ============================================================================

/// Load a connector from a YAML file
pub fn load_connector<P: AsRef<Path>>(path: P) -> Result<YamlConnector> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path).map_err(Error::Io)?;
    load_connector_from_str(&content)
}

/// Load a connector from a YAML string
pub fn load_connector_from_str(yaml: &str) -> Result<YamlConnector> {
    let config: ConnectorConfig = serde_yaml::from_str(yaml)?;
    Ok(YamlConnector::new(config))
}

/// Validate a connector configuration without loading
pub fn validate_connector<P: AsRef<Path>>(path: P) -> Result<()> {
    let _ = load_connector(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_result_success() {
        let result = CheckResult::success();
        assert!(result.success);
        assert!(result.message.is_none());
    }

    #[test]
    fn test_check_result_failure() {
        let result = CheckResult::failure("Connection failed");
        assert!(!result.success);
        assert_eq!(result.message, Some("Connection failed".to_string()));
    }

    #[test]
    fn test_load_connector_from_str() {
        let yaml = r#"
kind: connector
version: "1.0"
metadata:
  name: test
  title: "Test Connector"
spec:
  properties:
    api_key:
      type: string
      secret: true
      required: true
base_url: "https://api.example.com"
streams:
  - name: users
    endpoint: "/users"
    record_path: "$.data[*]"
    primary_key: [id]
"#;

        let connector = load_connector_from_str(yaml).unwrap();
        let spec = connector.spec();

        assert_eq!(spec.name, "test");
        assert_eq!(spec.title, "Test Connector");
        assert!(spec.spec.properties.contains_key("api_key"));
    }

    #[tokio::test]
    async fn test_discover() {
        let yaml = r#"
kind: connector
version: "1.0"
metadata:
  name: test
spec:
  properties: {}
base_url: "https://api.example.com"
streams:
  - name: customers
    endpoint: "/customers"
    record_path: "$.data[*]"
    primary_key: [id]
    incremental:
      cursor_field: "updated_at"
      cursor_param: "updated_since"
  - name: orders
    endpoint: "/orders"
    record_path: "$.data[*]"
"#;

        let connector = load_connector_from_str(yaml).unwrap();
        let catalog = connector.discover(&Value::Null).await.unwrap();

        assert_eq!(catalog.streams.len(), 2);

        let customers = &catalog.streams[0];
        assert_eq!(customers.name, "customers");
        assert_eq!(customers.supported_sync_modes.len(), 2);

        let orders = &catalog.streams[1];
        assert_eq!(orders.name, "orders");
        assert_eq!(orders.supported_sync_modes.len(), 1);
    }
}
