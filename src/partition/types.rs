//! Partition types and traits
//!
//! Defines the core partition abstractions.

use crate::error::Result;
use serde_json::Value;
use std::collections::HashMap;

/// A single partition value
#[derive(Debug, Clone)]
pub struct PartitionValue {
    /// Unique identifier for this partition
    pub id: String,
    /// Values to inject into templates/queries
    pub values: HashMap<String, Value>,
}

impl PartitionValue {
    /// Create a new partition value
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            values: HashMap::new(),
        }
    }

    /// Add a value to the partition
    #[must_use]
    pub fn with_value(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(key.into(), value.into());
        self
    }

    /// Add a string value
    #[must_use]
    pub fn with_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.values.insert(key.into(), Value::String(value.into()));
        self
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    /// Get a string value by key
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.values.get(key).and_then(Value::as_str)
    }
}

/// A partition definition
#[derive(Debug, Clone)]
pub struct Partition {
    /// Partition identifier
    pub id: String,
    /// Key-value pairs for this partition
    pub values: HashMap<String, Value>,
    /// Whether this partition is completed
    pub completed: bool,
}

impl Partition {
    /// Create a new partition
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            values: HashMap::new(),
            completed: false,
        }
    }

    /// Create from a partition value
    pub fn from_value(value: PartitionValue) -> Self {
        Self {
            id: value.id,
            values: value.values,
            completed: false,
        }
    }

    /// Add a value
    #[must_use]
    pub fn with_value(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.values.insert(key.into(), value.into());
        self
    }

    /// Mark as completed
    pub fn mark_completed(&mut self) {
        self.completed = true;
    }

    /// Get a value
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }
}

/// Configuration for partition routing
#[derive(Debug, Clone, Default)]
pub enum PartitionConfig {
    /// No partitioning
    #[default]
    None,

    /// Partition based on parent stream records
    Parent {
        /// Name of the parent stream
        parent_stream: String,
        /// Field to extract from parent records
        parent_key: String,
        /// Field name to use in partitions
        partition_field: String,
    },

    /// Partition based on a static list
    List {
        /// List of values
        values: Vec<String>,
        /// Field name to use in partitions
        partition_field: String,
    },

    /// Partition based on datetime ranges
    Datetime {
        /// Start datetime (template or value)
        start: String,
        /// End datetime (template or value)
        end: String,
        /// Step duration (e.g., "1d", "1h", "30m")
        step: String,
        /// Format string for datetime
        format: String,
        /// Parameter name for start
        start_param: String,
        /// Parameter name for end
        end_param: String,
    },

    /// Partition based on async job
    AsyncJob {
        /// Job creation config
        create_endpoint: String,
        /// Job poll config
        poll_endpoint: String,
        /// Job ID extraction path
        job_id_path: String,
        /// Completed status check path
        completed_path: String,
        /// Completed status value
        completed_value: Value,
    },
}

impl PartitionConfig {
    /// Create parent-based partition config
    pub fn parent(
        parent_stream: impl Into<String>,
        parent_key: impl Into<String>,
        partition_field: impl Into<String>,
    ) -> Self {
        Self::Parent {
            parent_stream: parent_stream.into(),
            parent_key: parent_key.into(),
            partition_field: partition_field.into(),
        }
    }

    /// Create list-based partition config
    pub fn list(values: Vec<String>, partition_field: impl Into<String>) -> Self {
        Self::List {
            values,
            partition_field: partition_field.into(),
        }
    }

    /// Create datetime-based partition config
    #[allow(clippy::too_many_arguments)]
    pub fn datetime(
        start: impl Into<String>,
        end: impl Into<String>,
        step: impl Into<String>,
        format: impl Into<String>,
        start_param: impl Into<String>,
        end_param: impl Into<String>,
    ) -> Self {
        Self::Datetime {
            start: start.into(),
            end: end.into(),
            step: step.into(),
            format: format.into(),
            start_param: start_param.into(),
            end_param: end_param.into(),
        }
    }
}

/// Trait for partition routers
pub trait PartitionRouter: Send + Sync {
    /// Generate partition values
    fn partitions(&self) -> Result<Vec<PartitionValue>>;

    /// Get the partition field name (for template interpolation)
    fn partition_field(&self) -> &str;
}
