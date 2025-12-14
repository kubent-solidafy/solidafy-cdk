//! Engine types
//!
//! Message types and configuration for the sync engine.

use arrow::record_batch::RecordBatch;
use serde_json::Value;

/// A message emitted during sync
#[derive(Debug, Clone)]
pub enum Message {
    /// A batch of records
    Record {
        /// Stream name
        stream: String,
        /// The record batch
        batch: RecordBatch,
    },
    /// State update
    State {
        /// Stream name
        stream: String,
        /// State data (cursor, partition info, etc.)
        data: Value,
    },
    /// Log message
    Log {
        /// Log level
        level: LogLevel,
        /// Log message
        message: String,
    },
}

/// Log level for engine messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Debug information
    Debug,
    /// General information
    Info,
    /// Warning
    Warn,
    /// Error (non-fatal)
    Error,
}

impl Message {
    /// Create a record message
    pub fn record(stream: impl Into<String>, batch: RecordBatch) -> Self {
        Self::Record {
            stream: stream.into(),
            batch,
        }
    }

    /// Create a state message
    pub fn state(stream: impl Into<String>, data: Value) -> Self {
        Self::State {
            stream: stream.into(),
            data,
        }
    }

    /// Create a log message
    pub fn log(level: LogLevel, message: impl Into<String>) -> Self {
        Self::Log {
            level,
            message: message.into(),
        }
    }

    /// Create an info log
    pub fn info(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Info, message)
    }

    /// Create a debug log
    pub fn debug(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Debug, message)
    }

    /// Create a warning log
    pub fn warn(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Warn, message)
    }

    /// Create an error log
    pub fn error(message: impl Into<String>) -> Self {
        Self::log(LogLevel::Error, message)
    }

    /// Check if this is a record message
    pub fn is_record(&self) -> bool {
        matches!(self, Self::Record { .. })
    }

    /// Check if this is a state message
    pub fn is_state(&self) -> bool {
        matches!(self, Self::State { .. })
    }

    /// Check if this is a log message
    pub fn is_log(&self) -> bool {
        matches!(self, Self::Log { .. })
    }
}

/// Configuration for sync operation
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Batch size for records
    pub batch_size: usize,
    /// Whether to emit state after each page
    pub emit_state_per_page: bool,
    /// Maximum records to sync (0 = unlimited)
    pub max_records: usize,
    /// Whether to fail fast on errors
    pub fail_fast: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            emit_state_per_page: false,
            max_records: 0,
            fail_fast: true,
        }
    }
}

impl SyncConfig {
    /// Create a new sync config
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set batch size
    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Emit state after each page
    #[must_use]
    pub fn with_state_per_page(mut self, emit: bool) -> Self {
        self.emit_state_per_page = emit;
        self
    }

    /// Set max records
    #[must_use]
    pub fn with_max_records(mut self, max: usize) -> Self {
        self.max_records = max;
        self
    }

    /// Set fail fast mode
    #[must_use]
    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }
}

/// Statistics from a sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Total records synced
    pub records_synced: usize,
    /// Total pages fetched
    pub pages_fetched: usize,
    /// Total streams synced
    pub streams_synced: usize,
    /// Total partitions synced
    pub partitions_synced: usize,
    /// Errors encountered
    pub errors: usize,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

impl SyncStats {
    /// Create new stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Add records
    pub fn add_records(&mut self, count: usize) {
        self.records_synced += count;
    }

    /// Add a page
    pub fn add_page(&mut self) {
        self.pages_fetched += 1;
    }

    /// Add a stream
    pub fn add_stream(&mut self) {
        self.streams_synced += 1;
    }

    /// Add a partition
    pub fn add_partition(&mut self) {
        self.partitions_synced += 1;
    }

    /// Add an error
    pub fn add_error(&mut self) {
        self.errors += 1;
    }

    /// Set duration
    pub fn set_duration(&mut self, ms: u64) {
        self.duration_ms = ms;
    }
}
