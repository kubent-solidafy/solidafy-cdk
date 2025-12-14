//! Partition router implementations
//!
//! Each router handles a specific partitioning strategy.

use super::types::{PartitionRouter, PartitionValue};
use crate::error::{Error, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;

// ============================================================================
// List Router
// ============================================================================

/// List-based partition router
///
/// Creates partitions from a static list of values.
#[derive(Debug, Clone)]
pub struct ListRouter {
    /// List of partition values
    values: Vec<String>,
    /// Field name for partition
    partition_field: String,
}

impl ListRouter {
    /// Create a new list router
    pub fn new(values: Vec<String>, partition_field: impl Into<String>) -> Self {
        Self {
            values,
            partition_field: partition_field.into(),
        }
    }
}

impl PartitionRouter for ListRouter {
    fn partitions(&self) -> Result<Vec<PartitionValue>> {
        Ok(self
            .values
            .iter()
            .map(|v| {
                PartitionValue::new(v.clone()).with_string(self.partition_field.clone(), v.clone())
            })
            .collect())
    }

    fn partition_field(&self) -> &str {
        &self.partition_field
    }
}

// ============================================================================
// Parent Router
// ============================================================================

/// Parent stream-based partition router
///
/// Creates partitions from records in a parent stream.
#[derive(Debug, Clone)]
pub struct ParentRouter {
    /// Records from parent stream
    parent_records: Vec<Value>,
    /// Key to extract from parent records
    parent_key: String,
    /// Field name for partition
    partition_field: String,
}

impl ParentRouter {
    /// Create a new parent router
    pub fn new(
        parent_records: Vec<Value>,
        parent_key: impl Into<String>,
        partition_field: impl Into<String>,
    ) -> Self {
        Self {
            parent_records,
            parent_key: parent_key.into(),
            partition_field: partition_field.into(),
        }
    }

    /// Create an empty parent router (for deferred loading)
    pub fn empty(parent_key: impl Into<String>, partition_field: impl Into<String>) -> Self {
        Self {
            parent_records: Vec::new(),
            parent_key: parent_key.into(),
            partition_field: partition_field.into(),
        }
    }

    /// Set parent records
    pub fn set_records(&mut self, records: Vec<Value>) {
        self.parent_records = records;
    }

    /// Extract value from a record using the parent key
    fn extract_key(&self, record: &Value) -> Option<String> {
        // Handle nested keys like "id" or "data.id"
        let parts: Vec<&str> = self.parent_key.split('.').collect();
        let mut current = record;

        for part in parts {
            current = current.get(part)?;
        }

        match current {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

impl PartitionRouter for ParentRouter {
    fn partitions(&self) -> Result<Vec<PartitionValue>> {
        let mut partitions = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for record in &self.parent_records {
            if let Some(key_value) = self.extract_key(record) {
                // Deduplicate
                if seen.insert(key_value.clone()) {
                    partitions.push(
                        PartitionValue::new(&key_value)
                            .with_string(self.partition_field.clone(), &key_value),
                    );
                }
            }
        }

        Ok(partitions)
    }

    fn partition_field(&self) -> &str {
        &self.partition_field
    }
}

// ============================================================================
// Datetime Router
// ============================================================================

/// Datetime-based partition router
///
/// Creates partitions from datetime ranges.
#[derive(Debug, Clone)]
pub struct DatetimeRouter {
    /// Start datetime
    start: DateTime<Utc>,
    /// End datetime
    end: DateTime<Utc>,
    /// Step duration
    step: Duration,
    /// Format string for datetime output
    format: String,
    /// Parameter name for start
    start_param: String,
    /// Parameter name for end
    end_param: String,
}

impl DatetimeRouter {
    /// Create a new datetime router
    pub fn new(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: Duration,
        format: impl Into<String>,
        start_param: impl Into<String>,
        end_param: impl Into<String>,
    ) -> Self {
        Self {
            start,
            end,
            step,
            format: format.into(),
            start_param: start_param.into(),
            end_param: end_param.into(),
        }
    }

    /// Create from string values
    pub fn from_strings(
        start: &str,
        end: &str,
        step: &str,
        format: impl Into<String>,
        start_param: impl Into<String>,
        end_param: impl Into<String>,
    ) -> Result<Self> {
        let start_dt = parse_datetime(start)?;
        let end_dt = if end == "now" || end == "{{ now }}" {
            Utc::now()
        } else {
            parse_datetime(end)?
        };
        let step_dur = parse_duration(step)?;

        Ok(Self::new(
            start_dt,
            end_dt,
            step_dur,
            format,
            start_param,
            end_param,
        ))
    }

    /// Format a datetime using the configured format
    fn format_datetime(&self, dt: DateTime<Utc>) -> String {
        dt.format(&self.format).to_string()
    }
}

impl PartitionRouter for DatetimeRouter {
    fn partitions(&self) -> Result<Vec<PartitionValue>> {
        let mut partitions = Vec::new();
        let mut current = self.start;
        let mut partition_num = 0;

        while current < self.end {
            let next = current + self.step;
            let partition_end = if next > self.end { self.end } else { next };

            let start_str = self.format_datetime(current);
            let end_str = self.format_datetime(partition_end);

            let id = format!("{partition_num}_{start_str}");

            let mut values = HashMap::new();
            values.insert(self.start_param.clone(), Value::String(start_str.clone()));
            values.insert(self.end_param.clone(), Value::String(end_str.clone()));
            values.insert("partition_start".to_string(), Value::String(start_str));
            values.insert("partition_end".to_string(), Value::String(end_str));

            partitions.push(PartitionValue { id, values });

            current = next;
            partition_num += 1;
        }

        Ok(partitions)
    }

    fn partition_field(&self) -> &str {
        &self.start_param
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse a datetime string into UTC DateTime
fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    // Try RFC 3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try common formats
    let formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ];

    for fmt in formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(DateTime::from_naive_utc_and_offset(ndt, Utc));
        }
        if let Ok(nd) = NaiveDate::parse_from_str(s, fmt) {
            let ndt = nd.and_hms_opt(0, 0, 0).unwrap();
            return Ok(DateTime::from_naive_utc_and_offset(ndt, Utc));
        }
    }

    Err(Error::config(format!("Invalid datetime format: {s}")))
}

/// Parse a duration string like "1d", "2h", "30m"
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();

    // Try to parse as number with suffix using strip_suffix
    let (num_str, suffix) = if let Some(stripped) = s.strip_suffix('d') {
        (stripped, 'd')
    } else if let Some(stripped) = s.strip_suffix('h') {
        (stripped, 'h')
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, 'm')
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, 's')
    } else if let Some(stripped) = s.strip_suffix('w') {
        (stripped, 'w')
    } else {
        // Assume days if no suffix
        (s, 'd')
    };

    let num: i64 = num_str
        .parse()
        .map_err(|_| Error::config(format!("Invalid duration number: {num_str}")))?;

    let duration = match suffix {
        'w' => Duration::weeks(num),
        'd' => Duration::days(num),
        'h' => Duration::hours(num),
        'm' => Duration::minutes(num),
        's' => Duration::seconds(num),
        _ => return Err(Error::config(format!("Invalid duration suffix: {suffix}"))),
    };

    Ok(duration)
}

// ============================================================================
// Async Job Router
// ============================================================================

/// Configuration for async job-based data extraction
///
/// Used for APIs like Salesforce Bulk API, BigQuery, etc. that follow
/// a create → poll → download pattern.
#[derive(Debug, Clone)]
pub struct AsyncJobConfig {
    /// HTTP method for job creation (POST, PUT)
    pub create_method: String,
    /// Endpoint path for job creation (can include templates)
    pub create_path: String,
    /// Request body for job creation (JSON template)
    pub create_body: Option<String>,
    /// JSONPath to extract job ID from creation response
    pub job_id_path: String,

    /// Endpoint path for polling job status (must include {{ job_id }})
    pub poll_path: String,
    /// Polling interval in seconds
    pub poll_interval_secs: u64,
    /// Maximum poll attempts before timeout
    pub poll_max_attempts: u64,
    /// JSONPath to check job status
    pub status_path: String,
    /// Value that indicates job completion
    pub completed_value: String,
    /// Optional: Values that indicate job failure (will error)
    pub failed_values: Vec<String>,

    /// Endpoint path for downloading results (must include {{ job_id }})
    pub download_path: String,
    /// JSONPath to extract records from download response
    pub records_path: Option<String>,
}

impl Default for AsyncJobConfig {
    fn default() -> Self {
        Self {
            create_method: "POST".to_string(),
            create_path: String::new(),
            create_body: None,
            job_id_path: "id".to_string(),
            poll_path: String::new(),
            poll_interval_secs: 5,
            poll_max_attempts: 120, // 10 minutes at 5 second intervals
            status_path: "state".to_string(),
            completed_value: "JobComplete".to_string(),
            failed_values: vec!["Failed".to_string(), "Aborted".to_string()],
            download_path: String::new(),
            records_path: None,
        }
    }
}

impl AsyncJobConfig {
    /// Create a new async job config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set job creation config
    #[must_use]
    pub fn with_create(mut self, method: &str, path: &str, body: Option<&str>) -> Self {
        self.create_method = method.to_string();
        self.create_path = path.to_string();
        self.create_body = body.map(String::from);
        self
    }

    /// Set job ID extraction path
    #[must_use]
    pub fn with_job_id_path(mut self, path: &str) -> Self {
        self.job_id_path = path.to_string();
        self
    }

    /// Set polling config
    #[must_use]
    pub fn with_poll(mut self, path: &str, interval_secs: u64, max_attempts: u64) -> Self {
        self.poll_path = path.to_string();
        self.poll_interval_secs = interval_secs;
        self.poll_max_attempts = max_attempts;
        self
    }

    /// Set status checking config
    #[must_use]
    pub fn with_status(mut self, path: &str, completed: &str, failed: Vec<&str>) -> Self {
        self.status_path = path.to_string();
        self.completed_value = completed.to_string();
        self.failed_values = failed.into_iter().map(String::from).collect();
        self
    }

    /// Set download config
    #[must_use]
    pub fn with_download(mut self, path: &str, records_path: Option<&str>) -> Self {
        self.download_path = path.to_string();
        self.records_path = records_path.map(String::from);
        self
    }
}

/// State of an async job
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncJobState {
    /// Job created, waiting to start
    Created,
    /// Job is running/in progress
    InProgress,
    /// Job completed successfully
    Completed,
    /// Job failed
    Failed(String),
    /// Job status unknown
    Unknown(String),
}

impl AsyncJobState {
    /// Check if job is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed(_))
    }

    /// Check if job completed successfully
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Check if job failed
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

/// An async job instance
#[derive(Debug, Clone)]
pub struct AsyncJob {
    /// Job ID from the API
    pub id: String,
    /// Current state
    pub state: AsyncJobState,
    /// Full job response (for extracting additional fields)
    pub response: Value,
}

impl AsyncJob {
    /// Create a new async job
    pub fn new(id: impl Into<String>, response: Value) -> Self {
        Self {
            id: id.into(),
            state: AsyncJobState::Created,
            response,
        }
    }

    /// Update job state from poll response
    pub fn update_state(&mut self, config: &AsyncJobConfig, response: &Value) {
        self.response = response.clone();

        // Extract status value
        let status = extract_json_path(response, &config.status_path)
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();

        // Check for completion
        if status == config.completed_value {
            self.state = AsyncJobState::Completed;
        } else if config.failed_values.contains(&status) {
            self.state = AsyncJobState::Failed(status);
        } else if status.is_empty() {
            self.state = AsyncJobState::Unknown("No status found".to_string());
        } else {
            self.state = AsyncJobState::InProgress;
        }
    }
}

/// Extract a value from JSON using a simple path (e.g., "data.id", "state")
pub fn extract_json_path(value: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let parts: Vec<&str> = path.split('.').collect();

    let mut current = value;
    for part in parts {
        // Handle array index like "records[0]"
        if let Some(bracket_pos) = part.find('[') {
            let key = &part[..bracket_pos];
            let idx_str = &part[bracket_pos + 1..part.len() - 1];

            current = current.get(key)?;

            if let Ok(idx) = idx_str.parse::<usize>() {
                current = current.get(idx)?;
            } else {
                return None;
            }
        } else {
            current = current.get(part)?;
        }
    }

    Some(current.clone())
}
