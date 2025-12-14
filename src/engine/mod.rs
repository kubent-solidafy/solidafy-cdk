//! Execution engine module
//!
//! Main read loop and stream orchestration.
//!
//! # Overview
//!
//! The engine module provides:
//! - `SyncEngine` - Orchestrates data sync with state management
//! - `SyncConfig` - Configuration for sync operations
//! - Message types for output (Record, State, Log)

mod types;

pub use types::{LogLevel, Message, SyncConfig, SyncStats};

use crate::decode::RecordDecoder;
use crate::error::Result;
use crate::http::{HttpClient, RequestConfig};
use crate::output::json_to_arrow;
use crate::pagination::{NextPage, PaginationState, Paginator};
use crate::partition::PartitionRouter;
use crate::state::StateManager;
use crate::template::{self, TemplateContext};
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::time::Instant;

/// Sync engine for orchestrating data extraction
pub struct SyncEngine {
    /// HTTP client
    client: HttpClient,
    /// State manager
    state: StateManager,
    /// Sync configuration
    config: SyncConfig,
    /// Statistics
    stats: SyncStats,
}

impl SyncEngine {
    /// Create a new sync engine
    pub fn new(client: HttpClient, state: StateManager) -> Self {
        Self {
            client,
            state,
            config: SyncConfig::default(),
            stats: SyncStats::default(),
        }
    }

    /// Set sync configuration
    #[must_use]
    pub fn with_config(mut self, config: SyncConfig) -> Self {
        self.config = config;
        self
    }

    /// Get the state manager
    pub fn state(&self) -> &StateManager {
        &self.state
    }

    /// Get mutable state manager
    pub fn state_mut(&mut self) -> &mut StateManager {
        &mut self.state
    }

    /// Get statistics
    pub fn stats(&self) -> &SyncStats {
        &self.stats
    }

    /// Sync a single stream without partitioning
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_stream(
        &mut self,
        stream_name: &str,
        url: &str,
        path: &str,
        query_params: &HashMap<String, String>,
        headers: &HashMap<String, String>,
        decoder: &dyn RecordDecoder,
        paginator: &dyn Paginator,
        context: &TemplateContext,
        cursor_field: Option<&str>,
    ) -> Result<Vec<Message>> {
        let start = Instant::now();
        let mut messages = Vec::new();

        messages.push(Message::info(format!(
            "Starting sync for stream: {stream_name}"
        )));

        let mut all_records = Vec::new();
        let mut page_count = 0;
        let mut pagination_state = PaginationState::new();

        loop {
            // Build URL with template
            let rendered_path = template::render(path, context)?;
            let full_url = format!("{}{}", url.trim_end_matches('/'), rendered_path);

            // Build request config with query params
            let mut req_config = RequestConfig::new();

            // Add template-rendered params
            for (key, value) in query_params {
                let rendered = template::render(value, context)?;
                if !rendered.is_empty() {
                    req_config = req_config.query(key, &rendered);
                }
            }

            // Add pagination params
            let pagination_params = paginator.initial_params(&pagination_state);
            for (key, value) in pagination_params {
                req_config = req_config.query(&key, &value);
            }

            // Add headers
            for (key, value) in headers {
                req_config = req_config.header(key, value);
            }

            // Make request
            let response = self.client.get_with_config(&full_url, req_config).await?;

            page_count += 1;
            self.stats.add_page();

            // Get response body as JSON
            let body_text = response.text().await.map_err(|e| {
                crate::error::Error::decode(format!("Failed to read response body: {e}"))
            })?;
            let response_json: serde_json::Value = serde_json::from_str(&body_text)?;

            // Decode records
            let records = decoder.decode(&body_text)?;
            let record_count = records.len();
            self.stats.add_records(record_count);

            messages.push(Message::debug(format!(
                "Page {page_count}: fetched {record_count} records"
            )));

            // Note: Don't call pagination_state.add_fetched here - process_response handles it
            all_records.extend(records.clone());

            // Check max records limit
            if self.config.max_records > 0 && all_records.len() >= self.config.max_records {
                all_records.truncate(self.config.max_records);
                // Correct the stats to reflect truncated count
                let overcounted = self.stats.records_synced - self.config.max_records;
                self.stats.records_synced -= overcounted;
                break;
            }

            // Process pagination (use empty HeaderMap for now)
            let empty_headers = HeaderMap::new();
            let next_page = paginator.process_response(
                &response_json,
                &empty_headers,
                record_count,
                &mut pagination_state,
            );

            match next_page {
                NextPage::Continue { .. } => {
                    // Continue to next page
                }
                NextPage::Done => {
                    break;
                }
            }

            // Emit state per page if configured
            if self.config.emit_state_per_page {
                if let Some(cursor) = &pagination_state.cursor {
                    self.state.set_cursor(stream_name, cursor.clone()).await?;
                    messages.push(Message::state(
                        stream_name,
                        serde_json::json!({ "cursor": cursor }),
                    ));
                }
            }

            // Emit record batch if we have enough records
            if all_records.len() >= self.config.batch_size {
                let batch_records: Vec<_> = all_records.drain(..self.config.batch_size).collect();
                let batch = json_to_arrow(&batch_records, None)?;
                messages.push(Message::record(stream_name, batch));
            }
        }

        // Emit remaining records
        if !all_records.is_empty() {
            let batch = json_to_arrow(&all_records, None)?;
            messages.push(Message::record(stream_name, batch));
        }

        // Extract and save cursor from records if cursor_field is specified
        if let Some(field) = cursor_field {
            if let Some(max_cursor) = self.extract_max_cursor(&all_records, field) {
                self.state
                    .set_cursor(stream_name, max_cursor.clone())
                    .await?;
                messages.push(Message::state(
                    stream_name,
                    serde_json::json!({ "cursor": max_cursor }),
                ));
            }
        }

        self.stats.add_stream();
        #[allow(clippy::cast_possible_truncation)]
        self.stats.set_duration(start.elapsed().as_millis() as u64);

        messages.push(Message::info(format!(
            "Completed sync for {stream_name}: {} records in {page_count} pages",
            self.stats.records_synced
        )));

        Ok(messages)
    }

    /// Extract the maximum cursor value from records
    fn extract_max_cursor(
        &self,
        records: &[serde_json::Value],
        cursor_field: &str,
    ) -> Option<String> {
        records
            .iter()
            .filter_map(|record| {
                // Support nested fields with dot notation (e.g., "data.timestamp")
                let mut current = record;
                for part in cursor_field.split('.') {
                    current = current.get(part)?;
                }
                // Convert to string for comparison
                match current {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                }
            })
            .max()
    }

    /// Sync a partitioned stream
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_partitioned_stream(
        &mut self,
        stream_name: &str,
        url: &str,
        path: &str,
        query_params: &HashMap<String, String>,
        headers: &HashMap<String, String>,
        decoder: &dyn RecordDecoder,
        paginator: &dyn Paginator,
        router: &dyn PartitionRouter,
        base_context: &TemplateContext,
    ) -> Result<Vec<Message>> {
        let start = Instant::now();
        let mut messages = Vec::new();

        messages.push(Message::info(format!(
            "Starting partitioned sync for stream: {stream_name}"
        )));

        // Get partitions
        let partitions = router.partitions()?;
        messages.push(Message::debug(format!(
            "Found {} partitions",
            partitions.len()
        )));

        for partition in partitions {
            // Check if partition is already completed
            if self
                .state
                .is_partition_completed(stream_name, &partition.id)
                .await
            {
                messages.push(Message::debug(format!(
                    "Skipping completed partition: {}",
                    partition.id
                )));
                continue;
            }

            messages.push(Message::debug(format!(
                "Processing partition: {}",
                partition.id
            )));

            // Build context with partition values
            let mut context = base_context.clone();
            let partition_json = serde_json::to_value(&partition.values).unwrap_or_default();
            context.set_partition(partition_json);

            // Sync this partition (no cursor tracking for partitioned streams yet)
            let partition_messages = self
                .sync_stream(
                    stream_name,
                    url,
                    path,
                    query_params,
                    headers,
                    decoder,
                    paginator,
                    &context,
                    None, // TODO: Add cursor_field support for partitioned streams
                )
                .await;

            match partition_messages {
                Ok(msgs) => {
                    messages.extend(msgs);
                    self.state
                        .mark_partition_completed(stream_name, &partition.id)
                        .await?;
                    self.stats.add_partition();
                }
                Err(e) => {
                    self.stats.add_error();
                    messages.push(Message::error(format!(
                        "Error in partition {}: {e}",
                        partition.id
                    )));
                    if self.config.fail_fast {
                        return Err(e);
                    }
                }
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        self.stats.set_duration(start.elapsed().as_millis() as u64);

        messages.push(Message::info(format!(
            "Completed partitioned sync for {stream_name}: {} partitions",
            self.stats.partitions_synced
        )));

        Ok(messages)
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = SyncStats::default();
    }
}

#[cfg(test)]
mod tests;
