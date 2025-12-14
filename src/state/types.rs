//! State types for tracking sync progress
//!
//! These types are serialized to JSON and persisted between runs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete state for a connector
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Per-stream state
    #[serde(default)]
    pub streams: HashMap<String, StreamState>,
}

impl State {
    /// Create a new empty state
    pub fn new() -> Self {
        Self::default()
    }

    /// Get state for a stream
    pub fn get_stream(&self, stream: &str) -> Option<&StreamState> {
        self.streams.get(stream)
    }

    /// Get mutable state for a stream, creating if needed
    pub fn get_stream_mut(&mut self, stream: &str) -> &mut StreamState {
        self.streams.entry(stream.to_string()).or_default()
    }

    /// Get cursor for a stream
    pub fn get_cursor(&self, stream: &str) -> Option<&str> {
        self.streams.get(stream)?.cursor.as_deref()
    }

    /// Set cursor for a stream
    pub fn set_cursor(&mut self, stream: &str, cursor: String) {
        self.get_stream_mut(stream).cursor = Some(cursor);
    }
}

/// State for a single stream
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamState {
    /// Current cursor value (for incremental sync)
    #[serde(default)]
    pub cursor: Option<String>,

    /// Per-partition state (for partitioned streams)
    #[serde(default)]
    pub partitions: HashMap<String, PartitionState>,
}

impl StreamState {
    /// Create a new empty stream state
    pub fn new() -> Self {
        Self::default()
    }

    /// Get partition state
    pub fn get_partition(&self, partition_id: &str) -> Option<&PartitionState> {
        self.partitions.get(partition_id)
    }

    /// Get mutable partition state, creating if needed
    pub fn get_partition_mut(&mut self, partition_id: &str) -> &mut PartitionState {
        self.partitions.entry(partition_id.to_string()).or_default()
    }

    /// Check if a partition is completed
    pub fn is_partition_completed(&self, partition_id: &str) -> bool {
        self.partitions
            .get(partition_id)
            .is_some_and(|p| p.completed)
    }

    /// Mark a partition as completed
    pub fn mark_partition_completed(&mut self, partition_id: &str) {
        self.get_partition_mut(partition_id).completed = true;
    }
}

/// State for a single partition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionState {
    /// Cursor value within this partition
    #[serde(default)]
    pub cursor: Option<String>,

    /// Whether this partition has been fully synced
    #[serde(default)]
    pub completed: bool,
}

impl PartitionState {
    /// Create a new empty partition state
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a completed partition state
    pub fn completed() -> Self {
        Self {
            cursor: None,
            completed: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_default() {
        let state = State::new();
        assert!(state.streams.is_empty());
    }

    #[test]
    fn test_state_cursor() {
        let mut state = State::new();
        assert!(state.get_cursor("users").is_none());

        state.set_cursor("users", "2024-01-01".to_string());
        assert_eq!(state.get_cursor("users"), Some("2024-01-01"));
    }

    #[test]
    fn test_stream_state_partitions() {
        let mut stream_state = StreamState::new();

        assert!(!stream_state.is_partition_completed("p1"));

        stream_state.mark_partition_completed("p1");
        assert!(stream_state.is_partition_completed("p1"));
        assert!(!stream_state.is_partition_completed("p2"));
    }

    #[test]
    fn test_state_serialization() {
        let mut state = State::new();
        state.set_cursor("users", "cursor123".to_string());
        state.get_stream_mut("users").mark_partition_completed("p1");

        let json = serde_json::to_string(&state).unwrap();
        let restored: State = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.get_cursor("users"), Some("cursor123"));
        assert!(restored
            .get_stream("users")
            .unwrap()
            .is_partition_completed("p1"));
    }
}
