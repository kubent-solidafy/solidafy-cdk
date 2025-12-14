//! State management module
//!
//! Handles cursor tracking, checkpointing, and resumability.
//! State is persisted between sync runs to enable incremental syncs.
//!
//! # Overview
//!
//! The state module provides:
//! - `State` - Core state structure with stream and partition tracking
//! - `StateManager` - File-based state persistence
//! - Checkpointing for resumable syncs

mod manager;
mod types;

pub use manager::StateManager;
pub use types::{PartitionState, State, StreamState};

#[cfg(test)]
mod manager_tests;
