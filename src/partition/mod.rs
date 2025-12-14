//! Partition routing module
//!
//! Supports: Parent stream, List, DateTime ranges, Async jobs
//!
//! # Overview
//!
//! Partitions allow splitting a stream into multiple parallel or sequential
//! sub-queries. This is useful for:
//! - Child resources that require a parent ID
//! - Date range slicing for large datasets
//! - Static list of values (e.g., regions, accounts)
//! - Async job-based exports (e.g., Salesforce bulk API)

mod routers;
mod types;

pub use routers::{
    extract_json_path, AsyncJob, AsyncJobConfig, AsyncJobState, DatetimeRouter, ListRouter,
    ParentRouter,
};
pub use types::{Partition, PartitionConfig, PartitionRouter, PartitionValue};

#[cfg(test)]
mod tests;
