//! Output module
//!
//! Handles Arrow RecordBatch creation and Parquet file writing.
//!
//! # Overview
//!
//! This module provides utilities for:
//! - Inferring Arrow schemas from JSON records
//! - Converting JSON to Arrow RecordBatches
//! - Writing Parquet files
//! - Cloud storage output (S3, R2, GCS, Azure)

mod cloud;
mod schema;
mod writer;

pub use cloud::{build_partitioned_dir, build_partitioned_path, CloudDestination};
pub use schema::{arrow_to_json, infer_schema, json_to_arrow, merge_schemas};
pub use writer::{
    write_batch_to_parquet, write_batches_to_parquet, ParquetWriter, ParquetWriterConfig,
};

#[cfg(test)]
mod tests;
