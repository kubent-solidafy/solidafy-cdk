//! Schema inference module
//!
//! Provides automatic JSON schema inference from API responses.
//!
//! # Features
//!
//! - **Type Inference**: Infers types from JSON values
//! - **Schema Merging**: Merges schemas from multiple records
//! - **Nullable Detection**: Detects nullable fields
//! - **Array Type Inference**: Infers array item types
//! - **Nested Object Support**: Handles nested objects recursively

mod inference;
mod types;

pub use inference::{infer_schema, merge_schemas, SchemaInferrer};
pub use types::{JsonSchema, JsonType, SchemaProperty};

#[cfg(test)]
mod tests;
