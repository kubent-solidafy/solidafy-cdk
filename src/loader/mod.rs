//! YAML Loader module
//!
//! Parse connector definitions from YAML files.
//!
//! # Overview
//!
//! The loader module provides:
//! - `ConnectorDefinition` - Declarative connector specification
//! - `StreamDefinition` - Stream configuration
//! - YAML parsing with validation

mod parser;
mod types;

pub use parser::{load_connector, load_connector_from_str};
pub use types::{
    AsyncJobCreateDef, AsyncJobDownloadDef, AsyncJobPollDef, AuthDefinition, ConnectorDefinition,
    DatabaseConnectionDef, DatabaseConnectorDefinition, DatabaseEngine, DatabaseStreamDefinition,
    DecoderDefinition, HttpDefinition, PaginationDefinition, PartitionDefinition,
    RequestDefinition, StopConditionDefinition, StreamDefinition, UnifiedConnectorDefinition,
};

#[cfg(test)]
mod tests;
