// Allow common clippy pedantic lints that aren't critical for this codebase
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::ref_option)]
#![allow(clippy::unused_self)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::unnecessary_wraps)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::unused_async)]

//! # Solidafy Connector Development Kit (CDK)
//!
//! A minimal, Rust-native framework for building data source connectors.
//! Airbyte-equivalent capability, 10x simpler implementation.
//!
//! ## Features
//!
//! - **REST/HTTP API Extraction**: Connect to any REST API with YAML configuration
//! - **Multiple Auth Types**: API Key, OAuth2, JWT, Session, Basic, and more
//! - **Smart Pagination**: Cursor, offset, page number, link header support
//! - **Incremental Sync**: Track state and only fetch new data
//! - **Arrow Output**: Native Arrow RecordBatch output for efficient data processing
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use solidafy_cdk::{load_connector, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Load connector from YAML
//!     let connector = load_connector("connectors/stripe.yaml")?;
//!
//!     // Check connection
//!     let config = serde_json::json!({ "api_key": "sk_test_..." });
//!     let status = connector.check(&config).await?;
//!
//!     // Discover available streams
//!     let catalog = connector.discover(&config).await?;
//!
//!     // Read data
//!     let mut stream = connector.read(&config, &catalog, None).await?;
//!     while let Some(msg) = stream.next().await {
//!         // Process messages
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Connector Interface                         │
//! │  spec() → ConfigSpec    check() → Status    discover() → Catalog│
//! │  read(catalog, state) → Stream<RecordBatch>                     │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//! ┌──────────┬───────────┬───────┴───────┬───────────┬─────────────┐
//! │   Auth   │   HTTP    │   Paginate    │ Partition │   Output    │
//! ├──────────┼───────────┼───────────────┼───────────┼─────────────┤
//! │ API Key  │ GET/POST  │ Cursor        │ Parent    │ Arrow       │
//! │ OAuth2   │ Retry     │ Offset        │ DateTime  │ Parquet     │
//! │ JWT      │ Rate Limit│ Page Number   │ List      │             │
//! │ Session  │ Backoff   │ Link Header   │ Async Job │             │
//! └──────────┴───────────┴───────────────┴───────────┴─────────────┘
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]
#![allow(missing_docs)] // TODO: Add docs before 1.0 release

// ============================================================================
// Module declarations
// ============================================================================

/// Error types for the CDK
pub mod error;

/// Common types and type aliases
pub mod types;

/// Authentication implementations
pub mod auth;

/// HTTP client with retry and rate limiting
pub mod http;

/// Pagination strategies
pub mod pagination;

/// Partition routing
pub mod partition;

/// Response decoders (JSON, CSV, XML)
pub mod decode;

/// State management and checkpointing
pub mod state;

/// Arrow/Parquet output
pub mod output;

/// Main execution engine
pub mod engine;

/// Configuration and connector definitions
pub mod config;

/// Connector trait and YAML loader
pub mod connector;

/// YAML loader for connector definitions
pub mod loader;

/// Template interpolation
pub mod template;

/// Command-line interface
pub mod cli;

/// Schema inference from JSON data
pub mod schema;

/// Built-in connector definitions
pub mod connectors;

/// Database connector support via DuckDB
pub mod database;

// ============================================================================
// Re-exports
// ============================================================================

pub use error::{Error, Result};
pub use types::*;

// Re-export commonly used types
pub use loader::{load_connector, load_connector_from_str, ConnectorDefinition};

/// Crate version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Crate name
pub const NAME: &str = env!("CARGO_PKG_NAME");
