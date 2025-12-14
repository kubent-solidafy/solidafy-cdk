//! CLI module
//!
//! Command-line interface for running connectors.
//!
//! # Commands
//!
//! - `check` - Test connection to the API
//! - `discover` - List available streams
//! - `read` - Extract data from streams
//! - `streams` - List stream names (lightweight)
//! - `serve` - Start HTTP server mode

mod commands;
mod runner;
mod server;

pub use commands::{Cli, Commands};
pub use runner::Runner;
pub use server::{serve, ServerConfig};
