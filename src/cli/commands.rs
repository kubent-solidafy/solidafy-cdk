//! CLI commands and argument parsing

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Solidafy Connector Development Kit CLI
#[derive(Parser, Debug)]
#[command(name = "solidafy-cdk")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Connector definition file (YAML)
    #[arg(short, long, global = true)]
    pub connector: Option<PathBuf>,

    /// Configuration file (JSON)
    #[arg(short = 'C', long, global = true)]
    pub config: Option<PathBuf>,

    /// State file (JSON)
    #[arg(short, long, global = true)]
    pub state: Option<PathBuf>,

    /// Inline state JSON
    #[arg(long, global = true)]
    pub state_json: Option<String>,

    /// Output format
    #[arg(short, long, global = true, default_value = "json")]
    pub format: OutputFormat,

    /// Verbose output
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

/// CLI subcommands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Test connection to the API
    Check {
        /// Inline config JSON
        #[arg(long)]
        config_json: Option<String>,
    },

    /// Discover available streams
    Discover {
        /// Inline config JSON
        #[arg(long)]
        config_json: Option<String>,

        /// Sample records to infer schema (0 = no sampling, use static schema)
        #[arg(long, default_value = "0")]
        sample: usize,
    },

    /// Read data from streams
    Read {
        /// Streams to sync (comma-separated, empty = all)
        #[arg(long)]
        streams: Option<String>,

        /// Inline config JSON
        #[arg(long)]
        config_json: Option<String>,

        /// Output destination (local path or cloud URL)
        /// Supports: /path, s3://bucket/path, r2://bucket/path, gs://bucket/path, az://container/path
        #[arg(short, long)]
        output: Option<String>,

        /// Maximum records per stream
        #[arg(long)]
        max_records: Option<usize>,

        /// Emit state after each page
        #[arg(long)]
        state_per_page: bool,
    },

    /// Show connector specification
    Spec,

    /// Validate connector definition
    Validate,

    /// List available stream names (lightweight, no schemas)
    /// For database connectors, requires --config-json with connection string
    Streams {
        /// Inline config JSON (required for database connectors)
        #[arg(long)]
        config_json: Option<String>,
    },

    /// List built-in connectors
    List,

    /// Start HTTP server mode
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Directory containing connector YAML files
        #[arg(long, default_value = "connectors")]
        connectors_dir: std::path::PathBuf,
    },
}

/// Output format
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// JSON output (one message per line)
    Json,
    /// Human-readable output
    Pretty,
    /// Parquet files
    Parquet,
}
