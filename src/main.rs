// Allow common clippy pedantic lints
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

//! Solidafy CDK CLI
//!
//! Command-line interface for running connectors

use clap::Parser;
use solidafy_cdk::cli::{Cli, Runner};

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();
    let runner = Runner::new(cli);

    if let Err(e) = runner.run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
