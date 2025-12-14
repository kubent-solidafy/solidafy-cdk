//! Database connector support via DuckDB
//!
//! This module provides database connectivity using DuckDB as the query engine.
//! DuckDB can connect to PostgreSQL, MySQL, SQLite, and other databases.

mod engine;

pub use engine::{DatabaseEngine, DatabaseSyncResult};

// Alias for backward compat with tests
pub use engine::DatabaseEngine as DbEngine;
