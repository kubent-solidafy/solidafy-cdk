//! DuckDB-based database query engine
//!
//! Provides unified access to PostgreSQL, MySQL, SQLite via DuckDB extensions.
//! DuckDB can write directly to Parquet/JSON at any location (local or cloud).

use crate::error::{Error, Result};
use crate::loader::{DatabaseConnectionDef, DatabaseEngine as DbType, DatabaseStreamDefinition};
use crate::template::{self, TemplateContext};
use duckdb::Connection;
use serde_json::Value;

/// Database query engine using DuckDB
pub struct DatabaseEngine {
    /// DuckDB connection
    conn: Connection,
    /// Database type
    db_type: DbType,
    /// Connection string used (for logging)
    connection_string: String,
}

/// Result of syncing a database stream
#[derive(Debug)]
pub struct DatabaseSyncResult {
    /// Stream name
    pub stream: String,
    /// Number of records synced
    pub record_count: usize,
    /// New cursor value (if incremental)
    pub cursor_value: Option<String>,
    /// Output path where data was written
    pub output_path: Option<String>,
    /// Records as JSON (if format is json and no output path)
    pub records: Option<Vec<Value>>,
}

impl DatabaseEngine {
    /// Create a new database engine
    pub fn new(
        db_type: DbType,
        connection: &DatabaseConnectionDef,
        context: &TemplateContext,
    ) -> Result<Self> {
        // Create in-memory DuckDB connection
        let conn = Connection::open_in_memory()
            .map_err(|e| Error::config(format!("Failed to create DuckDB connection: {e}")))?;

        // Build connection string
        let connection_string =
            Self::build_connection_string(db_type.clone(), connection, context)?;

        // Load appropriate extension and attach database
        let engine = Self {
            conn,
            db_type: db_type.clone(),
            connection_string: connection_string.clone(),
        };

        engine.attach_database(&connection_string)?;

        Ok(engine)
    }

    /// Build connection string from config
    fn build_connection_string(
        db_type: DbType,
        connection: &DatabaseConnectionDef,
        context: &TemplateContext,
    ) -> Result<String> {
        // If connection_string is provided, use it directly
        if let Some(ref conn_str) = connection.connection_string {
            return template::render(conn_str, context);
        }

        // Otherwise build from components
        let host = connection
            .host
            .as_ref()
            .map(|h| template::render(h, context))
            .transpose()?
            .unwrap_or_else(|| "localhost".to_string());

        let user = connection
            .user
            .as_ref()
            .map(|u| template::render(u, context))
            .transpose()?
            .unwrap_or_else(|| "postgres".to_string());

        let password = connection
            .password
            .as_ref()
            .map(|p| template::render(p, context))
            .transpose()?
            .unwrap_or_default();

        let database = connection
            .database
            .as_ref()
            .map(|d| template::render(d, context))
            .transpose()?
            .unwrap_or_else(|| "postgres".to_string());

        let port = connection.port.unwrap_or(match db_type {
            DbType::Postgres => 5432,
            DbType::Mysql => 3306,
            DbType::Sqlite | DbType::Duckdb => 0,
        });

        match db_type {
            DbType::Postgres => Ok(format!(
                "postgresql://{user}:{password}@{host}:{port}/{database}"
            )),
            DbType::Mysql => Ok(format!(
                "mysql://{user}:{password}@{host}:{port}/{database}"
            )),
            DbType::Sqlite => {
                // SQLite uses database as file path
                Ok(database)
            }
            DbType::Duckdb => {
                // DuckDB uses database as file path or :memory:
                Ok(database)
            }
        }
    }

    /// Attach external database to DuckDB
    fn attach_database(&self, connection_string: &str) -> Result<()> {
        match self.db_type {
            DbType::Postgres => {
                // Install and load postgres extension
                self.conn
                    .execute_batch("INSTALL postgres; LOAD postgres;")
                    .map_err(|e| {
                        Error::config(format!("Failed to load postgres extension: {e}"))
                    })?;

                // Attach PostgreSQL database
                let attach_sql = format!(
                    "ATTACH '{connection_string}' AS source_db (TYPE POSTGRES, READ_ONLY);"
                );
                self.conn
                    .execute_batch(&attach_sql)
                    .map_err(|e| Error::config(format!("Failed to attach PostgreSQL: {e}")))?;
            }
            DbType::Mysql => {
                // Install and load mysql extension
                self.conn
                    .execute_batch("INSTALL mysql; LOAD mysql;")
                    .map_err(|e| Error::config(format!("Failed to load mysql extension: {e}")))?;

                // Attach MySQL database
                let attach_sql =
                    format!("ATTACH '{connection_string}' AS source_db (TYPE MYSQL, READ_ONLY);");
                self.conn
                    .execute_batch(&attach_sql)
                    .map_err(|e| Error::config(format!("Failed to attach MySQL: {e}")))?;
            }
            DbType::Sqlite => {
                // Install and load sqlite extension
                self.conn
                    .execute_batch("INSTALL sqlite; LOAD sqlite;")
                    .map_err(|e| Error::config(format!("Failed to load sqlite extension: {e}")))?;

                // Attach SQLite database
                let attach_sql =
                    format!("ATTACH '{connection_string}' AS source_db (TYPE SQLITE, READ_ONLY);");
                self.conn
                    .execute_batch(&attach_sql)
                    .map_err(|e| Error::config(format!("Failed to attach SQLite: {e}")))?;
            }
            DbType::Duckdb => {
                // Native DuckDB - just attach directly
                if connection_string != ":memory:" {
                    let attach_sql =
                        format!("ATTACH '{connection_string}' AS source_db (READ_ONLY);");
                    self.conn
                        .execute_batch(&attach_sql)
                        .map_err(|e| Error::config(format!("Failed to attach DuckDB: {e}")))?;
                }
            }
        }

        Ok(())
    }

    /// Configure cloud storage credentials (S3, R2, GCS, Azure)
    pub fn configure_cloud_storage(&self) -> Result<()> {
        // Install and load httpfs for cloud storage
        self.conn
            .execute_batch("INSTALL httpfs; LOAD httpfs;")
            .map_err(|e| Error::config(format!("Failed to load httpfs extension: {e}")))?;

        // Configure S3 credentials from environment
        if let Ok(key_id) = std::env::var("AWS_ACCESS_KEY_ID") {
            if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                let region =
                    std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string());

                self.conn.execute_batch(&format!(
                    "SET s3_access_key_id = '{key_id}'; SET s3_secret_access_key = '{secret}'; SET s3_region = '{region}';"
                )).map_err(|e| Error::config(format!("Failed to configure S3: {e}")))?;

                // Check for custom endpoint (R2, MinIO, etc.)
                if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
                    self.conn
                        .execute_batch(&format!(
                            "SET s3_endpoint = '{}'; SET s3_url_style = 'path';",
                            endpoint
                                .trim_start_matches("https://")
                                .trim_start_matches("http://")
                        ))
                        .map_err(|e| {
                            Error::config(format!("Failed to configure S3 endpoint: {e}"))
                        })?;
                }
            }
        }

        // Configure GCS credentials
        if let Ok(service_account) = std::env::var("GOOGLE_SERVICE_ACCOUNT") {
            self.conn
                .execute_batch(&format!("SET gcs_credentials_file = '{service_account}';"))
                .map_err(|e| Error::config(format!("Failed to configure GCS: {e}")))?;
        }

        Ok(())
    }

    /// Test database connection
    pub fn check_connection(&self) -> Result<()> {
        let query = match self.db_type {
            DbType::Postgres => "SELECT 1 FROM source_db.pg_catalog.pg_tables LIMIT 1",
            DbType::Mysql => "SELECT 1 FROM source_db.information_schema.tables LIMIT 1",
            DbType::Sqlite => "SELECT 1 FROM source_db.sqlite_master LIMIT 1",
            DbType::Duckdb => "SELECT 1",
        };

        self.conn
            .execute(query, [])
            .map_err(|e| Error::config(format!("Connection check failed: {e}")))?;

        Ok(())
    }

    /// Get list of tables in the database
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let query = match self.db_type {
            DbType::Postgres => {
                "SELECT table_schema || '.' || table_name as full_name
                 FROM source_db.information_schema.tables
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY table_schema, table_name"
            }
            DbType::Mysql => {
                "SELECT CONCAT(table_schema, '.', table_name) as full_name
                 FROM source_db.information_schema.tables
                 WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                 ORDER BY table_schema, table_name"
            }
            DbType::Sqlite => {
                "SELECT name as full_name FROM source_db.sqlite_master WHERE type='table' ORDER BY name"
            }
            DbType::Duckdb => {
                "SELECT schema_name || '.' || table_name as full_name
                 FROM information_schema.tables
                 WHERE table_catalog = 'source_db'
                 ORDER BY schema_name, table_name"
            }
        };

        let mut stmt = self
            .conn
            .prepare(query)
            .map_err(|e| Error::config(format!("Failed to prepare query: {e}")))?;

        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| Error::config(format!("Failed to query tables: {e}")))?
            .filter_map(std::result::Result::ok)
            .collect();

        Ok(tables)
    }

    /// Sync a stream to Parquet file (local or cloud)
    pub fn sync_to_parquet(
        &self,
        stream: &DatabaseStreamDefinition,
        output_path: &str,
        cursor_value: Option<&str>,
    ) -> Result<DatabaseSyncResult> {
        let query = self.build_query(stream, cursor_value)?;

        tracing::debug!("Executing query: {}", query);

        // Use DuckDB's COPY TO to write directly to Parquet
        let copy_sql =
            format!("COPY ({query}) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');");

        self.conn
            .execute_batch(&copy_sql)
            .map_err(|e| Error::config(format!("Failed to write Parquet: {e}")))?;

        // Get row count and cursor value
        let (record_count, new_cursor) = self.get_sync_stats(stream, cursor_value)?;

        Ok(DatabaseSyncResult {
            stream: stream.name.clone(),
            record_count,
            cursor_value: new_cursor,
            output_path: Some(output_path.to_string()),
            records: None,
        })
    }

    /// Sync a stream to JSON (returns records in memory) with pagination
    pub fn sync_to_json(
        &self,
        stream: &DatabaseStreamDefinition,
        cursor_value: Option<&str>,
    ) -> Result<DatabaseSyncResult> {
        let batch_size = if stream.batch_size > 0 {
            stream.batch_size as usize
        } else {
            10_000
        };
        let mut all_records: Vec<Value> = Vec::new();
        let mut current_cursor = cursor_value.map(String::from);
        let mut total_count = 0usize;

        loop {
            // Build query for this batch
            let query =
                self.build_paginated_query(stream, current_cursor.as_deref(), batch_size)?;

            tracing::debug!("Executing batch query: {}", query);

            // Use DuckDB's native JSON export via a temp file
            let temp_file =
                std::env::temp_dir().join(format!("solidafy_json_{}.json", uuid_simple()));
            let temp_path = temp_file
                .to_str()
                .ok_or_else(|| Error::config("Invalid temp path"))?;

            let copy_sql = format!("COPY ({query}) TO '{temp_path}' (FORMAT JSON, ARRAY true);");

            self.conn
                .execute_batch(&copy_sql)
                .map_err(|e| Error::config(format!("Failed to export JSON: {e}")))?;

            let json_content = std::fs::read_to_string(&temp_file)
                .map_err(|e| Error::config(format!("Failed to read JSON file: {e}")))?;

            let _ = std::fs::remove_file(&temp_file);

            let batch_records: Vec<Value> = if json_content.trim().is_empty() {
                vec![]
            } else {
                serde_json::from_str(&json_content)
                    .map_err(|e| Error::config(format!("Failed to parse JSON: {e}")))?
            };

            let batch_count = batch_records.len();
            total_count += batch_count;

            // Update cursor from last record if cursor field is defined
            if let Some(cursor_field) = &stream.cursor_field {
                if let Some(last_record) = batch_records.last() {
                    if let Some(cursor_val) = last_record.get(cursor_field) {
                        current_cursor = match cursor_val {
                            Value::String(s) => Some(s.clone()),
                            Value::Number(n) => Some(n.to_string()),
                            _ => None,
                        };
                    }
                }
            }

            all_records.extend(batch_records);

            // Stop if we got fewer records than batch size (end of data)
            if batch_count < batch_size {
                break;
            }

            // Safety limit to prevent infinite loops
            if total_count >= 1_000_000 {
                tracing::warn!("Reached 1M record limit for stream {}", stream.name);
                break;
            }
        }

        Ok(DatabaseSyncResult {
            stream: stream.name.clone(),
            record_count: total_count,
            cursor_value: current_cursor,
            output_path: None,
            records: Some(all_records),
        })
    }

    /// Sync a stream to JSON file (local or cloud)
    pub fn sync_to_json_file(
        &self,
        stream: &DatabaseStreamDefinition,
        output_path: &str,
        cursor_value: Option<&str>,
    ) -> Result<DatabaseSyncResult> {
        let query = self.build_query(stream, cursor_value)?;

        tracing::debug!("Executing query: {}", query);

        // Use DuckDB's COPY TO to write directly to JSON
        let copy_sql = format!("COPY ({query}) TO '{output_path}' (FORMAT JSON, ARRAY true);");

        self.conn
            .execute_batch(&copy_sql)
            .map_err(|e| Error::config(format!("Failed to write JSON: {e}")))?;

        // Get row count and cursor value
        let (record_count, new_cursor) = self.get_sync_stats(stream, cursor_value)?;

        Ok(DatabaseSyncResult {
            stream: stream.name.clone(),
            record_count,
            cursor_value: new_cursor,
            output_path: Some(output_path.to_string()),
            records: None,
        })
    }

    /// Build base SQL query for a stream (without pagination)
    fn build_base_query(&self, stream: &DatabaseStreamDefinition) -> Result<String> {
        if let Some(ref query) = stream.query {
            // Custom query provided
            Ok(query.clone())
        } else if let Some(ref table) = stream.table {
            // Table name provided - add source_db prefix if needed
            let full_table = if table.contains('.') {
                format!("source_db.{table}")
            } else {
                format!("source_db.public.{table}") // Default to public schema for postgres
            };
            Ok(format!("SELECT * FROM {full_table}"))
        } else {
            Err(Error::config(format!(
                "Stream '{}' must have either 'table' or 'query' defined",
                stream.name
            )))
        }
    }

    /// Build SQL query for a stream with pagination support
    fn build_paginated_query(
        &self,
        stream: &DatabaseStreamDefinition,
        cursor_value: Option<&str>,
        batch_size: usize,
    ) -> Result<String> {
        let mut query = self.build_base_query(stream)?;

        // Add WHERE clause for cursor-based pagination (incremental sync)
        if let (Some(cursor_field), Some(cursor_val)) = (&stream.cursor_field, cursor_value) {
            if !cursor_val.is_empty() {
                let query_upper = query.to_uppercase();
                if query_upper.contains(" WHERE ") {
                    query = format!("{query} AND {cursor_field} > '{cursor_val}'");
                } else {
                    query = format!("{query} WHERE {cursor_field} > '{cursor_val}'");
                }
            }
        }

        // Add ORDER BY for cursor field (required for cursor-based pagination)
        if let Some(cursor_field) = &stream.cursor_field {
            let query_upper = query.to_uppercase();
            if !query_upper.contains(" ORDER BY ") {
                query = format!("{query} ORDER BY {cursor_field} ASC");
            }
        }

        // Add LIMIT for batch size
        query = format!("{query} LIMIT {batch_size}");

        Ok(query)
    }

    /// Build SQL query for full table export (no pagination - for Parquet)
    fn build_query(
        &self,
        stream: &DatabaseStreamDefinition,
        cursor_value: Option<&str>,
    ) -> Result<String> {
        let mut query = self.build_base_query(stream)?;

        // Add WHERE clause for incremental sync
        if let (Some(cursor_field), Some(cursor_val)) = (&stream.cursor_field, cursor_value) {
            if !cursor_val.is_empty() {
                let query_upper = query.to_uppercase();
                if query_upper.contains(" WHERE ") {
                    query = format!("{query} AND {cursor_field} > '{cursor_val}'");
                } else {
                    query = format!("{query} WHERE {cursor_field} > '{cursor_val}'");
                }
            }
        }

        // Add ORDER BY for cursor field
        if let Some(cursor_field) = &stream.cursor_field {
            let query_upper = query.to_uppercase();
            if !query_upper.contains(" ORDER BY ") {
                query = format!("{query} ORDER BY {cursor_field} ASC");
            }
        }

        Ok(query)
    }

    /// Get sync statistics (row count and new cursor value)
    fn get_sync_stats(
        &self,
        stream: &DatabaseStreamDefinition,
        cursor_value: Option<&str>,
    ) -> Result<(usize, Option<String>)> {
        let query = self.build_query(stream, cursor_value)?;

        // Count rows
        let count_sql = format!("SELECT COUNT(*) FROM ({query}) AS q");
        let record_count: i64 = self
            .conn
            .query_row(&count_sql, [], |row| row.get(0))
            .unwrap_or(0);

        // Get max cursor value
        let new_cursor = if let Some(cursor_field) = &stream.cursor_field {
            let max_sql = format!("SELECT MAX({cursor_field}) FROM ({query}) AS q");
            self.conn
                .query_row(&max_sql, [], |row| {
                    let value: duckdb::types::Value = row.get(0)?;
                    Ok(duckdb_value_to_string(value))
                })
                .ok()
                .flatten()
        } else {
            None
        };

        Ok((record_count as usize, new_cursor))
    }

    /// Get database type
    pub fn db_type(&self) -> &DbType {
        &self.db_type
    }

    /// Get connection string (for logging - password masked)
    pub fn connection_info(&self) -> String {
        // Mask password in connection string for logging
        if let Some(at_pos) = self.connection_string.find('@') {
            if let Some(colon_pos) = self.connection_string[..at_pos].rfind(':') {
                let before_pass = &self.connection_string[..=colon_pos];
                let after_at = &self.connection_string[at_pos..];
                return format!("{before_pass}****{after_at}");
            }
        }
        self.connection_string.clone()
    }
}

/// Generate a simple unique ID (timestamp + random)
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{timestamp:x}")
}

/// Convert DuckDB Value to JSON Value
#[allow(dead_code)]
fn duckdb_value_to_json(value: duckdb::types::Value) -> Value {
    match value {
        duckdb::types::Value::Null => Value::Null,
        duckdb::types::Value::Boolean(b) => Value::Bool(b),
        duckdb::types::Value::TinyInt(i) => Value::Number(i.into()),
        duckdb::types::Value::SmallInt(i) => Value::Number(i.into()),
        duckdb::types::Value::Int(i) => Value::Number(i.into()),
        duckdb::types::Value::BigInt(i) => Value::Number(i.into()),
        duckdb::types::Value::HugeInt(i) => Value::String(i.to_string()),
        duckdb::types::Value::UTinyInt(i) => Value::Number(i.into()),
        duckdb::types::Value::USmallInt(i) => Value::Number(i.into()),
        duckdb::types::Value::UInt(i) => Value::Number(i.into()),
        duckdb::types::Value::UBigInt(i) => Value::Number(i.into()),
        duckdb::types::Value::Float(f) => {
            serde_json::Number::from_f64(f64::from(f)).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Double(f) => {
            serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number)
        }
        duckdb::types::Value::Text(s) => Value::String(s),
        duckdb::types::Value::Blob(b) => Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        duckdb::types::Value::Timestamp(_, i) => {
            // Convert to ISO string
            let secs = i / 1_000_000;
            let nsecs = ((i % 1_000_000) * 1000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()))
                .unwrap_or(Value::Number(i.into()))
        }
        duckdb::types::Value::Date32(d) => {
            // Days since epoch (719163 is the number of days from 1 CE to 1970-01-01)
            chrono::NaiveDate::from_num_days_from_ce_opt(d + 719_163)
                .map(|date| Value::String(date.format("%Y-%m-%d").to_string()))
                .unwrap_or(Value::Number(d.into()))
        }
        duckdb::types::Value::Time64(_, t) => {
            // Microseconds since midnight
            let secs = t / 1_000_000;
            let micros = t % 1_000_000;
            Value::String(format!(
                "{:02}:{:02}:{:02}.{:06}",
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60,
                micros
            ))
        }
        _ => Value::String(format!("{value:?}")),
    }
}

/// Convert DuckDB Value to String (for cursor values)
fn duckdb_value_to_string(value: duckdb::types::Value) -> Option<String> {
    match value {
        duckdb::types::Value::Null => None,
        duckdb::types::Value::Text(s) => Some(s),
        duckdb::types::Value::BigInt(i) => Some(i.to_string()),
        duckdb::types::Value::Int(i) => Some(i.to_string()),
        duckdb::types::Value::Timestamp(_, i) => {
            let secs = i / 1_000_000;
            let nsecs = ((i % 1_000_000) * 1000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        }
        duckdb::types::Value::Date32(d) => {
            chrono::NaiveDate::from_num_days_from_ce_opt(d + 719_163)
                .map(|date| date.format("%Y-%m-%d").to_string())
        }
        _ => Some(format!("{value:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connection_string_postgres() {
        let conn = DatabaseConnectionDef {
            connection_string: None,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("testdb".to_string()),
            user: Some("testuser".to_string()),
            password: Some("testpass".to_string()),
            ssl_mode: "prefer".to_string(),
        };

        let context = TemplateContext::new();
        let result = DatabaseEngine::build_connection_string(DbType::Postgres, &conn, &context);

        assert!(result.is_ok());
        let conn_str = result.unwrap();
        assert!(conn_str.contains("postgresql://"));
        assert!(conn_str.contains("localhost:5432"));
        assert!(conn_str.contains("testdb"));
    }

    #[test]
    fn test_build_connection_string_from_template() {
        let conn = DatabaseConnectionDef {
            connection_string: Some(
                "postgresql://{{ config.user }}:{{ config.pass }}@localhost/db".to_string(),
            ),
            host: None,
            port: None,
            database: None,
            user: None,
            password: None,
            ssl_mode: "prefer".to_string(),
        };

        let mut context = TemplateContext::new();
        context.set_config(serde_json::json!({
            "user": "myuser",
            "pass": "mypass"
        }));

        let result = DatabaseEngine::build_connection_string(DbType::Postgres, &conn, &context);

        assert!(result.is_ok());
        let conn_str = result.unwrap();
        assert_eq!(conn_str, "postgresql://myuser:mypass@localhost/db");
    }

    #[test]
    fn test_duckdb_value_to_json() {
        assert_eq!(
            duckdb_value_to_json(duckdb::types::Value::Null),
            Value::Null
        );
        assert_eq!(
            duckdb_value_to_json(duckdb::types::Value::Boolean(true)),
            Value::Bool(true)
        );
        assert_eq!(
            duckdb_value_to_json(duckdb::types::Value::Int(42)),
            Value::Number(42.into())
        );
        assert_eq!(
            duckdb_value_to_json(duckdb::types::Value::Text("hello".to_string())),
            Value::String("hello".to_string())
        );
    }
}
