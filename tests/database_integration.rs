//! Database integration tests with real PostgreSQL
//!
//! These tests require a live PostgreSQL database.
//! Set POSTGRES_TEST_URL environment variable to run.

use solidafy_cdk::database::DbEngine;
use solidafy_cdk::loader::{
    DatabaseConnectionDef, DatabaseEngine as DbType, DatabaseStreamDefinition,
};
use solidafy_cdk::template::TemplateContext;

/// Get test connection string from environment or skip
fn get_test_connection() -> Option<String> {
    std::env::var("POSTGRES_TEST_URL").ok()
}

#[test]
fn test_postgres_connection() {
    let Some(conn_str) = get_test_connection() else {
        println!("Skipping: POSTGRES_TEST_URL not set");
        return;
    };

    let conn = DatabaseConnectionDef {
        connection_string: Some(conn_str),
        host: None,
        port: None,
        database: None,
        user: None,
        password: None,
        ssl_mode: "prefer".to_string(),
    };

    let context = TemplateContext::new();
    let engine = DbEngine::new(DbType::Postgres, &conn, &context);

    assert!(
        engine.is_ok(),
        "Failed to create engine: {:?}",
        engine.err()
    );

    let engine = engine.unwrap();
    let check = engine.check_connection();
    assert!(check.is_ok(), "Connection check failed: {:?}", check.err());

    println!("Connection check passed!");
}

#[test]
fn test_postgres_list_tables() {
    let Some(conn_str) = get_test_connection() else {
        println!("Skipping: POSTGRES_TEST_URL not set");
        return;
    };

    let conn = DatabaseConnectionDef {
        connection_string: Some(conn_str),
        host: None,
        port: None,
        database: None,
        user: None,
        password: None,
        ssl_mode: "prefer".to_string(),
    };

    let context = TemplateContext::new();
    let engine = DbEngine::new(DbType::Postgres, &conn, &context).unwrap();

    let tables = engine.list_tables();
    assert!(tables.is_ok(), "Failed to list tables: {:?}", tables.err());

    let tables = tables.unwrap();
    println!("Found {} tables:", tables.len());
    for table in &tables {
        println!("  - {}", table);
    }
}

#[test]
fn test_postgres_sync_to_json() {
    let Some(conn_str) = get_test_connection() else {
        println!("Skipping: POSTGRES_TEST_URL not set");
        return;
    };

    let conn = DatabaseConnectionDef {
        connection_string: Some(conn_str),
        host: None,
        port: None,
        database: None,
        user: None,
        password: None,
        ssl_mode: "prefer".to_string(),
    };

    let context = TemplateContext::new();
    let engine = DbEngine::new(DbType::Postgres, &conn, &context).unwrap();

    // List tables first to find one to test
    let tables = engine.list_tables().unwrap();
    if tables.is_empty() {
        println!("No tables found, skipping sync test");
        return;
    }

    // Use first table
    let table_name = &tables[0];
    println!("Testing sync with table: {}", table_name);

    let stream = DatabaseStreamDefinition {
        name: "test_stream".to_string(),
        table: Some(table_name.clone()),
        query: None,
        primary_key: vec![],
        cursor_field: None,
        batch_size: 10,
    };

    let result = engine.sync_to_json(&stream, None);
    assert!(result.is_ok(), "Sync failed: {:?}", result.err());

    let result = result.unwrap();
    println!("Synced {} records from {}", result.record_count, table_name);

    if let Some(records) = &result.records {
        if !records.is_empty() {
            println!(
                "First record: {}",
                serde_json::to_string_pretty(&records[0]).unwrap()
            );
        }
    }
}

#[test]
fn test_postgres_sync_to_parquet() {
    let Some(conn_str) = get_test_connection() else {
        println!("Skipping: POSTGRES_TEST_URL not set");
        return;
    };

    let conn = DatabaseConnectionDef {
        connection_string: Some(conn_str),
        host: None,
        port: None,
        database: None,
        user: None,
        password: None,
        ssl_mode: "prefer".to_string(),
    };

    let context = TemplateContext::new();
    let engine = DbEngine::new(DbType::Postgres, &conn, &context).unwrap();

    // List tables first
    let tables = engine.list_tables().unwrap();
    if tables.is_empty() {
        println!("No tables found, skipping parquet test");
        return;
    }

    let table_name = &tables[0];
    println!("Testing parquet sync with table: {}", table_name);

    let stream = DatabaseStreamDefinition {
        name: "test_stream".to_string(),
        table: Some(table_name.clone()),
        query: None,
        primary_key: vec![],
        cursor_field: None,
        batch_size: 10,
    };

    // Create temp directory for output
    let temp_dir = tempfile::tempdir().unwrap();
    let output_path = temp_dir.path().join("test.parquet");
    let output_str = output_path.to_str().unwrap();

    let result = engine.sync_to_parquet(&stream, output_str, None);
    assert!(result.is_ok(), "Parquet sync failed: {:?}", result.err());

    let result = result.unwrap();
    println!("Wrote {} records to {}", result.record_count, output_str);

    // Verify file exists
    assert!(output_path.exists(), "Parquet file was not created");
    let metadata = std::fs::metadata(&output_path).unwrap();
    println!("Parquet file size: {} bytes", metadata.len());
}
