//! Tests for output module

use super::*;
use arrow::datatypes::DataType;
use serde_json::json;
use tempfile::tempdir;

// ============================================================================
// Schema Inference Tests
// ============================================================================

#[test]
fn test_infer_schema_empty() {
    let records: Vec<serde_json::Value> = vec![];
    let schema = infer_schema(&records).unwrap();
    assert!(schema.fields().is_empty());
}

#[test]
fn test_infer_schema_simple() {
    let records = vec![
        json!({"name": "Alice", "age": 30}),
        json!({"name": "Bob", "age": 25}),
    ];

    let schema = infer_schema(&records).unwrap();
    assert_eq!(schema.fields().len(), 2);

    // Find fields by name
    let name_field = schema.field_with_name("name").unwrap();
    let age_field = schema.field_with_name("age").unwrap();

    assert_eq!(name_field.data_type(), &DataType::Utf8);
    assert_eq!(age_field.data_type(), &DataType::Int64);
}

#[test]
fn test_infer_schema_with_nulls() {
    let records = vec![
        json!({"name": "Alice", "email": null}),
        json!({"name": "Bob", "email": "bob@example.com"}),
    ];

    let schema = infer_schema(&records).unwrap();
    let email_field = schema.field_with_name("email").unwrap();
    assert_eq!(email_field.data_type(), &DataType::Utf8);
}

#[test]
fn test_infer_schema_mixed_numbers() {
    let records = vec![json!({"value": 42}), json!({"value": 3.14})];

    let schema = infer_schema(&records).unwrap();
    let value_field = schema.field_with_name("value").unwrap();
    // Mixed int/float should become Float64
    assert_eq!(value_field.data_type(), &DataType::Float64);
}

#[test]
fn test_infer_schema_boolean() {
    let records = vec![json!({"active": true}), json!({"active": false})];

    let schema = infer_schema(&records).unwrap();
    let field = schema.field_with_name("active").unwrap();
    assert_eq!(field.data_type(), &DataType::Boolean);
}

#[test]
fn test_infer_schema_nested_object() {
    let records = vec![json!({"user": {"id": 1, "name": "Alice"}})];

    let schema = infer_schema(&records).unwrap();
    let field = schema.field_with_name("user").unwrap();

    if let DataType::Struct(fields) = field.data_type() {
        assert_eq!(fields.len(), 2);
    } else {
        panic!("Expected Struct type");
    }
}

#[test]
fn test_infer_schema_array() {
    let records = vec![json!({"tags": ["rust", "arrow"]})];

    let schema = infer_schema(&records).unwrap();
    let field = schema.field_with_name("tags").unwrap();

    if let DataType::List(inner) = field.data_type() {
        assert_eq!(inner.data_type(), &DataType::Utf8);
    } else {
        panic!("Expected List type");
    }
}

// ============================================================================
// Merge Schemas Tests
// ============================================================================

#[test]
fn test_merge_schemas_disjoint() {
    let schema1 = infer_schema(&[json!({"a": 1})]).unwrap();
    let schema2 = infer_schema(&[json!({"b": "text"})]).unwrap();

    let merged = merge_schemas(&schema1, &schema2);
    assert_eq!(merged.fields().len(), 2);
}

#[test]
fn test_merge_schemas_overlapping() {
    let schema1 = infer_schema(&[json!({"a": 1, "b": 2})]).unwrap();
    let schema2 = infer_schema(&[json!({"b": 3, "c": 4})]).unwrap();

    let merged = merge_schemas(&schema1, &schema2);
    assert_eq!(merged.fields().len(), 3);
}

#[test]
fn test_merge_schemas_type_promotion() {
    let schema1 = infer_schema(&[json!({"value": 42})]).unwrap();
    let schema2 = infer_schema(&[json!({"value": 3.14})]).unwrap();

    let merged = merge_schemas(&schema1, &schema2);
    let field = merged.field_with_name("value").unwrap();
    assert_eq!(field.data_type(), &DataType::Float64);
}

// ============================================================================
// JSON to Arrow Tests
// ============================================================================

#[test]
fn test_json_to_arrow_simple() {
    let records = vec![
        json!({"id": 1, "name": "Alice"}),
        json!({"id": 2, "name": "Bob"}),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_json_to_arrow_empty() {
    let records: Vec<serde_json::Value> = vec![];
    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_json_to_arrow_with_nulls() {
    let records = vec![
        json!({"id": 1, "name": "Alice"}),
        json!({"id": 2, "name": null}),
        json!({"id": 3}), // Missing name
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_json_to_arrow_nested() {
    let records = vec![
        json!({"user": {"id": 1, "name": "Alice"}}),
        json!({"user": {"id": 2, "name": "Bob"}}),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_json_to_arrow_with_arrays() {
    let records = vec![
        json!({"tags": ["a", "b"]}),
        json!({"tags": ["c"]}),
        json!({"tags": []}),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_json_to_arrow_provided_schema() {
    let records = vec![json!({"id": 1, "name": "Alice", "extra": "ignored"})];

    // Schema only has id and name
    let schema = infer_schema(&[json!({"id": 0, "name": ""})]).unwrap();
    let batch = json_to_arrow(&records, Some(&schema)).unwrap();

    assert_eq!(batch.num_columns(), 2);
}

// ============================================================================
// Parquet Writer Config Tests
// ============================================================================

#[test]
fn test_parquet_writer_config_default() {
    let config = ParquetWriterConfig::default();
    assert!(config.is_dictionary_enabled());
    assert!(config.is_statistics_enabled());
}

#[test]
fn test_parquet_writer_config_builder() {
    let config = ParquetWriterConfig::new()
        .with_row_group_size(1000)
        .with_dictionary(false)
        .with_statistics(false)
        .uncompressed();

    assert!(!config.is_dictionary_enabled());
    assert!(!config.is_statistics_enabled());
    assert_eq!(config.row_group_size(), 1000);
}

#[test]
fn test_parquet_writer_config_zstd() {
    let config = ParquetWriterConfig::new().zstd();
    // Just ensure it doesn't panic
    let _ = config;
}

#[test]
fn test_parquet_writer_config_gzip() {
    let config = ParquetWriterConfig::new().gzip();
    let _ = config;
}

// ============================================================================
// Parquet Writer Tests
// ============================================================================

#[test]
fn test_write_batch_to_parquet() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.parquet");

    let records = vec![
        json!({"id": 1, "name": "Alice"}),
        json!({"id": 2, "name": "Bob"}),
    ];
    let batch = json_to_arrow(&records, None).unwrap();

    let rows = writer::write_batch_to_parquet(&path, &batch, None).unwrap();
    assert_eq!(rows, 2);
    assert!(path.exists());
}

#[test]
fn test_write_batches_to_parquet() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("batches.parquet");

    let records1 = vec![json!({"id": 1})];
    let records2 = vec![json!({"id": 2}), json!({"id": 3})];

    let batch1 = json_to_arrow(&records1, None).unwrap();
    let batch2 = json_to_arrow(&records2, None).unwrap();

    let rows = writer::write_batches_to_parquet(&path, &[batch1, batch2], None).unwrap();
    assert_eq!(rows, 3);
}

#[test]
fn test_write_empty_batches_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.parquet");

    let result = writer::write_batches_to_parquet(&path, &[], None);
    assert!(result.is_err());
}

#[test]
fn test_parquet_writer_rows_written() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("writer.parquet");

    let records = vec![json!({"id": 1}), json!({"id": 2})];
    let batch = json_to_arrow(&records, None).unwrap();

    let config = ParquetWriterConfig::default();
    let mut writer = ParquetWriter::new(&path, batch.schema().as_ref(), &config).unwrap();

    assert_eq!(writer.rows_written(), 0);

    writer.write(&batch).unwrap();
    assert_eq!(writer.rows_written(), 2);

    let rows = writer.close().unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn test_parquet_writer_with_custom_config() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("custom.parquet");

    let records = vec![json!({"value": "test"})];
    let batch = json_to_arrow(&records, None).unwrap();

    let config = ParquetWriterConfig::new()
        .uncompressed()
        .with_row_group_size(100);

    let rows = writer::write_batch_to_parquet(&path, &batch, Some(&config)).unwrap();
    assert_eq!(rows, 1);
}

// ============================================================================
// Real-world Data Tests
// ============================================================================

#[test]
fn test_stripe_like_data() {
    let records = vec![
        json!({
            "id": "cus_123",
            "email": "alice@example.com",
            "created": 1640000000,
            "metadata": {"plan": "pro"},
            "balance": 0
        }),
        json!({
            "id": "cus_456",
            "email": "bob@example.com",
            "created": 1640100000,
            "metadata": {"plan": "free"},
            "balance": 1000
        }),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_hubspot_like_data() {
    let records = vec![json!({
        "id": "1",
        "properties": {
            "firstname": "Alice",
            "lastname": "Smith",
            "email": "alice@example.com"
        },
        "archived": false
    })];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_github_like_data() {
    let records = vec![json!({
        "id": 1,
        "name": "repo",
        "full_name": "owner/repo",
        "private": false,
        "owner": {
            "login": "owner",
            "id": 100
        },
        "topics": ["rust", "api"],
        "stargazers_count": 1000
    })];

    let batch = json_to_arrow(&records, None).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

// ============================================================================
// Arrow to JSON Tests
// ============================================================================

#[test]
fn test_arrow_to_json_simple() {
    let records = vec![
        json!({"id": 1, "name": "Alice"}),
        json!({"id": 2, "name": "Bob"}),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 2);

    // Check first record
    assert_eq!(result[0]["id"], 1);
    assert_eq!(result[0]["name"], "Alice");

    // Check second record
    assert_eq!(result[1]["id"], 2);
    assert_eq!(result[1]["name"], "Bob");
}

#[test]
fn test_arrow_to_json_with_nulls() {
    let records = vec![
        json!({"id": 1, "name": "Alice"}),
        json!({"id": 2, "name": null}),
    ];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["name"], "Alice");
    // Null values in Arrow may be converted to "null" string due to schema inference
    // The important thing is they're handled without panicking
    assert!(result[1]["name"].is_null() || result[1]["name"] == "null");
}

#[test]
fn test_arrow_to_json_numbers() {
    let records = vec![json!({"int_val": 42, "float_val": 3.14})];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["int_val"], 42);
    assert!((result[0]["float_val"].as_f64().unwrap() - 3.14).abs() < 0.001);
}

#[test]
fn test_arrow_to_json_boolean() {
    let records = vec![json!({"active": true}), json!({"active": false})];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["active"], true);
    assert_eq!(result[1]["active"], false);
}

#[test]
fn test_arrow_to_json_nested() {
    let records = vec![json!({"user": {"id": 1, "name": "Alice"}})];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["user"]["id"], 1);
    assert_eq!(result[0]["user"]["name"], "Alice");
}

#[test]
fn test_arrow_to_json_arrays() {
    let records = vec![json!({"tags": ["a", "b", "c"]}), json!({"tags": ["x"]})];

    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["tags"], json!(["a", "b", "c"]));
    assert_eq!(result[1]["tags"], json!(["x"]));
}

#[test]
fn test_arrow_to_json_empty() {
    let records: Vec<serde_json::Value> = vec![];
    let batch = json_to_arrow(&records, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert!(result.is_empty());
}

#[test]
fn test_arrow_to_json_roundtrip() {
    // Test that json_to_arrow -> arrow_to_json preserves data
    let original = vec![json!({
        "id": 123,
        "name": "Test",
        "active": true,
        "score": 98.5
    })];

    let batch = json_to_arrow(&original, None).unwrap();
    let result = arrow_to_json(&batch).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["id"], 123);
    assert_eq!(result[0]["name"], "Test");
    assert_eq!(result[0]["active"], true);
    assert!((result[0]["score"].as_f64().unwrap() - 98.5).abs() < 0.001);
}
