//! Schema inference tests

use super::*;
use serde_json::json;

#[test]
fn test_infer_simple_object() {
    let value = json!({
        "name": "John",
        "age": 30,
        "active": true
    });

    let schema = infer_schema(&value);

    assert_eq!(schema.properties.len(), 3);
    assert!(schema.get_property("name").is_some());
    assert!(schema.get_property("age").is_some());
    assert!(schema.get_property("active").is_some());

    // Check types
    let name_prop = schema.get_property("name").unwrap();
    assert_eq!(name_prop.json_type.primary_type(), Some(&JsonType::String));

    let age_prop = schema.get_property("age").unwrap();
    assert_eq!(age_prop.json_type.primary_type(), Some(&JsonType::Integer));

    let active_prop = schema.get_property("active").unwrap();
    assert_eq!(
        active_prop.json_type.primary_type(),
        Some(&JsonType::Boolean)
    );
}

#[test]
fn test_infer_nested_object() {
    let value = json!({
        "user": {
            "name": "John",
            "email": "john@example.com"
        }
    });

    let schema = infer_schema(&value);

    let user_prop = schema.get_property("user").unwrap();
    assert_eq!(user_prop.json_type.primary_type(), Some(&JsonType::Object));

    let user_props = user_prop.properties.as_ref().unwrap();
    assert!(user_props.contains_key("name"));
    assert!(user_props.contains_key("email"));

    // Email should have format detected
    let email_prop = user_props.get("email").unwrap();
    assert_eq!(email_prop.format, Some("email".to_string()));
}

#[test]
fn test_infer_array() {
    let value = json!({
        "items": [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"}
        ]
    });

    let schema = infer_schema(&value);

    let items_prop = schema.get_property("items").unwrap();
    assert_eq!(items_prop.json_type.primary_type(), Some(&JsonType::Array));

    let item_schema = items_prop.items.as_ref().unwrap();
    let item_props = item_schema.properties.as_ref().unwrap();
    assert!(item_props.contains_key("id"));
    assert!(item_props.contains_key("name"));
}

#[test]
fn test_infer_datetime_format() {
    let value = json!({
        "created_at": "2024-01-15T10:30:00Z",
        "date": "2024-01-15"
    });

    let schema = infer_schema(&value);

    let created_prop = schema.get_property("created_at").unwrap();
    assert_eq!(created_prop.format, Some("date-time".to_string()));

    let date_prop = schema.get_property("date").unwrap();
    assert_eq!(date_prop.format, Some("date".to_string()));
}

#[test]
fn test_infer_uri_format() {
    let value = json!({
        "website": "https://example.com",
        "api": "http://api.example.com"
    });

    let schema = infer_schema(&value);

    let website_prop = schema.get_property("website").unwrap();
    assert_eq!(website_prop.format, Some("uri".to_string()));

    let api_prop = schema.get_property("api").unwrap();
    assert_eq!(api_prop.format, Some("uri".to_string()));
}

#[test]
fn test_infer_uuid_format() {
    let value = json!({
        "id": "550e8400-e29b-41d4-a716-446655440000"
    });

    let schema = infer_schema(&value);

    let id_prop = schema.get_property("id").unwrap();
    assert_eq!(id_prop.format, Some("uuid".to_string()));
}

#[test]
fn test_infer_from_multiple_records() {
    let records = vec![
        json!({"name": "John", "age": 30}),
        json!({"name": "Jane", "age": 25}),
        json!({"name": "Bob", "age": 35}),
    ];

    let mut inferrer = SchemaInferrer::new();
    let schema = inferrer.infer_from_records(&records);

    assert_eq!(schema.properties.len(), 2);
    assert!(schema.required.contains(&"name".to_string()));
    assert!(schema.required.contains(&"age".to_string()));
}

#[test]
fn test_infer_nullable_field() {
    let records = vec![
        json!({"name": "John", "email": "john@example.com"}),
        json!({"name": "Jane"}), // Missing email
        json!({"name": "Bob", "email": "bob@example.com"}),
    ];

    let mut inferrer = SchemaInferrer::new();
    let schema = inferrer.infer_from_records(&records);

    // Name should be required
    assert!(schema.required.contains(&"name".to_string()));

    // Email should be nullable (not in required)
    assert!(!schema.required.contains(&"email".to_string()));

    let email_prop = schema.get_property("email").unwrap();
    assert!(email_prop.is_nullable());
}

#[test]
fn test_infer_null_value() {
    let records = vec![
        json!({"name": "John", "middle_name": null}),
        json!({"name": "Jane", "middle_name": "Marie"}),
    ];

    let mut inferrer = SchemaInferrer::new();
    let schema = inferrer.infer_from_records(&records);

    let middle_prop = schema.get_property("middle_name").unwrap();
    assert!(middle_prop.is_nullable());
}

#[test]
fn test_merge_schemas() {
    let a = json!({
        "id": 1,
        "name": "John"
    });

    let b = json!({
        "id": 2,
        "email": "john@example.com"
    });

    let schema_a = infer_schema(&a);
    let schema_b = infer_schema(&b);

    let merged = merge_schemas(&schema_a, &schema_b);

    // Should have all three fields
    assert_eq!(merged.properties.len(), 3);
    assert!(merged.get_property("id").is_some());
    assert!(merged.get_property("name").is_some());
    assert!(merged.get_property("email").is_some());

    // Name and email should be nullable (not in both)
    let name_prop = merged.get_property("name").unwrap();
    assert!(name_prop.is_nullable());

    let email_prop = merged.get_property("email").unwrap();
    assert!(email_prop.is_nullable());
}

#[test]
fn test_type_merge_integer_to_number() {
    let records = vec![json!({"value": 42}), json!({"value": 3.14})];

    let mut inferrer = SchemaInferrer::new();
    let schema = inferrer.infer_from_records(&records);

    let value_prop = schema.get_property("value").unwrap();
    assert_eq!(value_prop.json_type.primary_type(), Some(&JsonType::Number));
}

#[test]
fn test_array_item_type_merge() {
    let value = json!({
        "values": [1, 2.5, 3]
    });

    let schema = infer_schema(&value);

    let values_prop = schema.get_property("values").unwrap();
    let item_schema = values_prop.items.as_ref().unwrap();

    // Should be number (merged from integer and number)
    assert_eq!(
        item_schema.json_type.primary_type(),
        Some(&JsonType::Number)
    );
}

#[test]
fn test_schema_to_json() {
    let value = json!({
        "name": "John",
        "age": 30
    });

    let schema = infer_schema(&value);
    let json = schema.to_json();

    assert!(json.is_object());
    assert_eq!(json["type"], "object");
    assert!(json["properties"]["name"].is_object());
    assert!(json["properties"]["age"].is_object());
}

#[test]
fn test_empty_object() {
    let value = json!({});
    let schema = infer_schema(&value);

    assert_eq!(schema.properties.len(), 0);
    assert!(schema.required.is_empty());
}

#[test]
fn test_empty_array() {
    let value = json!({
        "items": []
    });

    let schema = infer_schema(&value);
    let items_prop = schema.get_property("items").unwrap();

    assert_eq!(items_prop.json_type.primary_type(), Some(&JsonType::Array));
}

#[test]
fn test_deeply_nested() {
    let value = json!({
        "level1": {
            "level2": {
                "level3": {
                    "value": 42
                }
            }
        }
    });

    let schema = infer_schema(&value);

    let level1 = schema.get_property("level1").unwrap();
    let level2 = level1.properties.as_ref().unwrap().get("level2").unwrap();
    let level3 = level2.properties.as_ref().unwrap().get("level3").unwrap();
    let value_prop = level3.properties.as_ref().unwrap().get("value").unwrap();

    assert_eq!(
        value_prop.json_type.primary_type(),
        Some(&JsonType::Integer)
    );
}

#[test]
fn test_json_type_merge() {
    // Same types
    assert_eq!(
        JsonType::String.merge_with(&JsonType::String),
        JsonType::String
    );

    // Null with other
    assert_eq!(
        JsonType::Null.merge_with(&JsonType::String),
        JsonType::String
    );

    // Integer to Number
    assert_eq!(
        JsonType::Integer.merge_with(&JsonType::Number),
        JsonType::Number
    );
}

#[test]
fn test_schema_property_builder() {
    let prop = SchemaProperty::new(JsonType::String)
        .with_format("email")
        .with_description("User email address");

    assert_eq!(prop.format, Some("email".to_string()));
    assert_eq!(prop.description, Some("User email address".to_string()));
}

#[test]
fn test_disable_format_detection() {
    let value = json!({
        "email": "test@example.com",
        "date": "2024-01-15"
    });

    let inferrer = SchemaInferrer::new()
        .with_email_detection(false)
        .with_datetime_detection(false);

    let schema = inferrer.infer(&value);

    let email_prop = schema.get_property("email").unwrap();
    assert_eq!(email_prop.format, None);

    let date_prop = schema.get_property("date").unwrap();
    assert_eq!(date_prop.format, None);
}

#[test]
fn test_new_field_in_later_record() {
    let records = vec![
        json!({"id": 1}),
        json!({"id": 2, "name": "John"}),
        json!({"id": 3, "name": "Jane"}),
    ];

    let mut inferrer = SchemaInferrer::new();
    let schema = inferrer.infer_from_records(&records);

    // name should be nullable since it wasn't in the first record
    let name_prop = schema.get_property("name").unwrap();
    assert!(name_prop.is_nullable());
}
