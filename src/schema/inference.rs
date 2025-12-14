//! Schema inference from JSON values

use super::types::{JsonSchema, JsonType, SchemaProperty};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

/// Schema inferrer with configuration options
#[derive(Debug, Clone)]
pub struct SchemaInferrer {
    /// Detect date-time formats
    detect_datetime: bool,
    /// Detect URI formats
    detect_uri: bool,
    /// Detect email formats
    detect_email: bool,
    /// Detect UUID formats
    detect_uuid: bool,
    /// Maximum depth for nested objects
    max_depth: usize,
    /// Record count for required detection
    record_count: usize,
    /// Field occurrence count for required detection
    field_counts: BTreeMap<String, usize>,
}

impl Default for SchemaInferrer {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaInferrer {
    /// Create a new schema inferrer with default settings
    pub fn new() -> Self {
        Self {
            detect_datetime: true,
            detect_uri: true,
            detect_email: true,
            detect_uuid: true,
            max_depth: 10,
            record_count: 0,
            field_counts: BTreeMap::new(),
        }
    }

    /// Enable/disable datetime detection
    #[must_use]
    pub fn with_datetime_detection(mut self, enabled: bool) -> Self {
        self.detect_datetime = enabled;
        self
    }

    /// Enable/disable URI detection
    #[must_use]
    pub fn with_uri_detection(mut self, enabled: bool) -> Self {
        self.detect_uri = enabled;
        self
    }

    /// Enable/disable email detection
    #[must_use]
    pub fn with_email_detection(mut self, enabled: bool) -> Self {
        self.detect_email = enabled;
        self
    }

    /// Enable/disable UUID detection
    #[must_use]
    pub fn with_uuid_detection(mut self, enabled: bool) -> Self {
        self.detect_uuid = enabled;
        self
    }

    /// Set maximum depth for nested objects
    #[must_use]
    pub fn with_max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    /// Infer schema from a single JSON value
    pub fn infer(&self, value: &Value) -> JsonSchema {
        let mut schema = JsonSchema::new();

        if let Value::Object(map) = value {
            for (key, val) in map {
                let property = self.infer_property(val, 0);
                schema.add_property(key, property);
                // Single record - all fields are required
                schema.add_required(key);
            }
        }

        schema
    }

    /// Infer schema from multiple JSON values (records)
    pub fn infer_from_records(&mut self, records: &[Value]) -> JsonSchema {
        if records.is_empty() {
            return JsonSchema::new();
        }

        // Start with schema from first record
        let mut schema = self.infer(&records[0]);
        self.record_count = 1;

        // Track field occurrences
        if let Value::Object(map) = &records[0] {
            for key in map.keys() {
                self.field_counts.insert(key.clone(), 1);
            }
        }

        // Merge schemas from remaining records
        for record in records.iter().skip(1) {
            self.record_count += 1;
            self.merge_record_into_schema(&mut schema, record);
        }

        // Update required fields based on occurrence
        self.update_required(&mut schema);

        schema
    }

    /// Merge a record into an existing schema
    fn merge_record_into_schema(&mut self, schema: &mut JsonSchema, record: &Value) {
        if let Value::Object(map) = record {
            let record_keys: HashSet<_> = map.keys().cloned().collect();

            // Update field counts for existing fields
            for key in map.keys() {
                *self.field_counts.entry(key.clone()).or_insert(0) += 1;
            }

            // Check for new fields
            for (key, val) in map {
                if let Some(existing) = schema.get_property_mut(key) {
                    // Merge with existing property
                    let new_prop = self.infer_property(val, 0);
                    *existing = merge_property(existing, &new_prop);
                } else {
                    // New field - add as nullable since it wasn't in earlier records
                    let mut property = self.infer_property(val, 0);
                    property.make_nullable();
                    schema.add_property(key, property);
                }
            }

            // Mark missing fields as nullable
            let existing_keys: Vec<_> = schema.properties.keys().cloned().collect();
            for key in existing_keys {
                if !record_keys.contains(&key) {
                    if let Some(prop) = schema.get_property_mut(&key) {
                        prop.make_nullable();
                    }
                }
            }
        }
    }

    /// Update required fields based on field occurrence counts
    fn update_required(&self, schema: &mut JsonSchema) {
        schema.required.clear();
        for (key, count) in &self.field_counts {
            if *count == self.record_count {
                // Field appeared in every record
                if let Some(prop) = schema.get_property(key) {
                    if !prop.is_nullable() {
                        schema.add_required(key);
                    }
                }
            }
        }
    }

    /// Infer a property from a JSON value
    fn infer_property(&self, value: &Value, depth: usize) -> SchemaProperty {
        if depth >= self.max_depth {
            return SchemaProperty::new(JsonType::Object);
        }

        match value {
            Value::Null => SchemaProperty::nullable(JsonType::Null),
            Value::Bool(_) => SchemaProperty::new(JsonType::Boolean),
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    SchemaProperty::new(JsonType::Integer)
                } else {
                    SchemaProperty::new(JsonType::Number)
                }
            }
            Value::String(s) => self.infer_string_property(s),
            Value::Array(arr) => self.infer_array_property(arr, depth),
            Value::Object(map) => self.infer_object_property(map, depth),
        }
    }

    /// Infer property type from a string value
    fn infer_string_property(&self, s: &str) -> SchemaProperty {
        let mut prop = SchemaProperty::new(JsonType::String);

        // Detect formats
        if self.detect_datetime && is_datetime(s) {
            prop.format = Some("date-time".to_string());
        } else if self.detect_datetime && is_date(s) {
            prop.format = Some("date".to_string());
        } else if self.detect_uri && is_uri(s) {
            prop.format = Some("uri".to_string());
        } else if self.detect_email && is_email(s) {
            prop.format = Some("email".to_string());
        } else if self.detect_uuid && is_uuid(s) {
            prop.format = Some("uuid".to_string());
        }

        prop
    }

    /// Infer property type from an array value
    fn infer_array_property(&self, arr: &[Value], depth: usize) -> SchemaProperty {
        if arr.is_empty() {
            // Empty array - unknown item type
            return SchemaProperty::array(SchemaProperty::new(JsonType::Object));
        }

        // Infer item schema from first item
        let mut item_schema = self.infer_property(&arr[0], depth + 1);

        // Merge with remaining items
        for item in arr.iter().skip(1) {
            let item_prop = self.infer_property(item, depth + 1);
            item_schema = merge_property(&item_schema, &item_prop);
        }

        SchemaProperty::array(item_schema)
    }

    /// Infer property type from an object value
    fn infer_object_property(
        &self,
        map: &serde_json::Map<String, Value>,
        depth: usize,
    ) -> SchemaProperty {
        let mut properties = BTreeMap::new();

        for (key, val) in map {
            let prop = self.infer_property(val, depth + 1);
            properties.insert(key.clone(), prop);
        }

        SchemaProperty::object(properties)
    }
}

/// Infer schema from a single JSON value (convenience function)
pub fn infer_schema(value: &Value) -> JsonSchema {
    SchemaInferrer::new().infer(value)
}

/// Merge two schemas together
pub fn merge_schemas(a: &JsonSchema, b: &JsonSchema) -> JsonSchema {
    let mut result = a.clone();

    for (key, b_prop) in &b.properties {
        if let Some(a_prop) = result.get_property_mut(key) {
            *a_prop = merge_property(a_prop, b_prop);
        } else {
            // New field from b - add as nullable
            let mut prop = b_prop.clone();
            prop.make_nullable();
            result.add_property(key, prop);
        }
    }

    // Fields only in a should be nullable
    for key in a.properties.keys() {
        if !b.properties.contains_key(key) {
            if let Some(prop) = result.get_property_mut(key) {
                prop.make_nullable();
            }
        }
    }

    // Required = intersection of both required sets
    result.required = a
        .required
        .iter()
        .filter(|r| b.required.contains(r))
        .cloned()
        .collect();

    result
}

/// Merge two properties together
fn merge_property(a: &SchemaProperty, b: &SchemaProperty) -> SchemaProperty {
    // Merge types
    let merged_type = a.json_type.merge_with(&b.json_type);

    // Merge format (prefer non-None, or a's format if both have it)
    let format = match (&a.format, &b.format) {
        (Some(af), Some(bf)) if af == bf => Some(af.clone()),
        (Some(_), Some(_)) => None, // Conflicting formats - drop it
        (Some(f), None) | (None, Some(f)) => Some(f.clone()),
        (None, None) => None,
    };

    // Merge nested properties
    let properties = match (&a.properties, &b.properties) {
        (Some(a_props), Some(b_props)) => {
            let mut merged = a_props.clone();
            for (key, b_prop) in b_props {
                if let Some(a_prop) = merged.get_mut(key) {
                    *a_prop = merge_property(a_prop, b_prop);
                } else {
                    let mut prop = b_prop.clone();
                    prop.make_nullable();
                    merged.insert(key.clone(), prop);
                }
            }
            // Mark missing keys in b as nullable
            for key in a_props.keys() {
                if !b_props.contains_key(key) {
                    if let Some(prop) = merged.get_mut(key) {
                        prop.make_nullable();
                    }
                }
            }
            Some(merged)
        }
        (Some(props), None) | (None, Some(props)) => Some(props.clone()),
        (None, None) => None,
    };

    // Merge array items
    let items = match (&a.items, &b.items) {
        (Some(a_items), Some(b_items)) => Some(Box::new(merge_property(a_items, b_items))),
        (Some(items), None) | (None, Some(items)) => Some(items.clone()),
        (None, None) => None,
    };

    SchemaProperty {
        json_type: merged_type,
        description: a.description.clone().or_else(|| b.description.clone()),
        format,
        properties,
        additional_properties: a.additional_properties.or(b.additional_properties),
        items,
        enum_values: None, // Don't merge enums - too complex
        example: a.example.clone().or_else(|| b.example.clone()),
    }
}

// Format detection helpers

fn is_datetime(s: &str) -> bool {
    // ISO 8601 datetime patterns
    // 2024-01-15T10:30:00Z
    // 2024-01-15T10:30:00+00:00
    // 2024-01-15T10:30:00.123Z
    let patterns = [
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
    ];

    for pattern in &patterns {
        if regex::Regex::new(pattern)
            .map(|re| re.is_match(s))
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

fn is_date(s: &str) -> bool {
    // ISO 8601 date pattern: 2024-01-15
    regex::Regex::new(r"^\d{4}-\d{2}-\d{2}$")
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

fn is_uri(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

fn is_email(s: &str) -> bool {
    // Simple email check - contains @ and .
    s.contains('@') && s.contains('.') && s.len() > 5
}

fn is_uuid(s: &str) -> bool {
    // UUID pattern: 8-4-4-4-12 hex digits
    regex::Regex::new(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )
    .map(|re| re.is_match(s))
    .unwrap_or(false)
}
