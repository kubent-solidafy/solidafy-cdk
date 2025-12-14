//! Schema types

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// JSON Schema type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JsonType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
    Null,
}

impl JsonType {
    /// Check if this type can be merged with another
    pub fn can_merge_with(&self, other: &JsonType) -> bool {
        match (self, other) {
            // Same types can always merge
            (a, b) if a == b => true,
            // Null can merge with anything
            (JsonType::Null, _) | (_, JsonType::Null) => true,
            // Integer can be promoted to Number
            (JsonType::Integer, JsonType::Number) | (JsonType::Number, JsonType::Integer) => true,
            _ => false,
        }
    }

    /// Merge two types, returning the more general type
    pub fn merge_with(&self, other: &JsonType) -> JsonType {
        match (self, other) {
            (a, b) if a == b => a.clone(),
            (JsonType::Null, other) | (other, JsonType::Null) => other.clone(),
            (JsonType::Integer, JsonType::Number) | (JsonType::Number, JsonType::Integer) => {
                JsonType::Number
            }
            // Incompatible types - fall back to string
            _ => JsonType::String,
        }
    }
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonType::String => write!(f, "string"),
            JsonType::Number => write!(f, "number"),
            JsonType::Integer => write!(f, "integer"),
            JsonType::Boolean => write!(f, "boolean"),
            JsonType::Object => write!(f, "object"),
            JsonType::Array => write!(f, "array"),
            JsonType::Null => write!(f, "null"),
        }
    }
}

/// JSON Schema property definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaProperty {
    /// Property type(s)
    #[serde(rename = "type")]
    pub json_type: JsonTypeOrArray,

    /// Description (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Format hint (e.g., "date-time", "email", "uri")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Nested properties (for objects)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, SchemaProperty>>,

    /// Additional properties allowed (for objects)
    #[serde(
        rename = "additionalProperties",
        skip_serializing_if = "Option::is_none"
    )]
    pub additional_properties: Option<bool>,

    /// Array items schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<SchemaProperty>>,

    /// Enum values (for strings with known values)
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<serde_json::Value>>,

    /// Example value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub example: Option<serde_json::Value>,
}

/// JSON type can be a single type or array of types (for nullable)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonTypeOrArray {
    Single(JsonType),
    Multiple(Vec<JsonType>),
}

impl JsonTypeOrArray {
    /// Create a single type
    pub fn single(t: JsonType) -> Self {
        JsonTypeOrArray::Single(t)
    }

    /// Create a nullable type
    pub fn nullable(t: JsonType) -> Self {
        if t == JsonType::Null {
            JsonTypeOrArray::Single(JsonType::Null)
        } else {
            JsonTypeOrArray::Multiple(vec![t, JsonType::Null])
        }
    }

    /// Check if this type is nullable
    pub fn is_nullable(&self) -> bool {
        match self {
            JsonTypeOrArray::Single(JsonType::Null) => true,
            JsonTypeOrArray::Multiple(types) => types.contains(&JsonType::Null),
            _ => false,
        }
    }

    /// Get the primary (non-null) type
    pub fn primary_type(&self) -> Option<&JsonType> {
        match self {
            JsonTypeOrArray::Single(t) => Some(t),
            JsonTypeOrArray::Multiple(types) => types.iter().find(|t| **t != JsonType::Null),
        }
    }

    /// Make this type nullable
    pub fn make_nullable(&self) -> Self {
        if self.is_nullable() {
            self.clone()
        } else {
            match self {
                JsonTypeOrArray::Single(t) => JsonTypeOrArray::nullable(t.clone()),
                JsonTypeOrArray::Multiple(types) => {
                    let mut new_types = types.clone();
                    if !new_types.contains(&JsonType::Null) {
                        new_types.push(JsonType::Null);
                    }
                    JsonTypeOrArray::Multiple(new_types)
                }
            }
        }
    }

    /// Merge with another type
    pub fn merge_with(&self, other: &JsonTypeOrArray) -> JsonTypeOrArray {
        let self_nullable = self.is_nullable();
        let other_nullable = other.is_nullable();
        let result_nullable = self_nullable || other_nullable;

        let self_primary = self.primary_type();
        let other_primary = other.primary_type();

        let merged_type = match (self_primary, other_primary) {
            (Some(a), Some(b)) => a.merge_with(b),
            (Some(t), None) | (None, Some(t)) => t.clone(),
            (None, None) => JsonType::Null,
        };

        if result_nullable && merged_type != JsonType::Null {
            JsonTypeOrArray::nullable(merged_type)
        } else {
            JsonTypeOrArray::single(merged_type)
        }
    }
}

impl SchemaProperty {
    /// Create a new property with the given type
    pub fn new(json_type: JsonType) -> Self {
        Self {
            json_type: JsonTypeOrArray::single(json_type),
            description: None,
            format: None,
            properties: None,
            additional_properties: None,
            items: None,
            enum_values: None,
            example: None,
        }
    }

    /// Create a nullable property
    pub fn nullable(json_type: JsonType) -> Self {
        Self {
            json_type: JsonTypeOrArray::nullable(json_type),
            description: None,
            format: None,
            properties: None,
            additional_properties: None,
            items: None,
            enum_values: None,
            example: None,
        }
    }

    /// Create an object property with nested properties
    pub fn object(properties: BTreeMap<String, SchemaProperty>) -> Self {
        Self {
            json_type: JsonTypeOrArray::single(JsonType::Object),
            description: None,
            format: None,
            properties: Some(properties),
            additional_properties: Some(true),
            items: None,
            enum_values: None,
            example: None,
        }
    }

    /// Create an array property with item schema
    pub fn array(items: SchemaProperty) -> Self {
        Self {
            json_type: JsonTypeOrArray::single(JsonType::Array),
            description: None,
            format: None,
            properties: None,
            additional_properties: None,
            items: Some(Box::new(items)),
            enum_values: None,
            example: None,
        }
    }

    /// Set format hint
    #[must_use]
    pub fn with_format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Set description
    #[must_use]
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Set example value
    #[must_use]
    pub fn with_example(mut self, example: serde_json::Value) -> Self {
        self.example = Some(example);
        self
    }

    /// Make this property nullable
    pub fn make_nullable(&mut self) {
        self.json_type = self.json_type.make_nullable();
    }

    /// Check if nullable
    pub fn is_nullable(&self) -> bool {
        self.json_type.is_nullable()
    }
}

/// Full JSON Schema document
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonSchema {
    /// Schema version
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Schema type (always "object" for top-level)
    #[serde(rename = "type")]
    pub json_type: JsonType,

    /// Schema title
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Schema description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Object properties
    #[serde(default)]
    pub properties: BTreeMap<String, SchemaProperty>,

    /// Required properties
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required: Vec<String>,

    /// Allow additional properties
    #[serde(rename = "additionalProperties", default = "default_true")]
    pub additional_properties: bool,
}

fn default_true() -> bool {
    true
}

impl Default for JsonSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonSchema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            schema: Some("http://json-schema.org/draft-07/schema#".to_string()),
            json_type: JsonType::Object,
            title: None,
            description: None,
            properties: BTreeMap::new(),
            required: Vec::new(),
            additional_properties: true,
        }
    }

    /// Set the schema title
    #[must_use]
    pub fn with_title(mut self, title: &str) -> Self {
        self.title = Some(title.to_string());
        self
    }

    /// Set the schema description
    #[must_use]
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Add a property
    pub fn add_property(&mut self, name: &str, property: SchemaProperty) {
        self.properties.insert(name.to_string(), property);
    }

    /// Add a required property
    pub fn add_required(&mut self, name: &str) {
        if !self.required.contains(&name.to_string()) {
            self.required.push(name.to_string());
        }
    }

    /// Remove a property from required
    pub fn remove_required(&mut self, name: &str) {
        self.required.retain(|n| n != name);
    }

    /// Check if a property is required
    pub fn is_required(&self, name: &str) -> bool {
        self.required.contains(&name.to_string())
    }

    /// Get a property
    pub fn get_property(&self, name: &str) -> Option<&SchemaProperty> {
        self.properties.get(name)
    }

    /// Get a mutable property
    pub fn get_property_mut(&mut self, name: &str) -> Option<&mut SchemaProperty> {
        self.properties.get_mut(name)
    }

    /// Convert to JSON value
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }

    /// Convert to pretty JSON string
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}
