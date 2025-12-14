//! Decoder implementations
//!
//! Each decoder handles a specific response format.

use super::types::RecordDecoder;
use crate::error::{Error, Result};
use serde_json::{Map, Value};

// ============================================================================
// JSON Decoder
// ============================================================================

/// JSON decoder with optional record path extraction
#[derive(Debug, Clone, Default)]
pub struct JsonDecoder {
    /// JSONPath to extract records
    record_path: Option<String>,
}

impl JsonDecoder {
    /// Create a new JSON decoder
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a JSON decoder with a record path
    pub fn with_path(path: impl Into<String>) -> Self {
        Self {
            record_path: Some(path.into()),
        }
    }

    /// Extract records from a JSON value using a path
    fn extract_records(&self, value: &Value) -> Result<Vec<Value>> {
        match &self.record_path {
            Some(path) => {
                // Use simple path first - handles negative indices and basic paths
                // Only use jsonpath-rust for complex patterns like wildcards
                if path.contains('*') && !path.contains("[-") {
                    extract_with_jsonpath(value, path)
                } else {
                    // Simple dot-notation path (also handles array indexing)
                    match extract_simple_path(value, path) {
                        Some(Value::Array(arr)) => Ok(arr),
                        Some(v) => Ok(vec![v]),
                        None => Ok(vec![]),
                    }
                }
            }
            None => {
                // No path - treat entire response as records
                match value {
                    Value::Array(arr) => Ok(arr.clone()),
                    _ => Ok(vec![value.clone()]),
                }
            }
        }
    }
}

impl RecordDecoder for JsonDecoder {
    fn decode(&self, body: &str) -> Result<Vec<Value>> {
        let value: Value = serde_json::from_str(body).map_err(|e| Error::Decode {
            message: format!("Failed to parse JSON: {e}"),
        })?;
        self.extract_records(&value)
    }

    fn decode_raw(&self, body: &str) -> Result<Value> {
        serde_json::from_str(body).map_err(|e| Error::Decode {
            message: format!("Failed to parse JSON: {e}"),
        })
    }
}

// ============================================================================
// JSONL Decoder
// ============================================================================

/// JSON Lines decoder (one JSON object per line)
#[derive(Debug, Clone, Default)]
pub struct JsonlDecoder;

impl JsonlDecoder {
    /// Create a new JSONL decoder
    pub fn new() -> Self {
        Self
    }
}

impl RecordDecoder for JsonlDecoder {
    fn decode(&self, body: &str) -> Result<Vec<Value>> {
        let mut records = Vec::new();

        for (line_num, line) in body.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let value: Value = serde_json::from_str(line).map_err(|e| Error::Decode {
                message: format!("Failed to parse JSONL at line {}: {e}", line_num + 1),
            })?;

            records.push(value);
        }

        Ok(records)
    }

    fn decode_raw(&self, body: &str) -> Result<Value> {
        // Return as array
        let records = self.decode(body)?;
        Ok(Value::Array(records))
    }
}

// ============================================================================
// CSV Decoder
// ============================================================================

/// CSV decoder with configurable delimiter and header handling
#[derive(Debug, Clone)]
pub struct CsvDecoder {
    /// Field delimiter
    delimiter: char,
    /// Whether the first row is a header
    has_header: bool,
}

impl Default for CsvDecoder {
    fn default() -> Self {
        Self {
            delimiter: ',',
            has_header: true,
        }
    }
}

impl CsvDecoder {
    /// Create a new CSV decoder with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a CSV decoder with custom settings
    pub fn with_options(delimiter: char, has_header: bool) -> Self {
        Self {
            delimiter,
            has_header,
        }
    }
}

impl RecordDecoder for CsvDecoder {
    fn decode(&self, body: &str) -> Result<Vec<Value>> {
        let mut records = Vec::new();
        let mut lines = body.lines().peekable();

        // Get headers
        let headers: Vec<String> = if self.has_header {
            match lines.next() {
                Some(header_line) => parse_csv_line(header_line, self.delimiter),
                None => return Ok(records),
            }
        } else {
            // Generate numeric column names
            if let Some(first_line) = lines.peek() {
                let field_count = parse_csv_line(first_line, self.delimiter).len();
                (0..field_count).map(|i| format!("column_{i}")).collect()
            } else {
                return Ok(records);
            }
        };

        // Parse data rows
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let fields = parse_csv_line(line, self.delimiter);
            let mut obj = Map::new();

            for (i, header) in headers.iter().enumerate() {
                let value = fields.get(i).cloned().unwrap_or_default();
                // Try to parse as number or boolean
                let json_value = parse_csv_value(&value);
                obj.insert(header.clone(), json_value);
            }

            records.push(Value::Object(obj));
        }

        Ok(records)
    }

    fn decode_raw(&self, body: &str) -> Result<Value> {
        let records = self.decode(body)?;
        Ok(Value::Array(records))
    }
}

/// Parse a CSV line into fields
fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '"' {
            if in_quotes {
                // Check for escaped quote
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if c == delimiter && !in_quotes {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
    }

    fields.push(current.trim().to_string());
    fields
}

/// Parse a CSV value into JSON value
fn parse_csv_value(value: &str) -> Value {
    // Try integer
    if let Ok(n) = value.parse::<i64>() {
        return Value::Number(n.into());
    }

    // Try float
    if let Ok(n) = value.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return Value::Number(num);
        }
    }

    // Try boolean
    match value.to_lowercase().as_str() {
        "true" | "yes" | "1" => return Value::Bool(true),
        "false" | "no" | "0" if value != "0" => return Value::Bool(false),
        _ => {}
    }

    // Null/empty
    if value.is_empty() || value.eq_ignore_ascii_case("null") || value.eq_ignore_ascii_case("none")
    {
        return Value::Null;
    }

    // String
    Value::String(value.to_string())
}

// ============================================================================
// XML Decoder
// ============================================================================

/// XML decoder with record element extraction
#[derive(Debug, Clone, Default)]
pub struct XmlDecoder {
    /// Element name containing records
    record_element: Option<String>,
}

impl XmlDecoder {
    /// Create a new XML decoder
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an XML decoder with a record element name
    pub fn with_element(element: impl Into<String>) -> Self {
        Self {
            record_element: Some(element.into()),
        }
    }
}

impl RecordDecoder for XmlDecoder {
    fn decode(&self, body: &str) -> Result<Vec<Value>> {
        // Simple XML to JSON conversion
        // For a full implementation, we'd use quick-xml or similar
        // This is a basic implementation for common patterns

        let json = xml_to_json(body)?;

        match &self.record_element {
            Some(element) => {
                // Extract records from specified element
                if let Some(records) = extract_simple_path(&json, element) {
                    match records {
                        Value::Array(arr) => Ok(arr),
                        v => Ok(vec![v]),
                    }
                } else {
                    Ok(vec![])
                }
            }
            None => {
                // Return entire parsed result
                match json {
                    Value::Array(arr) => Ok(arr),
                    v => Ok(vec![v]),
                }
            }
        }
    }

    fn decode_raw(&self, body: &str) -> Result<Value> {
        xml_to_json(body)
    }
}

/// Basic XML to JSON conversion
/// This handles simple XML structures - for complex XML, consider quick-xml
fn xml_to_json(xml: &str) -> Result<Value> {
    // Very basic XML parsing - handles simple element structures
    // Real implementation would use a proper XML parser

    let xml = xml.trim();

    // Check if this looks like XML
    if !xml.starts_with('<') {
        return Err(Error::XmlParse {
            message: "Input does not appear to be XML".to_string(),
        });
    }

    // Simple recursive descent parser for basic XML
    parse_xml_element(xml).map(|(v, _)| v)
}

/// Parse an XML element and return the JSON value and remaining string
fn parse_xml_element(input: &str) -> Result<(Value, &str)> {
    let input = input.trim();

    // Check for text content (no tags)
    if !input.starts_with('<') {
        // Find the end of text content
        if let Some(pos) = input.find('<') {
            let text = input[..pos].trim();
            if text.is_empty() {
                return parse_xml_element(&input[pos..]);
            }
            return Ok((parse_text_value(text), &input[pos..]));
        }
        return Ok((parse_text_value(input.trim()), ""));
    }

    // Skip XML declaration
    if input.starts_with("<?") {
        if let Some(end) = input.find("?>") {
            return parse_xml_element(&input[end + 2..]);
        }
    }

    // Find tag name
    let tag_start = input.find('<').ok_or_else(|| Error::XmlParse {
        message: "Expected opening tag".to_string(),
    })?;

    let tag_end = input[tag_start..]
        .find(['>', ' ', '/'])
        .ok_or_else(|| Error::XmlParse {
            message: "Malformed tag".to_string(),
        })?;

    let tag_name = &input[tag_start + 1..tag_start + tag_end];

    // Self-closing tag
    if input[tag_start..].contains("/>") {
        let close_pos = input.find("/>").unwrap();
        return Ok((Value::Null, &input[close_pos + 2..]));
    }

    // Find closing tag
    let close_tag = format!("</{tag_name}>");
    let close_pos = input.find(&close_tag).ok_or_else(|| Error::XmlParse {
        message: format!("Missing closing tag for {tag_name}"),
    })?;

    // Find content start (after opening tag >)
    let content_start = input.find('>').unwrap() + 1;
    let content = &input[content_start..close_pos];
    let remaining = &input[close_pos + close_tag.len()..];

    // Parse content
    if content.trim().is_empty() {
        return Ok((Value::Null, remaining));
    }

    // Check if content contains child elements
    if content.contains('<') {
        // Parse child elements
        let mut obj = Map::new();
        let mut current = content.trim();

        while !current.is_empty() && current.contains('<') {
            // Skip text nodes
            if !current.starts_with('<') {
                if let Some(pos) = current.find('<') {
                    current = &current[pos..];
                } else {
                    break;
                }
            }

            // Get child tag name
            if let Some(child_tag_end) = current[1..].find(['>', ' ', '/']) {
                let child_tag = &current[1..=child_tag_end];

                // Skip closing tags
                if child_tag.starts_with('/') {
                    break;
                }

                let (child_value, rest) = parse_xml_element(current)?;

                // Handle arrays (multiple elements with same name)
                if let Some(existing) = obj.get_mut(child_tag) {
                    if let Value::Array(arr) = existing {
                        arr.push(child_value);
                    } else {
                        let arr = vec![existing.clone(), child_value];
                        obj.insert(child_tag.to_string(), Value::Array(arr));
                    }
                } else {
                    obj.insert(child_tag.to_string(), child_value);
                }

                current = rest.trim();
            } else {
                break;
            }
        }

        return Ok((Value::Object(obj), remaining));
    }

    // Text content only
    Ok((parse_text_value(content.trim()), remaining))
}

/// Parse text content into appropriate JSON value
fn parse_text_value(text: &str) -> Value {
    // Try integer
    if let Ok(n) = text.parse::<i64>() {
        return Value::Number(n.into());
    }

    // Try float
    if let Ok(n) = text.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return Value::Number(num);
        }
    }

    // Try boolean
    match text.to_lowercase().as_str() {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }

    Value::String(text.to_string())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract a value using simple dot-notation path
fn extract_simple_path(value: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let parts: Vec<&str> = path.split('.').collect();

    let mut current = value;
    for part in parts {
        // Handle array indexing like "data[0]" or "items[-1]"
        if let Some(bracket_pos) = part.find('[') {
            let name = &part[..bracket_pos];
            let index_str = &part[bracket_pos + 1..part.len() - 1];

            if !name.is_empty() {
                current = current.get(name)?;
            }

            // Handle special array syntax
            if index_str == "*" {
                // Return the array as-is for extraction
                return Some(current.clone());
            } else if let Ok(index) = index_str.parse::<i64>() {
                if let Value::Array(arr) = current {
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        clippy::cast_possible_wrap
                    )]
                    let idx = if index < 0 {
                        (arr.len() as i64 + index) as usize
                    } else {
                        index as usize
                    };
                    current = arr.get(idx)?;
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            current = current.get(part)?;
        }
    }

    Some(current.clone())
}

/// Extract records using jsonpath-rust
fn extract_with_jsonpath(value: &Value, path: &str) -> Result<Vec<Value>> {
    use jsonpath_rust::JsonPath;

    let jp = JsonPath::try_from(path).map_err(|e| Error::JsonPath {
        message: format!("Invalid JSONPath: {e}"),
    })?;

    let result = jp.find(value);

    // Handle the result based on what was returned
    match result {
        Value::Array(arr) => Ok(arr),
        Value::Null => Ok(vec![]),
        other => Ok(vec![other]),
    }
}
