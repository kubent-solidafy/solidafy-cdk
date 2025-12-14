//! Tests for decoder module

use super::*;

// ============================================================================
// DecoderConfig Tests
// ============================================================================

#[test]
fn test_decoder_format_default() {
    let format = DecoderFormat::default();
    assert_eq!(format, DecoderFormat::Json);
}

#[test]
fn test_decoder_config_json() {
    let config = DecoderConfig::json();
    assert_eq!(config.format, DecoderFormat::Json);
    assert!(config.record_path.is_none());
}

#[test]
fn test_decoder_config_json_with_path() {
    let config = DecoderConfig::json_with_path("$.data.items");
    assert_eq!(config.format, DecoderFormat::Json);
    assert_eq!(config.record_path, Some("$.data.items".to_string()));
}

#[test]
fn test_decoder_config_jsonl() {
    let config = DecoderConfig::jsonl();
    assert_eq!(config.format, DecoderFormat::Jsonl);
}

#[test]
fn test_decoder_config_csv() {
    let config = DecoderConfig::csv();
    assert_eq!(config.format, DecoderFormat::Csv);
    assert_eq!(config.csv_delimiter, Some(','));
    assert!(config.csv_has_header);
}

#[test]
fn test_decoder_config_csv_with_delimiter() {
    let config = DecoderConfig::csv_with_delimiter('\t', false);
    assert_eq!(config.format, DecoderFormat::Csv);
    assert_eq!(config.csv_delimiter, Some('\t'));
    assert!(!config.csv_has_header);
}

#[test]
fn test_decoder_config_xml() {
    let config = DecoderConfig::xml("item");
    assert_eq!(config.format, DecoderFormat::Xml);
    assert_eq!(config.xml_record_element, Some("item".to_string()));
}

#[test]
fn test_decoder_config_with_record_path() {
    let config = DecoderConfig::json().with_record_path("$.results");
    assert_eq!(config.record_path, Some("$.results".to_string()));
}

// ============================================================================
// JSON Decoder Tests
// ============================================================================

#[test]
fn test_json_decoder_array() {
    let decoder = JsonDecoder::new();
    let body = r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[2]["id"], 3);
}

#[test]
fn test_json_decoder_object() {
    let decoder = JsonDecoder::new();
    let body = r#"{"id": 1, "name": "test"}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 1);
}

#[test]
fn test_json_decoder_with_path() {
    let decoder = JsonDecoder::with_path("data");
    let body = r#"{"data": [{"id": 1}, {"id": 2}], "meta": {}}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
}

#[test]
fn test_json_decoder_nested_path() {
    let decoder = JsonDecoder::with_path("response.items");
    let body = r#"{"response": {"items": [{"id": 1}], "total": 1}}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 1);
}

#[test]
fn test_json_decoder_array_index() {
    let decoder = JsonDecoder::with_path("data[-1]");
    let body = r#"{"data": [{"id": 1}, {"id": 2}, {"id": 3}]}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 3);
}

#[test]
fn test_json_decoder_jsonpath_wildcard() {
    let decoder = JsonDecoder::with_path("$.data[*]");
    let body = r#"{"data": [{"id": 1}, {"id": 2}]}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
}

#[test]
fn test_json_decoder_raw() {
    let decoder = JsonDecoder::new();
    let body = r#"{"status": "ok", "data": []}"#;

    let raw = decoder.decode_raw(body).unwrap();
    assert_eq!(raw["status"], "ok");
}

#[test]
fn test_json_decoder_invalid() {
    let decoder = JsonDecoder::new();
    let body = "not valid json";

    let result = decoder.decode(body);
    assert!(result.is_err());
}

// ============================================================================
// JSONL Decoder Tests
// ============================================================================

#[test]
fn test_jsonl_decoder_basic() {
    let decoder = JsonlDecoder::new();
    let body = r#"{"id": 1}
{"id": 2}
{"id": 3}"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[1]["id"], 2);
    assert_eq!(records[2]["id"], 3);
}

#[test]
fn test_jsonl_decoder_empty_lines() {
    let decoder = JsonlDecoder::new();
    let body = r#"{"id": 1}

{"id": 2}
"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
}

#[test]
fn test_jsonl_decoder_raw() {
    let decoder = JsonlDecoder::new();
    let body = r#"{"id": 1}
{"id": 2}"#;

    let raw = decoder.decode_raw(body).unwrap();
    assert!(raw.is_array());
    assert_eq!(raw.as_array().unwrap().len(), 2);
}

#[test]
fn test_jsonl_decoder_invalid_line() {
    let decoder = JsonlDecoder::new();
    let body = r#"{"id": 1}
not valid json
{"id": 2}"#;

    let result = decoder.decode(body);
    assert!(result.is_err());
}

// ============================================================================
// CSV Decoder Tests
// ============================================================================

#[test]
fn test_csv_decoder_basic() {
    let decoder = CsvDecoder::new();
    let body = r#"id,name,age
1,Alice,30
2,Bob,25"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[0]["name"], "Alice");
    assert_eq!(records[0]["age"], 30);
    assert_eq!(records[1]["id"], 2);
    assert_eq!(records[1]["name"], "Bob");
}

#[test]
fn test_csv_decoder_quoted_fields() {
    let decoder = CsvDecoder::new();
    let body = r#"id,name,description
1,"Alice","Hello, World"
2,"Bob","He said ""Hi"""#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["description"], "Hello, World");
    assert_eq!(records[1]["description"], "He said \"Hi\"");
}

#[test]
fn test_csv_decoder_no_header() {
    let decoder = CsvDecoder::with_options(',', false);
    let body = r#"1,Alice,30
2,Bob,25"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["column_0"], 1);
    assert_eq!(records[0]["column_1"], "Alice");
}

#[test]
fn test_csv_decoder_tab_delimiter() {
    let decoder = CsvDecoder::with_options('\t', true);
    let body = "id\tname\n1\tAlice\n2\tBob";

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[0]["name"], "Alice");
}

#[test]
fn test_csv_decoder_booleans() {
    let decoder = CsvDecoder::new();
    let body = r#"id,active,deleted
1,true,false
2,yes,no"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records[0]["active"], true);
    assert_eq!(records[0]["deleted"], false);
    assert_eq!(records[1]["active"], true);
    assert_eq!(records[1]["deleted"], false);
}

#[test]
fn test_csv_decoder_nulls() {
    let decoder = CsvDecoder::new();
    let body = r#"id,value
1,
2,null
3,none"#;

    let records = decoder.decode(body).unwrap();
    assert!(records[0]["value"].is_null());
    assert!(records[1]["value"].is_null());
    assert!(records[2]["value"].is_null());
}

#[test]
fn test_csv_decoder_raw() {
    let decoder = CsvDecoder::new();
    let body = "id,name\n1,Alice";

    let raw = decoder.decode_raw(body).unwrap();
    assert!(raw.is_array());
}

// ============================================================================
// XML Decoder Tests
// ============================================================================

#[test]
fn test_xml_decoder_basic() {
    let decoder = XmlDecoder::new();
    let body = r#"<root><name>Test</name><value>42</value></root>"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "Test");
    assert_eq!(records[0]["value"], 42);
}

#[test]
fn test_xml_decoder_with_element() {
    let decoder = XmlDecoder::with_element("items.item");
    let body = r#"<root><items><item><id>1</id></item><item><id>2</id></item></items></root>"#;

    let raw = decoder.decode_raw(body).unwrap();
    // The XML structure should have items.item as an array
    assert!(raw["items"]["item"].is_array());
}

#[test]
fn test_xml_decoder_raw() {
    let decoder = XmlDecoder::new();
    let body = r#"<data><status>ok</status></data>"#;

    let raw = decoder.decode_raw(body).unwrap();
    assert_eq!(raw["status"], "ok");
}

#[test]
fn test_xml_decoder_nested() {
    let decoder = XmlDecoder::new();
    let body = r#"<user><profile><name>Alice</name><age>30</age></profile></user>"#;

    let raw = decoder.decode_raw(body).unwrap();
    assert_eq!(raw["profile"]["name"], "Alice");
    assert_eq!(raw["profile"]["age"], 30);
}

#[test]
fn test_xml_decoder_invalid() {
    let decoder = XmlDecoder::new();
    let body = "not xml";

    let result = decoder.decode(body);
    assert!(result.is_err());
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_stripe_like_response() {
    let decoder = JsonDecoder::with_path("$.data[*]");
    let body = r#"{
        "object": "list",
        "data": [
            {"id": "cus_1", "object": "customer"},
            {"id": "cus_2", "object": "customer"}
        ],
        "has_more": true,
        "url": "/v1/customers"
    }"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "cus_1");
}

#[test]
fn test_hubspot_like_response() {
    let decoder = JsonDecoder::with_path("results");
    let body = r#"{
        "results": [
            {"id": "1", "properties": {"name": "Company A"}},
            {"id": "2", "properties": {"name": "Company B"}}
        ],
        "paging": {"next": {"after": "2"}}
    }"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
}

#[test]
fn test_github_like_response() {
    // GitHub returns arrays directly
    let decoder = JsonDecoder::new();
    let body = r#"[
        {"id": 1, "name": "repo1"},
        {"id": 2, "name": "repo2"}
    ]"#;

    let records = decoder.decode(body).unwrap();
    assert_eq!(records.len(), 2);
}
