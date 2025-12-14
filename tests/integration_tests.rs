//! Integration tests using mock HTTP server
//!
//! Tests the full end-to-end flow: YAML connector → HTTP requests → JSON/Parquet output

use serde_json::json;
use solidafy_cdk::decode::{JsonDecoder, RecordDecoder};
use solidafy_cdk::http::{HttpClient, HttpClientConfig, RequestConfig};
use solidafy_cdk::loader::load_connector_from_str;
use solidafy_cdk::output::{infer_schema, json_to_arrow};
use solidafy_cdk::types::BackoffType;
use std::time::Duration;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ============================================================================
// HTTP Client Integration Tests
// ============================================================================

#[tokio::test]
async fn test_http_client_get_json() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/users", mock_server.uri()))
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();

    assert_eq!(body["users"].as_array().unwrap().len(), 2);
    assert_eq!(body["users"][0]["name"], "Alice");
}

#[tokio::test]
async fn test_http_client_with_headers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/protected"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ok"})))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let mut config = RequestConfig::new();
    config
        .headers
        .insert("Authorization".to_string(), "Bearer test-token".to_string());

    let response = client
        .get_with_config(&format!("{}/api/protected", mock_server.uri()), config)
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_http_client_retry_on_500() {
    let mock_server = MockServer::start().await;

    // First request fails, second succeeds
    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .max_retries(3)
        .backoff(
            BackoffType::Constant,
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .build();
    let client = HttpClient::with_config(config);

    let response = client
        .get(&format!("{}/api/flaky", mock_server.uri()))
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["ok"], true);
}

// ============================================================================
// Decoder Integration Tests
// ============================================================================

#[tokio::test]
async fn test_json_decoder_with_records_path() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/products"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "products": [
                    {"id": "prod_1", "name": "Widget", "price": 9.99},
                    {"id": "prod_2", "name": "Gadget", "price": 19.99}
                ]
            },
            "meta": {"total": 2}
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/products", mock_server.uri()))
        .await
        .unwrap();
    let body_text = response.text().await.unwrap();

    let decoder = JsonDecoder::with_path("data.products");
    let records = decoder.decode(&body_text).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "prod_1");
    assert_eq!(records[1]["name"], "Gadget");
}

#[tokio::test]
async fn test_json_decoder_root_array() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1},
            {"id": 2},
            {"id": 3}
        ])))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/items", mock_server.uri()))
        .await
        .unwrap();
    let body_text = response.text().await.unwrap();

    let decoder = JsonDecoder::new();
    let records = decoder.decode(&body_text).unwrap();

    assert_eq!(records.len(), 3);
}

// ============================================================================
// Pagination Integration Tests
// ============================================================================

#[tokio::test]
async fn test_cursor_pagination_flow() {
    let mock_server = MockServer::start().await;

    // Page 1
    Mock::given(method("GET"))
        .and(path("/api/customers"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": "cus_1", "name": "Alice"},
                {"id": "cus_2", "name": "Bob"}
            ],
            "has_more": true
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Page 2
    Mock::given(method("GET"))
        .and(path("/api/customers"))
        .and(query_param("limit", "2"))
        .and(query_param("starting_after", "cus_2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": "cus_3", "name": "Charlie"}
            ],
            "has_more": false
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();

    // Fetch page 1
    let response1 = client
        .get(&format!("{}/api/customers?limit=2", mock_server.uri()))
        .await
        .unwrap();
    let body1: serde_json::Value = response1.json().await.unwrap();

    assert_eq!(body1["data"].as_array().unwrap().len(), 2);
    let cursor = body1["data"][1]["id"].as_str().unwrap();
    assert_eq!(cursor, "cus_2");

    // Fetch page 2 using cursor
    let response2 = client
        .get(&format!(
            "{}/api/customers?limit=2&starting_after={}",
            mock_server.uri(),
            cursor
        ))
        .await
        .unwrap();
    let body2: serde_json::Value = response2.json().await.unwrap();

    assert_eq!(body2["data"].as_array().unwrap().len(), 1);
    assert_eq!(body2["has_more"], false);
}

#[tokio::test]
async fn test_link_header_pagination_flow() {
    let mock_server = MockServer::start().await;

    let page2_url = format!("{}/api/repos?page=2", mock_server.uri());

    Mock::given(method("GET"))
        .and(path("/api/repos"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([{"id": 1, "name": "repo1"}]))
                .insert_header("Link", format!("<{}>; rel=\"next\"", page2_url)),
        )
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/repos", mock_server.uri()))
        .await
        .unwrap();

    let link_header = response.headers().get("Link").unwrap().to_str().unwrap();
    assert!(link_header.contains("rel=\"next\""));
    assert!(link_header.contains("page=2"));
}

// ============================================================================
// Arrow/Parquet Output Integration Tests
// ============================================================================

#[tokio::test]
async fn test_json_to_arrow_conversion() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "metrics": [
                {"name": "cpu", "value": 85.5, "timestamp": 1700000000},
                {"name": "memory", "value": 72.3, "timestamp": 1700000060},
                {"name": "disk", "value": 45.0, "timestamp": 1700000120}
            ]
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/metrics", mock_server.uri()))
        .await
        .unwrap();
    let body_text = response.text().await.unwrap();

    let decoder = JsonDecoder::with_path("metrics");
    let records = decoder.decode(&body_text).unwrap();

    // Convert to Arrow
    let batch = json_to_arrow(&records, None).unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3); // name, value, timestamp
}

#[tokio::test]
async fn test_schema_inference() {
    let records = vec![
        json!({"id": 1, "name": "Alice", "active": true, "score": 95.5}),
        json!({"id": 2, "name": "Bob", "active": false, "score": 87.0}),
    ];

    let schema = infer_schema(&records).unwrap();

    assert_eq!(schema.fields().len(), 4);

    // Check field types
    let id_field = schema.field_with_name("id").unwrap();
    assert!(matches!(
        id_field.data_type(),
        arrow::datatypes::DataType::Int64
    ));

    let name_field = schema.field_with_name("name").unwrap();
    assert!(matches!(
        name_field.data_type(),
        arrow::datatypes::DataType::Utf8
    ));

    let active_field = schema.field_with_name("active").unwrap();
    assert!(matches!(
        active_field.data_type(),
        arrow::datatypes::DataType::Boolean
    ));
}

// ============================================================================
// YAML Connector Loading Integration Tests
// ============================================================================

#[test]
fn test_load_connector_basic() {
    let yaml = r#"
name: test-api
version: "1.0.0"
base_url: "https://api.example.com"

auth:
  type: bearer
  token: "{{ config.api_key }}"

http:
  timeout_secs: 30
  max_retries: 3
  rate_limit_rps: 10

streams:
  - name: users
    request:
      path: /users
    decoder:
      type: json
      records_path: data
    pagination:
      type: cursor
      cursor_param: after
      cursor_path: meta.next_cursor
    primary_key:
      - id
    cursor_field: updated_at
"#;

    let connector = load_connector_from_str(yaml).unwrap();

    assert_eq!(connector.name, "test-api");
    assert_eq!(connector.version, "1.0.0");
    assert_eq!(connector.base_url, "https://api.example.com");
    assert_eq!(connector.streams.len(), 1);
    assert_eq!(connector.streams[0].name, "users");
}

#[test]
fn test_load_connector_with_multiple_streams() {
    let yaml = r#"
name: multi-stream
version: "1.0.0"
base_url: "https://api.example.com"

auth:
  type: api_key
  key: X-API-Key
  value: "{{ config.key }}"
  location: header

streams:
  - name: products
    request:
      path: /products
    decoder:
      type: json
      records_path: products
    primary_key:
      - id

  - name: orders
    request:
      path: /orders
      params:
        status: all
    decoder:
      type: json
      records_path: orders
    pagination:
      type: offset
      offset_param: offset
      limit_param: limit
      limit: 100
    primary_key:
      - id
    cursor_field: created_at

  - name: customers
    request:
      path: /customers
    decoder:
      type: json
      records_path: data.customers
    pagination:
      type: page_number
      page_param: page
      page_size: 50
    primary_key:
      - id
"#;

    let connector = load_connector_from_str(yaml).unwrap();

    assert_eq!(connector.streams.len(), 3);
    assert_eq!(connector.streams[0].name, "products");
    assert_eq!(connector.streams[1].name, "orders");
    assert_eq!(connector.streams[2].name, "customers");
}

#[test]
fn test_load_connector_oauth2() {
    let yaml = r#"
name: oauth-api
version: "1.0.0"
base_url: "https://api.example.com"

auth:
  type: oauth2_refresh_token
  token_url: "https://auth.example.com/token"
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  refresh_token: "{{ config.refresh_token }}"

streams:
  - name: data
    request:
      path: /data
    decoder:
      type: json
    primary_key:
      - id
"#;

    let connector = load_connector_from_str(yaml).unwrap();

    assert_eq!(connector.name, "oauth-api");
    assert!(connector.auth.is_some());
}

#[test]
fn test_load_connector_async_job() {
    let yaml = r#"
name: bulk-api
version: "1.0.0"
base_url: "https://api.example.com"

auth:
  type: bearer
  token: "{{ config.token }}"

streams:
  - name: bulk_export
    partition:
      type: async_job
      create:
        method: POST
        path: /jobs
        body: |
          {"query": "SELECT * FROM table"}
        job_id_path: id
      poll:
        path: /jobs/{{ job_id }}
        interval_secs: 5
        max_attempts: 60
        status_path: state
        completed_value: done
        failed_values:
          - failed
          - error
      download:
        path: /jobs/{{ job_id }}/results
    request:
      path: /jobs/{{ job_id }}/results
    decoder:
      type: csv
    primary_key:
      - id
"#;

    let connector = load_connector_from_str(yaml).unwrap();

    assert_eq!(connector.name, "bulk-api");
    assert_eq!(connector.streams.len(), 1);
    assert!(connector.streams[0].partition.is_some());
}

// ============================================================================
// End-to-End Mock API Test
// ============================================================================

#[tokio::test]
async fn test_full_sync_flow() {
    let mock_server = MockServer::start().await;

    // Mock paginated API
    Mock::given(method("GET"))
        .and(path("/api/v1/records"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "records": [
                {"id": "rec_1", "value": 100, "created": "2024-01-01"},
                {"id": "rec_2", "value": 200, "created": "2024-01-02"},
                {"id": "rec_3", "value": 300, "created": "2024-01-03"}
            ],
            "pagination": {
                "has_more": false,
                "next_cursor": null
            }
        })))
        .mount(&mock_server)
        .await;

    // Create connector YAML
    let yaml = format!(
        r#"
name: test-connector
version: "1.0.0"
base_url: "{}"

auth:
  type: none

streams:
  - name: records
    request:
      path: /api/v1/records
    decoder:
      type: json
      records_path: records
    primary_key:
      - id
    cursor_field: created
"#,
        mock_server.uri()
    );

    let connector = load_connector_from_str(&yaml).unwrap();

    // Fetch data
    let client = HttpClient::new();
    let url = format!(
        "{}{}",
        connector.base_url, connector.streams[0].request.path
    );
    let response = client.get(&url).await.unwrap();
    let body_text = response.text().await.unwrap();

    // Decode
    let decoder = JsonDecoder::with_path("records");
    let records = decoder.decode(&body_text).unwrap();

    assert_eq!(records.len(), 3);

    // Convert to Arrow
    let batch = json_to_arrow(&records, None).unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3); // id, value, created
}

#[tokio::test]
async fn test_auth_header_injection() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/secure"))
        .and(header("Authorization", "Bearer secret-token-123"))
        .and(header("X-Custom-Header", "custom-value"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"authenticated": true})))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let mut config = RequestConfig::new();
    config.headers.insert(
        "Authorization".to_string(),
        "Bearer secret-token-123".to_string(),
    );
    config
        .headers
        .insert("X-Custom-Header".to_string(), "custom-value".to_string());

    let response = client
        .get_with_config(&format!("{}/api/secure", mock_server.uri()), config)
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["authenticated"], true);
}

#[tokio::test]
async fn test_multiple_requests_same_client() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/endpoint"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .expect(3) // Expect exactly 3 requests
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();

    // Make 3 requests with same client
    for i in 0..3 {
        let response = client
            .get(&format!("{}/api/endpoint", mock_server.uri()))
            .await
            .unwrap();
        assert!(response.status().is_success(), "Request {} failed", i);
    }
}

#[tokio::test]
async fn test_nested_json_records_path() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/nested"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "response": {
                "data": {
                    "items": [
                        {"id": 1, "value": "a"},
                        {"id": 2, "value": "b"}
                    ]
                },
                "meta": {"count": 2}
            }
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/nested", mock_server.uri()))
        .await
        .unwrap();
    let body_text = response.text().await.unwrap();

    let decoder = JsonDecoder::with_path("response.data.items");
    let records = decoder.decode(&body_text).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[1]["value"], "b");
}

#[tokio::test]
async fn test_empty_response_handling() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/empty"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": []
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/empty", mock_server.uri()))
        .await
        .unwrap();
    let body_text = response.text().await.unwrap();

    let decoder = JsonDecoder::with_path("items");
    let records = decoder.decode(&body_text).unwrap();

    assert!(records.is_empty());
}

#[tokio::test]
async fn test_http_error_handling() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/not-found"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": "Not found"
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/not-found", mock_server.uri()))
        .await;

    // HttpClient returns HttpStatus error for non-2xx responses
    assert!(response.is_err());
    let err = response.unwrap_err();
    // Check it's an HttpStatus error with 404 status
    match err {
        solidafy_cdk::error::Error::HttpStatus { status, body } => {
            assert_eq!(status, 404);
            assert!(body.contains("Not found"));
        }
        _ => panic!("Expected HttpStatus error, got {:?}", err),
    }
}

#[tokio::test]
async fn test_query_parameters() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/search"))
        .and(query_param("q", "test"))
        .and(query_param("limit", "10"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 1}]
        })))
        .mount(&mock_server)
        .await;

    let client = HttpClient::new();
    let response = client
        .get(&format!("{}/api/search?q=test&limit=10", mock_server.uri()))
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["results"].as_array().unwrap().len(), 1);
}
