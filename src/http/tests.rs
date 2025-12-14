//! Tests for the HTTP client module

use super::*;
use crate::types::BackoffType;
use std::time::Duration;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn test_http_client_config_default() {
    let config = HttpClientConfig::default();
    assert_eq!(config.timeout, Duration::from_secs(30));
    assert_eq!(config.max_retries, 3);
    assert!(config.base_url.is_none());
    assert!(config.rate_limit.is_some());
}

#[test]
fn test_http_client_config_builder() {
    let config = HttpClientConfig::builder()
        .base_url("https://api.example.com")
        .timeout(Duration::from_secs(60))
        .max_retries(5)
        .backoff(
            BackoffType::Linear,
            Duration::from_millis(200),
            Duration::from_secs(30),
        )
        .header("X-Custom", "value")
        .user_agent("test-agent/1.0")
        .build();

    assert_eq!(config.base_url, Some("https://api.example.com".to_string()));
    assert_eq!(config.timeout, Duration::from_secs(60));
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.backoff_type, BackoffType::Linear);
    assert_eq!(config.initial_backoff, Duration::from_millis(200));
    assert_eq!(config.max_backoff, Duration::from_secs(30));
    assert_eq!(
        config.default_headers.get("X-Custom"),
        Some(&"value".to_string())
    );
    assert_eq!(config.user_agent, "test-agent/1.0");
}

#[test]
fn test_request_config_builder() {
    let config = RequestConfig::new()
        .query("page", "1")
        .query("limit", "10")
        .header("X-Request-Id", "abc123")
        .json(serde_json::json!({"key": "value"}))
        .timeout(Duration::from_secs(10))
        .retries(2);

    assert_eq!(config.query.get("page"), Some(&"1".to_string()));
    assert_eq!(config.query.get("limit"), Some(&"10".to_string()));
    assert_eq!(
        config.headers.get("X-Request-Id"),
        Some(&"abc123".to_string())
    );
    assert!(config.body.is_some());
    assert_eq!(config.timeout, Some(Duration::from_secs(10)));
    assert_eq!(config.max_retries, Some(2));
}

#[tokio::test]
async fn test_http_client_get() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "users": [{"id": 1, "name": "Alice"}]
        })))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client.get("/api/users").await.unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_get_json() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/data"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "value": 42
        })))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let data: serde_json::Value = client.get_json("/api/data").await.unwrap();

    assert_eq!(data["value"], 42);
}

#[tokio::test]
async fn test_http_client_post() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": 123,
            "created": true
        })))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client
        .post("/api/items", serde_json::json!({"name": "test"}))
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
}

#[tokio::test]
async fn test_http_client_query_params() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/search"))
        .and(query_param("q", "test"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "results": []
        })))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client
        .get_with_config(
            "/api/search",
            RequestConfig::new().query("q", "test").query("page", "2"),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_custom_headers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/secure"))
        .and(header("X-API-Key", "secret123"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .header("X-API-Key", "secret123")
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client.get("/api/secure").await.unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_request_headers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/data"))
        .and(header("X-Request-Id", "req-456"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client
        .get_with_config(
            "/api/data",
            RequestConfig::new().header("X-Request-Id", "req-456"),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_404_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("Not found"))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let result = client.get("/api/missing").await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(
        err,
        crate::error::Error::HttpStatus { status: 404, .. }
    ));
}

#[tokio::test]
async fn test_http_client_retry_on_500() {
    let mock_server = MockServer::start().await;

    // First two calls return 500, third succeeds
    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(2)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .max_retries(3)
        .backoff(
            BackoffType::Constant,
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client.get("/api/flaky").await.unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_rate_limit_retry() {
    let mock_server = MockServer::start().await;

    // First call returns 429 with retry-after
    Mock::given(method("GET"))
        .and(path("/api/limited"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "1")
                .set_body_string("Rate limited"),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second call succeeds
    Mock::given(method("GET"))
        .and(path("/api/limited"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .max_retries(2)
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let response = client.get("/api/limited").await.unwrap();

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_http_client_max_retries_exceeded() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/always-fail"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Server error"))
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .max_retries(2)
        .backoff(
            BackoffType::Constant,
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);
    let result = client.get("/api/always-fail").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_http_client_full_url() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/test"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    // Client without base URL
    let config = HttpClientConfig::builder().no_rate_limit().build();
    let client = HttpClient::with_config(config);

    // Use full URL
    let response = client
        .get(&format!("{}/api/test", mock_server.uri()))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[test]
fn test_calculate_backoff_constant() {
    let config = HttpClientConfig::builder()
        .backoff(
            BackoffType::Constant,
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);

    assert_eq!(client.calculate_backoff(0), Duration::from_millis(100));
    assert_eq!(client.calculate_backoff(1), Duration::from_millis(100));
    assert_eq!(client.calculate_backoff(5), Duration::from_millis(100));
}

#[test]
fn test_calculate_backoff_linear() {
    let config = HttpClientConfig::builder()
        .backoff(
            BackoffType::Linear,
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);

    assert_eq!(client.calculate_backoff(0), Duration::from_millis(100));
    assert_eq!(client.calculate_backoff(1), Duration::from_millis(200));
    assert_eq!(client.calculate_backoff(2), Duration::from_millis(300));
}

#[test]
fn test_calculate_backoff_exponential() {
    let config = HttpClientConfig::builder()
        .backoff(
            BackoffType::Exponential,
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);

    assert_eq!(client.calculate_backoff(0), Duration::from_millis(100));
    assert_eq!(client.calculate_backoff(1), Duration::from_millis(200));
    assert_eq!(client.calculate_backoff(2), Duration::from_millis(400));
    assert_eq!(client.calculate_backoff(3), Duration::from_millis(800));
}

#[test]
fn test_calculate_backoff_respects_max() {
    let config = HttpClientConfig::builder()
        .backoff(
            BackoffType::Exponential,
            Duration::from_millis(100),
            Duration::from_millis(500), // Low max
        )
        .no_rate_limit()
        .build();

    let client = HttpClient::with_config(config);

    // After a few attempts, should cap at max
    assert_eq!(client.calculate_backoff(10), Duration::from_millis(500));
}

#[test]
fn test_http_client_debug() {
    let client = HttpClient::new();
    let debug_str = format!("{:?}", client);
    assert!(debug_str.contains("HttpClient"));
    assert!(debug_str.contains("config"));
}

#[tokio::test]
async fn test_http_client_with_rate_limiter() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/data"))
        .respond_with(ResponseTemplate::new(200))
        .expect(3)
        .mount(&mock_server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(mock_server.uri())
        .rate_limit(RateLimiterConfig::new(100, 10))
        .build();

    let client = HttpClient::with_config(config);

    // Make 3 requests
    for _ in 0..3 {
        let response = client.get("/api/data").await.unwrap();
        assert_eq!(response.status(), 200);
    }
}

#[test]
fn test_http_client_default() {
    let client = HttpClient::default();
    // Should have rate limiter by default
    assert!(client.has_rate_limiter());
}
