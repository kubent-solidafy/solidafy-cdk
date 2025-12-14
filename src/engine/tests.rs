//! Tests for engine module

use super::*;
use crate::decode::JsonDecoder;
use crate::http::HttpClientConfig;
use crate::pagination::{NoPaginator, OffsetPaginator, StopCondition};
use crate::partition::ListRouter;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ============================================================================
// Message Tests
// ============================================================================

#[test]
fn test_message_record() {
    let batch = crate::output::json_to_arrow(&[json!({"id": 1})], None).unwrap();
    let msg = Message::record("users", batch);
    assert!(msg.is_record());
    assert!(!msg.is_state());
    assert!(!msg.is_log());
}

#[test]
fn test_message_state() {
    let msg = Message::state("users", json!({"cursor": "abc"}));
    assert!(msg.is_state());
    assert!(!msg.is_record());
}

#[test]
fn test_message_log() {
    let msg = Message::info("test message");
    assert!(msg.is_log());
    assert!(!msg.is_record());

    let msg = Message::debug("debug");
    assert!(msg.is_log());

    let msg = Message::warn("warning");
    assert!(msg.is_log());

    let msg = Message::error("error");
    assert!(msg.is_log());
}

// ============================================================================
// SyncConfig Tests
// ============================================================================

#[test]
fn test_sync_config_default() {
    let config = SyncConfig::default();
    assert_eq!(config.batch_size, 1000);
    assert!(!config.emit_state_per_page);
    assert_eq!(config.max_records, 0);
    assert!(config.fail_fast);
}

#[test]
fn test_sync_config_builder() {
    let config = SyncConfig::new()
        .with_batch_size(500)
        .with_state_per_page(true)
        .with_max_records(1000)
        .with_fail_fast(false);

    assert_eq!(config.batch_size, 500);
    assert!(config.emit_state_per_page);
    assert_eq!(config.max_records, 1000);
    assert!(!config.fail_fast);
}

// ============================================================================
// SyncStats Tests
// ============================================================================

#[test]
fn test_sync_stats_default() {
    let stats = SyncStats::new();
    assert_eq!(stats.records_synced, 0);
    assert_eq!(stats.pages_fetched, 0);
    assert_eq!(stats.streams_synced, 0);
    assert_eq!(stats.partitions_synced, 0);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_sync_stats_mutations() {
    let mut stats = SyncStats::new();

    stats.add_records(100);
    assert_eq!(stats.records_synced, 100);

    stats.add_page();
    stats.add_page();
    assert_eq!(stats.pages_fetched, 2);

    stats.add_stream();
    assert_eq!(stats.streams_synced, 1);

    stats.add_partition();
    stats.add_partition();
    stats.add_partition();
    assert_eq!(stats.partitions_synced, 3);

    stats.add_error();
    assert_eq!(stats.errors, 1);

    stats.set_duration(1500);
    assert_eq!(stats.duration_ms, 1500);
}

// ============================================================================
// LogLevel Tests
// ============================================================================

#[test]
fn test_log_level_equality() {
    assert_eq!(LogLevel::Debug, LogLevel::Debug);
    assert_eq!(LogLevel::Info, LogLevel::Info);
    assert_eq!(LogLevel::Warn, LogLevel::Warn);
    assert_eq!(LogLevel::Error, LogLevel::Error);
    assert_ne!(LogLevel::Debug, LogLevel::Error);
}

// ============================================================================
// SyncEngine Tests
// ============================================================================

#[tokio::test]
async fn test_sync_engine_simple_stream() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        })))
        .mount(&server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(server.uri())
        .no_rate_limit()
        .build();
    let client = HttpClient::with_config(config);
    let state = crate::state::StateManager::in_memory();

    let mut engine = SyncEngine::new(client, state);

    let decoder = JsonDecoder::with_path("users");
    let paginator = NoPaginator;
    let context = TemplateContext::new();

    let messages = engine
        .sync_stream(
            "users",
            &server.uri(),
            "/api/users",
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &decoder,
            &paginator,
            &context,
            None,
        )
        .await
        .unwrap();

    // Should have info logs and record message
    let record_msgs: Vec<_> = messages.iter().filter(|m| m.is_record()).collect();
    assert_eq!(record_msgs.len(), 1);

    let stats = engine.stats();
    assert_eq!(stats.records_synced, 2);
    assert_eq!(stats.pages_fetched, 1);
}

#[tokio::test]
async fn test_sync_engine_with_pagination() {
    let server = MockServer::start().await;

    // Page 1
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}, {"id": 2}],
            "total": 4
        })))
        .mount(&server)
        .await;

    // Page 2
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 3}, {"id": 4}],
            "total": 4
        })))
        .mount(&server)
        .await;

    let config = HttpClientConfig::builder()
        .base_url(server.uri())
        .no_rate_limit()
        .build();
    let client = HttpClient::with_config(config);
    let state = crate::state::StateManager::in_memory();

    let mut engine = SyncEngine::new(client, state);

    let decoder = JsonDecoder::with_path("items");
    let paginator = OffsetPaginator::new("offset", "limit", 2, StopCondition::total_count("total"));
    let context = TemplateContext::new();

    let messages = engine
        .sync_stream(
            "items",
            &server.uri(),
            "/api/items",
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &decoder,
            &paginator,
            &context,
            None,
        )
        .await
        .unwrap();

    let record_msgs: Vec<_> = messages.iter().filter(|m| m.is_record()).collect();
    assert!(!record_msgs.is_empty());

    let stats = engine.stats();
    assert_eq!(stats.records_synced, 4);
    assert_eq!(stats.pages_fetched, 2);
}

#[tokio::test]
async fn test_sync_engine_max_records() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
        })))
        .mount(&server)
        .await;

    let http_config = HttpClientConfig::builder()
        .base_url(server.uri())
        .no_rate_limit()
        .build();
    let client = HttpClient::with_config(http_config);
    let state = crate::state::StateManager::in_memory();

    let config = SyncConfig::new().with_max_records(3);
    let mut engine = SyncEngine::new(client, state).with_config(config);

    let decoder = JsonDecoder::with_path("items");
    let paginator = NoPaginator;
    let context = TemplateContext::new();

    engine
        .sync_stream(
            "items",
            &server.uri(),
            "/api/items",
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &decoder,
            &paginator,
            &context,
            None,
        )
        .await
        .unwrap();

    let stats = engine.stats();
    assert_eq!(stats.records_synced, 3); // Limited to 3
}

#[tokio::test]
async fn test_sync_engine_partitioned() {
    let server = MockServer::start().await;

    // Region A
    Mock::given(method("GET"))
        .and(path("/api/data"))
        .and(query_param("region", "us-east"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1, "region": "us-east"}]
        })))
        .mount(&server)
        .await;

    // Region B
    Mock::given(method("GET"))
        .and(path("/api/data"))
        .and(query_param("region", "us-west"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 2, "region": "us-west"}]
        })))
        .mount(&server)
        .await;

    let http_config = HttpClientConfig::builder()
        .base_url(server.uri())
        .no_rate_limit()
        .build();
    let client = HttpClient::with_config(http_config);
    let state = crate::state::StateManager::in_memory();

    let mut engine = SyncEngine::new(client, state);

    let decoder = JsonDecoder::with_path("data");
    let paginator = NoPaginator;
    let router = ListRouter::new(vec!["us-east".to_string(), "us-west".to_string()], "region");

    let mut query_params = std::collections::HashMap::new();
    query_params.insert("region".to_string(), "{{ partition.region }}".to_string());

    let context = TemplateContext::new();

    let messages = engine
        .sync_partitioned_stream(
            "regional_data",
            &server.uri(),
            "/api/data",
            &query_params,
            &std::collections::HashMap::new(),
            &decoder,
            &paginator,
            &router,
            &context,
        )
        .await
        .unwrap();

    let record_msgs: Vec<_> = messages.iter().filter(|m| m.is_record()).collect();
    assert_eq!(record_msgs.len(), 2); // One per partition

    let stats = engine.stats();
    assert_eq!(stats.partitions_synced, 2);
}

#[tokio::test]
async fn test_sync_engine_skips_completed_partitions() {
    let server = MockServer::start().await;

    // Only region-b should be called
    Mock::given(method("GET"))
        .and(path("/api/data"))
        .and(query_param("region", "region-b"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 2}]
        })))
        .expect(1) // Should only be called once
        .mount(&server)
        .await;

    let http_config = HttpClientConfig::builder()
        .base_url(server.uri())
        .no_rate_limit()
        .build();
    let client = HttpClient::with_config(http_config);
    let state = crate::state::StateManager::in_memory();

    // Mark region-a as completed
    state
        .mark_partition_completed("stream", "region-a")
        .await
        .unwrap();

    let mut engine = SyncEngine::new(client, state);

    let decoder = JsonDecoder::with_path("data");
    let paginator = NoPaginator;
    let router = ListRouter::new(
        vec!["region-a".to_string(), "region-b".to_string()],
        "region",
    );

    let mut query_params = std::collections::HashMap::new();
    query_params.insert("region".to_string(), "{{ partition.region }}".to_string());

    let context = TemplateContext::new();

    engine
        .sync_partitioned_stream(
            "stream",
            &server.uri(),
            "/api/data",
            &query_params,
            &std::collections::HashMap::new(),
            &decoder,
            &paginator,
            &router,
            &context,
        )
        .await
        .unwrap();

    // region-a was skipped, only region-b was synced
    let stats = engine.stats();
    assert_eq!(stats.partitions_synced, 1);
}
