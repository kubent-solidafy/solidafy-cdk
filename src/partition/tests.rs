//! Tests for partition module

use super::*;
use chrono::{Duration, TimeZone, Utc};
use serde_json::json;

// ============================================================================
// PartitionValue Tests
// ============================================================================

#[test]
fn test_partition_value_new() {
    let pv = PartitionValue::new("test-id");
    assert_eq!(pv.id, "test-id");
    assert!(pv.values.is_empty());
}

#[test]
fn test_partition_value_with_value() {
    let pv = PartitionValue::new("id1")
        .with_value("key1", "value1")
        .with_value("key2", 42);

    assert_eq!(pv.get("key1"), Some(&json!("value1")));
    assert_eq!(pv.get("key2"), Some(&json!(42)));
}

#[test]
fn test_partition_value_with_string() {
    let pv = PartitionValue::new("id1").with_string("name", "test");

    assert_eq!(pv.get_string("name"), Some("test"));
}

// ============================================================================
// Partition Tests
// ============================================================================

#[test]
fn test_partition_new() {
    let p = Partition::new("partition-1");
    assert_eq!(p.id, "partition-1");
    assert!(!p.completed);
}

#[test]
fn test_partition_from_value() {
    let pv = PartitionValue::new("id1").with_string("key", "value");
    let p = Partition::from_value(pv);

    assert_eq!(p.id, "id1");
    assert_eq!(p.get("key"), Some(&json!("value")));
    assert!(!p.completed);
}

#[test]
fn test_partition_mark_completed() {
    let mut p = Partition::new("p1");
    assert!(!p.completed);

    p.mark_completed();
    assert!(p.completed);
}

// ============================================================================
// PartitionConfig Tests
// ============================================================================

#[test]
fn test_partition_config_default() {
    let config = PartitionConfig::default();
    assert!(matches!(config, PartitionConfig::None));
}

#[test]
fn test_partition_config_parent() {
    let config = PartitionConfig::parent("customers", "id", "customer_id");
    match config {
        PartitionConfig::Parent {
            parent_stream,
            parent_key,
            partition_field,
        } => {
            assert_eq!(parent_stream, "customers");
            assert_eq!(parent_key, "id");
            assert_eq!(partition_field, "customer_id");
        }
        _ => panic!("Expected Parent config"),
    }
}

#[test]
fn test_partition_config_list() {
    let config = PartitionConfig::list(vec!["a".to_string(), "b".to_string()], "value");
    match config {
        PartitionConfig::List {
            values,
            partition_field,
        } => {
            assert_eq!(values, vec!["a", "b"]);
            assert_eq!(partition_field, "value");
        }
        _ => panic!("Expected List config"),
    }
}

#[test]
fn test_partition_config_datetime() {
    let config =
        PartitionConfig::datetime("2024-01-01", "2024-01-31", "1d", "%Y-%m-%d", "start", "end");
    match config {
        PartitionConfig::Datetime {
            start,
            end,
            step,
            format,
            start_param,
            end_param,
        } => {
            assert_eq!(start, "2024-01-01");
            assert_eq!(end, "2024-01-31");
            assert_eq!(step, "1d");
            assert_eq!(format, "%Y-%m-%d");
            assert_eq!(start_param, "start");
            assert_eq!(end_param, "end");
        }
        _ => panic!("Expected Datetime config"),
    }
}

// ============================================================================
// ListRouter Tests
// ============================================================================

#[test]
fn test_list_router_basic() {
    let router = ListRouter::new(
        vec!["region-1".to_string(), "region-2".to_string()],
        "region",
    );

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].id, "region-1");
    assert_eq!(partitions[0].get_string("region"), Some("region-1"));
    assert_eq!(partitions[1].id, "region-2");
}

#[test]
fn test_list_router_empty() {
    let router = ListRouter::new(vec![], "value");

    let partitions = router.partitions().unwrap();
    assert!(partitions.is_empty());
}

#[test]
fn test_list_router_partition_field() {
    let router = ListRouter::new(vec![], "my_field");
    assert_eq!(router.partition_field(), "my_field");
}

// ============================================================================
// ParentRouter Tests
// ============================================================================

#[test]
fn test_parent_router_basic() {
    let records = vec![
        json!({"id": "cus_1", "name": "Customer 1"}),
        json!({"id": "cus_2", "name": "Customer 2"}),
    ];

    let router = ParentRouter::new(records, "id", "customer_id");

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].id, "cus_1");
    assert_eq!(partitions[0].get_string("customer_id"), Some("cus_1"));
}

#[test]
fn test_parent_router_numeric_key() {
    let records = vec![json!({"id": 123}), json!({"id": 456})];

    let router = ParentRouter::new(records, "id", "item_id");

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].id, "123");
}

#[test]
fn test_parent_router_nested_key() {
    let records = vec![json!({"data": {"id": "a"}}), json!({"data": {"id": "b"}})];

    let router = ParentRouter::new(records, "data.id", "nested_id");

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(partitions[0].id, "a");
}

#[test]
fn test_parent_router_deduplicates() {
    let records = vec![
        json!({"id": "dup"}),
        json!({"id": "unique"}),
        json!({"id": "dup"}), // Duplicate
    ];

    let router = ParentRouter::new(records, "id", "item_id");

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2); // Only 2 unique
}

#[test]
fn test_parent_router_missing_key() {
    let records = vec![
        json!({"id": "has_id"}),
        json!({"other": "no_id"}), // Missing key
    ];

    let router = ParentRouter::new(records, "id", "item_id");

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 1); // Only the one with id
}

#[test]
fn test_parent_router_empty() {
    let router = ParentRouter::empty("id", "item_id");
    let partitions = router.partitions().unwrap();
    assert!(partitions.is_empty());
}

#[test]
fn test_parent_router_set_records() {
    let mut router = ParentRouter::empty("id", "item_id");
    assert!(router.partitions().unwrap().is_empty());

    router.set_records(vec![json!({"id": "new_record"})]);
    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 1);
}

// ============================================================================
// DatetimeRouter Tests
// ============================================================================

#[test]
fn test_datetime_router_daily() {
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();

    let router = DatetimeRouter::new(
        start,
        end,
        Duration::days(1),
        "%Y-%m-%d",
        "start_date",
        "end_date",
    );

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 3); // 3 days

    assert_eq!(partitions[0].get_string("start_date"), Some("2024-01-01"));
    assert_eq!(partitions[0].get_string("end_date"), Some("2024-01-02"));

    assert_eq!(partitions[1].get_string("start_date"), Some("2024-01-02"));
    assert_eq!(partitions[1].get_string("end_date"), Some("2024-01-03"));

    assert_eq!(partitions[2].get_string("start_date"), Some("2024-01-03"));
    assert_eq!(partitions[2].get_string("end_date"), Some("2024-01-04"));
}

#[test]
fn test_datetime_router_hourly() {
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2024, 1, 1, 3, 0, 0).unwrap();

    let router = DatetimeRouter::new(
        start,
        end,
        Duration::hours(1),
        "%Y-%m-%dT%H:%M:%S",
        "start",
        "end",
    );

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 3);
}

#[test]
fn test_datetime_router_from_strings() {
    let router =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-03", "1d", "%Y-%m-%d", "start", "end")
            .unwrap();

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);
}

#[test]
fn test_datetime_router_partial_last() {
    // End doesn't align with step - last partition should be partial
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2024, 1, 2, 12, 0, 0).unwrap(); // 1.5 days

    let router = DatetimeRouter::new(
        start,
        end,
        Duration::days(1),
        "%Y-%m-%d %H:%M",
        "start",
        "end",
    );

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 2);

    // Last partition should end at actual end time
    assert_eq!(partitions[1].get_string("end"), Some("2024-01-02 12:00"));
}

#[test]
fn test_datetime_router_partition_field() {
    let router = DatetimeRouter::from_strings(
        "2024-01-01",
        "2024-01-02",
        "1d",
        "%Y-%m-%d",
        "my_start",
        "end",
    )
    .unwrap();

    assert_eq!(router.partition_field(), "my_start");
}

// ============================================================================
// Duration Parsing Tests
// ============================================================================

#[test]
fn test_parse_duration_days() {
    let router =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-10", "3d", "%Y-%m-%d", "s", "e")
            .unwrap();
    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 3); // 9 days / 3 = 3 partitions
}

#[test]
fn test_parse_duration_hours() {
    let router =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-01T06:00:00", "2h", "%H:%M", "s", "e")
            .unwrap();
    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 3); // 6 hours / 2 = 3 partitions
}

#[test]
fn test_parse_duration_weeks() {
    let router =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-22", "1w", "%Y-%m-%d", "s", "e")
            .unwrap();
    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 3); // 21 days / 7 = 3 partitions
}

#[test]
fn test_parse_duration_invalid() {
    let result =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-02", "invalid", "%Y-%m-%d", "s", "e");
    assert!(result.is_err());
}

// ============================================================================
// Datetime Parsing Tests
// ============================================================================

#[test]
fn test_parse_datetime_iso() {
    let router = DatetimeRouter::from_strings(
        "2024-01-15T10:30:00Z",
        "2024-01-16T10:30:00Z",
        "1d",
        "%Y-%m-%d",
        "s",
        "e",
    )
    .unwrap();

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 1);
}

#[test]
fn test_parse_datetime_date_only() {
    let router =
        DatetimeRouter::from_strings("2024-01-01", "2024-01-02", "1d", "%Y-%m-%d", "s", "e")
            .unwrap();

    let partitions = router.partitions().unwrap();
    assert_eq!(partitions.len(), 1);
}

#[test]
fn test_parse_datetime_invalid() {
    let result =
        DatetimeRouter::from_strings("not-a-date", "2024-01-02", "1d", "%Y-%m-%d", "s", "e");
    assert!(result.is_err());
}

// ============================================================================
// Async Job Tests
// ============================================================================

#[test]
fn test_async_job_config_default() {
    let config = AsyncJobConfig::default();
    assert_eq!(config.create_method, "POST");
    assert_eq!(config.poll_interval_secs, 5);
    assert_eq!(config.poll_max_attempts, 120);
    assert_eq!(config.completed_value, "JobComplete");
}

#[test]
fn test_async_job_config_builder() {
    let config = AsyncJobConfig::new()
        .with_create("POST", "/jobs", Some(r#"{"query": "SELECT *"}"#))
        .with_job_id_path("data.job_id")
        .with_poll("/jobs/{{ job_id }}/status", 10, 60)
        .with_status("status", "done", vec!["error", "cancelled"])
        .with_download("/jobs/{{ job_id }}/results", Some("records"));

    assert_eq!(config.create_method, "POST");
    assert_eq!(config.create_path, "/jobs");
    assert_eq!(
        config.create_body,
        Some(r#"{"query": "SELECT *"}"#.to_string())
    );
    assert_eq!(config.job_id_path, "data.job_id");
    assert_eq!(config.poll_path, "/jobs/{{ job_id }}/status");
    assert_eq!(config.poll_interval_secs, 10);
    assert_eq!(config.poll_max_attempts, 60);
    assert_eq!(config.status_path, "status");
    assert_eq!(config.completed_value, "done");
    assert_eq!(config.failed_values, vec!["error", "cancelled"]);
    assert_eq!(config.download_path, "/jobs/{{ job_id }}/results");
    assert_eq!(config.records_path, Some("records".to_string()));
}

#[test]
fn test_async_job_state_terminal() {
    assert!(!AsyncJobState::Created.is_terminal());
    assert!(!AsyncJobState::InProgress.is_terminal());
    assert!(AsyncJobState::Completed.is_terminal());
    assert!(AsyncJobState::Failed("error".to_string()).is_terminal());
}

#[test]
fn test_async_job_state_completed() {
    assert!(!AsyncJobState::Created.is_completed());
    assert!(!AsyncJobState::InProgress.is_completed());
    assert!(AsyncJobState::Completed.is_completed());
    assert!(!AsyncJobState::Failed("error".to_string()).is_completed());
}

#[test]
fn test_async_job_state_failed() {
    assert!(!AsyncJobState::Created.is_failed());
    assert!(!AsyncJobState::InProgress.is_failed());
    assert!(!AsyncJobState::Completed.is_failed());
    assert!(AsyncJobState::Failed("error".to_string()).is_failed());
}

#[test]
fn test_async_job_new() {
    let response = json!({"id": "job_123", "status": "pending"});
    let job = AsyncJob::new("job_123", response.clone());

    assert_eq!(job.id, "job_123");
    assert_eq!(job.state, AsyncJobState::Created);
    assert_eq!(job.response, response);
}

#[test]
fn test_async_job_update_state_completed() {
    let config = AsyncJobConfig::new().with_status("state", "JobComplete", vec!["Failed"]);

    let mut job = AsyncJob::new("job_123", json!({}));
    let poll_response = json!({"state": "JobComplete"});

    job.update_state(&config, &poll_response);
    assert_eq!(job.state, AsyncJobState::Completed);
}

#[test]
fn test_async_job_update_state_failed() {
    let config =
        AsyncJobConfig::new().with_status("state", "JobComplete", vec!["Failed", "Aborted"]);

    let mut job = AsyncJob::new("job_123", json!({}));
    let poll_response = json!({"state": "Failed"});

    job.update_state(&config, &poll_response);
    assert!(job.state.is_failed());
}

#[test]
fn test_async_job_update_state_in_progress() {
    let config = AsyncJobConfig::new().with_status("state", "JobComplete", vec!["Failed"]);

    let mut job = AsyncJob::new("job_123", json!({}));
    let poll_response = json!({"state": "InProgress"});

    job.update_state(&config, &poll_response);
    assert_eq!(job.state, AsyncJobState::InProgress);
}

#[test]
fn test_async_job_update_state_nested_path() {
    let config = AsyncJobConfig::new().with_status("job.status", "complete", vec!["error"]);

    let mut job = AsyncJob::new("job_123", json!({}));
    let poll_response = json!({"job": {"status": "complete"}});

    job.update_state(&config, &poll_response);
    assert_eq!(job.state, AsyncJobState::Completed);
}

// ============================================================================
// JSON Path Extraction Tests
// ============================================================================

#[test]
fn test_extract_json_path_simple() {
    let value = json!({"id": "123", "name": "test"});

    assert_eq!(extract_json_path(&value, "id"), Some(json!("123")));
    assert_eq!(extract_json_path(&value, "name"), Some(json!("test")));
}

#[test]
fn test_extract_json_path_nested() {
    let value = json!({
        "data": {
            "job": {
                "id": "job_456"
            }
        }
    });

    assert_eq!(
        extract_json_path(&value, "data.job.id"),
        Some(json!("job_456"))
    );
}

#[test]
fn test_extract_json_path_with_dollar() {
    let value = json!({"id": "123"});

    // Should work with or without $.
    assert_eq!(extract_json_path(&value, "$.id"), Some(json!("123")));
    assert_eq!(extract_json_path(&value, "id"), Some(json!("123")));
}

#[test]
fn test_extract_json_path_array_index() {
    let value = json!({
        "items": [
            {"id": "first"},
            {"id": "second"}
        ]
    });

    assert_eq!(
        extract_json_path(&value, "items[0].id"),
        Some(json!("first"))
    );
    assert_eq!(
        extract_json_path(&value, "items[1].id"),
        Some(json!("second"))
    );
}

#[test]
fn test_extract_json_path_missing() {
    let value = json!({"id": "123"});

    assert_eq!(extract_json_path(&value, "missing"), None);
    assert_eq!(extract_json_path(&value, "a.b.c"), None);
}

#[test]
fn test_extract_json_path_number() {
    let value = json!({"count": 42, "price": 19.99});

    assert_eq!(extract_json_path(&value, "count"), Some(json!(42)));
    assert_eq!(extract_json_path(&value, "price"), Some(json!(19.99)));
}

#[test]
fn test_extract_json_path_boolean() {
    let value = json!({"active": true, "deleted": false});

    assert_eq!(extract_json_path(&value, "active"), Some(json!(true)));
    assert_eq!(extract_json_path(&value, "deleted"), Some(json!(false)));
}
