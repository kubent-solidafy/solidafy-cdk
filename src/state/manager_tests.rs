//! Tests for StateManager

use super::*;
use tempfile::tempdir;

// ============================================================================
// Construction Tests
// ============================================================================

#[test]
fn test_state_manager_new() {
    let manager = StateManager::new("/tmp/test-state.json");
    assert!(!manager.is_in_memory());
    assert_eq!(manager.path().to_str().unwrap(), "/tmp/test-state.json");
}

#[test]
fn test_state_manager_without_auto_save() {
    let manager = StateManager::without_auto_save("/tmp/test-state.json");
    assert!(!manager.is_in_memory());
}

#[test]
fn test_state_manager_in_memory() {
    let manager = StateManager::in_memory();
    assert!(manager.is_in_memory());
}

// ============================================================================
// Cursor Tests
// ============================================================================

#[tokio::test]
async fn test_get_set_cursor() {
    let manager = StateManager::in_memory();

    // Initially no cursor
    assert!(manager.get_cursor("users").await.is_none());

    // Set cursor
    manager
        .set_cursor("users", "2024-01-01".to_string())
        .await
        .unwrap();

    // Get cursor
    assert_eq!(
        manager.get_cursor("users").await,
        Some("2024-01-01".to_string())
    );
}

#[tokio::test]
async fn test_cursor_update() {
    let manager = StateManager::in_memory();

    manager
        .set_cursor("users", "cursor1".to_string())
        .await
        .unwrap();
    manager
        .set_cursor("users", "cursor2".to_string())
        .await
        .unwrap();

    assert_eq!(
        manager.get_cursor("users").await,
        Some("cursor2".to_string())
    );
}

#[tokio::test]
async fn test_multiple_stream_cursors() {
    let manager = StateManager::in_memory();

    manager
        .set_cursor("users", "user_cursor".to_string())
        .await
        .unwrap();
    manager
        .set_cursor("orders", "order_cursor".to_string())
        .await
        .unwrap();

    assert_eq!(
        manager.get_cursor("users").await,
        Some("user_cursor".to_string())
    );
    assert_eq!(
        manager.get_cursor("orders").await,
        Some("order_cursor".to_string())
    );
}

// ============================================================================
// Partition Tests
// ============================================================================

#[tokio::test]
async fn test_partition_completed() {
    let manager = StateManager::in_memory();

    // Initially not completed
    assert!(!manager.is_partition_completed("stream", "p1").await);

    // Mark completed
    manager
        .mark_partition_completed("stream", "p1")
        .await
        .unwrap();

    // Now completed
    assert!(manager.is_partition_completed("stream", "p1").await);
    // Other partition still not completed
    assert!(!manager.is_partition_completed("stream", "p2").await);
}

#[tokio::test]
async fn test_partition_cursor() {
    let manager = StateManager::in_memory();

    // Initially no cursor
    assert!(manager.get_partition_cursor("stream", "p1").await.is_none());

    // Set cursor
    manager
        .set_partition_cursor("stream", "p1", "part_cursor".to_string())
        .await
        .unwrap();

    // Get cursor
    assert_eq!(
        manager.get_partition_cursor("stream", "p1").await,
        Some("part_cursor".to_string())
    );
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[tokio::test]
async fn test_save_and_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");

    // Create manager and set state
    let manager = StateManager::without_auto_save(&path);
    manager
        .set_cursor("users", "saved_cursor".to_string())
        .await
        .unwrap();
    manager
        .mark_partition_completed("users", "p1")
        .await
        .unwrap();
    manager.save().await.unwrap();

    // Create new manager and load
    let manager2 = StateManager::new(&path);
    manager2.load().await.unwrap();

    assert_eq!(
        manager2.get_cursor("users").await,
        Some("saved_cursor".to_string())
    );
    assert!(manager2.is_partition_completed("users", "p1").await);
}

#[tokio::test]
async fn test_load_nonexistent_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nonexistent.json");

    let manager = StateManager::new(&path);
    // Should not error on nonexistent file
    manager.load().await.unwrap();

    // State should be empty
    assert!(manager.get_cursor("users").await.is_none());
}

#[tokio::test]
async fn test_auto_save() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("auto_state.json");

    // Create manager with auto-save
    let manager = StateManager::new(&path);
    manager
        .set_cursor("users", "auto_cursor".to_string())
        .await
        .unwrap();

    // Create new manager and load (should have auto-saved)
    let manager2 = StateManager::new(&path);
    manager2.load().await.unwrap();

    assert_eq!(
        manager2.get_cursor("users").await,
        Some("auto_cursor".to_string())
    );
}

#[tokio::test]
async fn test_save_in_memory_noop() {
    let manager = StateManager::in_memory();
    manager
        .set_cursor("users", "cursor".to_string())
        .await
        .unwrap();
    // Should not error
    manager.save().await.unwrap();
}

// ============================================================================
// Clear Tests
// ============================================================================

#[tokio::test]
async fn test_clear_all() {
    let manager = StateManager::in_memory();

    manager
        .set_cursor("users", "cursor1".to_string())
        .await
        .unwrap();
    manager
        .set_cursor("orders", "cursor2".to_string())
        .await
        .unwrap();

    manager.clear().await.unwrap();

    assert!(manager.get_cursor("users").await.is_none());
    assert!(manager.get_cursor("orders").await.is_none());
}

#[tokio::test]
async fn test_clear_stream() {
    let manager = StateManager::in_memory();

    manager
        .set_cursor("users", "cursor1".to_string())
        .await
        .unwrap();
    manager
        .set_cursor("orders", "cursor2".to_string())
        .await
        .unwrap();

    manager.clear_stream("users").await.unwrap();

    assert!(manager.get_cursor("users").await.is_none());
    assert_eq!(
        manager.get_cursor("orders").await,
        Some("cursor2".to_string())
    );
}

// ============================================================================
// State Access Tests
// ============================================================================

#[tokio::test]
async fn test_state_read_access() {
    let manager = StateManager::in_memory();
    manager
        .set_cursor("users", "cursor".to_string())
        .await
        .unwrap();

    let state = manager.state().await;
    assert_eq!(state.get_cursor("users"), Some("cursor"));
}

#[tokio::test]
async fn test_state_write_access() {
    let manager = StateManager::in_memory();

    {
        let mut state = manager.state_mut().await;
        state.set_cursor("users", "direct_cursor".to_string());
    }

    assert_eq!(
        manager.get_cursor("users").await,
        Some("direct_cursor".to_string())
    );
}

// ============================================================================
// Clone Tests
// ============================================================================

#[tokio::test]
async fn test_clone_shares_state() {
    let manager = StateManager::in_memory();
    let cloned = manager.clone();

    manager
        .set_cursor("users", "shared_cursor".to_string())
        .await
        .unwrap();

    // Clone should see the same state
    assert_eq!(
        cloned.get_cursor("users").await,
        Some("shared_cursor".to_string())
    );
}

// ============================================================================
// Checkpoint Tests
// ============================================================================

#[tokio::test]
async fn test_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("checkpoint_state.json");

    let manager = StateManager::without_auto_save(&path);
    manager
        .set_cursor("users", "checkpoint_cursor".to_string())
        .await
        .unwrap();
    manager.checkpoint().await.unwrap();

    // Load in new manager
    let manager2 = StateManager::new(&path);
    manager2.load().await.unwrap();

    assert_eq!(
        manager2.get_cursor("users").await,
        Some("checkpoint_cursor".to_string())
    );
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_load_invalid_json() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid.json");

    // Write invalid JSON
    tokio::fs::write(&path, "{ invalid json }").await.unwrap();

    let manager = StateManager::new(&path);
    let result = manager.load().await;

    assert!(result.is_err());
}
