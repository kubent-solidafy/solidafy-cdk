//! State manager implementation
//!
//! Provides file-based state persistence with atomic writes.

use super::types::State;
use crate::error::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// State manager for persisting and loading state
#[derive(Debug)]
pub struct StateManager {
    /// Path to the state file
    path: PathBuf,
    /// Current state (cached)
    state: Arc<RwLock<State>>,
    /// Whether to auto-save on every update
    auto_save: bool,
}

impl StateManager {
    /// Create a new state manager with the given path
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            state: Arc::new(RwLock::new(State::new())),
            auto_save: true,
        }
    }

    /// Create a state manager with auto-save disabled
    pub fn without_auto_save(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            state: Arc::new(RwLock::new(State::new())),
            auto_save: false,
        }
    }

    /// Create an in-memory state manager (no file persistence)
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            state: Arc::new(RwLock::new(State::new())),
            auto_save: false,
        }
    }

    /// Create a state manager from a file, loading existing state if present
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            let contents = std::fs::read_to_string(&path).map_err(|e| Error::State {
                message: format!("Failed to read state file: {e}"),
            })?;
            serde_json::from_str(&contents).map_err(|e| Error::State {
                message: format!("Failed to parse state file: {e}"),
            })?
        } else {
            State::new()
        };

        Ok(Self {
            path,
            state: Arc::new(RwLock::new(state)),
            auto_save: true,
        })
    }

    /// Create a state manager from inline JSON string
    pub fn from_json(json: &str) -> Result<Self> {
        let state: State = serde_json::from_str(json).map_err(|e| Error::State {
            message: format!("Failed to parse state JSON: {e}"),
        })?;

        Ok(Self {
            path: PathBuf::new(),
            state: Arc::new(RwLock::new(state)),
            auto_save: false,
        })
    }

    /// Save state to a specific file path
    pub async fn save_to_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let state = self.state.read().await;
        let contents = serde_json::to_string_pretty(&*state).map_err(|e| Error::State {
            message: format!("Failed to serialize state: {e}"),
        })?;

        // Write to temp file first, then rename for atomicity
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");
        tokio::fs::write(&temp_path, &contents)
            .await
            .map_err(|e| Error::State {
                message: format!("Failed to write state file: {e}"),
            })?;

        tokio::fs::rename(&temp_path, path)
            .await
            .map_err(|e| Error::State {
                message: format!("Failed to rename state file: {e}"),
            })?;

        Ok(())
    }

    /// Load state from file
    pub async fn load(&self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        let contents = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| Error::State {
                message: format!("Failed to read state file: {e}"),
            })?;

        let loaded_state: State = serde_json::from_str(&contents).map_err(|e| Error::State {
            message: format!("Failed to parse state file: {e}"),
        })?;

        let mut state = self.state.write().await;
        *state = loaded_state;

        Ok(())
    }

    /// Save current state to file
    pub async fn save(&self) -> Result<()> {
        if self.path.as_os_str().is_empty() {
            return Ok(()); // In-memory mode
        }

        let state = self.state.read().await;
        let contents = serde_json::to_string_pretty(&*state).map_err(|e| Error::State {
            message: format!("Failed to serialize state: {e}"),
        })?;

        // Write to temp file first, then rename for atomicity
        let temp_path = self.path.with_extension("tmp");
        tokio::fs::write(&temp_path, &contents)
            .await
            .map_err(|e| Error::State {
                message: format!("Failed to write state file: {e}"),
            })?;

        tokio::fs::rename(&temp_path, &self.path)
            .await
            .map_err(|e| Error::State {
                message: format!("Failed to rename state file: {e}"),
            })?;

        Ok(())
    }

    /// Get a read lock on the current state
    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, State> {
        self.state.read().await
    }

    /// Export state as JSON string
    pub async fn to_json(&self) -> Result<String> {
        let state = self.state.read().await;
        serde_json::to_string(&*state).map_err(|e| Error::State {
            message: format!("Failed to serialize state: {e}"),
        })
    }

    /// Export state as pretty-printed JSON string
    pub async fn to_json_pretty(&self) -> Result<String> {
        let state = self.state.read().await;
        serde_json::to_string_pretty(&*state).map_err(|e| Error::State {
            message: format!("Failed to serialize state: {e}"),
        })
    }

    /// Get a write lock on the current state
    pub async fn state_mut(&self) -> tokio::sync::RwLockWriteGuard<'_, State> {
        self.state.write().await
    }

    /// Get cursor for a stream
    pub async fn get_cursor(&self, stream: &str) -> Option<String> {
        let state = self.state.read().await;
        state.get_cursor(stream).map(ToString::to_string)
    }

    /// Set cursor for a stream
    pub async fn set_cursor(&self, stream: &str, cursor: String) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.set_cursor(stream, cursor);
        }

        if self.auto_save {
            self.save().await?;
        }

        Ok(())
    }

    /// Check if a partition is completed
    pub async fn is_partition_completed(&self, stream: &str, partition_id: &str) -> bool {
        let state = self.state.read().await;
        state
            .get_stream(stream)
            .is_some_and(|s| s.is_partition_completed(partition_id))
    }

    /// Mark a partition as completed
    pub async fn mark_partition_completed(&self, stream: &str, partition_id: &str) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state
                .get_stream_mut(stream)
                .mark_partition_completed(partition_id);
        }

        if self.auto_save {
            self.save().await?;
        }

        Ok(())
    }

    /// Get partition cursor
    pub async fn get_partition_cursor(&self, stream: &str, partition_id: &str) -> Option<String> {
        let state = self.state.read().await;
        state
            .get_stream(stream)?
            .get_partition(partition_id)?
            .cursor
            .clone()
    }

    /// Set partition cursor
    pub async fn set_partition_cursor(
        &self,
        stream: &str,
        partition_id: &str,
        cursor: String,
    ) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state
                .get_stream_mut(stream)
                .get_partition_mut(partition_id)
                .cursor = Some(cursor);
        }

        if self.auto_save {
            self.save().await?;
        }

        Ok(())
    }

    /// Clear all state
    pub async fn clear(&self) -> Result<()> {
        {
            let mut state = self.state.write().await;
            *state = State::new();
        }

        if self.auto_save {
            self.save().await?;
        }

        Ok(())
    }

    /// Clear state for a specific stream
    pub async fn clear_stream(&self, stream: &str) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.streams.remove(stream);
        }

        if self.auto_save {
            self.save().await?;
        }

        Ok(())
    }

    /// Get the state file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check if using in-memory mode
    pub fn is_in_memory(&self) -> bool {
        self.path.as_os_str().is_empty()
    }

    /// Create a checkpoint (alias for save)
    pub async fn checkpoint(&self) -> Result<()> {
        self.save().await
    }
}

impl Clone for StateManager {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            state: Arc::clone(&self.state),
            auto_save: self.auto_save,
        }
    }
}
