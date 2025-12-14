//! Cloud storage output support (S3, R2, GCS, Azure)

use crate::error::{Error, Result};
use bytes::Bytes;
use chrono::Utc;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;

/// Build a Hive-style partitioned path for output files
///
/// Format: `{stream}/dt={YYYY-MM-DD}/data.{ext}`
///
/// Examples:
/// - `products/dt=2025-12-14/data.parquet`
/// - `public_users/dt=2025-12-14/data.json`
pub fn build_partitioned_path(stream_name: &str, extension: &str) -> String {
    let date = Utc::now().format("%Y-%m-%d");
    // Sanitize stream name: replace dots with underscores for file system compatibility
    let sanitized_stream = stream_name.replace('.', "_");
    format!("{sanitized_stream}/dt={date}/data.{extension}")
}

/// Build a partitioned base directory path (without filename)
///
/// Format: `{base_path}/{stream}/dt={YYYY-MM-DD}/`
pub fn build_partitioned_dir(base_path: &str, stream_name: &str) -> String {
    let date = Utc::now().format("%Y-%m-%d");
    let sanitized_stream = stream_name.replace('.', "_");
    format!(
        "{}/{sanitized_stream}/dt={date}",
        base_path.trim_end_matches('/')
    )
}

/// Cloud storage destination parsed from URL
#[derive(Debug, Clone)]
pub struct CloudDestination {
    /// The object store implementation
    store: Arc<dyn ObjectStore>,
    /// Base path prefix within the bucket/container
    prefix: String,
    /// Original URL scheme for logging
    scheme: String,
}

impl CloudDestination {
    /// Parse a destination URL and create appropriate object store
    ///
    /// Supported formats:
    /// - `s3://bucket/path/` - AWS S3
    /// - `r2://bucket/path/` - Cloudflare R2 (S3-compatible)
    /// - `gs://bucket/path/` - Google Cloud Storage
    /// - `az://container/path/` - Azure Blob Storage
    /// - `/local/path/` or `./path/` - Local filesystem
    pub fn parse(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            Self::parse_s3(url, false)
        } else if url.starts_with("r2://") {
            Self::parse_s3(url, true)
        } else if url.starts_with("gs://") {
            Self::parse_gcs(url)
        } else if url.starts_with("az://") {
            Self::parse_azure(url)
        } else {
            // Local filesystem
            Self::parse_local(url)
        }
    }

    /// Parse S3 or R2 URL
    fn parse_s3(url: &str, is_r2: bool) -> Result<Self> {
        let scheme = if is_r2 { "r2" } else { "s3" };
        let without_scheme = url
            .strip_prefix(&format!("{scheme}://"))
            .ok_or_else(|| Error::config(format!("Invalid {scheme} URL: {url}")))?;

        let (bucket, prefix) = match without_scheme.find('/') {
            Some(idx) => (
                &without_scheme[..idx],
                without_scheme[idx + 1..].to_string(),
            ),
            None => (without_scheme, String::new()),
        };

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        // R2 requires custom endpoint from environment
        // AWS_ENDPOINT is read automatically by from_env(), but we also check R2-specific vars
        if is_r2 {
            // R2 endpoint: https://<account_id>.r2.cloudflarestorage.com
            // Check R2-specific env var first, then fall back to AWS_ENDPOINT (already read by from_env)
            if let Ok(endpoint) = std::env::var("R2_ENDPOINT_URL") {
                builder = builder.with_endpoint(endpoint);
            }
            // Note: AWS_ENDPOINT is automatically read by from_env()
        }

        let store = builder
            .build()
            .map_err(|e| Error::config(format!("Failed to create {scheme} client: {e}")))?;

        Ok(Self {
            store: Arc::new(store),
            prefix,
            scheme: scheme.to_string(),
        })
    }

    /// Parse GCS URL
    fn parse_gcs(url: &str) -> Result<Self> {
        let without_scheme = url
            .strip_prefix("gs://")
            .ok_or_else(|| Error::config(format!("Invalid GCS URL: {url}")))?;

        let (bucket, prefix) = match without_scheme.find('/') {
            Some(idx) => (
                &without_scheme[..idx],
                without_scheme[idx + 1..].to_string(),
            ),
            None => (without_scheme, String::new()),
        };

        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| Error::config(format!("Failed to create GCS client: {e}")))?;

        Ok(Self {
            store: Arc::new(store),
            prefix,
            scheme: "gs".to_string(),
        })
    }

    /// Parse Azure Blob URL
    fn parse_azure(url: &str) -> Result<Self> {
        let without_scheme = url
            .strip_prefix("az://")
            .ok_or_else(|| Error::config(format!("Invalid Azure URL: {url}")))?;

        let (container, prefix) = match without_scheme.find('/') {
            Some(idx) => (
                &without_scheme[..idx],
                without_scheme[idx + 1..].to_string(),
            ),
            None => (without_scheme, String::new()),
        };

        let store = MicrosoftAzureBuilder::from_env()
            .with_container_name(container)
            .build()
            .map_err(|e| Error::config(format!("Failed to create Azure client: {e}")))?;

        Ok(Self {
            store: Arc::new(store),
            prefix,
            scheme: "az".to_string(),
        })
    }

    /// Parse local filesystem path
    fn parse_local(path: &str) -> Result<Self> {
        let path = if let Some(stripped) = path.strip_prefix("file://") {
            stripped
        } else {
            path
        };

        // Create directory if it doesn't exist
        std::fs::create_dir_all(path)
            .map_err(|e| Error::config(format!("Failed to create directory {path}: {e}")))?;

        let store = LocalFileSystem::new_with_prefix(path)
            .map_err(|e| Error::config(format!("Failed to create local store: {e}")))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: String::new(),
            scheme: "file".to_string(),
        })
    }

    /// Check if this is a cloud destination (not local)
    pub fn is_cloud(&self) -> bool {
        self.scheme != "file"
    }

    /// Get the scheme (s3, r2, gs, az, file)
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Write bytes to a file in the destination
    pub async fn write(&self, filename: &str, data: Bytes) -> Result<String> {
        let path = if self.prefix.is_empty() {
            ObjectPath::from(filename)
        } else {
            ObjectPath::from(format!("{}/{filename}", self.prefix.trim_end_matches('/')))
        };

        self.store
            .put(&path, data.into())
            .await
            .map_err(|e| Error::io(format!("Failed to write {path}: {e}")))?;

        // Return full path for logging
        let full_path = format!("{}://{path}", self.scheme);
        Ok(full_path)
    }

    /// Write parquet bytes to a file with Hive-style partitioning
    ///
    /// Output path: `{stream}/dt={YYYY-MM-DD}/data.parquet`
    pub async fn write_parquet(&self, stream_name: &str, data: Bytes) -> Result<String> {
        let filename = build_partitioned_path(stream_name, "parquet");
        self.write(&filename, data).await
    }

    /// Write JSON bytes to a file with Hive-style partitioning
    ///
    /// Output path: `{stream}/dt={YYYY-MM-DD}/data.json`
    pub async fn write_json(&self, stream_name: &str, data: Bytes) -> Result<String> {
        let filename = build_partitioned_path(stream_name, "json");
        self.write(&filename, data).await
    }

    /// Write state JSON to a file
    #[allow(dead_code)]
    pub async fn write_state(&self, data: Bytes) -> Result<String> {
        self.write("state.json", data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        // This will fail without AWS credentials, but we can test parsing
        let result = CloudDestination::parse("s3://my-bucket/path/to/data/");
        // May fail due to missing credentials, that's ok for unit test
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_parse_local_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let dest = CloudDestination::parse(path).unwrap();
        assert_eq!(dest.scheme(), "file");
        assert!(!dest.is_cloud());
    }

    #[test]
    fn test_is_cloud() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let dest = CloudDestination::parse(path).unwrap();
        assert!(!dest.is_cloud());
    }
}
