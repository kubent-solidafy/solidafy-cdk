//! Parquet file writer
//!
//! Provides utilities for writing Arrow RecordBatches to Parquet files.

use crate::error::{Error, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Configuration for Parquet writer
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    compression: Compression,
    row_group_size: usize,
    dictionary_enabled: bool,
    statistics_enabled: bool,
}

impl ParquetWriterConfig {
    /// Get dictionary encoding enabled
    #[must_use]
    pub fn is_dictionary_enabled(&self) -> bool {
        self.dictionary_enabled
    }

    /// Get statistics enabled
    #[must_use]
    pub fn is_statistics_enabled(&self) -> bool {
        self.statistics_enabled
    }

    /// Get row group size
    #[must_use]
    pub fn row_group_size(&self) -> usize {
        self.row_group_size
    }
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: Compression::SNAPPY,
            row_group_size: 1024 * 1024, // 1M rows
            dictionary_enabled: true,
            statistics_enabled: true,
        }
    }
}

impl ParquetWriterConfig {
    /// Create a new config with default settings
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set compression algorithm
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set row group size
    #[must_use]
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Enable or disable dictionary encoding
    #[must_use]
    pub fn with_dictionary(mut self, enabled: bool) -> Self {
        self.dictionary_enabled = enabled;
        self
    }

    /// Enable or disable statistics
    #[must_use]
    pub fn with_statistics(mut self, enabled: bool) -> Self {
        self.statistics_enabled = enabled;
        self
    }

    /// Use no compression
    #[must_use]
    pub fn uncompressed(mut self) -> Self {
        self.compression = Compression::UNCOMPRESSED;
        self
    }

    /// Use ZSTD compression
    #[must_use]
    pub fn zstd(mut self) -> Self {
        self.compression = Compression::ZSTD(parquet::basic::ZstdLevel::default());
        self
    }

    /// Use GZIP compression
    #[must_use]
    pub fn gzip(mut self) -> Self {
        self.compression = Compression::GZIP(parquet::basic::GzipLevel::default());
        self
    }

    /// Build writer properties
    fn build_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size);

        if !self.dictionary_enabled {
            builder = builder.set_dictionary_enabled(false);
        }

        if !self.statistics_enabled {
            builder =
                builder.set_statistics_enabled(parquet::file::properties::EnabledStatistics::None);
        }

        builder.build()
    }
}

/// Parquet file writer
pub struct ParquetWriter {
    /// Arrow writer
    writer: ArrowWriter<File>,
    /// Number of rows written
    rows_written: usize,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(
        path: impl AsRef<Path>,
        schema: &Schema,
        config: &ParquetWriterConfig,
    ) -> Result<Self> {
        let file = File::create(path.as_ref()).map_err(|e| Error::Output {
            message: format!("Failed to create file: {e}"),
        })?;

        let props = config.build_properties();
        let writer =
            ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).map_err(|e| {
                Error::Output {
                    message: format!("Failed to create Parquet writer: {e}"),
                }
            })?;

        Ok(Self {
            writer,
            rows_written: 0,
        })
    }

    /// Write a RecordBatch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(|e| Error::Output {
            message: format!("Failed to write batch: {e}"),
        })?;

        self.rows_written += batch.num_rows();
        Ok(())
    }

    /// Get the number of rows written so far
    #[must_use]
    pub fn rows_written(&self) -> usize {
        self.rows_written
    }

    /// Close the writer and finalize the file
    pub fn close(self) -> Result<usize> {
        let rows = self.rows_written;
        self.writer.close().map_err(|e| Error::Output {
            message: format!("Failed to close Parquet writer: {e}"),
        })?;
        Ok(rows)
    }
}

/// Write a single RecordBatch to a Parquet file
pub fn write_batch_to_parquet(
    path: impl AsRef<Path>,
    batch: &RecordBatch,
    config: Option<&ParquetWriterConfig>,
) -> Result<usize> {
    let default_config = ParquetWriterConfig::default();
    let config = config.unwrap_or(&default_config);

    let mut writer = ParquetWriter::new(path, batch.schema().as_ref(), config)?;
    writer.write(batch)?;
    writer.close()
}

/// Write multiple RecordBatches to a Parquet file
pub fn write_batches_to_parquet(
    path: impl AsRef<Path>,
    batches: &[RecordBatch],
    config: Option<&ParquetWriterConfig>,
) -> Result<usize> {
    if batches.is_empty() {
        return Err(Error::Output {
            message: "No batches to write".to_string(),
        });
    }

    let default_config = ParquetWriterConfig::default();
    let config = config.unwrap_or(&default_config);

    let mut writer = ParquetWriter::new(path, batches[0].schema().as_ref(), config)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()
}
