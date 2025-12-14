# Iceberg Output Support (Planned)

This document outlines the planned implementation for direct Iceberg table output in solidafy-cdk.

## Overview

Currently solidafy-cdk outputs to:
- **Parquet** files (local or cloud: S3, R2, GCS, Azure)
- **JSON** (stdout or file)

The planned Iceberg support will allow writing directly to Iceberg tables via REST catalog, enabling:
- Schema evolution
- Time travel queries
- ACID transactions
- Compatible with DuckDB, Spark, Trino, etc.

## Planned Usage

```bash
# Write to Iceberg table via REST catalog
solidafy-cdk read \
  --connector stripe \
  --config-json '{"api_key":"sk_live_..."}' \
  --streams customers \
  --format iceberg \
  --catalog-uri "https://catalog.example.com/..." \
  --catalog-token "..." \
  --warehouse "s3://bucket/warehouse" \
  --destination "namespace.customers"
```

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Connector      │ --> │ Arrow 56     │ --> │ Arrow 55        │
│  (API/DB sync)  │     │ RecordBatch  │     │ Conversion      │
└─────────────────┘     └──────────────┘     └─────────────────┘
                                                      │
                                                      v
                              ┌─────────────────────────────────┐
                              │ Iceberg DataFileWriter          │
                              │ - Writes Parquet with field IDs │
                              │ - ZSTD compression              │
                              └─────────────────────────────────┘
                                                      │
                                                      v
                              ┌─────────────────────────────────┐
                              │ REST Catalog                    │
                              │ - Create namespace/table        │
                              │ - fast_append commit            │
                              └─────────────────────────────────┘
                                                      │
                                                      v
                              ┌─────────────────────────────────┐
                              │ Object Storage (S3/R2/GCS)      │
                              │ - Parquet data files            │
                              │ - Iceberg metadata              │
                              └─────────────────────────────────┘
```

## Version Alignment with solidafy-agent

To ensure compatibility, we align versions with the working solidafy-agent implementation:

| Component | solidafy-agent | solidafy-cdk (current) | solidafy-cdk (target) |
|-----------|---------------|------------------------|----------------------|
| DuckDB | 1.4 | 1.1 | **1.4** |
| Arrow (DuckDB) | 56 | 53 | **56** |
| Parquet | 56 | 53 | **56** |
| iceberg | 0.7 | - | **0.7** |
| iceberg-catalog-rest | 0.7 | - | **0.7** |
| Arrow 55 (iceberg) | 55 | - | **55** |
| Parquet 55 (iceberg) | 55 | - | **55** |

**Key Insight**: DuckDB 1.4 uses Arrow 56, but iceberg-rust 0.7 uses Arrow 55 internally.
We need BOTH Arrow versions side-by-side. Cargo handles this with package renaming.

## Dependencies Required

```toml
# Upgrade DuckDB to 1.4 (uses Arrow 56)
duckdb = { version = "1.4", features = ["bundled"] }

# Arrow/Parquet 56 to match DuckDB 1.4
arrow = { version = "56", features = ["json"] }
parquet = { version = "56", features = ["arrow"] }

# Apache Iceberg for REST catalog + direct S3 writes
# Uses Arrow 55 internally - Cargo handles both 55 and 56 side-by-side
iceberg = { version = "0.7", default-features = false, features = ["storage-s3", "tokio"] }
iceberg-catalog-rest = "0.7"

# Arrow 55 for iceberg-rust compatibility (renamed to avoid conflicts)
arrow-array-55 = { package = "arrow-array", version = "55" }
arrow-schema-55 = { package = "arrow-schema", version = "55" }
arrow-buffer-55 = { package = "arrow-buffer", version = "55" }

# Parquet 55 for iceberg-rust compatibility
parquet-55 = { package = "parquet", version = "55" }
```

## Implementation Components

### 1. IcebergConfig

```rust
pub struct IcebergConfig {
    /// Catalog URI (e.g., "https://catalog.cloudflarestorage.com/...")
    pub catalog_uri: String,
    /// API token for authentication
    pub token: String,
    /// Warehouse identifier
    pub warehouse: String,
    /// S3/R2 endpoint URL (for data files)
    pub s3_endpoint: Option<String>,
    /// AWS region
    pub region: String,
    /// AWS credentials
    pub access_key_id: String,
    pub secret_access_key: String,
}
```

### 2. Partition Configuration

Iceberg supports various partition transforms:

```rust
pub enum PartitionTransform {
    Identity,           // Use value as-is
    Year,              // Extract year from date/timestamp
    Month,             // Extract month
    Day,               // Extract day
    Hour,              // Extract hour
    Bucket(u32),       // Hash into N buckets
    Truncate(u32),     // Truncate to width W
}

pub struct PartitionField {
    pub source_column: String,
    pub transform: PartitionTransform,
    pub name: Option<String>,
}
```

### 3. Arrow Version Conversion

solidafy-cdk uses Arrow 53 (via DuckDB 1.2), but iceberg-rust uses Arrow 55. We need a conversion layer:

```rust
pub fn convert_arrow53_to_arrow55(
    batch: &arrow::array::RecordBatch,  // Arrow 53
) -> Result<arrow55_array::RecordBatch>  // Arrow 55
```

This converts by rebuilding batches column by column, matching data types.

### 4. Schema Conversion

Convert Arrow schema to Iceberg schema with field IDs:

```rust
fn arrow_schema_to_iceberg(arrow_schema: &arrow55_schema::Schema) -> Result<Schema> {
    // Map Arrow types to Iceberg types
    // Assign field IDs (1-based)
    // Handle nested types (Struct, List, Map)
}
```

### 5. Data File Writer

Write Arrow batches to Parquet files in S3 with Iceberg metadata:

```rust
pub async fn write_batches_to_iceberg(
    table: &Table,
    batches: Vec<arrow55_array::RecordBatch>,
) -> Result<Vec<DataFile>> {
    // Use iceberg-rust's DataFileWriter
    // Adds PARQUET:field_id metadata to columns
    // Uses ZSTD compression
    // Returns DataFile metadata for commit
}
```

### 6. Catalog Operations

```rust
impl IcebergCatalog {
    /// Connect to REST catalog
    pub async fn connect(config: IcebergConfig) -> Result<Self>;

    /// Load existing table
    pub async fn load_table(&self, namespace: &str, table: &str) -> Result<Table>;

    /// Create table with schema and partition spec
    pub async fn create_table(
        &self,
        namespace: &str,
        table: &str,
        arrow_schema: &arrow55_schema::Schema,
        partition_config: Option<&PartitionConfig>,
    ) -> Result<Table>;

    /// Load or create table
    pub async fn load_or_create_table(...) -> Result<(Table, bool)>;
}
```

### 7. Transaction Commit

```rust
pub async fn fast_append_files(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    data_files: Vec<DataFile>,
) -> Result<i64> {
    // Create transaction
    // fast_append (no rewrite, just add files)
    // Commit and return snapshot ID
}
```

## Compatible Catalogs

- **Cloudflare R2 Data Catalog** (REST API)
- **AWS Glue** (via REST adapter)
- **Tabular** (REST API)
- **Nessie** (REST API)
- **Polaris** (REST API)
- Custom REST catalogs following Iceberg REST specification

## Querying Iceberg Tables

Once data is written, query with DuckDB:

```sql
-- Attach Iceberg catalog
ATTACH 'https://catalog.example.com/...' AS iceberg (TYPE ICEBERG);

-- Query table
SELECT * FROM iceberg.namespace.customers;

-- Time travel
SELECT * FROM iceberg.namespace.customers
FOR VERSION AS OF 123456789;
```

## Reference Implementation

The working implementation exists in `solidafy-agent` and can be largely copied:

| File | Description | Reuse Strategy |
|------|-------------|----------------|
| `src/utils/iceberg_writer.rs` | Full IcebergCatalog, Arrow conversion, DataFileWriter | Copy directly, rename Arrow 56→55 refs |
| `src/tasks/transform.rs` | PartitionConfig, PartitionTransform types | Copy partition types only |

### Key Functions to Copy

```rust
// From iceberg_writer.rs - these work as-is with Arrow 55/56:
pub struct IcebergConfig { ... }
pub struct IcebergCatalog { ... }
impl IcebergCatalog {
    pub async fn connect(config: IcebergConfig) -> Result<Self>;
    pub async fn load_table(&self, namespace: &str, table: &str) -> Result<Table>;
    pub async fn create_table(...) -> Result<Table>;
    pub async fn load_or_create_table(...) -> Result<(Table, bool)>;
}

fn arrow_schema_to_iceberg(arrow_schema: &arrow55_schema::Schema) -> Result<Schema>;
fn build_partition_spec(...) -> Result<UnboundPartitionSpec>;
pub fn convert_arrow56_to_arrow55(batch: &RecordBatch) -> Result<arrow55_array::RecordBatch>;
pub async fn write_batches_to_iceberg(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>>;
pub async fn fast_append_files(catalog: &Arc<dyn Catalog>, table: &Table, data_files: Vec<DataFile>) -> Result<i64>;
```

## Binary Size Impact

Adding Iceberg support will increase binary size by approximately 5-10MB due to:
- iceberg-rust crate (~3MB)
- Additional Arrow version 55 (~2MB)
- Additional Parquet version 55 (~2MB)

Current binary: ~26MB → Expected with Iceberg: ~32-36MB

## Implementation Plan

### Phase 1: Upgrade Dependencies (Low Risk)
- [ ] Upgrade DuckDB 1.1 → 1.4
- [ ] Upgrade Arrow 53 → 56
- [ ] Upgrade Parquet 53 → 56
- [ ] Fix any breaking changes from upgrades
- [ ] Verify all existing tests pass

### Phase 2: Add Iceberg Dependencies
- [ ] Add iceberg = "0.7" with storage-s3 feature
- [ ] Add iceberg-catalog-rest = "0.7"
- [ ] Add Arrow 55 packages (renamed)
- [ ] Add Parquet 55 (renamed)
- [ ] Verify compilation succeeds

### Phase 3: Copy Core Implementation
- [ ] Create `src/iceberg/mod.rs` module
- [ ] Copy IcebergConfig, IcebergCatalog from solidafy-agent
- [ ] Copy Arrow 56→55 conversion functions
- [ ] Copy schema conversion (arrow_schema_to_iceberg)
- [ ] Copy partition spec builder
- [ ] Copy write_batches_to_iceberg
- [ ] Copy fast_append_files

### Phase 4: Integrate with CLI
- [ ] Add `--format iceberg` option
- [ ] Add `--catalog-uri`, `--catalog-token`, `--warehouse` flags
- [ ] Add `--destination` flag for table name (namespace.table)
- [ ] Add `--partition` flag for partition config
- [ ] Update runner.rs to handle Iceberg output path

### Phase 5: Integrate with HTTP Server
- [ ] Add `/sync` endpoint support for format=iceberg
- [ ] Return snapshot_id in response
- [ ] Handle table creation vs append

### Phase 6: Testing & Documentation
- [ ] Unit tests for Arrow conversion
- [ ] Integration tests with mock REST catalog
- [ ] Update README with Iceberg examples
- [ ] Update Medium article (if publishing)
