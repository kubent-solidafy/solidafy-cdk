# Solidafy CDK

A Rust-native Connector Development Kit for building high-performance REST API data connectors. **Built-in connectors are embedded in the binary** - just use `--connector stripe`. Custom connectors can be defined in YAML. Sync data to JSON or Parquet, with built-in support for pagination, authentication, incremental sync, and more.

## Why Solidafy CDK?

Built as a lightweight alternative to Airbyte for REST API data extraction:

| Feature | Airbyte | Solidafy CDK |
|---------|---------|--------------|
| **Binary size** | N/A (Docker images) | **26MB** (with DuckDB for database connectors) |
| **Memory usage** | 2-4GB minimum | **~10MB** |
| **Connector storage** | ~200MB per connector | Embedded in binary |
| **Startup time** | 30-60 seconds | **<1 second** |
| **Dependencies** | Docker, PostgreSQL, Scheduler | **None** |
| **Connector definition** | Python SDK | **YAML files** |
| **Cloud output** | Via destinations | **Direct to S3/R2/GCS/Azure** |
| **HTTP API** | Complex setup | **Single binary server** |
| **Lambda/Edge compatible** | No | **Yes** |

**Use Solidafy CDK when:**
- You need a lightweight extraction tool that runs anywhere
- You're syncing REST APIs to a data lake (Parquet files)
- You want to embed data sync in Lambda, Edge, or small containers
- You prefer YAML over Python for connector definitions

**Use Airbyte when:**
- You need 300+ pre-built connectors
- You want managed scheduling and UI
- You want built-in transformations (dbt integration)

## Features

- **YAML-based connector definitions** - No code required for REST API connectors
- **Database connectors** - PostgreSQL, MySQL, SQLite via embedded DuckDB (no YAML needed)
- **Multiple authentication methods** - Bearer, API Key, Basic Auth, OAuth2
- **Flexible pagination** - Cursor, Offset, Page Number, Link Header
- **Incremental sync** - Track state per stream with cursor fields
- **Multiple output formats** - JSON (streaming), Parquet files
- **Cloud storage output** - Write directly to S3, R2, GCS, or Azure
- **Partition routing** - List-based, Date Range, Parent-Child, Async Jobs
- **Rate limiting** - Built-in rate limiter with configurable RPS
- **Retry logic** - Exponential/linear/constant backoff with configurable retries
- **Template engine** - Jinja-like templates for dynamic URLs and parameters

## Installation

### Pre-built Binaries (Recommended)

```bash
# Linux (x86_64)
curl -sL https://github.com/kubent-solidafy/solidafy-cdk/releases/latest/download/solidafy-cdk-linux-x86_64 -o solidafy-cdk
chmod +x solidafy-cdk
sudo mv solidafy-cdk /usr/local/bin/

# Linux (ARM64)
curl -sL https://github.com/kubent-solidafy/solidafy-cdk/releases/latest/download/solidafy-cdk-linux-aarch64 -o solidafy-cdk
chmod +x solidafy-cdk
sudo mv solidafy-cdk /usr/local/bin/

# macOS (Intel)
curl -sL https://github.com/kubent-solidafy/solidafy-cdk/releases/latest/download/solidafy-cdk-darwin-x86_64 -o solidafy-cdk
chmod +x solidafy-cdk
sudo mv solidafy-cdk /usr/local/bin/

# macOS (Apple Silicon)
curl -sL https://github.com/kubent-solidafy/solidafy-cdk/releases/latest/download/solidafy-cdk-darwin-aarch64 -o solidafy-cdk
chmod +x solidafy-cdk
sudo mv solidafy-cdk /usr/local/bin/
```

### Docker

```bash
docker pull ghcr.io/YOUR_ORG/solidafy-cdk:latest
docker run --rm ghcr.io/YOUR_ORG/solidafy-cdk list
```

### Build from Source

```bash
cargo build --release
# Binary will be at target/release/solidafy-cdk
```

## Built-in Connectors

| Name | Aliases | Category | Description |
|------|---------|----------|-------------|
| `stripe` | - | Payments | Stripe payments, customers, invoices, subscriptions |
| `openai` | `openai-billing` | AI/ML | OpenAI API usage, costs, and billing data |
| `anthropic` | `anthropic-billing` | AI/ML | Anthropic API usage and billing data |
| `cloudflare` | `cloudflare-billing` | Infrastructure | Cloudflare billing and usage data |
| `github` | `github-billing` | Developer Tools | GitHub Actions, Copilot, Packages billing |
| `salesforce` | - | CRM | Salesforce CRM objects via REST API |
| `salesforce-bulk` | - | CRM | Salesforce CRM objects via Bulk API 2.0 |
| `hubspot` | - | CRM | HubSpot CRM contacts, companies, deals |
| `shopify` | - | E-commerce | Shopify orders, products, customers |
| `zendesk` | - | Support | Zendesk tickets, users, organizations |
| `postgres` | `postgresql` | Database | PostgreSQL tables via DuckDB |
| `mysql` | `mariadb` | Database | MySQL/MariaDB tables via DuckDB |
| `sqlite` | - | Database | SQLite database tables via DuckDB |

## Quick Start

```bash
# List built-in connectors
solidafy-cdk list

# Check connection (using built-in connector)
solidafy-cdk check --connector stripe --config-json '{"api_key": "sk_live_..."}'

# List available streams
solidafy-cdk streams --connector stripe

# Sync data to JSON
solidafy-cdk read \
  --connector stripe \
  --config-json '{"api_key": "sk_live_..."}' \
  --streams customers,invoices

# Sync data to Parquet with state tracking
solidafy-cdk read \
  --connector stripe \
  --config-json '{"api_key": "sk_live_..."}' \
  --streams customers,invoices \
  --output /path/to/output \
  --format parquet \
  --state /path/to/state.json

# Use custom connector from YAML file
solidafy-cdk read --connector ./my-custom-connector.yaml --config-json '...'
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `list` | List built-in connectors embedded in the binary |
| `check` | Test connection to the API |
| `streams` | List available stream names (lightweight, no schemas) |
| `discover` | Discover available streams with full JSON schemas |
| `read` | Sync data from streams |
| `spec` | Show connector specification |
| `validate` | Validate connector YAML definition |
| `serve` | Start HTTP server mode for REST API access |

### Read Command Options

```
solidafy-cdk read [OPTIONS]

Options:
  -c, --connector <CONNECTOR>    Connector definition file (YAML)
      --streams <STREAMS>        Streams to sync (comma-separated, empty = all)
      --config-json <JSON>       Inline config JSON
  -C, --config <CONFIG>          Configuration file (JSON)
  -o, --output <OUTPUT>          Output directory for parquet files
  -s, --state <STATE>            State file (JSON)
      --state-json <JSON>        Inline state JSON
  -f, --format <FORMAT>          Output format: json, pretty, parquet [default: json]
      --max-records <N>          Maximum records per stream
      --state-per-page           Emit state after each page
  -v, --verbose                  Verbose output
```

## HTTP Server Mode

For frontend/backend integration, run solidafy-cdk as an HTTP server.
**Built-in connectors are embedded** - no `--connectors-dir` needed.

```bash
solidafy-cdk serve --port 8080
```

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/connectors` | List built-in connectors with metadata |
| GET | `/connectors/:name/streams` | Get stream names (use `stripe`, `openai`, etc.) |
| POST | `/streams` | Get stream names (with body) |
| POST | `/check` | Test API connection |
| POST | `/discover` | Get full catalog with schemas |
| POST | `/sync` | **Sync data** - full read operation via HTTP |

### Example Usage

```bash
# Health check
curl http://localhost:8080/health
# {"status":"ok"}

# Get all connectors with config_schema AND streams (one call = everything)
curl http://localhost:8080/connectors | jq '.data.connectors[0]'
# {
#   "name": "stripe",
#   "description": "Stripe payments, customers, invoices, subscriptions",
#   "category": "Payments",
#   "aliases": [],
#   "config_schema": [
#     {"name": "api_key", "type": "string", "required": true, "secret": true, "description": "Stripe API key..."}
#   ],
#   "streams": ["customers", "products", "prices", "charges", "payment_intents", ...]
# }

# Test connection with user-provided credentials
curl -X POST http://localhost:8080/check \
  -H "Content-Type: application/json" \
  -d '{"connector":"stripe","config":{"api_key":"sk_live_..."}}'
# {"success":true,"data":{"type":"CONNECTION_STATUS","connectionStatus":{"status":"SUCCEEDED"}}}

# Optional: Discover schemas (for schema-aware storage)
curl -X POST http://localhost:8080/discover \
  -H "Content-Type: application/json" \
  -d '{"connector":"openai","config":{"admin_api_key":"sk-admin-..."},"sample":0}'
```

### POST /sync - Full Data Sync via HTTP

Sync data directly via HTTP without shelling out to the CLI:

```bash
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "shopify",
    "config": {
      "shop_domain": "my-store.myshopify.com",
      "access_token": "shpat_..."
    },
    "streams": ["products", "orders"],
    "format": "json",
    "max_records": 100
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "type": "SYNC_RESULT",
    "result": {
      "status": "SUCCEEDED",
      "connector": "shopify",
      "total_records": 100,
      "streams": [
        {"stream": "products", "status": "SUCCESS", "records_synced": 50},
        {"stream": "orders", "status": "SUCCESS", "records_synced": 50}
      ],
      "state": {"streams": {"products": {"cursor": "2024-12-01T00:00:00Z"}}},
      "records": [
        {"stream": "products", "data": {"id": 123, "title": "..."}, "emitted_at": 1702500000000}
      ]
    }
  }
}
```

**Sync to Cloud Storage (Parquet):**
```bash
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "stripe",
    "config": {"api_key": "sk_live_..."},
    "streams": ["customers", "invoices"],
    "format": "parquet",
    "output": "s3://my-bucket/stripe/"
  }'
```

**Request Body Fields:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connector` | string | Yes | Connector name (e.g., `stripe`, `shopify`) |
| `config` | object | Yes | Connector configuration |
| `streams` | array | No | Streams to sync (default: all) |
| `format` | string | No | `json` or `parquet` (default: `json`) |
| `output` | string | No | Cloud destination for parquet (s3://, r2://, gs://, az://) |
| `state` | object | No | Previous state for incremental sync |
| `max_records` | number | No | Limit records per stream |

### Frontend Integration Flow

**Everything via HTTP - no CLI needed:**

1. `GET /connectors` - Returns all connectors with:
   - `config_schema`: Fields needed for config forms (type, required, secret, description)
   - `streams`: Available data streams
   - `description`, `category`, `aliases`
2. `POST /check` - Validate user credentials
3. `POST /sync` - Pull data (records in response or write to cloud)

No subprocess spawning. No stdout parsing. Just REST.

## Database Connectors

Solidafy CDK includes native database support via embedded DuckDB. Connect to PostgreSQL, MySQL, or SQLite databases without YAML files - streams (tables) are discovered dynamically.

### Supported Databases

| Connector | Aliases | Description |
|-----------|---------|-------------|
| `postgres` | `postgresql` | PostgreSQL via DuckDB postgres extension |
| `mysql` | `mariadb` | MySQL/MariaDB via DuckDB mysql extension |
| `sqlite` | - | SQLite via DuckDB sqlite extension |

### Database vs REST API Connectors

| Feature | REST API Connectors | Database Connectors |
|---------|---------------------|---------------------|
| **Definition** | YAML files | Native (no YAML) |
| **Streams** | Defined in YAML | Auto-discovered from tables |
| **Pagination** | Cursor/Offset/Page | Automatic batching |
| **Auth** | OAuth, API Key, etc. | Connection string |

### HTTP API for Databases

Database connectors work through the same HTTP endpoints, but streams are dynamic:

```bash
# Check connection
curl -X POST http://localhost:8080/check \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {
      "connection_string": "postgresql://user:pass@host:5432/database"
    }
  }'
# {"success":true,"data":{"connectionStatus":{"status":"SUCCEEDED","message":"Connection successful. Found 42 tables."}}}

# Discover tables (streams)
curl -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {
      "connection_string": "postgresql://user:pass@host:5432/database"
    }
  }'
# {"success":true,"data":{"streams":["public.users","public.orders","public.products"...]}}

# Sync specific tables to JSON
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {
      "connection_string": "postgresql://user:pass@host:5432/database"
    },
    "streams": ["public.users", "public.orders"],
    "format": "json"
  }'

# Sync to Parquet with incremental cursor
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {
      "connection_string": "postgresql://user:pass@host:5432/database"
    },
    "streams": ["public.orders"],
    "format": "parquet",
    "output": "s3://my-bucket/postgres/",
    "cursor_fields": {
      "public.orders": "updated_at"
    }
  }'
```

### Incremental Sync for Databases

Use `cursor_fields` to enable incremental sync. The sync returns state that you pass to the next sync:

```bash
# First sync - full table
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {"connection_string": "postgresql://..."},
    "streams": ["public.orders"],
    "cursor_fields": {"public.orders": "created_at"}
  }'
# Response includes: "state": {"public.orders": {"cursor": "2024-12-15T10:30:00Z"}}

# Subsequent sync - only new records
curl -X POST http://localhost:8080/sync \
  -H "Content-Type: application/json" \
  -d '{
    "connector": "postgres",
    "config": {"connection_string": "postgresql://..."},
    "streams": ["public.orders"],
    "cursor_fields": {"public.orders": "created_at"},
    "state": {"public.orders": {"cursor": "2024-12-15T10:30:00Z"}}
  }'
# Only returns records where created_at > "2024-12-15T10:30:00Z"
```

### Database Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection_string` | string | Yes* | Full connection URL |
| `host` | string | No | Database host (if not using connection_string) |
| `port` | number | No | Database port |
| `database` | string | No | Database name |
| `user` | string | No | Username |
| `password` | string | No | Password |
| `ssl_mode` | string | No | SSL mode (default: "prefer") |

*Either `connection_string` or individual fields are required.

### Connection String Formats

```bash
# PostgreSQL
postgresql://user:password@host:5432/database

# MySQL
mysql://user:password@host:3306/database

# SQLite (local file)
/path/to/database.db
```

### GET /connectors Response for Databases

Database connectors show `streams_dynamic: true` to indicate tables must be discovered:

```json
{
  "name": "postgres",
  "description": "PostgreSQL database tables via DuckDB",
  "category": "Database",
  "streams": null,
  "streams_dynamic": true,
  "streams_hint": "Call POST /streams with connection config to discover tables",
  "config_schema": [
    {"name": "connection_string", "type": "string", "required": true, "secret": true}
  ]
}
```

## Connector YAML Schema

```yaml
name: my-connector
version: "1.0.0"
base_url: "https://api.example.com/v1"

# Authentication
auth:
  type: bearer  # bearer, api_key, basic, oauth2_refresh_token
  token: "{{ config.api_key }}"

# HTTP settings
http:
  timeout_secs: 30
  max_retries: 3
  rate_limit_rps: 10

# Connection check endpoint
check:
  path: /me

# Default headers
headers:
  Content-Type: application/json

# Stream definitions
streams:
  - name: users
    request:
      path: /users
      params:
        limit: "100"
    decoder:
      type: json
      records_path: data.users
    pagination:
      type: cursor
      cursor_param: cursor
      cursor_path: meta.next_cursor
    primary_key:
      - id
    cursor_field: updated_at
```

## Authentication Types

### Bearer Token
```yaml
auth:
  type: bearer
  token: "{{ config.api_key }}"
```

### API Key
```yaml
auth:
  type: api_key
  key: X-API-Key           # Header name
  value: "{{ config.key }}"
  location: header         # header or query
```

### Basic Auth
```yaml
auth:
  type: basic
  username: "{{ config.username }}"
  password: "{{ config.password }}"
```

### OAuth2 Refresh Token
```yaml
auth:
  type: oauth2_refresh_token
  token_url: "https://oauth.example.com/token"
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  refresh_token: "{{ config.refresh_token }}"
```

## Pagination Types

### Cursor-based
```yaml
pagination:
  type: cursor
  cursor_param: cursor        # Query parameter name
  cursor_path: meta.next      # JSON path to next cursor
  location: query             # query or header
```

### Offset-based
```yaml
pagination:
  type: offset
  offset_param: offset
  limit_param: limit
  limit: 100
  stop:
    type: total_count
    path: meta.total
```

### Page Number
```yaml
pagination:
  type: page_number
  page_param: page
  start_page: 1
  page_size_param: per_page
  page_size: 50
```

### Link Header (RFC 5988)
```yaml
pagination:
  type: link_header
  rel: next
```

## Partition Routers

### List Partition
```yaml
partition:
  type: list
  field: region
  values:
    - us-east
    - us-west
    - eu-west
```

### Date Range Partition
```yaml
partition:
  type: date_range
  field: date
  start: "{{ config.start_date }}"
  end: "{{ config.end_date }}"
  step: P1D  # ISO 8601 duration
  format: "%Y-%m-%d"
```

### Parent Stream Partition
```yaml
partition:
  type: parent_stream
  stream: organizations
  field: org_id
  parent_field: id
```

### Async Job (Bulk APIs)
```yaml
partition:
  type: async_job
  create:
    method: POST
    path: /jobs/query
    body: |
      {"query": "SELECT * FROM Account"}
    job_id_path: id
  poll:
    path: /jobs/query/{{ job_id }}
    interval_secs: 5
    max_attempts: 120
    status_path: state
    completed_value: JobComplete
    failed_values:
      - Failed
      - Aborted
  download:
    path: /jobs/query/{{ job_id }}/results
```

## Decoder Types

### JSON
```yaml
decoder:
  type: json
  records_path: data.items  # Optional, extracts nested array
```

### CSV
```yaml
decoder:
  type: csv
```

### XML
```yaml
decoder:
  type: xml
  records_path: response.items.item
```

## Incremental Sync

Solidafy CDK tracks sync state per stream using cursor fields.

### First Sync
```bash
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_..."}' \
  --streams customers

# Output ends with STATE message:
# {"type":"STATE","state":{"streams":{"customers":{"cursor":"2024-01-15T10:30:00Z"}}}}
```

### Subsequent Syncs
```bash
# Pass previous state to resume from last cursor
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_..."}' \
  --state-json '{"streams":{"customers":{"cursor":"2024-01-15T10:30:00Z"}}}' \
  --streams customers
```

### State File
```bash
# Or use a state file for persistence
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_..."}' \
  --state /path/to/state.json \
  --streams customers
```

## Output Formats

### JSON (Streaming)
```bash
solidafy-cdk read --connector connector.yaml --format json
```

Output is newline-delimited JSON (one message per line). See [Output Message Protocol](#output-message-protocol) for complete details.

### Parquet (Local)
```bash
solidafy-cdk read \
  --connector connector.yaml \
  --format parquet \
  --output /path/to/output

# Creates Hive-style partitioned structure:
# /path/to/output/users/dt=2025-12-14/data.parquet
# /path/to/output/orders/dt=2025-12-14/data.parquet
```

### Output Directory Structure

Solidafy CDK uses **Hive-style partitioning** for organized, query-optimized output:

```
output/
├── customers/
│   └── dt=2025-12-14/
│       └── data.parquet
├── orders/
│   └── dt=2025-12-14/
│       └── data.parquet
└── products/
    └── dt=2025-12-14/
        └── data.json
```

**Why Hive partitioning?**
- **Query optimization** - Tools like DuckDB, Spark, and Athena can prune partitions
- **Incremental loads** - Each day's data is isolated, making it easy to reload or delete
- **Standard format** - Compatible with data lake tools and cataloging systems
- **Time travel** - Keep historical snapshots by date

The partition uses the **ingestion date** (`dt=YYYY-MM-DD`), not the data's timestamp, ensuring consistent organization regardless of source data dates.

### Parquet (Cloud Storage)

Write Parquet files directly to cloud storage without local filesystem:

```bash
# AWS S3
solidafy-cdk read --connector stripe --format parquet --output s3://my-bucket/stripe/

# Cloudflare R2 (set R2_ENDPOINT_URL or AWS_ENDPOINT_URL)
export R2_ENDPOINT_URL=https://accountid.r2.cloudflarestorage.com
solidafy-cdk read --connector stripe --format parquet --output r2://my-bucket/stripe/

# Google Cloud Storage
solidafy-cdk read --connector stripe --format parquet --output gs://my-bucket/stripe/

# Azure Blob Storage
solidafy-cdk read --connector stripe --format parquet --output az://my-container/stripe/
```

**Environment Variables for Cloud Auth:**

| Provider | Variable | Description |
|----------|----------|-------------|
| **S3** | `AWS_ACCESS_KEY_ID` | Access key ID |
| | `AWS_SECRET_ACCESS_KEY` | Secret access key |
| | `AWS_DEFAULT_REGION` | Region (e.g., `us-east-1`) |
| | `AWS_SESSION_TOKEN` | Session token (optional, for temporary credentials) |
| | `AWS_ENDPOINT` | Custom endpoint URL (for S3-compatible services) |
| **R2** | `AWS_ACCESS_KEY_ID` | R2 Access Key ID |
| | `AWS_SECRET_ACCESS_KEY` | R2 Secret Access Key |
| | `AWS_ENDPOINT` | `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` |
| **GCS** | `GOOGLE_SERVICE_ACCOUNT` | Path to service account JSON file |
| | `GOOGLE_SERVICE_ACCOUNT_KEY` | Service account JSON as string |
| **Azure** | `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name |
| | `AZURE_STORAGE_ACCOUNT_KEY` | Storage account key |
| | `AZURE_STORAGE_CLIENT_ID` | Client ID (for service principal) |
| | `AZURE_STORAGE_CLIENT_SECRET` | Client secret (for service principal) |
| | `AZURE_STORAGE_TENANT_ID` | Tenant ID (for service principal) |

---

## Output Message Protocol

Every command outputs newline-delimited JSON messages to stdout. Parse each line as JSON.

### Message Types

| Type | Description | When Emitted |
|------|-------------|--------------|
| `LOG` | Informational messages | During sync |
| `RECORD` | Data records | During sync |
| `STATE` | Sync state for incremental | During and after sync |
| `SYNC_SUMMARY` | Final sync result | **Always last** after `read` |
| `CONNECTION_STATUS` | Connection test result | After `check` |
| `CATALOG` | Available streams | After `discover` |

### LOG Message
```json
{
  "type": "LOG",
  "log": {
    "level": "INFO",
    "message": "Starting sync for stream: customers"
  }
}
```
Levels: `DEBUG`, `INFO`, `WARN`, `ERROR`

### RECORD Message
```json
{
  "type": "RECORD",
  "record": {
    "stream": "customers",
    "data": {
      "id": "cus_123",
      "email": "user@example.com",
      "created": 1701388800
    },
    "emitted_at": 1702500000000
  }
}
```

### STATE Message (Per-Stream)
Emitted during sync with cursor updates:
```json
{
  "type": "STATE",
  "state": {
    "type": "STREAM",
    "stream": {
      "stream_descriptor": { "name": "customers" },
      "stream_state": { "cursor": "2024-12-01T00:00:00Z" }
    }
  }
}
```

### STATE Message (Final/Global)
Emitted at end of sync with all stream cursors:
```json
{
  "type": "STATE",
  "state": {
    "streams": {
      "customers": { "cursor": "2024-12-01T00:00:00Z" },
      "invoices": { "cursor": "2024-12-15T10:30:00Z" }
    }
  }
}
```
**Save this state and pass it to `--state-json` for incremental sync.**

### SYNC_SUMMARY Message (Always Last)
**This is the most important message for programmatic consumption.** It is always the last message after a `read` command.

```json
{
  "type": "SYNC_SUMMARY",
  "summary": {
    "status": "SUCCEEDED",
    "connector": "stripe",
    "total_records": 1500,
    "total_streams": 3,
    "successful_streams": 3,
    "failed_streams": 0,
    "duration_ms": 12500,
    "output": {
      "format": "parquet",
      "directory": "/data/stripe",
      "state_file": "/data/stripe/state.json"
    },
    "streams": [
      {
        "stream": "customers",
        "status": "SUCCESS",
        "records_synced": 500,
        "duration_ms": 3200,
        "output_file": "/data/stripe/customers/dt=2025-12-14/data.parquet"
      },
      {
        "stream": "invoices",
        "status": "SUCCESS",
        "records_synced": 1000,
        "duration_ms": 8500,
        "output_file": "/data/stripe/invoices/dt=2025-12-14/data.parquet"
      }
    ]
  }
}
```

**Summary Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `SUCCEEDED`, `FAILED`, or `PARTIAL` |
| `connector` | string | Connector name from YAML |
| `total_records` | number | Total records synced across all streams |
| `total_streams` | number | Number of streams attempted |
| `successful_streams` | number | Number of streams that succeeded |
| `failed_streams` | number | Number of streams that failed |
| `duration_ms` | number | Total sync duration in milliseconds |
| `output.format` | string | Output format: `json`, `pretty`, or `parquet` |
| `output.directory` | string? | Output directory (if `--output` specified) |
| `output.state_file` | string? | State file path (if `--state` specified) |

**Per-Stream Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `stream` | string | Stream name |
| `status` | string | `SUCCESS` or `FAILED` |
| `records_synced` | number | Records synced for this stream |
| `duration_ms` | number | Stream sync duration in milliseconds |
| `output_file` | string? | Parquet file path (only for parquet format) |
| `error` | string? | Error message (only if `FAILED`) |

**Failed Stream Example:**
```json
{
  "stream": "payments",
  "status": "FAILED",
  "error": "HTTP 401: Unauthorized",
  "records_synced": 0,
  "duration_ms": 800
}
```

**JSON Output Example (no files):**
```json
{
  "type": "SYNC_SUMMARY",
  "summary": {
    "status": "SUCCEEDED",
    "connector": "stripe",
    "total_records": 100,
    "total_streams": 1,
    "successful_streams": 1,
    "failed_streams": 0,
    "duration_ms": 2500,
    "output": {
      "format": "json",
      "directory": null,
      "state_file": null
    },
    "streams": [
      {
        "stream": "customers",
        "status": "SUCCESS",
        "records_synced": 100,
        "duration_ms": 2500
      }
    ]
  }
}
```

### CONNECTION_STATUS Message
Result of `check` command:
```json
{
  "type": "CONNECTION_STATUS",
  "connectionStatus": {
    "status": "SUCCEEDED",
    "message": "Connection successful"
  }
}
```

### STREAMS Message
Result of `streams` command (lightweight alternative to CATALOG when you only need stream names):
```json
{
  "type": "STREAMS",
  "connector": "stripe",
  "streams": ["customers", "charges", "invoices", "subscriptions"]
}
```

### CATALOG Message
Result of `discover` command:
```json
{
  "type": "CATALOG",
  "catalog": {
    "streams": [
      {
        "name": "customers",
        "json_schema": { "type": "object", "properties": {...} },
        "supported_sync_modes": ["full_refresh", "incremental"],
        "source_defined_cursor": true,
        "default_cursor_field": ["created"],
        "source_defined_primary_key": [["id"]]
      }
    ]
  }
}
```

---

## Message Order (read command)

Messages are emitted in this order:

1. `LOG` (INFO): "Starting sync for stream: X"
2. `LOG` (DEBUG): "Page N: fetched M records" (during pagination)
3. `RECORD`: Data records (may be batched)
4. `STATE` (per-stream): Cursor updates
5. `LOG` (INFO): "Completed sync for X: N records in M pages"
6. *(Repeat 1-5 for each stream)*
7. `STATE` (global): Final state with all cursors
8. `SYNC_SUMMARY`: **Always last** - overall sync result

---

## Parsing Output (Examples)

### Python (JSON output)
```python
import subprocess
import json

result = subprocess.run([
    'solidafy-cdk', 'read',
    '--connector', 'connectors/stripe.yaml',
    '--config-json', '{"api_key":"sk_live_xxx"}',
    '--streams', 'customers,invoices'
], capture_output=True, text=True)

records = []
sync_summary = None

for line in result.stdout.strip().split('\n'):
    if not line:
        continue
    msg = json.loads(line)

    if msg['type'] == 'RECORD':
        records.append(msg['record']['data'])
    elif msg['type'] == 'SYNC_SUMMARY':
        sync_summary = msg['summary']

# Check sync result
if sync_summary['status'] == 'SUCCEEDED':
    print(f"Success! Synced {sync_summary['total_records']} records")
elif sync_summary['status'] == 'PARTIAL':
    failed = [s for s in sync_summary['streams'] if s['status'] == 'FAILED']
    print(f"Partial success. Failed streams: {[s['stream'] for s in failed]}")
else:
    print(f"Sync failed!")
    for s in sync_summary['streams']:
        if s['status'] == 'FAILED':
            print(f"  {s['stream']}: {s['error']}")
```

### Python (Parquet output with state file)
```python
import subprocess
import json

result = subprocess.run([
    'solidafy-cdk', 'read',
    '--connector', 'connectors/stripe.yaml',
    '--config-json', '{"api_key":"sk_live_xxx"}',
    '--streams', 'customers,invoices',
    '--format', 'parquet',
    '--output', '/data/stripe',
    '--state', '/data/stripe/state.json'
], capture_output=True, text=True)

# Parse SYNC_SUMMARY (always last line)
for line in result.stdout.strip().split('\n'):
    msg = json.loads(line)
    if msg['type'] == 'SYNC_SUMMARY':
        summary = msg['summary']
        break

# Check results
print(f"Status: {summary['status']}")
print(f"Total records: {summary['total_records']}")
print(f"Duration: {summary['duration_ms']}ms")

# Get file paths from SYNC_SUMMARY
print(f"\nOutput format: {summary['output']['format']}")
print(f"Output directory: {summary['output']['directory']}")
print(f"State file: {summary['output']['state_file']}")

# Get per-stream parquet files
print("\nParquet files:")
for stream in summary['streams']:
    if stream['status'] == 'SUCCESS':
        print(f"  {stream['stream']}: {stream.get('output_file', 'N/A')}")
        print(f"    Records: {stream['records_synced']}, Duration: {stream['duration_ms']}ms")
    else:
        print(f"  {stream['stream']}: FAILED - {stream['error']}")
```

### Bash (jq)
```bash
# Run sync with parquet output
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key":"sk_live_xxx"}' \
  --streams customers,invoices \
  --format parquet \
  --output /data/stripe \
  --state /data/stripe/state.json > output.jsonl

# Get sync summary (always last line)
SUMMARY=$(tail -1 output.jsonl)

# Check status
echo $SUMMARY | jq -r '.summary.status'
# Output: SUCCEEDED

# Get state file path
echo $SUMMARY | jq -r '.summary.output.state_file'
# Output: /data/stripe/state.json

# Get all parquet file paths
echo $SUMMARY | jq -r '.summary.streams[] | select(.status=="SUCCESS") | .output_file'
# Output:
# /data/stripe/customers/dt=2025-12-14/data.parquet
# /data/stripe/invoices/dt=2025-12-14/data.parquet

# Get per-stream results as table
echo $SUMMARY | jq -r '.summary.streams[] | [.stream, .status, .records_synced, .output_file // "N/A"] | @tsv'
```

## Template Variables

Templates use `{{ variable }}` syntax with access to:

| Variable | Description |
|----------|-------------|
| `config.*` | Configuration values from `--config-json` |
| `partition.*` | Current partition values |
| `state.*` | Current state values |
| `job_id` | Async job ID (in async_job partition) |

Example:
```yaml
request:
  path: /accounts/{{ config.account_id }}/transactions
  params:
    start_date: "{{ config.start_date }}"
    region: "{{ partition.region }}"
```

---

## Included Connectors

### Tested with Live APIs

| Connector | Streams | Auth Type | Status |
|-----------|---------|-----------|--------|
| **OpenAI Billing** | 16 streams (usage_completions, costs, embeddings, etc.) | Bearer (Admin API Key) | Tested |
| **Anthropic Billing** | 17 streams (usage_messages, cost_report, workspaces, etc.) | API Key (Admin) | Tested |
| **Cloudflare Billing** | 3 streams (account, billing_profile, subscriptions) | Bearer (API Token) | Tested |
| **Stripe** | 20 streams (customers, invoices, subscriptions, etc.) | Bearer | Tested |
| **GitHub Billing** | 8 streams (actions_billing, copilot_billing, copilot_seats, etc.) | Bearer (PAT) | Tested |

### Defined (Not Live Tested)

| Connector | Streams | Auth Type | Status |
|-----------|---------|-----------|--------|
| **Salesforce** | 6 streams (accounts, contacts, leads, etc.) | OAuth2 | Defined |
| **Salesforce Bulk** | 4 streams (async job pattern) | OAuth2 | Defined |
| **HubSpot** | 8 streams (contacts, companies, deals, etc.) | Bearer | Defined |
| **Shopify** | 3 streams (products, orders, customers) | API Key | Defined |
| **Zendesk** | 6 streams (tickets, users, organizations, etc.) | Basic Auth | Defined |

---

## Connector Configuration Reference

Each connector requires specific configuration values passed via `--config-json` or `--config` file. This section documents all required and optional fields for each connector.

> **Note**: Connector YAML files are NOT compiled into the binary. They are external files that you reference with `--connector path/to/connector.yaml`. This makes it easy to customize connectors or add new ones without recompiling.

---

### OpenAI Billing (`openai-billing.yaml`)

Syncs usage and cost data from OpenAI's Admin API.

#### How to Get Credentials

1. Go to [OpenAI Platform Settings](https://platform.openai.com/settings)
2. Navigate to **Organization** → **Admin API Keys**
3. Click **Create admin key**
4. Copy the key (starts with `sk-admin-`)

> **Important**: Regular API keys (`sk-...`) won't work. You need an **Admin API Key** for billing data.

#### Configuration Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `admin_api_key` | **Yes** | string | Admin API key starting with `sk-admin-` |
| `start_time` | **Yes** | string | Unix timestamp (seconds) for data start date |

#### Example Config

```json
{
  "admin_api_key": "sk-admin-abc123...",
  "start_time": "1701388800"
}
```

#### Getting `start_time`

```bash
# Get Unix timestamp for a specific date
date -d "2024-12-01" +%s          # Linux
date -j -f "%Y-%m-%d" "2024-12-01" +%s  # macOS

# Example: December 1, 2024 = 1701388800
```

#### Full Example

```bash
# Sync all usage and cost data since Dec 1, 2024
solidafy-cdk read \
  --connector connectors/openai-billing.yaml \
  --config-json '{"admin_api_key": "sk-admin-abc123...", "start_time": "1701388800"}' \
  --streams costs,usage_completions,usage_embeddings
```

#### Streams (16 total)

| Stream | Description |
|--------|-------------|
| `usage_completions` | GPT-4, GPT-3.5, o1 usage (aggregated) |
| `usage_completions_by_model` | Usage grouped by model |
| `usage_completions_by_project` | Usage grouped by project |
| `usage_embeddings` | Embeddings usage |
| `usage_embeddings_by_model` | Embeddings by model |
| `usage_images` | DALL-E usage |
| `usage_images_by_model` | Images by model |
| `usage_audio_speeches` | TTS usage |
| `usage_audio_transcriptions` | Whisper usage |
| `usage_moderations` | Moderation API usage |
| `usage_vector_stores` | Vector store usage |
| `usage_code_interpreter` | Code interpreter usage |
| `costs` | Daily costs (matches invoices) |
| `costs_by_project` | Costs by project |
| `costs_by_line_item` | Costs by line item |

---

### Anthropic Billing (`anthropic-billing.yaml`)

Syncs usage, cost, and organization data from Anthropic's Admin API.

#### How to Get Credentials

1. Go to [Anthropic Console](https://console.anthropic.com/)
2. Navigate to **Settings** → **Admin API Keys**
3. Click **Create Key** with Admin permissions
4. Copy the key (starts with `sk-ant-admin`)

> **Important**: Regular API keys won't work. You need an **Admin API Key** for billing data.

#### Configuration Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `admin_api_key` | **Yes** | string | Admin API key starting with `sk-ant-admin` |
| `start_date` | **Yes** | string | ISO 8601 datetime for usage/cost data |
| `start_date_short` | **Yes** | string | Date only (YYYY-MM-DD) for Claude Code analytics |

#### Example Config

```json
{
  "admin_api_key": "sk-ant-admin01-abc123...",
  "start_date": "2024-12-01T00:00:00Z",
  "start_date_short": "2024-12-01"
}
```

> **Note**: Two date formats are needed because different API endpoints use different formats.

#### Full Example

```bash
# Sync usage, costs, and organization data
solidafy-cdk read \
  --connector connectors/anthropic-billing.yaml \
  --config-json '{
    "admin_api_key": "sk-ant-admin01-abc123...",
    "start_date": "2024-12-01T00:00:00Z",
    "start_date_short": "2024-12-01"
  }' \
  --streams usage_messages,cost_report,workspaces

# Sync only Claude Code analytics
solidafy-cdk read \
  --connector connectors/anthropic-billing.yaml \
  --config-json '{
    "admin_api_key": "sk-ant-admin01-abc123...",
    "start_date": "2024-12-01T00:00:00Z",
    "start_date_short": "2024-12-01"
  }' \
  --streams usage_claude_code,usage_claude_code_by_user
```

#### Streams (17 total)

| Stream | Description |
|--------|-------------|
| `organization` | Organization info |
| `usage_messages` | Messages API usage (aggregated) |
| `usage_messages_by_model` | Usage by model |
| `usage_messages_by_workspace` | Usage by workspace |
| `usage_messages_by_api_key` | Usage by API key |
| `usage_messages_detailed` | Full breakdown (model + workspace + tier) |
| `cost_report` | Daily costs |
| `cost_report_by_workspace` | Costs by workspace |
| `cost_report_by_line_item` | Costs by line item |
| `usage_claude_code` | Claude Code analytics |
| `usage_claude_code_by_user` | Claude Code by user |
| `workspaces` | Organization workspaces |
| `api_keys` | API keys |
| `users` | Organization members |
| `invites` | Pending invites |

---

### Cloudflare Billing (`cloudflare-billing.yaml`)

Syncs billing and subscription data from Cloudflare.

#### How to Get Credentials

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to **My Profile** → **API Tokens**
3. Click **Create Token**
4. Create a **Custom Token** with these permissions:
   - **Account** → **Billing** → **Read**
5. Copy the token
6. Get your Account ID from the URL: `dash.cloudflare.com/{account_id}/...`

> **Important**: Global API Keys work but are less secure. Use API Tokens with minimal permissions.

#### Configuration Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `api_token` | **Yes** | string | API Token with Billing Read permission |
| `account_id` | **Yes** | string | Your Cloudflare Account ID (32 hex chars) |

#### Example Config

```json
{
  "api_token": "gwR5UHWhisV9lO9jklS4g0igHd2RCYMYP-DHUToo",
  "account_id": "8a63ca6ad692678ffdd8aff0b1aaaa4a"
}
```

#### Full Example

```bash
# Sync all billing data
solidafy-cdk read \
  --connector connectors/cloudflare-billing.yaml \
  --config-json '{
    "api_token": "gwR5UHWhisV9lO9jklS4g0igHd2RCYMYP-DHUToo",
    "account_id": "8a63ca6ad692678ffdd8aff0b1aaaa4a"
  }'

# Sync only subscriptions
solidafy-cdk read \
  --connector connectors/cloudflare-billing.yaml \
  --config-json '{
    "api_token": "...",
    "account_id": "..."
  }' \
  --streams subscriptions
```

#### Streams (3 total)

| Stream | Description |
|--------|-------------|
| `account` | Account details (name, type, settings) |
| `billing_profile` | Billing profile (name, email, address) |
| `subscriptions` | Active subscriptions (Workers, R2, Pages, etc.) |

---

### Stripe (`stripe.yaml`)

Syncs payment and subscription data from Stripe.

#### How to Get Credentials

1. Go to [Stripe Dashboard](https://dashboard.stripe.com/)
2. Navigate to **Developers** → **API Keys**
3. Copy your **Secret key** (starts with `sk_live_` or `sk_test_`)

> **Note**: Use `sk_test_` keys for development. Live keys access real customer data.

#### Configuration Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `api_key` | **Yes** | string | Stripe Secret Key |
| `start_date_ts` | No | string | Unix timestamp for filtering created records |

#### Example Config

```json
{
  "api_key": "sk_live_abc123..."
}
```

```json
{
  "api_key": "sk_live_abc123...",
  "start_date_ts": "1701388800"
}
```

#### Full Example

```bash
# Sync all data
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_live_abc123..."}'

# Sync specific streams with date filter
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_live_abc123...", "start_date_ts": "1701388800"}' \
  --streams customers,invoices,subscriptions

# Output to Parquet files
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_live_abc123..."}' \
  --streams customers,invoices \
  --output /data/stripe \
  --format parquet
```

#### Streams (20 total)

| Category | Streams |
|----------|---------|
| **Core** | `customers`, `products`, `prices` |
| **Payments** | `charges`, `payment_intents`, `refunds`, `disputes` |
| **Billing** | `subscriptions`, `invoices`, `invoice_items`, `plans`, `coupons` |
| **Payouts** | `balance_transactions`, `payouts`, `transfers` |
| **Other** | `events`, `checkout_sessions`, `payment_methods`, `setup_intents` |

---

### GitHub Billing (`github-billing.yaml`)

Syncs billing and usage data from GitHub's organization billing API.

#### How to Get Credentials

1. Go to [GitHub Settings](https://github.com/settings/tokens)
2. Click **Generate new token** → **Generate new token (classic)**
3. Select the `admin:org` scope (required for billing endpoints)
4. Copy the token (starts with `ghp_`)

> **Important**: You must be an organization owner or have billing access to read billing data.

#### Configuration Fields

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `access_token` | **Yes** | string | Personal Access Token with `admin:org` scope |
| `org` | **Yes** | string | GitHub organization name |

#### Example Config

```json
{
  "access_token": "ghp_abc123...",
  "org": "my-organization"
}
```

#### Full Example

```bash
# Sync all billing data
solidafy-cdk read \
  --connector github \
  --config-json '{"access_token": "ghp_abc123...", "org": "my-org"}'

# Sync only Copilot billing and seats
solidafy-cdk read \
  --connector github \
  --config-json '{"access_token": "ghp_abc123...", "org": "my-org"}' \
  --streams copilot_billing,copilot_seats

# Sync Actions usage
solidafy-cdk read \
  --connector github \
  --config-json '{"access_token": "ghp_abc123...", "org": "my-org"}' \
  --streams actions_billing,actions_usage
```

#### Streams (8 total)

| Stream | Description |
|--------|-------------|
| `actions_billing` | Actions minutes usage and spending |
| `packages_billing` | Packages storage and transfer usage |
| `shared_storage_billing` | Shared storage (LFS, Actions artifacts, Packages) |
| `copilot_billing` | Copilot billing overview |
| `copilot_seats` | Copilot seat assignments (who has access) |
| `actions_usage` | Actions usage breakdown |
| `org_members` | Organization members for seat tracking |
| `org_repos` | Organization repositories for storage tracking |

---

## Quick Config Reference

Copy-paste templates for each connector:

### OpenAI
```bash
solidafy-cdk read \
  --connector connectors/openai-billing.yaml \
  --config-json '{"admin_api_key": "sk-admin-...", "start_time": "1701388800"}'
```

### Anthropic
```bash
solidafy-cdk read \
  --connector connectors/anthropic-billing.yaml \
  --config-json '{"admin_api_key": "sk-ant-admin...", "start_date": "2024-12-01T00:00:00Z", "start_date_short": "2024-12-01"}'
```

### Cloudflare
```bash
solidafy-cdk read \
  --connector connectors/cloudflare-billing.yaml \
  --config-json '{"api_token": "...", "account_id": "..."}'
```

### Stripe
```bash
solidafy-cdk read \
  --connector connectors/stripe.yaml \
  --config-json '{"api_key": "sk_live_..."}'
```

### GitHub
```bash
solidafy-cdk read \
  --connector github \
  --config-json '{"access_token": "ghp_...", "org": "your-organization"}'
```

---

## Using as a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
solidafy-cdk = { path = "../solidafy-cdk" }
```

### Example: Sync and Process Records

```rust
use std::process::Command;
use serde_json::Value;

fn sync_billing_data(state_json: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    let mut cmd = Command::new("solidafy-cdk");
    cmd.args([
        "read",
        "--connector", "connectors/openai-billing.yaml",
        "--config-json", r#"{"admin_api_key": "sk-...", "start_time": "1762000000"}"#,
        "--streams", "costs,usage_completions",
        "--output", "/tmp/billing",
        "--format", "parquet",
    ]);

    // Pass previous state for incremental sync
    if let Some(state) = state_json {
        cmd.args(["--state-json", state]);
    }

    let output = cmd.output()?;
    let stdout = String::from_utf8(output.stdout)?;

    // Extract final state from output
    let final_state = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["type"] == "STATE")
        .last()
        .map(|v| v["state"].to_string())
        .unwrap_or_default();

    // Parquet files are at:
    // /tmp/billing/costs.parquet
    // /tmp/billing/usage_completions.parquet

    Ok(final_state)  // Save this to your database
}
```

---

## Architecture

```
src/
├── auth/           # Authentication (Bearer, API Key, OAuth2, Basic)
├── cli/            # CLI commands and runner
├── decode/         # Response decoders (JSON, CSV, XML)
├── engine/         # Sync engine orchestration
├── error.rs        # Error types
├── http/           # HTTP client with retry and rate limiting
├── loader/         # YAML connector loader
├── output/         # Output writers (JSON, Parquet)
├── pagination/     # Pagination strategies
├── partition/      # Partition routers (List, Date, Parent, AsyncJob)
├── schema/         # Schema inference
├── state/          # State management for incremental sync
├── template.rs     # Template engine
└── types.rs        # Core type definitions

connectors/         # YAML connector definitions
tests/              # Integration tests
```

## Test Coverage

- **317 unit tests** - Core functionality
- **20 integration tests** - End-to-end with mock HTTP server
- **Total: 337 tests passing**

```bash
cargo test
```

---

## Binary Size

```bash
cargo build --release
ls -lh target/release/solidafy-cdk
# ~26 MB (includes DuckDB with postgres/mysql/sqlite extensions)
```

---

## License

MIT

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

---

## Roadmap

- [ ] More connectors (AWS Cost Explorer, GCP Billing, Azure)
- [ ] Webhook/streaming support
- [ ] Schema evolution handling
- [ ] Connection pooling
- [ ] Parallel stream sync
- [ ] Plugin system for custom decoders
