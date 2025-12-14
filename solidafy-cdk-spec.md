# Solidafy Connector Development Kit (CDK)

## Overview

A minimal, Rust-native framework for building data source connectors. Airbyte-equivalent capability, 10x simpler implementation.

**What it does:**
- Extracts data from REST/HTTP APIs
- Outputs Arrow RecordBatches → Parquet → Iceberg
- Handles auth, pagination, incremental sync, retries

**What it doesn't do:**
- Database connectors (use DuckDB extensions)
- File connectors (use DuckDB)
- Transformations (do in SQL downstream)
- Schema management (Arrow infers, Iceberg evolves)

**Target:** ~1,500 lines of Rust + YAML configs per connector

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Solidafy CDK                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       Connector Interface                               │ │
│  │  spec() → ConfigSpec    check() → Status    discover() → Catalog       │ │
│  │  read(catalog, state) → Stream<RecordBatch>                            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                       │
│  ┌──────────┬───────────┬───────────┼───────────┬───────────┬────────────┐  │
│  │   Auth   │   HTTP    │  Paginate │  Partition│   State   │   Output   │  │
│  ├──────────┼───────────┼───────────┼───────────┼───────────┼────────────┤  │
│  │ API Key  │ GET/POST  │ Cursor    │ Parent    │ Cursors   │ Arrow      │  │
│  │ Bearer   │ Retry     │ Offset    │ DateTime  │ Partition │ Parquet    │  │
│  │ Basic    │ Rate Limit│ Page #    │ List      │ Checkpoint│ Iceberg    │  │
│  │ OAuth2   │ Backoff   │ Link Hdr  │ Async Job │           │            │  │
│  │ Session  │ Decode    │ Next URL  │           │           │            │  │
│  │ JWT      │           │           │           │           │            │  │
│  └──────────┴───────────┴───────────┴───────────┴───────────┴────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Connector Interface

Every connector implements these four methods:

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    /// Returns configuration schema (for UI/validation)
    fn spec(&self) -> ConnectorSpec;
    
    /// Tests if credentials and config are valid
    async fn check(&self, config: &Value) -> Result<CheckResult>;
    
    /// Lists available streams
    async fn discover(&self, config: &Value) -> Result<Catalog>;
    
    /// Reads data from selected streams
    async fn read(
        &self,
        config: &Value,
        catalog: &ConfiguredCatalog,
        state: Option<&State>,
    ) -> Result<impl Stream<Item = Result<Message>>>;
}

pub enum Message {
    Record(RecordBatch),
    State(State),
    Log(LogMessage),
}
```

---

## File Structure

```
solidafy-cdk/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API
│   ├── connector.rs        # Connector trait + YAML loader
│   ├── auth.rs             # All auth implementations (~200 lines)
│   ├── http.rs             # HTTP client with retry/rate limit (~150 lines)
│   ├── pagination.rs       # All pagination strategies (~200 lines)
│   ├── partition.rs        # Partition routers (~250 lines)
│   ├── state.rs            # State management + checkpointing (~100 lines)
│   ├── decode.rs           # Response decoders (~80 lines)
│   ├── output.rs           # Arrow/Parquet output (~100 lines)
│   └── engine.rs           # Main read loop (~300 lines)
│
├── connectors/             # YAML connector definitions
│   ├── stripe.yaml
│   ├── hubspot.yaml
│   ├── salesforce.yaml
│   ├── shopify.yaml
│   └── google_sheets.yaml
│
└── tests/
    └── integration/
```

---

## Configuration Schema

### Top-Level Connector Definition

```yaml
# stripe.yaml
kind: connector
version: "1.0"

metadata:
  name: stripe
  title: "Stripe"
  description: "Extract data from Stripe API"

# ============================================================================
# SPEC - What configuration does this connector need?
# ============================================================================
spec:
  properties:
    api_key:
      type: string
      title: "API Key"
      description: "Stripe secret key (sk_live_... or sk_test_...)"
      secret: true
      required: true
    
    account_id:
      type: string
      title: "Connected Account ID"
      description: "Optional Stripe Connect account ID"
      required: false
    
    start_date:
      type: string
      format: date
      title: "Start Date"
      description: "Only sync data created after this date"
      default: "2020-01-01"

# ============================================================================
# CHECK - How to validate the connection
# ============================================================================
check:
  endpoint: "/v1/balance"
  expect_status: 200

# ============================================================================
# BASE CONFIGURATION
# ============================================================================
base_url: "https://api.stripe.com"

auth:
  type: api_key
  location: header
  header_name: "Authorization"
  prefix: "Bearer "
  value: "{{ config.api_key }}"

http:
  timeout_seconds: 30
  max_retries: 5
  rate_limit:
    requests_per_second: 25
    respect_headers: true

request_defaults:
  headers:
    Stripe-Version: "2023-10-16"

# ============================================================================
# STREAMS
# ============================================================================
streams:
  - name: customers
    endpoint: "/v1/customers"
    primary_key: [id]
    cursor_field: created
    
    record_path: "$.data[*]"
    
    pagination:
      type: cursor
      cursor_param: "starting_after"
      cursor_path: "$.data[-1:].id"
      stop_condition:
        path: "$.has_more"
        value: false
    
    incremental:
      cursor_field: "created"
      cursor_param: "created[gte]"
      cursor_format: unix
```

---

## Authentication

### All Supported Auth Types

```yaml
# ============================================================================
# 1. API KEY - Header
# ============================================================================
auth:
  type: api_key
  location: header
  header_name: "Authorization"      # or "X-API-Key"
  prefix: "Bearer "                 # optional
  value: "{{ config.api_key }}"

# ============================================================================
# 2. API KEY - Query Parameter
# ============================================================================
auth:
  type: api_key
  location: query
  query_param: "api_key"
  value: "{{ config.api_key }}"

# ============================================================================
# 3. BASIC AUTH
# ============================================================================
auth:
  type: basic
  username: "{{ config.username }}"
  password: "{{ config.password }}"

# ============================================================================
# 4. BEARER TOKEN (static)
# ============================================================================
auth:
  type: bearer
  token: "{{ config.access_token }}"

# ============================================================================
# 5. OAUTH2 - Client Credentials
# ============================================================================
auth:
  type: oauth2_client_credentials
  token_url: "https://api.example.com/oauth/token"
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  scopes:
    - "read:data"
  token_body:
    audience: "https://api.example.com"

# ============================================================================
# 6. OAUTH2 - Refresh Token
# ============================================================================
auth:
  type: oauth2_refresh
  token_url: "https://api.example.com/oauth/token"
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  refresh_token: "{{ config.refresh_token }}"

# ============================================================================
# 7. SESSION TOKEN (Login Endpoint)
# ============================================================================
auth:
  type: session
  login_url: "https://api.example.com/auth/login"
  login_method: POST
  login_body:
    email: "{{ config.email }}"
    password: "{{ config.password }}"
  token_path: "$.data.access_token"
  token_header: "Authorization"
  token_prefix: "Bearer "
  expires_in_path: "$.data.expires_in"

# ============================================================================
# 8. JWT (Service Account / Google-style)
# ============================================================================
auth:
  type: jwt
  issuer: "{{ config.client_email }}"
  subject: "{{ config.impersonate_email }}"  # optional
  audience: "https://api.example.com"
  private_key: "{{ config.private_key }}"
  algorithm: RS256
  token_lifetime_seconds: 3600
  claims:
    scope: "read:all write:all"
  token_url: "https://oauth2.googleapis.com/token"  # for two-step

# ============================================================================
# 9. CUSTOM HEADERS
# ============================================================================
auth:
  type: custom_headers
  headers:
    X-API-Key: "{{ config.api_key }}"
    X-Account-ID: "{{ config.account_id }}"

# ============================================================================
# 10. NO AUTH
# ============================================================================
auth:
  type: none
```

### Rust Implementation

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    None,
    
    ApiKey {
        location: Location,
        #[serde(default)]
        header_name: Option<String>,
        #[serde(default)]
        query_param: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
        value: String,
    },
    
    Basic { username: String, password: String },
    
    Bearer { token: String },
    
    Oauth2ClientCredentials {
        token_url: String,
        client_id: String,
        client_secret: String,
        #[serde(default)]
        scopes: Vec<String>,
        #[serde(default)]
        token_body: HashMap<String, String>,
    },
    
    Oauth2Refresh {
        token_url: String,
        client_id: String,
        client_secret: String,
        refresh_token: String,
    },
    
    Session {
        login_url: String,
        #[serde(default = "default_post")]
        login_method: Method,
        login_body: HashMap<String, String>,
        token_path: String,
        token_header: String,
        #[serde(default)]
        token_prefix: Option<String>,
        #[serde(default)]
        expires_in_path: Option<String>,
    },
    
    Jwt {
        issuer: String,
        #[serde(default)]
        subject: Option<String>,
        audience: String,
        private_key: String,
        #[serde(default = "default_rs256")]
        algorithm: JwtAlgorithm,
        #[serde(default = "default_3600")]
        token_lifetime_seconds: u64,
        #[serde(default)]
        claims: HashMap<String, String>,
        #[serde(default)]
        token_url: Option<String>,
    },
    
    CustomHeaders { headers: HashMap<String, String> },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Location { Header, Query }

pub struct Authenticator {
    config: AuthConfig,
    cached_token: RwLock<Option<CachedToken>>,
    http: reqwest::Client,
}

impl Authenticator {
    pub async fn apply(&self, mut req: RequestBuilder) -> Result<RequestBuilder> {
        match &self.config {
            AuthConfig::None => Ok(req),
            
            AuthConfig::ApiKey { location, header_name, query_param, prefix, value } => {
                let val = format!("{}{}", prefix.as_deref().unwrap_or(""), value);
                match location {
                    Location::Header => Ok(req.header(header_name.as_deref().unwrap_or("Authorization"), val)),
                    Location::Query => Ok(req.query(&[(query_param.as_deref().unwrap_or("api_key"), val)])),
                }
            }
            
            AuthConfig::Basic { username, password } => Ok(req.basic_auth(username, Some(password))),
            AuthConfig::Bearer { token } => Ok(req.bearer_auth(token)),
            
            AuthConfig::Oauth2ClientCredentials { .. } |
            AuthConfig::Oauth2Refresh { .. } |
            AuthConfig::Session { .. } |
            AuthConfig::Jwt { .. } => {
                let token = self.get_or_refresh_token().await?;
                Ok(req.bearer_auth(token))
            }
            
            AuthConfig::CustomHeaders { headers } => {
                for (k, v) in headers { req = req.header(k, v); }
                Ok(req)
            }
        }
    }
    
    async fn get_or_refresh_token(&self) -> Result<String> {
        // Check cache, refresh if expired, return token
        // Implementation handles OAuth2, Session, JWT flows
    }
}
```

---

## HTTP Client

### Configuration

```yaml
http:
  timeout_seconds: 30
  connect_timeout_seconds: 10
  
  max_retries: 5
  retry_statuses: [429, 500, 502, 503, 504]
  retry_backoff:
    type: exponential
    initial_ms: 100
    max_ms: 60000
    multiplier: 2.0
  
  rate_limit:
    requests_per_second: 10
    respect_headers: true
    remaining_header: "X-RateLimit-Remaining"
    reset_header: "X-RateLimit-Reset"
```

### Rust Implementation

```rust
pub struct HttpClient {
    inner: reqwest::Client,
    rate_limiter: RateLimiter,
    config: HttpConfig,
}

impl HttpClient {
    pub async fn execute(&self, request: Request) -> Result<Response> {
        let mut attempts = 0;
        
        loop {
            self.rate_limiter.acquire().await;
            
            let req = request.try_clone().ok_or(Error::RequestNotCloneable)?;
            
            match self.inner.execute(req).await {
                Ok(resp) => {
                    self.rate_limiter.update_from_response(&resp);
                    
                    if resp.status().is_success() {
                        return Ok(resp);
                    }
                    
                    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                        let wait = self.parse_retry_after(&resp);
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    
                    if self.config.retry_statuses.contains(&resp.status().as_u16()) 
                        && attempts < self.config.max_retries 
                    {
                        attempts += 1;
                        tokio::time::sleep(self.calculate_backoff(attempts)).await;
                        continue;
                    }
                    
                    return Err(Error::HttpStatus { status: resp.status(), body: resp.text().await.ok() });
                }
                
                Err(e) if (e.is_connect() || e.is_timeout()) && attempts < self.config.max_retries => {
                    attempts += 1;
                    tokio::time::sleep(self.calculate_backoff(attempts)).await;
                    continue;
                }
                
                Err(e) => return Err(e.into()),
            }
        }
    }
}
```

---

## Response Decoders

```yaml
# JSON (default)
response_format: json

# JSON Lines
response_format: jsonl

# CSV
response_format: csv

# XML
response_format:
  type: xml
  record_element: "item"
```

```rust
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormat {
    #[default]
    Json,
    Jsonl,
    Csv,
    Xml { record_element: String },
}

pub trait Decoder: Send + Sync {
    fn decode(&self, bytes: &[u8]) -> Result<Vec<Value>>;
}
```

---

## Pagination

### All Pagination Types

```yaml
# 1. NO PAGINATION
pagination:
  type: none

# 2. CURSOR-BASED
pagination:
  type: cursor
  cursor_param: "starting_after"
  cursor_path: "$.data[-1:].id"
  stop_condition:
    type: field
    path: "$.has_more"
    value: false

# 3. OFFSET-BASED
pagination:
  type: offset
  offset_param: "offset"
  limit_param: "limit"
  limit_value: 100
  stop_condition:
    type: empty_page

# 4. PAGE NUMBER
pagination:
  type: page_number
  page_param: "page"
  start_page: 1
  page_size_param: "per_page"
  page_size: 100
  stop_condition:
    type: total_pages
    path: "$.meta.total_pages"

# 5. LINK HEADER (RFC 5988)
pagination:
  type: link_header
  rel: "next"

# 6. NEXT URL IN BODY
pagination:
  type: next_url
  path: "$.paging.next.link"
```

### Rust Implementation

```rust
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
    #[default]
    None,
    Cursor { cursor_param: String, cursor_path: String, #[serde(default)] stop_condition: StopCondition },
    Offset { offset_param: String, limit_param: String, limit_value: u32, #[serde(default)] stop_condition: StopCondition },
    PageNumber { page_param: String, #[serde(default = "one")] start_page: u32, page_size_param: Option<String>, page_size: Option<u32>, #[serde(default)] stop_condition: StopCondition },
    LinkHeader { #[serde(default = "next")] rel: String },
    NextUrl { path: String },
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StopCondition {
    #[default]
    EmptyPage,
    Field { path: String, value: Value },
    TotalCount { path: String },
    TotalPages { path: String },
}

pub struct Paginator {
    config: PaginationConfig,
    state: PaginatorState,
    total_records: u64,
}

impl Paginator {
    pub fn new(config: &PaginationConfig) -> Self;
    pub fn apply(&self, params: &mut HashMap<String, String>);
    pub fn next_url(&self) -> Option<&str>;
    pub fn advance(&mut self, response: &Value, headers: &HeaderMap, records_count: usize);
    pub fn is_done(&self) -> bool;
}
```

---

## Partition Routers

### Types of Partitioning

```yaml
# ============================================================================
# 1. PARENT STREAM (Substream)
# ============================================================================
streams:
  - name: repositories
    endpoint: "/repos"
    primary_key: [id]
    record_path: "$.data[*]"
    
  - name: commits
    endpoint: "/repos/{{ partition.repo_id }}/commits"
    primary_key: [sha]
    record_path: "$.data[*]"
    partition:
      type: parent
      parent_stream: repositories
      parent_key: id
      partition_field: repo_id

# ============================================================================
# 2. LIST PARTITION
# ============================================================================
streams:
  - name: reports
    endpoint: "/reports/{{ partition.region }}"
    record_path: "$.data[*]"
    partition:
      type: list
      values: ["us-east", "us-west", "eu-west"]
      partition_field: region

# ============================================================================
# 3. DATETIME PARTITION
# ============================================================================
streams:
  - name: events
    endpoint: "/events"
    record_path: "$.data[*]"
    params:
      start_date: "{{ partition.start }}"
      end_date: "{{ partition.end }}"
    partition:
      type: datetime
      start: "{{ config.start_date }}"
      end: "{{ today }}"
      step: P1M                        # ISO 8601: 1 month
      format: "%Y-%m-%d"
      start_param: "start_date"
      end_param: "end_date"

# ============================================================================
# 4. ASYNC JOB (Create → Poll → Download)
# ============================================================================
streams:
  - name: analytics_report
    partition:
      type: async_job
      
      create:
        endpoint: "/reports/create"
        method: POST
        body:
          report_type: "analytics"
          date_range: "last_30_days"
        job_id_path: "$.job_id"
      
      poll:
        endpoint: "/reports/{{ job_id }}/status"
        interval_seconds: 10
        max_attempts: 60
        completed_condition:
          path: "$.status"
          value: "completed"
        failed_condition:
          path: "$.status"
          value: "failed"
      
      download:
        endpoint: "/reports/{{ job_id }}/download"
        # OR: url_path: "$.download_url"
    
    record_path: "$.data[*]"
```

### Rust Implementation

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionConfig {
    None,
    Parent { parent_stream: String, parent_key: String, partition_field: String },
    List { values: Vec<String>, partition_field: String },
    Datetime { start: String, end: String, step: String, format: String, start_param: String, end_param: String },
    AsyncJob { create: AsyncJobCreate, poll: AsyncJobPoll, download: AsyncJobDownload },
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: String,
    pub values: HashMap<String, String>,
}

#[async_trait]
pub trait PartitionRouter: Send + Sync {
    async fn partitions(&self, ctx: &Context) -> Result<Vec<Partition>>;
}
```

---

## Stream Configuration

### Full Schema

```yaml
streams:
  - name: customers                       # Required: unique identifier
    
    # === Endpoint ===
    endpoint: "/v1/customers"             # Required
    method: GET                           # GET (default) or POST
    
    body:                                 # For POST
      type: json                          # json or form
      content:
        query: "SELECT * FROM users"
    
    params:                               # Query parameters
      expand: "data.source"
    
    headers:                              # Additional headers
      X-Custom: "value"
    
    # === Records ===
    record_path: "$.data[*]"              # Required: JSONPath
    primary_key: [id]                     # For dedup
    response_format: json                 # json, jsonl, csv
    
    # === Pagination ===
    pagination:
      type: cursor
      cursor_param: "starting_after"
      cursor_path: "$.data[-1:].id"
      stop_condition:
        path: "$.has_more"
        value: false
    
    # === Incremental ===
    cursor_field: created
    incremental:
      cursor_field: "created"
      cursor_param: "created[gte]"
      cursor_format: unix                 # unix, unix_ms, iso8601, string
      lookback_seconds: 3600              # Re-fetch window
    
    # === Partitioning ===
    partition:
      type: parent
      parent_stream: accounts
      parent_key: id
      partition_field: account_id
    
    # === Error Handling ===
    error_handling:
      strategy: skip                      # fail, skip, retry
      max_errors: 100
```

### Rust Types

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub endpoint: String,
    #[serde(default)]
    pub method: Method,
    #[serde(default)]
    pub body: Option<RequestBody>,
    #[serde(default)]
    pub params: HashMap<String, String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    
    pub record_path: String,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub response_format: ResponseFormat,
    #[serde(default)]
    pub pagination: PaginationConfig,
    #[serde(default)]
    pub cursor_field: Option<String>,
    #[serde(default)]
    pub incremental: Option<IncrementalConfig>,
    #[serde(default)]
    pub partition: PartitionConfig,
    #[serde(default)]
    pub error_handling: ErrorHandling,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncrementalConfig {
    pub cursor_field: String,
    pub cursor_param: String,
    #[serde(default)]
    pub cursor_format: CursorFormat,
    #[serde(default)]
    pub lookback_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CursorFormat { #[default] Iso8601, Unix, UnixMs, String }
```

---

## State Management

### Schema

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct State {
    pub streams: HashMap<String, StreamState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamState {
    pub cursor: Option<String>,
    #[serde(default)]
    pub partitions: HashMap<String, PartitionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionState {
    pub cursor: Option<String>,
    pub completed: bool,
}
```

### State Manager

```rust
pub struct StateManager {
    state: State,
    checkpoint_interval: usize,
    records_since_checkpoint: usize,
    on_checkpoint: Box<dyn Fn(&State) + Send + Sync>,
}

impl StateManager {
    pub fn get_cursor(&self, stream: &str) -> Option<&str>;
    pub fn get_partition_cursor(&self, stream: &str, partition: &str) -> Option<&str>;
    pub fn is_partition_completed(&self, stream: &str, partition: &str) -> bool;
    pub fn update_cursor(&mut self, stream: &str, cursor: String);
    pub fn update_partition(&mut self, stream: &str, partition: &str, cursor: Option<String>, completed: bool);
    pub fn record_emitted(&mut self);
    pub fn checkpoint(&mut self);
}
```

---

## Output

### Messages

```rust
pub enum Message {
    Record { stream: String, data: RecordBatch, emitted_at: DateTime<Utc> },
    State(State),
    Log { level: LogLevel, message: String },
}

pub enum LogLevel { Debug, Info, Warn, Error }
```

### Arrow Output

```rust
pub fn json_to_arrow(records: &[Value]) -> Result<RecordBatch> {
    // Infer schema from records
    // Build Arrow arrays for each column
    // Return RecordBatch
}
```

---

## Main Engine

```rust
pub struct Engine {
    config: ConnectorConfig,
    http: HttpClient,
    auth: Authenticator,
}

impl Engine {
    pub async fn read(
        &self,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
        checkpoint_fn: impl Fn(&State) + Send + Sync + 'static,
    ) -> Result<impl Stream<Item = Result<Message>>> {
        let mut state_mgr = StateManager::new(state, 1000, checkpoint_fn);
        
        for stream in &catalog.streams {
            let partitions = self.get_partitions(stream, &state_mgr).await?;
            
            for partition in partitions {
                if state_mgr.is_partition_completed(&stream.name, &partition.id) {
                    continue; // Resumable full refresh
                }
                
                self.read_partition(stream, &partition, &mut state_mgr).await?;
                state_mgr.update_partition(&stream.name, &partition.id, None, true);
            }
        }
        
        state_mgr.checkpoint();
    }
    
    async fn read_partition(&self, stream: &StreamConfig, partition: &Partition, state: &mut StateManager) -> Result<()> {
        let mut paginator = Paginator::new(&stream.pagination);
        
        loop {
            let url = self.build_url(stream, partition, &paginator)?;
            let mut params = stream.params.clone();
            paginator.apply(&mut params);
            
            // Apply incremental cursor
            if let Some(inc) = &stream.incremental {
                if let Some(cursor) = state.get_cursor(&stream.name) {
                    params.insert(inc.cursor_param.clone(), cursor.to_string());
                }
            }
            
            let response = self.http.execute(self.build_request(&url, &params).await?).await?;
            let body: Value = response.json().await?;
            let records = extract_records(&body, &stream.record_path)?;
            
            // Track max cursor, emit records
            let mut max_cursor = None;
            for record in &records {
                if let Some(cursor) = extract_cursor(record, &stream.incremental) {
                    max_cursor = max_cursor_value(max_cursor, Some(cursor));
                }
            }
            
            // Emit Arrow batch
            let batch = json_to_arrow(&records)?;
            emit(Message::Record { stream: stream.name.clone(), data: batch, emitted_at: Utc::now() });
            
            // Update state
            if let Some(cursor) = max_cursor {
                state.update_cursor(&stream.name, cursor);
            }
            
            paginator.advance(&body, response.headers(), records.len());
            if paginator.is_done() { break; }
        }
    }
}
```

---

## Concurrency

```rust
pub struct ConcurrentEngine {
    engine: Engine,
    concurrency: usize,
}

impl ConcurrentEngine {
    pub async fn read(&self, catalog: &ConfiguredCatalog, state: Option<State>) -> Result<impl Stream<Item = Result<Message>>> {
        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        
        for stream in &catalog.streams {
            let partitions = self.engine.get_partitions(stream).await?;
            
            let handles: Vec<_> = partitions
                .into_iter()
                .map(|partition| {
                    let permit = semaphore.clone().acquire_owned();
                    tokio::spawn(async move {
                        let _permit = permit.await?;
                        self.engine.read_partition(stream, &partition).await
                    })
                })
                .collect();
            
            for handle in handles {
                handle.await??;
            }
        }
    }
}
```

---

## Example Connectors

### Stripe

```yaml
kind: connector
version: "1.0"
metadata: { name: stripe, title: "Stripe" }

spec:
  properties:
    api_key: { type: string, secret: true, required: true }

check: { endpoint: "/v1/balance", expect_status: 200 }
base_url: "https://api.stripe.com"

auth:
  type: api_key
  location: header
  header_name: "Authorization"
  prefix: "Bearer "
  value: "{{ config.api_key }}"

http:
  rate_limit: { requests_per_second: 25, respect_headers: true }

request_defaults:
  headers: { Stripe-Version: "2023-10-16" }

streams:
  - name: customers
    endpoint: "/v1/customers"
    primary_key: [id]
    record_path: "$.data[*]"
    pagination:
      type: cursor
      cursor_param: "starting_after"
      cursor_path: "$.data[-1:].id"
      stop_condition: { path: "$.has_more", value: false }
    incremental:
      cursor_field: "created"
      cursor_param: "created[gte]"
      cursor_format: unix
```

### HubSpot

```yaml
kind: connector
version: "1.0"
metadata: { name: hubspot, title: "HubSpot" }

spec:
  properties:
    access_token: { type: string, secret: true, required: true }

check: { endpoint: "/crm/v3/objects/contacts", params: { limit: 1 }, expect_status: 200 }
base_url: "https://api.hubapi.com"

auth:
  type: bearer
  token: "{{ config.access_token }}"

streams:
  - name: contacts
    endpoint: "/crm/v3/objects/contacts"
    primary_key: [id]
    record_path: "$.results[*]"
    params: { limit: 100, properties: "firstname,lastname,email" }
    pagination:
      type: next_url
      path: "$.paging.next.link"
```

### Shopify

```yaml
kind: connector
version: "1.0"
metadata: { name: shopify, title: "Shopify" }

spec:
  properties:
    shop_name: { type: string, required: true }
    access_token: { type: string, secret: true, required: true }

check: { endpoint: "/admin/api/2024-01/shop.json", expect_status: 200 }
base_url: "https://{{ config.shop_name }}.myshopify.com"

auth:
  type: api_key
  location: header
  header_name: "X-Shopify-Access-Token"
  value: "{{ config.access_token }}"

http:
  rate_limit: { requests_per_second: 2, respect_headers: true }

streams:
  - name: orders
    endpoint: "/admin/api/2024-01/orders.json"
    primary_key: [id]
    record_path: "$.orders[*]"
    params: { limit: 250, status: any }
    pagination: { type: link_header }
    incremental:
      cursor_field: "updated_at"
      cursor_param: "updated_at_min"
      cursor_format: iso8601
```

### Salesforce

```yaml
kind: connector
version: "1.0"
metadata: { name: salesforce, title: "Salesforce" }

spec:
  properties:
    instance_url: { type: string, required: true }
    client_id: { type: string, required: true }
    client_secret: { type: string, secret: true, required: true }
    refresh_token: { type: string, secret: true, required: true }

check: { endpoint: "/services/data/v59.0/limits", expect_status: 200 }
base_url: "{{ config.instance_url }}"

auth:
  type: oauth2_refresh
  token_url: "https://login.salesforce.com/services/oauth2/token"
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  refresh_token: "{{ config.refresh_token }}"

streams:
  - name: accounts
    endpoint: "/services/data/v59.0/query"
    primary_key: [Id]
    record_path: "$.records[*]"
    params: { q: "SELECT Id,Name,Industry FROM Account" }
    pagination: { type: next_url, path: "$.nextRecordsUrl" }
```

### Google Sheets

```yaml
kind: connector
version: "1.0"
metadata: { name: google_sheets, title: "Google Sheets" }

spec:
  properties:
    credentials_json: { type: string, secret: true, required: true }
    spreadsheet_id: { type: string, required: true }
    sheet_name: { type: string, default: "Sheet1" }

check: { endpoint: "/v4/spreadsheets/{{ config.spreadsheet_id }}", expect_status: 200 }
base_url: "https://sheets.googleapis.com"

auth:
  type: jwt
  issuer: "{{ config.credentials_json.client_email }}"
  audience: "https://sheets.googleapis.com"
  private_key: "{{ config.credentials_json.private_key }}"
  algorithm: RS256
  claims: { scope: "https://www.googleapis.com/auth/spreadsheets.readonly" }
  token_url: "https://oauth2.googleapis.com/token"

streams:
  - name: sheet_data
    endpoint: "/v4/spreadsheets/{{ config.spreadsheet_id }}/values/{{ config.sheet_name }}"
    record_path: "$.values[1:]"
    params: { majorDimension: "ROWS" }
    pagination: { type: none }
```

---

## CLI Usage

```bash
# Check connection
solidafy-cdk check --connector stripe.yaml --config config.json

# Discover streams
solidafy-cdk discover --connector stripe.yaml --config config.json

# Read data
solidafy-cdk read \
  --connector stripe.yaml \
  --config config.json \
  --catalog catalog.json \
  --state state.json \
  --output parquet \
  --output-dir ./output
```

---

## Library Usage

```rust
use solidafy_cdk::{Connector, load_connector};

#[tokio::main]
async fn main() -> Result<()> {
    let connector = load_connector("connectors/stripe.yaml")?;
    let config = load_config("config.json")?;
    
    // Check
    let status = connector.check(&config).await?;
    
    // Discover
    let catalog = connector.discover(&config).await?;
    
    // Read
    let mut stream = connector.read(&config, &catalog, state).await?;
    
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Record { stream, data, .. } => write_to_iceberg(&stream, data)?,
            Message::State(s) => save_state(&s)?,
            Message::Log { level, message } => log::log!(level, "{}", message),
        }
    }
}
```

---

## Summary

### What We Support

| Category | Features |
|----------|----------|
| **Auth** | API Key, Basic, Bearer, OAuth2 (client/refresh), Session, JWT, Custom Headers |
| **HTTP** | GET/POST, JSON/Form body, Retries, Rate limiting, Backoff |
| **Pagination** | Cursor, Offset, Page number, Link header, Next URL |
| **Partitioning** | Parent stream, List, DateTime ranges, Async jobs |
| **Incremental** | Cursor-based, Lookback window, Per-partition state |
| **Resumability** | Checkpointing, Partition completion |
| **Output** | Arrow RecordBatch → Parquet → Iceberg |
| **Response** | JSON, JSONL, CSV |

### What We Skip (handled elsewhere)

| Feature | Handled By |
|---------|-----------|
| Transforms | SQL downstream |
| Schema validation | Arrow infers |
| Schema evolution | Iceberg |
| Database connectors | DuckDB extensions |
| File connectors | DuckDB |

### Size Estimate

| Component | Lines |
|-----------|-------|
| Auth | ~200 |
| HTTP | ~150 |
| Pagination | ~200 |
| Partitioning | ~250 |
| State | ~100 |
| Decoders | ~80 |
| Output | ~100 |
| Engine | ~300 |
| Connector loader | ~100 |
| **Total** | **~1,500** |

**30x smaller than Airbyte CDK with equivalent REST API functionality.**
