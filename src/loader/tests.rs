//! Tests for YAML loader module

use super::*;

// ============================================================================
// Basic Loading Tests
// ============================================================================

#[test]
fn test_load_minimal_connector() {
    let yaml = r#"
name: test-connector
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.name, "test-connector");
    assert_eq!(def.base_url, "https://api.example.com");
    assert_eq!(def.streams.len(), 1);
    assert_eq!(def.streams[0].name, "users");
    assert_eq!(def.streams[0].request.path, "/users");
    assert_eq!(def.streams[0].request.method, "GET");
}

#[test]
fn test_load_connector_with_version() {
    let yaml = r#"
name: test-connector
version: "1.0.0"
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.version, "1.0.0");
}

#[test]
fn test_load_connector_default_version() {
    let yaml = r#"
name: test-connector
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.version, "0.1.0");
}

// ============================================================================
// Auth Definition Tests
// ============================================================================

#[test]
fn test_load_api_key_auth() {
    let yaml = r#"
name: test
base_url: https://api.example.com
auth:
  type: api_key
  key: X-API-Key
  value: "{{ config.api_key }}"
  location: header
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match def.auth.unwrap() {
        AuthDefinition::ApiKey {
            key,
            value,
            location,
        } => {
            assert_eq!(key, "X-API-Key");
            assert_eq!(value, "{{ config.api_key }}");
            assert_eq!(location, "header");
        }
        _ => panic!("Expected ApiKey auth"),
    }
}

#[test]
fn test_load_bearer_auth() {
    let yaml = r#"
name: test
base_url: https://api.example.com
auth:
  type: bearer
  token: "{{ config.token }}"
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match def.auth.unwrap() {
        AuthDefinition::Bearer { token } => {
            assert_eq!(token, "{{ config.token }}");
        }
        _ => panic!("Expected Bearer auth"),
    }
}

#[test]
fn test_load_basic_auth() {
    let yaml = r#"
name: test
base_url: https://api.example.com
auth:
  type: basic
  username: "{{ config.username }}"
  password: "{{ config.password }}"
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match def.auth.unwrap() {
        AuthDefinition::Basic { username, password } => {
            assert_eq!(username, "{{ config.username }}");
            assert_eq!(password, "{{ config.password }}");
        }
        _ => panic!("Expected Basic auth"),
    }
}

#[test]
fn test_load_oauth2_client_credentials() {
    let yaml = r#"
name: test
base_url: https://api.example.com
auth:
  type: oauth2_client_credentials
  token_url: https://auth.example.com/token
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  scopes:
    - read
    - write
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match def.auth.unwrap() {
        AuthDefinition::OAuth2ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scopes,
        } => {
            assert_eq!(token_url, "https://auth.example.com/token");
            assert_eq!(client_id, "{{ config.client_id }}");
            assert_eq!(client_secret, "{{ config.client_secret }}");
            assert_eq!(scopes, vec!["read", "write"]);
        }
        _ => panic!("Expected OAuth2ClientCredentials auth"),
    }
}

// ============================================================================
// HTTP Configuration Tests
// ============================================================================

#[test]
fn test_load_http_config() {
    let yaml = r#"
name: test
base_url: https://api.example.com
http:
  timeout_secs: 60
  max_retries: 5
  rate_limit_rps: 10
  user_agent: "MyApp/1.0"
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.http.timeout_secs, 60);
    assert_eq!(def.http.max_retries, 5);
    assert_eq!(def.http.rate_limit_rps, Some(10));
    assert_eq!(def.http.user_agent, Some("MyApp/1.0".to_string()));
}

#[test]
fn test_load_http_defaults() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: data
    request:
      path: /data
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.http.timeout_secs, 30);
    assert_eq!(def.http.max_retries, 3);
    assert_eq!(def.http.rate_limit_rps, None);
}

// ============================================================================
// Stream Definition Tests
// ============================================================================

#[test]
fn test_load_stream_with_params() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
      params:
        status: active
        limit: "100"
"#;

    let def = load_connector_from_str(yaml).unwrap();
    let stream = &def.streams[0];
    assert_eq!(
        stream.request.params.get("status"),
        Some(&"active".to_string())
    );
    assert_eq!(stream.request.params.get("limit"), Some(&"100".to_string()));
}

#[test]
fn test_load_stream_with_post() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: search
    request:
      method: POST
      path: /search
      body: '{"query": "test"}'
      content_type: application/json
"#;

    let def = load_connector_from_str(yaml).unwrap();
    let stream = &def.streams[0];
    assert_eq!(stream.request.method, "POST");
    assert_eq!(
        stream.request.body,
        Some(r#"{"query": "test"}"#.to_string())
    );
    assert_eq!(
        stream.request.content_type,
        Some("application/json".to_string())
    );
}

#[test]
fn test_load_stream_with_primary_key() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
    primary_key:
      - id
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.streams[0].primary_key, vec!["id"]);
}

#[test]
fn test_load_stream_with_cursor() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: events
    request:
      path: /events
    cursor_field: updated_at
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert_eq!(def.streams[0].cursor_field, Some("updated_at".to_string()));
}

// ============================================================================
// Decoder Tests
// ============================================================================

#[test]
fn test_load_json_decoder() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
    decoder:
      type: json
      records_path: data.users
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].decoder {
        DecoderDefinition::Json { records_path } => {
            assert_eq!(records_path, &Some("data.users".to_string()));
        }
        _ => panic!("Expected JSON decoder"),
    }
}

#[test]
fn test_load_jsonl_decoder() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: logs
    request:
      path: /logs
    decoder:
      type: jsonl
"#;

    let def = load_connector_from_str(yaml).unwrap();
    assert!(matches!(def.streams[0].decoder, DecoderDefinition::Jsonl));
}

#[test]
fn test_load_csv_decoder() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: export
    request:
      path: /export
    decoder:
      type: csv
      delimiter: ";"
      has_header: true
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].decoder {
        DecoderDefinition::Csv {
            delimiter,
            has_header,
        } => {
            assert_eq!(*delimiter, ';');
            assert!(*has_header);
        }
        _ => panic!("Expected CSV decoder"),
    }
}

// ============================================================================
// Pagination Tests
// ============================================================================

#[test]
fn test_load_offset_pagination() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
    pagination:
      type: offset
      offset_param: offset
      limit_param: limit
      limit: 100
      stop:
        type: total_count
        path: meta.total
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].pagination {
        Some(PaginationDefinition::Offset {
            offset_param,
            limit_param,
            limit,
            stop,
        }) => {
            assert_eq!(offset_param, "offset");
            assert_eq!(limit_param, "limit");
            assert_eq!(*limit, 100);
            match stop {
                StopConditionDefinition::TotalCount { path } => {
                    assert_eq!(path, "meta.total");
                }
                _ => panic!("Expected TotalCount stop condition"),
            }
        }
        _ => panic!("Expected offset pagination"),
    }
}

#[test]
fn test_load_page_number_pagination() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
    pagination:
      type: page_number
      page_param: page
      start_page: 1
      page_size_param: per_page
      page_size: 50
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].pagination {
        Some(PaginationDefinition::PageNumber {
            page_param,
            start_page,
            page_size_param,
            page_size,
            ..
        }) => {
            assert_eq!(page_param, "page");
            assert_eq!(*start_page, 1);
            assert_eq!(page_size_param, &Some("per_page".to_string()));
            assert_eq!(*page_size, Some(50));
        }
        _ => panic!("Expected page number pagination"),
    }
}

#[test]
fn test_load_cursor_pagination() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: items
    request:
      path: /items
    pagination:
      type: cursor
      cursor_param: cursor
      cursor_path: meta.next_cursor
      location: query
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].pagination {
        Some(PaginationDefinition::Cursor {
            cursor_param,
            cursor_path,
            location,
        }) => {
            assert_eq!(cursor_param, "cursor");
            assert_eq!(cursor_path, "meta.next_cursor");
            assert_eq!(location, "query");
        }
        _ => panic!("Expected cursor pagination"),
    }
}

#[test]
fn test_load_link_header_pagination() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: items
    request:
      path: /items
    pagination:
      type: link_header
      rel: next
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].pagination {
        Some(PaginationDefinition::LinkHeader { rel }) => {
            assert_eq!(rel, "next");
        }
        _ => panic!("Expected link header pagination"),
    }
}

// ============================================================================
// Partition Tests
// ============================================================================

#[test]
fn test_load_list_partition() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: regional_data
    request:
      path: /data
      params:
        region: "{{ partition.region }}"
    partition:
      type: list
      field: region
      values:
        - us-east
        - us-west
        - eu-west
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].partition {
        Some(PartitionDefinition::List { field, values }) => {
            assert_eq!(field, "region");
            assert_eq!(values, &["us-east", "us-west", "eu-west"]);
        }
        _ => panic!("Expected list partition"),
    }
}

#[test]
fn test_load_parent_partition() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
  - name: user_orders
    request:
      path: "/users/{{ partition.user_id }}/orders"
    partition:
      type: parent
      stream: users
      parent_field: id
      partition_field: user_id
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[1].partition {
        Some(PartitionDefinition::Parent {
            stream,
            parent_field,
            partition_field,
        }) => {
            assert_eq!(stream, "users");
            assert_eq!(parent_field, "id");
            assert_eq!(partition_field, "user_id");
        }
        _ => panic!("Expected parent partition"),
    }
}

#[test]
fn test_load_date_range_partition() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: events
    request:
      path: /events
      params:
        start_date: "{{ partition.start }}"
        end_date: "{{ partition.end }}"
    partition:
      type: date_range
      start: "{{ config.start_date }}"
      end: "{{ now() }}"
      step: 1d
      start_field: start
      end_field: end
"#;

    let def = load_connector_from_str(yaml).unwrap();
    match &def.streams[0].partition {
        Some(PartitionDefinition::DateRange {
            start,
            end,
            step,
            start_field,
            end_field,
        }) => {
            assert_eq!(start, "{{ config.start_date }}");
            assert_eq!(end, "{{ now() }}");
            assert_eq!(step, "1d");
            assert_eq!(start_field, "start");
            assert_eq!(end_field, "end");
        }
        _ => panic!("Expected date range partition"),
    }
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validation_empty_name() {
    let yaml = r#"
name: ""
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("name cannot be empty"));
}

#[test]
fn test_validation_empty_base_url() {
    let yaml = r#"
name: test
base_url: ""
streams:
  - name: users
    request:
      path: /users
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("base_url cannot be empty"));
}

#[test]
fn test_validation_no_streams() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams: []
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("at least one stream"));
}

#[test]
fn test_validation_duplicate_streams() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      path: /users
  - name: users
    request:
      path: /users2
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Duplicate stream names"));
}

#[test]
fn test_validation_empty_stream_name() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: ""
    request:
      path: /users
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Stream name cannot be empty"));
}

#[test]
fn test_validation_invalid_method() {
    let yaml = r#"
name: test
base_url: https://api.example.com
streams:
  - name: users
    request:
      method: INVALID
      path: /users
"#;

    let result = load_connector_from_str(yaml);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("invalid HTTP method"));
}

// ============================================================================
// Full Connector Tests
// ============================================================================

#[test]
fn test_load_full_connector() {
    let yaml = r#"
name: github-connector
version: "1.0.0"
base_url: https://api.github.com

auth:
  type: bearer
  token: "{{ config.access_token }}"

http:
  timeout_secs: 30
  max_retries: 3
  rate_limit_rps: 30

headers:
  Accept: application/vnd.github.v3+json
  X-GitHub-Api-Version: "2022-11-28"

streams:
  - name: repositories
    request:
      path: /user/repos
      params:
        type: owner
        sort: updated
    decoder:
      type: json
    pagination:
      type: link_header
      rel: next
    primary_key:
      - id

  - name: issues
    request:
      path: "/repos/{{ partition.repo }}/issues"
      params:
        state: all
    decoder:
      type: json
    pagination:
      type: page_number
      page_param: page
      page_size_param: per_page
      page_size: 100
    partition:
      type: list
      field: repo
      values:
        - owner/repo1
        - owner/repo2
    cursor_field: updated_at
"#;

    let def = load_connector_from_str(yaml).unwrap();

    assert_eq!(def.name, "github-connector");
    assert_eq!(def.version, "1.0.0");
    assert_eq!(def.base_url, "https://api.github.com");
    assert!(matches!(def.auth, Some(AuthDefinition::Bearer { .. })));
    assert_eq!(def.http.rate_limit_rps, Some(30));
    assert_eq!(
        def.headers.get("Accept"),
        Some(&"application/vnd.github.v3+json".to_string())
    );
    assert_eq!(def.streams.len(), 2);

    // Check repositories stream
    let repos = &def.streams[0];
    assert_eq!(repos.name, "repositories");
    assert!(matches!(
        repos.pagination,
        Some(PaginationDefinition::LinkHeader { .. })
    ));

    // Check issues stream
    let issues = &def.streams[1];
    assert_eq!(issues.name, "issues");
    assert!(matches!(
        issues.partition,
        Some(PartitionDefinition::List { .. })
    ));
    assert_eq!(issues.cursor_field, Some("updated_at".to_string()));
}
