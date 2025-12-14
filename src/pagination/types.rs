//! Pagination types and traits
//!
//! Defines the core pagination abstractions used by all strategies.

use crate::auth::extract_jsonpath;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

/// Result of the next page computation
#[derive(Debug, Clone)]
pub enum NextPage {
    /// More pages available with these parameters
    Continue {
        /// Query parameters to add/replace
        query_params: HashMap<String, String>,
        /// Optional new URL (for next_url pagination)
        url: Option<String>,
    },
    /// No more pages
    Done,
}

impl NextPage {
    /// Create a continuation with query parameters
    pub fn with_params(params: HashMap<String, String>) -> Self {
        Self::Continue {
            query_params: params,
            url: None,
        }
    }

    /// Create a continuation with a single parameter
    pub fn with_param(key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut params = HashMap::new();
        params.insert(key.into(), value.into());
        Self::Continue {
            query_params: params,
            url: None,
        }
    }

    /// Create a continuation with a new URL
    pub fn with_url(url: impl Into<String>) -> Self {
        Self::Continue {
            query_params: HashMap::new(),
            url: Some(url.into()),
        }
    }

    /// Check if this is a done result
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    /// Check if this is a continue result
    pub fn is_continue(&self) -> bool {
        matches!(self, Self::Continue { .. })
    }
}

/// Configuration for pagination behavior
#[derive(Debug, Clone, Default)]
pub enum PaginationConfig {
    /// No pagination
    #[default]
    None,

    /// Cursor-based pagination (e.g., Stripe)
    Cursor {
        /// Query parameter name for cursor (e.g., "starting_after")
        cursor_param: String,
        /// JSONPath to extract cursor from response
        cursor_path: String,
        /// Stop condition
        stop_condition: StopCondition,
    },

    /// Offset-based pagination
    Offset {
        /// Query parameter name for offset
        offset_param: String,
        /// Query parameter name for limit
        limit_param: String,
        /// Number of records per page
        limit_value: u32,
        /// Stop condition
        stop_condition: StopCondition,
    },

    /// Page number pagination
    PageNumber {
        /// Query parameter name for page number
        page_param: String,
        /// First page number (usually 0 or 1)
        start_page: u32,
        /// Optional page size parameter name
        page_size_param: Option<String>,
        /// Page size value
        page_size: Option<u32>,
        /// Stop condition
        stop_condition: StopCondition,
    },

    /// Link header pagination (RFC 5988)
    LinkHeader {
        /// Rel value to follow (default: "next")
        rel: String,
    },

    /// Next URL in response body
    NextUrl {
        /// JSONPath to extract next URL from response
        path: String,
    },
}

impl PaginationConfig {
    /// Create cursor pagination config
    pub fn cursor(
        cursor_param: impl Into<String>,
        cursor_path: impl Into<String>,
        stop_condition: StopCondition,
    ) -> Self {
        Self::Cursor {
            cursor_param: cursor_param.into(),
            cursor_path: cursor_path.into(),
            stop_condition,
        }
    }

    /// Create offset pagination config
    pub fn offset(
        offset_param: impl Into<String>,
        limit_param: impl Into<String>,
        limit_value: u32,
        stop_condition: StopCondition,
    ) -> Self {
        Self::Offset {
            offset_param: offset_param.into(),
            limit_param: limit_param.into(),
            limit_value,
            stop_condition,
        }
    }

    /// Create page number pagination config
    pub fn page_number(page_param: impl Into<String>, start_page: u32) -> Self {
        Self::PageNumber {
            page_param: page_param.into(),
            start_page,
            page_size_param: None,
            page_size: None,
            stop_condition: StopCondition::EmptyPage,
        }
    }

    /// Create link header pagination config
    pub fn link_header(rel: impl Into<String>) -> Self {
        Self::LinkHeader { rel: rel.into() }
    }

    /// Create next URL pagination config
    pub fn next_url(path: impl Into<String>) -> Self {
        Self::NextUrl { path: path.into() }
    }
}

/// Stop conditions for pagination
#[derive(Debug, Clone, Default)]
pub enum StopCondition {
    /// Stop when page is empty (no records)
    #[default]
    EmptyPage,

    /// Stop when a field has a specific value
    Field {
        /// JSONPath to the field
        path: String,
        /// Expected value to stop
        value: Value,
    },

    /// Stop when offset reaches total count
    TotalCount {
        /// JSONPath to total count field
        path: String,
    },

    /// Stop when page number reaches total pages
    TotalPages {
        /// JSONPath to total pages field
        path: String,
    },
}

impl StopCondition {
    /// Create a field-based stop condition
    pub fn field(path: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Field {
            path: path.into(),
            value: value.into(),
        }
    }

    /// Create a total count stop condition
    pub fn total_count(path: impl Into<String>) -> Self {
        Self::TotalCount { path: path.into() }
    }

    /// Create a total pages stop condition
    pub fn total_pages(path: impl Into<String>) -> Self {
        Self::TotalPages { path: path.into() }
    }
}

/// Result of checking a stop condition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopResult {
    /// Continue pagination
    Continue,
    /// Stop pagination
    Stop,
}

impl StopResult {
    /// Check if we should continue
    pub fn should_continue(&self) -> bool {
        matches!(self, Self::Continue)
    }

    /// Check if we should stop
    pub fn should_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }
}

/// Tracks pagination state during iteration
#[derive(Debug, Clone, Default)]
pub struct PaginationState {
    /// Current page number (for page-based pagination)
    pub page: u32,
    /// Current offset (for offset-based pagination)
    pub offset: u32,
    /// Current cursor value
    pub cursor: Option<String>,
    /// Total records fetched so far
    pub total_fetched: u64,
    /// Is pagination complete?
    pub done: bool,
}

impl PaginationState {
    /// Create a new pagination state
    pub fn new() -> Self {
        Self::default()
    }

    /// Create state with a starting page
    pub fn with_page(page: u32) -> Self {
        Self {
            page,
            ..Default::default()
        }
    }

    /// Mark pagination as complete
    pub fn mark_done(&mut self) {
        self.done = true;
    }

    /// Increment page number
    pub fn next_page(&mut self) {
        self.page += 1;
    }

    /// Add offset
    pub fn add_offset(&mut self, amount: u32) {
        self.offset += amount;
    }

    /// Set cursor
    pub fn set_cursor(&mut self, cursor: String) {
        self.cursor = Some(cursor);
    }

    /// Add to total fetched
    pub fn add_fetched(&mut self, count: u64) {
        self.total_fetched += count;
    }
}

/// Core trait for pagination strategies
pub trait Paginator: Send + Sync {
    /// Get initial query parameters for the first request
    fn initial_params(&self, state: &PaginationState) -> HashMap<String, String>;

    /// Process a response and determine if there's a next page
    fn process_response(
        &self,
        body: &Value,
        headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage;
}

/// Check a stop condition against a response
pub fn check_stop_condition(
    condition: &StopCondition,
    body: &Value,
    records_count: usize,
    state: &PaginationState,
) -> StopResult {
    match condition {
        StopCondition::EmptyPage => {
            if records_count == 0 {
                StopResult::Stop
            } else {
                StopResult::Continue
            }
        }
        StopCondition::Field { path, value } => {
            if let Some(field_value) = extract_jsonpath_value(body, path) {
                if &field_value == value {
                    StopResult::Stop
                } else {
                    StopResult::Continue
                }
            } else {
                StopResult::Continue
            }
        }
        StopCondition::TotalCount { path } => {
            if let Some(total) = extract_jsonpath(body, path).and_then(|s| s.parse::<u64>().ok()) {
                if state.total_fetched >= total {
                    StopResult::Stop
                } else {
                    StopResult::Continue
                }
            } else {
                StopResult::Continue
            }
        }
        StopCondition::TotalPages { path } => {
            if let Some(total_pages) =
                extract_jsonpath(body, path).and_then(|s| s.parse::<u32>().ok())
            {
                if state.page >= total_pages {
                    StopResult::Stop
                } else {
                    StopResult::Continue
                }
            } else {
                StopResult::Continue
            }
        }
    }
}

/// Extract a JSON value from a path (returns Value instead of String)
fn extract_jsonpath_value(value: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let parts: Vec<&str> = path.split('.').collect();

    let mut current = value;
    for part in parts {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }

    Some(current.clone())
}
