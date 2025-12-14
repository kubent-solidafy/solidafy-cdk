//! Pagination strategy implementations
//!
//! Each strategy handles a specific pagination pattern.

use super::types::{
    check_stop_condition, NextPage, PaginationState, Paginator, StopCondition, StopResult,
};
use crate::auth::extract_jsonpath;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

// ============================================================================
// Cursor Pagination
// ============================================================================

/// Cursor-based pagination (e.g., Stripe, Slack)
///
/// Uses a cursor value from the response to fetch the next page.
/// Common patterns:
/// - `?starting_after=obj_123`
/// - `?cursor=abc123`
#[derive(Debug, Clone)]
pub struct CursorPaginator {
    /// Query parameter name for cursor
    pub cursor_param: String,
    /// JSONPath to extract cursor from response
    pub cursor_path: String,
    /// Stop condition
    pub stop_condition: StopCondition,
}

impl CursorPaginator {
    /// Create a new cursor paginator
    pub fn new(
        cursor_param: impl Into<String>,
        cursor_path: impl Into<String>,
        stop_condition: StopCondition,
    ) -> Self {
        Self {
            cursor_param: cursor_param.into(),
            cursor_path: cursor_path.into(),
            stop_condition,
        }
    }
}

impl Paginator for CursorPaginator {
    fn initial_params(&self, state: &PaginationState) -> HashMap<String, String> {
        let mut params = HashMap::new();
        if let Some(cursor) = &state.cursor {
            params.insert(self.cursor_param.clone(), cursor.clone());
        }
        params
    }

    fn process_response(
        &self,
        body: &Value,
        _headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);

        // Check stop condition first
        if check_stop_condition(&self.stop_condition, body, records_count, state)
            == StopResult::Stop
        {
            state.mark_done();
            return NextPage::Done;
        }

        // Extract cursor for next page
        if let Some(cursor) = extract_jsonpath(body, &self.cursor_path) {
            if cursor.is_empty() {
                state.mark_done();
                return NextPage::Done;
            }
            state.set_cursor(cursor.clone());
            NextPage::with_param(&self.cursor_param, cursor)
        } else {
            state.mark_done();
            NextPage::Done
        }
    }
}

// ============================================================================
// Offset Pagination
// ============================================================================

/// Offset-based pagination (e.g., SQL-style pagination)
///
/// Uses offset and limit parameters to paginate.
/// Common patterns:
/// - `?offset=100&limit=50`
/// - `?skip=100&take=50`
#[derive(Debug, Clone)]
pub struct OffsetPaginator {
    /// Query parameter name for offset
    pub offset_param: String,
    /// Query parameter name for limit
    pub limit_param: String,
    /// Number of records per page
    pub limit_value: u32,
    /// Stop condition
    pub stop_condition: StopCondition,
}

impl OffsetPaginator {
    /// Create a new offset paginator
    pub fn new(
        offset_param: impl Into<String>,
        limit_param: impl Into<String>,
        limit_value: u32,
        stop_condition: StopCondition,
    ) -> Self {
        Self {
            offset_param: offset_param.into(),
            limit_param: limit_param.into(),
            limit_value,
            stop_condition,
        }
    }
}

impl Paginator for OffsetPaginator {
    fn initial_params(&self, state: &PaginationState) -> HashMap<String, String> {
        let mut params = HashMap::new();
        params.insert(self.offset_param.clone(), state.offset.to_string());
        params.insert(self.limit_param.clone(), self.limit_value.to_string());
        params
    }

    fn process_response(
        &self,
        body: &Value,
        _headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);

        // Check stop condition
        if check_stop_condition(&self.stop_condition, body, records_count, state)
            == StopResult::Stop
        {
            state.mark_done();
            return NextPage::Done;
        }

        // If we got fewer records than limit, we're done
        if records_count < self.limit_value as usize {
            state.mark_done();
            return NextPage::Done;
        }

        // Advance offset
        state.add_offset(self.limit_value);

        let mut params = HashMap::new();
        params.insert(self.offset_param.clone(), state.offset.to_string());
        params.insert(self.limit_param.clone(), self.limit_value.to_string());
        NextPage::with_params(params)
    }
}

// ============================================================================
// Page Number Pagination
// ============================================================================

/// Page number pagination (e.g., traditional web pagination)
///
/// Uses page number parameter to paginate.
/// Common patterns:
/// - `?page=2`
/// - `?page=2&per_page=50`
#[derive(Debug, Clone)]
pub struct PageNumberPaginator {
    /// Query parameter name for page number
    pub page_param: String,
    /// First page number (usually 0 or 1)
    pub start_page: u32,
    /// Optional page size parameter name
    pub page_size_param: Option<String>,
    /// Page size value
    pub page_size: Option<u32>,
    /// Stop condition
    pub stop_condition: StopCondition,
}

impl PageNumberPaginator {
    /// Create a new page number paginator
    pub fn new(page_param: impl Into<String>, start_page: u32) -> Self {
        Self {
            page_param: page_param.into(),
            start_page,
            page_size_param: None,
            page_size: None,
            stop_condition: StopCondition::EmptyPage,
        }
    }

    /// Set page size parameter
    #[must_use]
    pub fn with_page_size(mut self, param: impl Into<String>, size: u32) -> Self {
        self.page_size_param = Some(param.into());
        self.page_size = Some(size);
        self
    }

    /// Set stop condition
    #[must_use]
    pub fn with_stop_condition(mut self, condition: StopCondition) -> Self {
        self.stop_condition = condition;
        self
    }
}

impl Paginator for PageNumberPaginator {
    fn initial_params(&self, state: &PaginationState) -> HashMap<String, String> {
        let mut params = HashMap::new();
        let page = if state.page == 0 {
            self.start_page
        } else {
            state.page
        };
        params.insert(self.page_param.clone(), page.to_string());
        if let (Some(param), Some(size)) = (&self.page_size_param, self.page_size) {
            params.insert(param.clone(), size.to_string());
        }
        params
    }

    fn process_response(
        &self,
        body: &Value,
        _headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);

        // Check stop condition
        if check_stop_condition(&self.stop_condition, body, records_count, state)
            == StopResult::Stop
        {
            state.mark_done();
            return NextPage::Done;
        }

        // If we have a page size and got fewer records, we're done
        if let Some(size) = self.page_size {
            if records_count < size as usize {
                state.mark_done();
                return NextPage::Done;
            }
        }

        // Advance page
        state.next_page();

        let mut params = HashMap::new();
        params.insert(self.page_param.clone(), state.page.to_string());
        if let (Some(param), Some(size)) = (&self.page_size_param, self.page_size) {
            params.insert(param.clone(), size.to_string());
        }
        NextPage::with_params(params)
    }
}

// ============================================================================
// Link Header Pagination
// ============================================================================

/// Link header pagination (RFC 5988)
///
/// Extracts next page URL from the Link header.
/// Common in GitHub, GitLab APIs.
/// Format: `Link: <https://api.github.com/...?page=2>; rel="next", ...`
#[derive(Debug, Clone)]
pub struct LinkHeaderPaginator {
    /// Rel value to follow (default: "next")
    pub rel: String,
}

impl Default for LinkHeaderPaginator {
    fn default() -> Self {
        Self {
            rel: "next".to_string(),
        }
    }
}

impl LinkHeaderPaginator {
    /// Create a new link header paginator
    pub fn new(rel: impl Into<String>) -> Self {
        Self { rel: rel.into() }
    }
}

impl Paginator for LinkHeaderPaginator {
    fn initial_params(&self, _state: &PaginationState) -> HashMap<String, String> {
        HashMap::new()
    }

    fn process_response(
        &self,
        _body: &Value,
        headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);

        // Parse Link header
        if let Some(link_header) = headers.get("link").and_then(|v| v.to_str().ok()) {
            if let Some(next_url) = parse_link_header(link_header, &self.rel) {
                state.next_page();
                return NextPage::with_url(next_url);
            }
        }

        state.mark_done();
        NextPage::Done
    }
}

/// Parse a Link header and extract the URL for the given rel
fn parse_link_header(header: &str, target_rel: &str) -> Option<String> {
    // Link header format: <url>; rel="next", <url>; rel="prev"
    for part in header.split(',') {
        let part = part.trim();
        let mut url = None;
        let mut rel = None;

        for segment in part.split(';') {
            let segment = segment.trim();
            if segment.starts_with('<') && segment.ends_with('>') {
                url = Some(&segment[1..segment.len() - 1]);
            } else if let Some(stripped) = segment.strip_prefix("rel=") {
                let rel_value = stripped.trim_matches('"').trim_matches('\'');
                rel = Some(rel_value);
            }
        }

        if let (Some(u), Some(r)) = (url, rel) {
            if r == target_rel {
                return Some(u.to_string());
            }
        }
    }

    None
}

// ============================================================================
// Next URL Pagination
// ============================================================================

/// Next URL pagination (URL in response body)
///
/// Extracts next page URL from a field in the response body.
/// Common patterns:
/// - `{ "next": "https://api.example.com/items?page=2" }`
/// - `{ "pagination": { "next_url": "..." } }`
#[derive(Debug, Clone)]
pub struct NextUrlPaginator {
    /// JSONPath to extract next URL from response
    pub path: String,
}

impl NextUrlPaginator {
    /// Create a new next URL paginator
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }
}

impl Paginator for NextUrlPaginator {
    fn initial_params(&self, _state: &PaginationState) -> HashMap<String, String> {
        HashMap::new()
    }

    fn process_response(
        &self,
        body: &Value,
        _headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);

        // Extract next URL from response
        if let Some(next_url) = extract_jsonpath(body, &self.path) {
            if !next_url.is_empty() {
                state.next_page();
                return NextPage::with_url(next_url);
            }
        }

        state.mark_done();
        NextPage::Done
    }
}

// ============================================================================
// No Pagination
// ============================================================================

/// No pagination - single request
#[derive(Debug, Clone, Default)]
pub struct NoPaginator;

impl Paginator for NoPaginator {
    fn initial_params(&self, _state: &PaginationState) -> HashMap<String, String> {
        HashMap::new()
    }

    fn process_response(
        &self,
        _body: &Value,
        _headers: &HeaderMap,
        records_count: usize,
        state: &mut PaginationState,
    ) -> NextPage {
        state.add_fetched(records_count as u64);
        state.mark_done();
        NextPage::Done
    }
}
