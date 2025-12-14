//! Tests for pagination module

use super::*;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::json;

// ============================================================================
// NextPage Tests
// ============================================================================

#[test]
fn test_next_page_with_param() {
    let next = NextPage::with_param("page", "2");
    assert!(next.is_continue());
    assert!(!next.is_done());

    if let NextPage::Continue { query_params, url } = next {
        assert_eq!(query_params.get("page"), Some(&"2".to_string()));
        assert!(url.is_none());
    } else {
        panic!("Expected Continue");
    }
}

#[test]
fn test_next_page_with_url() {
    let next = NextPage::with_url("https://api.example.com/page2");
    assert!(next.is_continue());

    if let NextPage::Continue { query_params, url } = next {
        assert!(query_params.is_empty());
        assert_eq!(url, Some("https://api.example.com/page2".to_string()));
    } else {
        panic!("Expected Continue");
    }
}

#[test]
fn test_next_page_done() {
    let next = NextPage::Done;
    assert!(next.is_done());
    assert!(!next.is_continue());
}

// ============================================================================
// PaginationState Tests
// ============================================================================

#[test]
fn test_pagination_state_default() {
    let state = PaginationState::new();
    assert_eq!(state.page, 0);
    assert_eq!(state.offset, 0);
    assert!(state.cursor.is_none());
    assert_eq!(state.total_fetched, 0);
    assert!(!state.done);
}

#[test]
fn test_pagination_state_with_page() {
    let state = PaginationState::with_page(5);
    assert_eq!(state.page, 5);
}

#[test]
fn test_pagination_state_mutations() {
    let mut state = PaginationState::new();

    state.next_page();
    assert_eq!(state.page, 1);

    state.add_offset(50);
    assert_eq!(state.offset, 50);

    state.set_cursor("cursor123".to_string());
    assert_eq!(state.cursor, Some("cursor123".to_string()));

    state.add_fetched(100);
    assert_eq!(state.total_fetched, 100);

    state.mark_done();
    assert!(state.done);
}

// ============================================================================
// StopCondition Tests
// ============================================================================

#[test]
fn test_stop_condition_empty_page() {
    let condition = StopCondition::EmptyPage;
    let body = json!({});
    let state = PaginationState::new();

    let result = types::check_stop_condition(&condition, &body, 0, &state);
    assert_eq!(result, StopResult::Stop);

    let result = types::check_stop_condition(&condition, &body, 10, &state);
    assert_eq!(result, StopResult::Continue);
}

#[test]
fn test_stop_condition_field() {
    let condition = StopCondition::field("has_more", false);
    let state = PaginationState::new();

    let body = json!({"has_more": false});
    let result = types::check_stop_condition(&condition, &body, 10, &state);
    assert_eq!(result, StopResult::Stop);

    let body = json!({"has_more": true});
    let result = types::check_stop_condition(&condition, &body, 10, &state);
    assert_eq!(result, StopResult::Continue);
}

#[test]
fn test_stop_condition_total_count() {
    let condition = StopCondition::total_count("total");

    let body = json!({"total": 100});

    // Not at total yet
    let mut state = PaginationState::new();
    state.add_fetched(50);
    let result = types::check_stop_condition(&condition, &body, 50, &state);
    assert_eq!(result, StopResult::Continue);

    // At total
    state.add_fetched(50);
    let result = types::check_stop_condition(&condition, &body, 50, &state);
    assert_eq!(result, StopResult::Stop);
}

#[test]
fn test_stop_condition_total_pages() {
    let condition = StopCondition::total_pages("total_pages");
    let body = json!({"total_pages": 5});

    // Not at last page
    let mut state = PaginationState::new();
    state.page = 3;
    let result = types::check_stop_condition(&condition, &body, 10, &state);
    assert_eq!(result, StopResult::Continue);

    // At last page
    state.page = 5;
    let result = types::check_stop_condition(&condition, &body, 10, &state);
    assert_eq!(result, StopResult::Stop);
}

// ============================================================================
// Cursor Paginator Tests
// ============================================================================

#[test]
fn test_cursor_paginator_initial_params() {
    let paginator =
        CursorPaginator::new("starting_after", "$.data[-1:].id", StopCondition::EmptyPage);

    // No cursor initially
    let state = PaginationState::new();
    let params = paginator.initial_params(&state);
    assert!(params.is_empty());

    // With cursor
    let mut state = PaginationState::new();
    state.set_cursor("obj_123".to_string());
    let params = paginator.initial_params(&state);
    assert_eq!(params.get("starting_after"), Some(&"obj_123".to_string()));
}

#[test]
fn test_cursor_paginator_continues() {
    let paginator = CursorPaginator::new("starting_after", "next_cursor", StopCondition::EmptyPage);

    let body = json!({"data": [{"id": 1}, {"id": 2}], "next_cursor": "cursor_abc"});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    let next = paginator.process_response(&body, &headers, 2, &mut state);

    assert!(next.is_continue());
    assert_eq!(state.cursor, Some("cursor_abc".to_string()));
    assert_eq!(state.total_fetched, 2);

    if let NextPage::Continue { query_params, .. } = next {
        assert_eq!(
            query_params.get("starting_after"),
            Some(&"cursor_abc".to_string())
        );
    }
}

#[test]
fn test_cursor_paginator_stops_on_empty() {
    let paginator = CursorPaginator::new("starting_after", "next_cursor", StopCondition::EmptyPage);

    let body = json!({"data": [], "next_cursor": null});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    let next = paginator.process_response(&body, &headers, 0, &mut state);

    assert!(next.is_done());
    assert!(state.done);
}

#[test]
fn test_cursor_paginator_stops_on_field() {
    let paginator = CursorPaginator::new(
        "starting_after",
        "next_cursor",
        StopCondition::field("has_more", false),
    );

    let body = json!({"data": [{"id": 1}], "next_cursor": "abc", "has_more": false});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    let next = paginator.process_response(&body, &headers, 1, &mut state);

    assert!(next.is_done());
}

// ============================================================================
// Offset Paginator Tests
// ============================================================================

#[test]
fn test_offset_paginator_initial_params() {
    let paginator = OffsetPaginator::new("offset", "limit", 50, StopCondition::EmptyPage);
    let state = PaginationState::new();

    let params = paginator.initial_params(&state);
    assert_eq!(params.get("offset"), Some(&"0".to_string()));
    assert_eq!(params.get("limit"), Some(&"50".to_string()));
}

#[test]
fn test_offset_paginator_continues() {
    let paginator = OffsetPaginator::new("offset", "limit", 50, StopCondition::EmptyPage);
    let body = json!({"items": []});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    // Full page of results
    let next = paginator.process_response(&body, &headers, 50, &mut state);

    assert!(next.is_continue());
    assert_eq!(state.offset, 50);
    assert_eq!(state.total_fetched, 50);

    if let NextPage::Continue { query_params, .. } = next {
        assert_eq!(query_params.get("offset"), Some(&"50".to_string()));
        assert_eq!(query_params.get("limit"), Some(&"50".to_string()));
    }
}

#[test]
fn test_offset_paginator_stops_on_partial_page() {
    let paginator = OffsetPaginator::new("offset", "limit", 50, StopCondition::EmptyPage);
    let body = json!({"items": []});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    // Partial page (less than limit)
    let next = paginator.process_response(&body, &headers, 25, &mut state);

    assert!(next.is_done());
    assert!(state.done);
}

#[test]
fn test_offset_paginator_stops_on_total_count() {
    let paginator =
        OffsetPaginator::new("offset", "limit", 50, StopCondition::total_count("total"));
    let body = json!({"items": [], "total": 75});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    // First page
    let next = paginator.process_response(&body, &headers, 50, &mut state);
    assert!(next.is_continue());

    // Second page
    let next = paginator.process_response(&body, &headers, 50, &mut state);
    assert!(next.is_done()); // total_fetched (100) >= total (75)
}

// ============================================================================
// Page Number Paginator Tests
// ============================================================================

#[test]
fn test_page_number_paginator_initial_params() {
    let paginator = PageNumberPaginator::new("page", 1);
    let state = PaginationState::new();

    let params = paginator.initial_params(&state);
    assert_eq!(params.get("page"), Some(&"1".to_string()));
}

#[test]
fn test_page_number_paginator_with_size() {
    let paginator = PageNumberPaginator::new("page", 1).with_page_size("per_page", 25);
    let state = PaginationState::new();

    let params = paginator.initial_params(&state);
    assert_eq!(params.get("page"), Some(&"1".to_string()));
    assert_eq!(params.get("per_page"), Some(&"25".to_string()));
}

#[test]
fn test_page_number_paginator_continues() {
    let paginator = PageNumberPaginator::new("page", 1).with_page_size("per_page", 25);
    let body = json!({"items": []});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    // Full page
    let next = paginator.process_response(&body, &headers, 25, &mut state);

    assert!(next.is_continue());
    assert_eq!(state.page, 1);

    if let NextPage::Continue { query_params, .. } = next {
        assert_eq!(query_params.get("page"), Some(&"1".to_string()));
    }
}

#[test]
fn test_page_number_paginator_stops_on_partial() {
    let paginator = PageNumberPaginator::new("page", 1).with_page_size("per_page", 25);
    let body = json!({"items": []});
    let headers = HeaderMap::new();
    let mut state = PaginationState::new();

    // Partial page
    let next = paginator.process_response(&body, &headers, 15, &mut state);

    assert!(next.is_done());
}

#[test]
fn test_page_number_paginator_stops_on_total_pages() {
    let paginator = PageNumberPaginator::new("page", 1)
        .with_stop_condition(StopCondition::total_pages("total_pages"));
    let body = json!({"items": [], "total_pages": 3});
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    state.page = 3;

    let next = paginator.process_response(&body, &headers, 10, &mut state);
    assert!(next.is_done());
}

// ============================================================================
// Link Header Paginator Tests
// ============================================================================

#[test]
fn test_link_header_paginator_initial_params() {
    let paginator = LinkHeaderPaginator::default();
    let state = PaginationState::new();

    let params = paginator.initial_params(&state);
    assert!(params.is_empty());
}

#[test]
fn test_link_header_paginator_continues() {
    let paginator = LinkHeaderPaginator::new("next");
    let body = json!({"items": []});

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static(
            "<https://api.example.com/items?page=2>; rel=\"next\", <https://api.example.com/items?page=1>; rel=\"prev\"",
        ),
    );

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_continue());
    if let NextPage::Continue { url, .. } = next {
        assert_eq!(
            url,
            Some("https://api.example.com/items?page=2".to_string())
        );
    }
}

#[test]
fn test_link_header_paginator_stops_no_header() {
    let paginator = LinkHeaderPaginator::default();
    let body = json!({"items": []});
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_done());
}

#[test]
fn test_link_header_paginator_stops_no_next() {
    let paginator = LinkHeaderPaginator::default();
    let body = json!({"items": []});

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static("<https://api.example.com/items?page=1>; rel=\"prev\""),
    );

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_done());
}

// ============================================================================
// Next URL Paginator Tests
// ============================================================================

#[test]
fn test_next_url_paginator_initial_params() {
    let paginator = NextUrlPaginator::new("next_url");
    let state = PaginationState::new();

    let params = paginator.initial_params(&state);
    assert!(params.is_empty());
}

#[test]
fn test_next_url_paginator_continues() {
    let paginator = NextUrlPaginator::new("next_url");
    let body = json!({
        "items": [],
        "next_url": "https://api.example.com/items?page=2"
    });
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_continue());
    if let NextPage::Continue { url, .. } = next {
        assert_eq!(
            url,
            Some("https://api.example.com/items?page=2".to_string())
        );
    }
}

#[test]
fn test_next_url_paginator_nested_path() {
    let paginator = NextUrlPaginator::new("pagination.next");
    let body = json!({
        "items": [],
        "pagination": {
            "next": "https://api.example.com/items?cursor=abc",
            "prev": null
        }
    });
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_continue());
    if let NextPage::Continue { url, .. } = next {
        assert_eq!(
            url,
            Some("https://api.example.com/items?cursor=abc".to_string())
        );
    }
}

#[test]
fn test_next_url_paginator_stops_null() {
    let paginator = NextUrlPaginator::new("next_url");
    let body = json!({
        "items": [],
        "next_url": null
    });
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_done());
}

#[test]
fn test_next_url_paginator_stops_empty() {
    let paginator = NextUrlPaginator::new("next_url");
    let body = json!({
        "items": [],
        "next_url": ""
    });
    let headers = HeaderMap::new();

    let mut state = PaginationState::new();
    let next = paginator.process_response(&body, &headers, 10, &mut state);

    assert!(next.is_done());
}

// ============================================================================
// PaginationConfig Tests
// ============================================================================

#[test]
fn test_pagination_config_cursor() {
    let config = PaginationConfig::cursor("cursor", "$.next", StopCondition::EmptyPage);
    assert!(matches!(config, PaginationConfig::Cursor { .. }));
}

#[test]
fn test_pagination_config_offset() {
    let config = PaginationConfig::offset("offset", "limit", 50, StopCondition::EmptyPage);
    assert!(matches!(config, PaginationConfig::Offset { .. }));
}

#[test]
fn test_pagination_config_page_number() {
    let config = PaginationConfig::page_number("page", 1);
    assert!(matches!(config, PaginationConfig::PageNumber { .. }));
}

#[test]
fn test_pagination_config_link_header() {
    let config = PaginationConfig::link_header("next");
    assert!(matches!(config, PaginationConfig::LinkHeader { .. }));
}

#[test]
fn test_pagination_config_next_url() {
    let config = PaginationConfig::next_url("$.next");
    assert!(matches!(config, PaginationConfig::NextUrl { .. }));
}
