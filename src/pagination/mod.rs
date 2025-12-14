//! Pagination module
//!
//! Supports: Cursor, Offset, Page Number, Link Header, Next URL, Response Body
//!
//! # Overview
//!
//! The pagination module provides a unified interface for handling different
//! API pagination patterns. Each strategy extracts the next page parameters
//! from responses and tracks when pagination is complete.

mod strategies;
mod types;

pub use strategies::{
    CursorPaginator, LinkHeaderPaginator, NextUrlPaginator, NoPaginator, OffsetPaginator,
    PageNumberPaginator,
};
pub use types::{
    NextPage, PaginationConfig, PaginationState, Paginator, StopCondition, StopResult,
};

#[cfg(test)]
mod tests;
