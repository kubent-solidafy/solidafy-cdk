//! HTTP client module
//!
//! Provides HTTP client with retry, rate limiting, and backoff strategies.
//!
//! # Features
//!
//! - **Automatic Retries**: Configurable retry logic with backoff
//! - **Rate Limiting**: Token bucket rate limiter using governor
//! - **Backoff Strategies**: Constant, linear, and exponential backoff
//! - **Authentication**: Integration with auth module

mod client;
mod rate_limit;

pub use client::{HttpClient, HttpClientConfig, RequestConfig};
pub use rate_limit::{RateLimiter, RateLimiterConfig};

#[cfg(test)]
mod tests;
