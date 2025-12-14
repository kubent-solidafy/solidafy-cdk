//! Rate limiting implementation
//!
//! Uses the governor crate for token bucket rate limiting.

use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter as Governor};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum number of requests per second
    pub requests_per_second: u32,
    /// Burst size (max tokens in bucket)
    pub burst_size: u32,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 10,
            burst_size: 10,
        }
    }
}

impl RateLimiterConfig {
    /// Create a new rate limiter config
    pub fn new(requests_per_second: u32, burst_size: u32) -> Self {
        Self {
            requests_per_second,
            burst_size,
        }
    }

    /// Create config for high throughput (100 rps)
    pub fn high_throughput() -> Self {
        Self {
            requests_per_second: 100,
            burst_size: 100,
        }
    }

    /// Create config for low throughput (1 rps)
    pub fn low_throughput() -> Self {
        Self {
            requests_per_second: 1,
            burst_size: 1,
        }
    }
}

/// Token bucket rate limiter
#[derive(Clone)]
pub struct RateLimiter {
    limiter: Arc<Governor<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
}

impl RateLimiter {
    /// Create a new rate limiter with the given config
    pub fn new(config: &RateLimiterConfig) -> Self {
        let quota = Quota::per_second(
            NonZeroU32::new(config.requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap()),
        )
        .allow_burst(NonZeroU32::new(config.burst_size).unwrap_or(NonZeroU32::new(1).unwrap()));

        Self {
            limiter: Arc::new(Governor::direct(quota)),
        }
    }

    /// Create a rate limiter with default settings
    pub fn default_limiter() -> Self {
        Self::new(&RateLimiterConfig::default())
    }

    /// Wait until a request can be made (blocks)
    pub async fn wait(&self) {
        self.limiter.until_ready().await;
    }

    /// Check if a request can be made immediately
    pub fn check(&self) -> bool {
        self.limiter.check().is_ok()
    }

    /// Try to acquire a permit, returning immediately
    pub fn try_acquire(&self) -> bool {
        self.limiter.check().is_ok()
    }

    /// Wait with a timeout
    pub async fn wait_with_timeout(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, self.limiter.until_ready())
            .await
            .is_ok()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::default_limiter()
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter").finish()
    }
}

#[cfg(test)]
mod rate_limit_tests {
    use super::*;

    #[test]
    fn test_rate_limiter_config_default() {
        let config = RateLimiterConfig::default();
        assert_eq!(config.requests_per_second, 10);
        assert_eq!(config.burst_size, 10);
    }

    #[test]
    fn test_rate_limiter_config_new() {
        let config = RateLimiterConfig::new(50, 25);
        assert_eq!(config.requests_per_second, 50);
        assert_eq!(config.burst_size, 25);
    }

    #[test]
    fn test_rate_limiter_config_presets() {
        let high = RateLimiterConfig::high_throughput();
        assert_eq!(high.requests_per_second, 100);

        let low = RateLimiterConfig::low_throughput();
        assert_eq!(low.requests_per_second, 1);
    }

    #[tokio::test]
    async fn test_rate_limiter_allows_burst() {
        let limiter = RateLimiter::new(&RateLimiterConfig::new(10, 5));

        // Should allow burst of 5 requests immediately
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_wait() {
        let limiter = RateLimiter::new(&RateLimiterConfig::new(100, 10));

        // Should complete without blocking (within burst)
        limiter.wait().await;
    }

    #[tokio::test]
    async fn test_rate_limiter_wait_with_timeout() {
        let limiter = RateLimiter::new(&RateLimiterConfig::new(100, 10));

        // Should succeed within timeout
        let result = limiter.wait_with_timeout(Duration::from_millis(100)).await;
        assert!(result);
    }
}
