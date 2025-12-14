//! HTTP client with retry and rate limiting
//!
//! Provides a robust HTTP client that handles:
//! - Automatic retries with configurable backoff
//! - Rate limiting to prevent API throttling
//! - Response body parsing
//! - Error classification for retry decisions

use super::rate_limit::{RateLimiter, RateLimiterConfig};
use crate::auth::{AuthConfig, Authenticator};
use crate::error::{Error, Result};
use crate::types::BackoffType;
use reqwest::{Client, Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

/// Configuration for the HTTP client
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Base URL for all requests
    pub base_url: Option<String>,
    /// Request timeout
    pub timeout: Duration,
    /// Maximum number of retries
    pub max_retries: u32,
    /// Initial delay for backoff
    pub initial_backoff: Duration,
    /// Maximum delay for backoff
    pub max_backoff: Duration,
    /// Type of backoff strategy
    pub backoff_type: BackoffType,
    /// Rate limiter configuration
    pub rate_limit: Option<RateLimiterConfig>,
    /// Default headers for all requests
    pub default_headers: HashMap<String, String>,
    /// User agent string
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            backoff_type: BackoffType::Exponential,
            rate_limit: Some(RateLimiterConfig::default()),
            default_headers: HashMap::new(),
            user_agent: format!("solidafy-cdk/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

impl HttpClientConfig {
    /// Create a new config builder
    pub fn builder() -> HttpClientConfigBuilder {
        HttpClientConfigBuilder::default()
    }
}

/// Builder for HTTP client config
#[derive(Default)]
pub struct HttpClientConfigBuilder {
    config: HttpClientConfig,
}

impl HttpClientConfigBuilder {
    /// Set the base URL
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.config.base_url = Some(url.into());
        self
    }

    /// Set the request timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Set max retries
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set backoff configuration
    pub fn backoff(mut self, backoff_type: BackoffType, initial: Duration, max: Duration) -> Self {
        self.config.backoff_type = backoff_type;
        self.config.initial_backoff = initial;
        self.config.max_backoff = max;
        self
    }

    /// Set rate limiter
    pub fn rate_limit(mut self, config: RateLimiterConfig) -> Self {
        self.config.rate_limit = Some(config);
        self
    }

    /// Disable rate limiting
    pub fn no_rate_limit(mut self) -> Self {
        self.config.rate_limit = None;
        self
    }

    /// Add a default header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.default_headers.insert(key.into(), value.into());
        self
    }

    /// Set user agent
    pub fn user_agent(mut self, agent: impl Into<String>) -> Self {
        self.config.user_agent = agent.into();
        self
    }

    /// Build the config
    pub fn build(self) -> HttpClientConfig {
        self.config
    }
}

/// Configuration for a single request
#[derive(Debug, Clone, Default)]
pub struct RequestConfig {
    /// Query parameters
    pub query: HashMap<String, String>,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Request body (JSON)
    pub body: Option<Value>,
    /// Override timeout for this request
    pub timeout: Option<Duration>,
    /// Override max retries for this request
    pub max_retries: Option<u32>,
}

impl RequestConfig {
    /// Create a new request config
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a query parameter
    #[must_use]
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.insert(key.into(), value.into());
        self
    }

    /// Add a header
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set JSON body
    #[must_use]
    pub fn json(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    /// Set timeout
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set max retries
    #[must_use]
    pub fn retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }
}

/// HTTP client with retry and rate limiting
pub struct HttpClient {
    client: Client,
    config: HttpClientConfig,
    authenticator: Option<Authenticator>,
    rate_limiter: Option<RateLimiter>,
}

impl HttpClient {
    /// Create a new HTTP client with default configuration
    pub fn new() -> Self {
        Self::with_config(HttpClientConfig::default())
    }

    /// Create a new HTTP client with custom configuration
    pub fn with_config(config: HttpClientConfig) -> Self {
        let client = Client::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .build()
            .expect("Failed to build HTTP client");

        let rate_limiter = config.rate_limit.as_ref().map(RateLimiter::new);

        Self {
            client,
            config,
            authenticator: None,
            rate_limiter,
        }
    }

    /// Create a client with authentication
    pub fn with_auth(config: HttpClientConfig, auth_config: AuthConfig) -> Self {
        let mut client = Self::with_config(config);
        client.authenticator = Some(Authenticator::with_client(
            auth_config,
            client.client.clone(),
        ));
        client
    }

    /// Set the authenticator
    pub fn set_authenticator(&mut self, auth_config: AuthConfig) {
        self.authenticator = Some(Authenticator::with_client(auth_config, self.client.clone()));
    }

    /// Get the underlying reqwest client
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Make a GET request
    pub async fn get(&self, url: &str) -> Result<Response> {
        self.request(Method::GET, url, RequestConfig::default())
            .await
    }

    /// Make a GET request with config
    pub async fn get_with_config(&self, url: &str, config: RequestConfig) -> Result<Response> {
        self.request(Method::GET, url, config).await
    }

    /// Make a POST request
    pub async fn post(&self, url: &str, body: Value) -> Result<Response> {
        self.request(Method::POST, url, RequestConfig::default().json(body))
            .await
    }

    /// Make a POST request with config
    pub async fn post_with_config(&self, url: &str, config: RequestConfig) -> Result<Response> {
        self.request(Method::POST, url, config).await
    }

    /// Make a generic request
    #[allow(clippy::too_many_lines)]
    pub async fn request(
        &self,
        method: Method,
        url: &str,
        config: RequestConfig,
    ) -> Result<Response> {
        let full_url = self.build_url(url);
        let max_retries = config.max_retries.unwrap_or(self.config.max_retries);
        let timeout = config.timeout.unwrap_or(self.config.timeout);

        let mut last_error = None;
        let mut attempt = 0;

        while attempt <= max_retries {
            // Wait for rate limiter
            if let Some(ref limiter) = self.rate_limiter {
                limiter.wait().await;
            }

            // Build request
            let mut req = self.client.request(method.clone(), &full_url);

            // Add default headers
            for (key, value) in &self.config.default_headers {
                req = req.header(key.as_str(), value.as_str());
            }

            // Add request-specific headers
            for (key, value) in &config.headers {
                req = req.header(key.as_str(), value.as_str());
            }

            // Add query parameters
            if !config.query.is_empty() {
                req = req.query(&config.query);
            }

            // Add body
            if let Some(ref body) = config.body {
                req = req.json(body);
            }

            // Set timeout
            req = req.timeout(timeout);

            // Apply authentication
            if let Some(ref auth) = self.authenticator {
                req = auth.apply(req).await?;
            }

            // Send request
            match req.send().await {
                Ok(response) => {
                    let status = response.status();

                    // Check for rate limiting
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        let retry_after = extract_retry_after(&response);
                        if attempt < max_retries {
                            warn!(
                                "Rate limited (429), attempt {}/{}, waiting {}s",
                                attempt + 1,
                                max_retries + 1,
                                retry_after
                            );
                            tokio::time::sleep(Duration::from_secs(retry_after)).await;
                            attempt += 1;
                            continue;
                        }
                        return Err(Error::RateLimited {
                            retry_after_seconds: retry_after,
                        });
                    }

                    // Check for retryable server errors
                    if is_retryable_status(status) && attempt < max_retries {
                        let delay = self.calculate_backoff(attempt);
                        warn!(
                            "Request failed with {}, attempt {}/{}, retrying in {:?}",
                            status.as_u16(),
                            attempt + 1,
                            max_retries + 1,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        last_error = Some(Error::HttpStatus {
                            status: status.as_u16(),
                            body: String::new(),
                        });
                        continue;
                    }

                    // Check for client errors (non-retryable)
                    if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
                        let body = response.text().await.unwrap_or_default();
                        return Err(Error::HttpStatus {
                            status: status.as_u16(),
                            body,
                        });
                    }

                    // Check for server errors (after retries exhausted)
                    if status.is_server_error() {
                        let body = response.text().await.unwrap_or_default();
                        return Err(Error::HttpStatus {
                            status: status.as_u16(),
                            body,
                        });
                    }

                    debug!("Request succeeded: {} {}", method, full_url);
                    return Ok(response);
                }
                Err(e) => {
                    if e.is_timeout() {
                        if attempt < max_retries {
                            let delay = self.calculate_backoff(attempt);
                            warn!(
                                "Request timeout, attempt {}/{}, retrying in {:?}",
                                attempt + 1,
                                max_retries + 1,
                                delay
                            );
                            tokio::time::sleep(delay).await;
                            attempt += 1;
                            #[allow(clippy::cast_possible_truncation)]
                            {
                                last_error = Some(Error::Timeout {
                                    timeout_ms: timeout.as_millis() as u64,
                                });
                            }
                            continue;
                        }
                        #[allow(clippy::cast_possible_truncation)]
                        return Err(Error::Timeout {
                            timeout_ms: timeout.as_millis() as u64,
                        });
                    }

                    if e.is_connect() && attempt < max_retries {
                        let delay = self.calculate_backoff(attempt);
                        warn!(
                            "Connection error, attempt {}/{}, retrying in {:?}",
                            attempt + 1,
                            max_retries + 1,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        last_error = Some(Error::Http(e));
                        continue;
                    }

                    return Err(Error::Http(e));
                }
            }
        }

        // Exhausted all retries
        Err(last_error.unwrap_or_else(|| Error::MaxRetriesExceeded { max_retries }))
    }

    /// Make a request and parse JSON response
    pub async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        url: &str,
        config: RequestConfig,
    ) -> Result<T> {
        let response = self.request(method, url, config).await?;
        let json: T = response.json().await.map_err(Error::Http)?;
        Ok(json)
    }

    /// Make a GET request and parse JSON response
    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T> {
        self.request_json(Method::GET, url, RequestConfig::default())
            .await
    }

    /// Make a GET request with config and parse JSON response
    pub async fn get_json_with_config<T: DeserializeOwned>(
        &self,
        url: &str,
        config: RequestConfig,
    ) -> Result<T> {
        self.request_json(Method::GET, url, config).await
    }

    /// Check if rate limiting is enabled
    pub fn has_rate_limiter(&self) -> bool {
        self.rate_limiter.is_some()
    }

    /// Build full URL from path
    fn build_url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            return path.to_string();
        }

        match &self.config.base_url {
            Some(base) => {
                let base = base.trim_end_matches('/');
                let path = path.trim_start_matches('/');
                format!("{base}/{path}")
            }
            None => path.to_string(),
        }
    }

    /// Calculate backoff delay for a given attempt
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        let delay = match self.config.backoff_type {
            BackoffType::Constant => self.config.initial_backoff,
            BackoffType::Linear => self.config.initial_backoff * (attempt + 1),
            BackoffType::Exponential => {
                let factor = 2u32.saturating_pow(attempt);
                self.config.initial_backoff * factor
            }
        };

        std::cmp::min(delay, self.config.max_backoff)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("config", &self.config)
            .field("has_authenticator", &self.authenticator.is_some())
            .field("has_rate_limiter", &self.rate_limiter.is_some())
            .finish_non_exhaustive()
    }
}

/// Check if an HTTP status is retryable
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        429 | 500 | 502 | 503 | 504 | 520 | 521 | 522 | 523 | 524
    )
}

/// Extract retry-after header value
fn extract_retry_after(response: &Response) -> u64 {
    response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}
