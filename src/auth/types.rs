//! Auth configuration types
//!
//! These types represent the runtime auth configuration after template
//! interpolation has been applied.

use crate::types::JwtAlgorithm;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Location for API key placement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Location {
    /// Place in HTTP header
    #[default]
    Header,
    /// Place in query parameter
    Query,
}

/// Authentication configuration (after template interpolation)
#[derive(Debug, Clone, Default)]
pub enum AuthConfig {
    /// No authentication required
    #[default]
    None,

    /// API Key authentication (header or query)
    ApiKey {
        /// Where to place the API key
        location: Location,
        /// Header name (for header location)
        header_name: Option<String>,
        /// Query parameter name (for query location)
        query_param: Option<String>,
        /// Prefix to add before the value (e.g., "Bearer ")
        prefix: Option<String>,
        /// The API key value
        value: String,
    },

    /// HTTP Basic authentication
    Basic {
        /// Username
        username: String,
        /// Password
        password: String,
    },

    /// Bearer token authentication
    Bearer {
        /// The bearer token
        token: String,
    },

    /// OAuth2 Client Credentials flow
    Oauth2ClientCredentials {
        /// Token endpoint URL
        token_url: String,
        /// Client ID
        client_id: String,
        /// Client secret
        client_secret: String,
        /// Requested scopes
        scopes: Vec<String>,
        /// Additional token request body parameters
        token_body: HashMap<String, String>,
    },

    /// OAuth2 Refresh Token flow
    Oauth2Refresh {
        /// Token endpoint URL
        token_url: String,
        /// Client ID
        client_id: String,
        /// Client secret
        client_secret: String,
        /// Refresh token
        refresh_token: String,
    },

    /// Session-based authentication (login endpoint)
    Session {
        /// Login endpoint URL
        login_url: String,
        /// HTTP method for login (POST by default)
        login_method: reqwest::Method,
        /// Login request body
        login_body: HashMap<String, String>,
        /// JSONPath to extract token from response
        token_path: String,
        /// Header name to use for the token
        token_header: String,
        /// Prefix for token value (e.g., "Bearer ")
        token_prefix: Option<String>,
        /// JSONPath to extract expiration time
        expires_in_path: Option<String>,
    },

    /// JWT authentication (service account style)
    Jwt {
        /// Token issuer (iss claim)
        issuer: String,
        /// Token subject (sub claim, optional)
        subject: Option<String>,
        /// Token audience (aud claim)
        audience: String,
        /// Private key for signing (PEM format)
        private_key: String,
        /// Signing algorithm
        algorithm: JwtAlgorithm,
        /// Token lifetime in seconds
        token_lifetime_seconds: u64,
        /// Additional claims
        claims: HashMap<String, String>,
        /// Optional token endpoint for two-step auth (like Google)
        token_url: Option<String>,
    },

    /// Custom headers
    CustomHeaders {
        /// Headers to add to each request
        headers: HashMap<String, String>,
    },
}

/// Cached token with expiration
#[derive(Debug, Clone)]
pub struct CachedToken {
    /// The access token
    pub token: String,
    /// When the token expires
    pub expires_at: Option<DateTime<Utc>>,
}

impl CachedToken {
    /// Create a new cached token
    pub fn new(token: String, expires_at: Option<DateTime<Utc>>) -> Self {
        Self { token, expires_at }
    }

    /// Create a token that expires in N seconds from now
    pub fn expires_in(token: String, seconds: i64) -> Self {
        let expires_at = Utc::now() + chrono::Duration::seconds(seconds);
        Self {
            token,
            expires_at: Some(expires_at),
        }
    }

    /// Check if the token is expired (with 30 second buffer)
    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => {
                let buffer = chrono::Duration::seconds(30);
                Utc::now() + buffer >= expires_at
            }
            None => false, // No expiration = never expires
        }
    }
}

#[cfg(test)]
mod type_tests {
    use super::*;

    #[test]
    fn test_cached_token_not_expired() {
        let token = CachedToken::expires_in("test".to_string(), 3600);
        assert!(!token.is_expired());
    }

    #[test]
    fn test_cached_token_expired() {
        let token = CachedToken::expires_in("test".to_string(), -100);
        assert!(token.is_expired());
    }

    #[test]
    fn test_cached_token_no_expiration() {
        let token = CachedToken::new("test".to_string(), None);
        assert!(!token.is_expired());
    }

    #[test]
    fn test_auth_config_default() {
        let config = AuthConfig::default();
        assert!(matches!(config, AuthConfig::None));
    }
}
