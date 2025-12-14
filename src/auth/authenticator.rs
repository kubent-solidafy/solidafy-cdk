//! Authenticator implementation
//!
//! Handles applying authentication to requests and managing token refresh.

use super::types::{AuthConfig, CachedToken, Location};
use crate::error::{Error, Result};
use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Authenticator handles applying authentication to HTTP requests
pub struct Authenticator {
    /// Auth configuration
    config: AuthConfig,
    /// Cached token for OAuth2/Session/JWT auth
    cached_token: Arc<RwLock<Option<CachedToken>>>,
    /// HTTP client for token requests
    http_client: Client,
}

impl Authenticator {
    /// Create a new authenticator with the given config
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            cached_token: Arc::new(RwLock::new(None)),
            http_client: Client::new(),
        }
    }

    /// Create an authenticator with a custom HTTP client
    pub fn with_client(config: AuthConfig, http_client: Client) -> Self {
        Self {
            config,
            cached_token: Arc::new(RwLock::new(None)),
            http_client,
        }
    }

    /// Apply authentication to a request builder
    pub async fn apply(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        match &self.config {
            AuthConfig::None => Ok(req),

            AuthConfig::ApiKey {
                location,
                header_name,
                query_param,
                prefix,
                value,
            } => {
                let val = format!("{}{}", prefix.as_deref().unwrap_or(""), value);
                match location {
                    Location::Header => {
                        let header = header_name.as_deref().unwrap_or("Authorization");
                        Ok(req.header(header, val))
                    }
                    Location::Query => {
                        let param = query_param.as_deref().unwrap_or("api_key");
                        Ok(req.query(&[(param, val)]))
                    }
                }
            }

            AuthConfig::Basic { username, password } => {
                Ok(req.basic_auth(username, Some(password)))
            }

            AuthConfig::Bearer { token } => Ok(req.bearer_auth(token)),

            AuthConfig::Oauth2ClientCredentials { .. }
            | AuthConfig::Oauth2Refresh { .. }
            | AuthConfig::Session { .. }
            | AuthConfig::Jwt { .. } => {
                let token = self.get_or_refresh_token().await?;
                Ok(req.bearer_auth(token))
            }

            AuthConfig::CustomHeaders { headers } => {
                let mut req = req;
                for (key, value) in headers {
                    req = req.header(key.as_str(), value.as_str());
                }
                Ok(req)
            }
        }
    }

    /// Get a valid token, refreshing if necessary
    async fn get_or_refresh_token(&self) -> Result<String> {
        // Check if we have a valid cached token
        {
            let cached = self.cached_token.read().await;
            if let Some(token) = cached.as_ref() {
                if !token.is_expired() {
                    return Ok(token.token.clone());
                }
            }
        }

        // Need to refresh - acquire write lock
        let mut cached = self.cached_token.write().await;

        // Double-check after acquiring write lock (another task might have refreshed)
        if let Some(token) = cached.as_ref() {
            if !token.is_expired() {
                return Ok(token.token.clone());
            }
        }

        // Refresh the token
        let new_token = self.fetch_new_token().await?;
        let token_str = new_token.token.clone();
        *cached = Some(new_token);

        Ok(token_str)
    }

    /// Fetch a new token based on auth type
    async fn fetch_new_token(&self) -> Result<CachedToken> {
        match &self.config {
            AuthConfig::Oauth2ClientCredentials {
                token_url,
                client_id,
                client_secret,
                scopes,
                token_body,
            } => {
                self.fetch_oauth2_client_credentials(
                    token_url,
                    client_id,
                    client_secret,
                    scopes,
                    token_body,
                )
                .await
            }

            AuthConfig::Oauth2Refresh {
                token_url,
                client_id,
                client_secret,
                refresh_token,
            } => {
                self.fetch_oauth2_refresh(token_url, client_id, client_secret, refresh_token)
                    .await
            }

            AuthConfig::Session {
                login_url,
                login_method,
                login_body,
                token_path,
                expires_in_path,
                ..
            } => {
                self.fetch_session_token(
                    login_url,
                    login_method.clone(),
                    login_body,
                    token_path,
                    expires_in_path.as_deref(),
                )
                .await
            }

            AuthConfig::Jwt {
                issuer,
                subject,
                audience,
                private_key,
                algorithm,
                token_lifetime_seconds,
                claims,
                token_url,
            } => {
                self.generate_jwt(
                    issuer,
                    subject.as_deref(),
                    audience,
                    private_key,
                    *algorithm,
                    *token_lifetime_seconds,
                    claims,
                    token_url.as_deref(),
                )
                .await
            }

            _ => Err(Error::auth(
                "Token refresh not supported for this auth type",
            )),
        }
    }

    /// Fetch OAuth2 token using client credentials flow
    async fn fetch_oauth2_client_credentials(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scopes: &[String],
        extra_body: &HashMap<String, String>,
    ) -> Result<CachedToken> {
        let mut form = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", client_id.to_string()),
            ("client_secret", client_secret.to_string()),
        ];

        if !scopes.is_empty() {
            form.push(("scope", scopes.join(" ")));
        }

        for (key, value) in extra_body {
            form.push((key.as_str(), value.clone()));
        }

        let response = self
            .http_client
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(Error::Http)?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::OAuth2 {
                message: format!("Token request failed with status {status}: {body}"),
            });
        }

        let token_response: TokenResponse = response.json().await.map_err(Error::Http)?;
        Ok(token_response.into_cached_token())
    }

    /// Fetch OAuth2 token using refresh token flow
    async fn fetch_oauth2_refresh(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
    ) -> Result<CachedToken> {
        let form = [
            ("grant_type", "refresh_token"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
        ];

        let response = self
            .http_client
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(Error::Http)?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::TokenRefresh {
                message: format!("Refresh token request failed with status {status}: {body}"),
            });
        }

        let token_response: TokenResponse = response.json().await.map_err(Error::Http)?;
        Ok(token_response.into_cached_token())
    }

    /// Fetch session token by logging in
    async fn fetch_session_token(
        &self,
        login_url: &str,
        login_method: reqwest::Method,
        login_body: &HashMap<String, String>,
        token_path: &str,
        expires_in_path: Option<&str>,
    ) -> Result<CachedToken> {
        let response = self
            .http_client
            .request(login_method, login_url)
            .json(login_body)
            .send()
            .await
            .map_err(Error::Http)?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Auth {
                message: format!("Login request failed with status {status}: {body}"),
            });
        }

        let body: Value = response.json().await.map_err(Error::Http)?;

        // Extract token using JSONPath
        let token = extract_jsonpath(&body, token_path).ok_or_else(|| Error::Auth {
            message: format!("Could not extract token from path: {token_path}"),
        })?;

        // Extract expiration if path provided
        let expires_at = if let Some(path) = expires_in_path {
            extract_jsonpath(&body, path)
                .and_then(|v| v.parse::<i64>().ok())
                .map(|secs| Utc::now() + chrono::Duration::seconds(secs))
        } else {
            None
        };

        Ok(CachedToken::new(token, expires_at))
    }

    /// Generate a JWT and optionally exchange it for an access token
    #[allow(clippy::too_many_arguments)]
    async fn generate_jwt(
        &self,
        issuer: &str,
        subject: Option<&str>,
        audience: &str,
        private_key: &str,
        algorithm: crate::types::JwtAlgorithm,
        lifetime_seconds: u64,
        extra_claims: &HashMap<String, String>,
        token_url: Option<&str>,
    ) -> Result<CachedToken> {
        let now = Utc::now().timestamp();
        #[allow(clippy::cast_possible_wrap)]
        let exp = now + lifetime_seconds as i64;

        // Build claims
        let claims = JwtClaims {
            iss: issuer.to_string(),
            sub: subject.map(String::from),
            aud: audience.to_string(),
            iat: now,
            exp,
            extra: extra_claims.clone(),
        };

        // Create header with algorithm
        let header = Header::new(algorithm.into());

        // Encode the JWT
        let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes()).map_err(|e| {
            Error::JwtGeneration {
                message: format!("Invalid private key: {e}"),
            }
        })?;

        let jwt = encode(&header, &claims, &encoding_key).map_err(|e| Error::JwtGeneration {
            message: format!("Failed to encode JWT: {e}"),
        })?;

        // If token_url provided, exchange JWT for access token (Google-style)
        if let Some(url) = token_url {
            let form = [
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ];

            let response = self
                .http_client
                .post(url)
                .form(&form)
                .send()
                .await
                .map_err(Error::Http)?;

            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                return Err(Error::JwtGeneration {
                    message: format!("JWT token exchange failed with status {status}: {body}"),
                });
            }

            let token_response: TokenResponse = response.json().await.map_err(Error::Http)?;
            Ok(token_response.into_cached_token())
        } else {
            // Use the JWT directly as the bearer token
            #[allow(clippy::cast_possible_wrap)]
            Ok(CachedToken::expires_in(jwt, lifetime_seconds as i64))
        }
    }

    /// Clear the cached token (useful for testing or forced refresh)
    pub async fn clear_cache(&self) {
        let mut cached = self.cached_token.write().await;
        *cached = None;
    }

    /// Get the current auth config
    pub fn config(&self) -> &AuthConfig {
        &self.config
    }
}

/// OAuth2 token response
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    #[allow(dead_code)]
    token_type: Option<String>,
}

impl TokenResponse {
    fn into_cached_token(self) -> CachedToken {
        match self.expires_in {
            Some(secs) => CachedToken::expires_in(self.access_token, secs),
            None => CachedToken::new(self.access_token, None),
        }
    }
}

/// JWT claims structure
#[derive(Debug, Serialize)]
struct JwtClaims {
    iss: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub: Option<String>,
    aud: String,
    iat: i64,
    exp: i64,
    #[serde(flatten)]
    extra: HashMap<String, String>,
}

/// Extract a value from JSON using a simple JSONPath expression
/// Supports basic paths like "$.data.token" or "data.token"
pub fn extract_jsonpath(value: &Value, path: &str) -> Option<String> {
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

    match current {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
