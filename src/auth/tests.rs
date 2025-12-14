//! Tests for the auth module

use super::*;
use base64::Engine;
use std::collections::HashMap;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_no_auth() {
    let auth = Authenticator::new(AuthConfig::None);
    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");

    let result = auth.apply(req).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_api_key_header() {
    let auth = Authenticator::new(AuthConfig::ApiKey {
        location: Location::Header,
        header_name: Some("X-API-Key".to_string()),
        query_param: None,
        prefix: None,
        value: "test-key-123".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    // Build the request to inspect headers
    let built = req.build().unwrap();
    assert_eq!(built.headers().get("X-API-Key").unwrap(), "test-key-123");
}

#[tokio::test]
async fn test_api_key_header_with_prefix() {
    let auth = Authenticator::new(AuthConfig::ApiKey {
        location: Location::Header,
        header_name: Some("Authorization".to_string()),
        query_param: None,
        prefix: Some("Bearer ".to_string()),
        value: "my-token".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(
        built.headers().get("Authorization").unwrap(),
        "Bearer my-token"
    );
}

#[tokio::test]
async fn test_api_key_query() {
    let auth = Authenticator::new(AuthConfig::ApiKey {
        location: Location::Query,
        header_name: None,
        query_param: Some("apikey".to_string()),
        prefix: None,
        value: "secret123".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert!(built.url().query().unwrap().contains("apikey=secret123"));
}

#[tokio::test]
async fn test_basic_auth() {
    let auth = Authenticator::new(AuthConfig::Basic {
        username: "user".to_string(),
        password: "pass".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    let auth_header = built
        .headers()
        .get("Authorization")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(auth_header.starts_with("Basic "));

    // Verify base64 encoding
    let encoded = auth_header.strip_prefix("Basic ").unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .unwrap();
    assert_eq!(String::from_utf8(decoded).unwrap(), "user:pass");
}

#[tokio::test]
async fn test_bearer_auth() {
    let auth = Authenticator::new(AuthConfig::Bearer {
        token: "my-bearer-token".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(
        built.headers().get("Authorization").unwrap(),
        "Bearer my-bearer-token"
    );
}

#[tokio::test]
async fn test_custom_headers() {
    let mut headers = HashMap::new();
    headers.insert("X-Custom-1".to_string(), "value1".to_string());
    headers.insert("X-Custom-2".to_string(), "value2".to_string());

    let auth = Authenticator::new(AuthConfig::CustomHeaders { headers });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(built.headers().get("X-Custom-1").unwrap(), "value1");
    assert_eq!(built.headers().get("X-Custom-2").unwrap(), "value2");
}

#[tokio::test]
async fn test_oauth2_client_credentials() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains("client_id=my-client"))
        .and(body_string_contains("client_secret=my-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "oauth-token-123",
            "expires_in": 3600,
            "token_type": "Bearer"
        })))
        .mount(&mock_server)
        .await;

    let auth = Authenticator::new(AuthConfig::Oauth2ClientCredentials {
        token_url: format!("{}/oauth/token", mock_server.uri()),
        client_id: "my-client".to_string(),
        client_secret: "my-secret".to_string(),
        scopes: vec!["read".to_string(), "write".to_string()],
        token_body: HashMap::new(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(
        built.headers().get("Authorization").unwrap(),
        "Bearer oauth-token-123"
    );
}

#[tokio::test]
async fn test_oauth2_token_caching() {
    let mock_server = MockServer::start().await;

    // This should only be called once due to caching
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "cached-token",
            "expires_in": 3600
        })))
        .expect(1) // Expect exactly 1 call
        .mount(&mock_server)
        .await;

    let auth = Authenticator::new(AuthConfig::Oauth2ClientCredentials {
        token_url: format!("{}/oauth/token", mock_server.uri()),
        client_id: "client".to_string(),
        client_secret: "secret".to_string(),
        scopes: vec![],
        token_body: HashMap::new(),
    });

    let client = reqwest::Client::new();

    // First request - should fetch token
    let req1 = client.get("https://example.com/api");
    let _ = auth.apply(req1).await.unwrap();

    // Second request - should use cached token
    let req2 = client.get("https://example.com/api");
    let _ = auth.apply(req2).await.unwrap();

    // Third request - should still use cached token
    let req3 = client.get("https://example.com/api");
    let _ = auth.apply(req3).await.unwrap();
}

#[tokio::test]
async fn test_oauth2_refresh_token() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("refresh_token=my-refresh-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "refreshed-token",
            "expires_in": 3600
        })))
        .mount(&mock_server)
        .await;

    let auth = Authenticator::new(AuthConfig::Oauth2Refresh {
        token_url: format!("{}/oauth/token", mock_server.uri()),
        client_id: "client".to_string(),
        client_secret: "secret".to_string(),
        refresh_token: "my-refresh-token".to_string(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(
        built.headers().get("Authorization").unwrap(),
        "Bearer refreshed-token"
    );
}

#[tokio::test]
async fn test_session_auth() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/auth/login"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "access_token": "session-token-xyz",
                "expires_in": 7200
            }
        })))
        .mount(&mock_server)
        .await;

    let mut login_body = HashMap::new();
    login_body.insert("email".to_string(), "test@example.com".to_string());
    login_body.insert("password".to_string(), "secret".to_string());

    let auth = Authenticator::new(AuthConfig::Session {
        login_url: format!("{}/auth/login", mock_server.uri()),
        login_method: reqwest::Method::POST,
        login_body,
        token_path: "$.data.access_token".to_string(),
        token_header: "Authorization".to_string(),
        token_prefix: Some("Bearer ".to_string()),
        expires_in_path: Some("$.data.expires_in".to_string()),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let req = auth.apply(req).await.unwrap();

    let built = req.build().unwrap();
    assert_eq!(
        built.headers().get("Authorization").unwrap(),
        "Bearer session-token-xyz"
    );
}

#[tokio::test]
async fn test_clear_cache() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "token",
            "expires_in": 3600
        })))
        .expect(2) // Expect 2 calls due to cache clear
        .mount(&mock_server)
        .await;

    let auth = Authenticator::new(AuthConfig::Oauth2ClientCredentials {
        token_url: format!("{}/oauth/token", mock_server.uri()),
        client_id: "client".to_string(),
        client_secret: "secret".to_string(),
        scopes: vec![],
        token_body: HashMap::new(),
    });

    let client = reqwest::Client::new();

    // First request
    let req1 = client.get("https://example.com/api");
    let _ = auth.apply(req1).await.unwrap();

    // Clear cache
    auth.clear_cache().await;

    // Second request - should fetch new token
    let req2 = client.get("https://example.com/api");
    let _ = auth.apply(req2).await.unwrap();
}

#[tokio::test]
async fn test_oauth2_error_handling() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": "Client authentication failed"
        })))
        .mount(&mock_server)
        .await;

    let auth = Authenticator::new(AuthConfig::Oauth2ClientCredentials {
        token_url: format!("{}/oauth/token", mock_server.uri()),
        client_id: "bad-client".to_string(),
        client_secret: "bad-secret".to_string(),
        scopes: vec![],
        token_body: HashMap::new(),
    });

    let client = reqwest::Client::new();
    let req = client.get("https://example.com/api");
    let result = auth.apply(req).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("401"));
}

// Test for extract_jsonpath helper
#[test]
fn test_extract_jsonpath() {
    use super::authenticator::extract_jsonpath;
    use serde_json::json;

    let data = json!({
        "data": {
            "token": "abc123",
            "count": 42
        }
    });

    assert_eq!(
        extract_jsonpath(&data, "$.data.token"),
        Some("abc123".to_string())
    );
    assert_eq!(
        extract_jsonpath(&data, "data.token"),
        Some("abc123".to_string())
    );
    assert_eq!(
        extract_jsonpath(&data, "$.data.count"),
        Some("42".to_string())
    );
    assert_eq!(extract_jsonpath(&data, "$.missing"), None);
}
