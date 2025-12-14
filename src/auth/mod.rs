//! Authentication module
//!
//! Supports: API Key, Basic, Bearer, OAuth2, Session, JWT, Custom Headers
//!
//! The `Authenticator` handles all auth types and manages token caching
//! for auth types that require token refresh.

mod authenticator;
mod types;

pub use authenticator::{extract_jsonpath, Authenticator};
pub use types::{AuthConfig, CachedToken, Location};

#[cfg(test)]
mod tests;
