//! YAML parser for connector definitions
//!
//! Parses and validates connector YAML files.
//! Supports both built-in connectors (by name) and custom YAML files (by path).

use crate::connectors;
use crate::error::{Error, Result};
use crate::loader::types::ConnectorDefinition;
use std::fs;
use std::path::Path;

/// Load a connector definition from a name or file path
///
/// This function first checks if the input is a built-in connector name (e.g., "stripe"),
/// then falls back to loading from a file path.
///
/// # Examples
///
/// ```ignore
/// // Load built-in connector by name
/// let connector = load_connector("stripe")?;
///
/// // Load custom connector from file
/// let connector = load_connector("./my-connector.yaml")?;
/// ```
pub fn load_connector(path: impl AsRef<Path>) -> Result<ConnectorDefinition> {
    let path = path.as_ref();
    let path_str = path.to_string_lossy();

    // First, check if this is a built-in connector name (no path separators, no .yaml extension)
    if !path_str.contains('/')
        && !path_str.contains('\\')
        && !path_str.ends_with(".yaml")
        && !path_str.ends_with(".yml")
    {
        if let Some(yaml) = connectors::get_builtin(&path_str) {
            return load_connector_from_str(yaml);
        }
    }

    // Fall back to loading from file
    let content = fs::read_to_string(path).map_err(|e| {
        // If file not found, provide helpful error with list of built-in connectors
        if e.kind() == std::io::ErrorKind::NotFound {
            let builtin_list = connectors::list_builtin().join(", ");
            Error::config(format!(
                "Connector '{}' not found. Built-in connectors: {}. Or provide a path to a YAML file.",
                path.display(),
                builtin_list
            ))
        } else {
            Error::config(format!(
                "Failed to read connector file '{}': {}",
                path.display(),
                e
            ))
        }
    })?;
    load_connector_from_str(&content)
}

/// Load a connector definition from a YAML string
pub fn load_connector_from_str(yaml: &str) -> Result<ConnectorDefinition> {
    let def: ConnectorDefinition = serde_yaml::from_str(yaml)
        .map_err(|e| Error::config(format!("Failed to parse connector YAML: {e}")))?;

    validate_connector(&def)?;
    Ok(def)
}

/// Validate a connector definition
fn validate_connector(def: &ConnectorDefinition) -> Result<()> {
    // Validate name
    if def.name.is_empty() {
        return Err(Error::config("Connector name cannot be empty"));
    }

    // Validate base URL
    if def.base_url.is_empty() {
        return Err(Error::config("Connector base_url cannot be empty"));
    }

    // Validate streams
    if def.streams.is_empty() {
        return Err(Error::config("Connector must have at least one stream"));
    }

    // Validate each stream
    let stream_names: std::collections::HashSet<_> = def.streams.iter().map(|s| &s.name).collect();

    if stream_names.len() != def.streams.len() {
        return Err(Error::config("Duplicate stream names found"));
    }

    for stream in &def.streams {
        validate_stream(stream)?;
    }

    Ok(())
}

/// Validate a stream definition
fn validate_stream(stream: &crate::loader::types::StreamDefinition) -> Result<()> {
    if stream.name.is_empty() {
        return Err(Error::config("Stream name cannot be empty"));
    }

    if stream.request.path.is_empty() {
        return Err(Error::config(format!(
            "Stream '{}' path cannot be empty",
            stream.name
        )));
    }

    // Validate method
    let valid_methods = ["GET", "POST", "PUT", "PATCH", "DELETE"];
    if !valid_methods.contains(&stream.request.method.to_uppercase().as_str()) {
        return Err(Error::config(format!(
            "Stream '{}' has invalid HTTP method: {}",
            stream.name, stream.request.method
        )));
    }

    Ok(())
}
