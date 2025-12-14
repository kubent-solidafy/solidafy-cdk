//! Template interpolation for YAML configs
//!
//! Handles `{{ variable }}` interpolation in connector configurations.
//! Supports nested access like `{{ config.api_key }}` and `{{ partition.id }}`.

use crate::error::{Error, Result};
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

/// Regex for matching template variables: {{ variable.path }}
static TEMPLATE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*\}\}").unwrap()
});

/// Context for template interpolation
#[derive(Debug, Clone, Default)]
pub struct TemplateContext {
    /// Connector configuration values
    pub config: Value,
    /// Current partition values
    pub partition: Value,
    /// State/cursor values
    pub state: Value,
    /// Additional context variables
    pub vars: Value,
}

impl TemplateContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Create context with config values
    pub fn with_config(config: Value) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    /// Set config values
    pub fn set_config(&mut self, config: Value) -> &mut Self {
        self.config = config;
        self
    }

    /// Set partition values
    pub fn set_partition(&mut self, partition: Value) -> &mut Self {
        self.partition = partition;
        self
    }

    /// Set state values
    pub fn set_state(&mut self, state: Value) -> &mut Self {
        self.state = state;
        self
    }

    /// Set additional variables
    pub fn set_vars(&mut self, vars: Value) -> &mut Self {
        self.vars = vars;
        self
    }

    /// Get a value by path (e.g., "config.api_key")
    pub fn get(&self, path: &str) -> Option<&Value> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() {
            return None;
        }

        // First part determines the root object
        let root = match parts[0] {
            "config" => &self.config,
            "partition" => &self.partition,
            "state" => &self.state,
            "vars" => &self.vars,
            // Also support top-level access to config fields directly
            _ => {
                // Try config first
                if let Some(val) = get_nested_value(&self.config, &parts) {
                    return Some(val);
                }
                // Then vars
                return get_nested_value(&self.vars, &parts);
            }
        };

        // Navigate the remaining path
        if parts.len() == 1 {
            Some(root)
        } else {
            get_nested_value(root, &parts[1..])
        }
    }
}

/// Get a nested value from a JSON value by path
fn get_nested_value<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for part in path {
        match current {
            Value::Object(map) => {
                current = map.get(*part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Render a template string with the given context
pub fn render(template: &str, ctx: &TemplateContext) -> Result<String> {
    let mut result = template.to_string();
    let mut errors = Vec::new();

    for cap in TEMPLATE_REGEX.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let var_path = cap.get(1).unwrap().as_str();

        match ctx.get(var_path) {
            Some(value) => {
                let replacement = value_to_string(value);
                result = result.replace(full_match, &replacement);
            }
            None => {
                errors.push(var_path.to_string());
            }
        }
    }

    if errors.is_empty() {
        Ok(result)
    } else {
        Err(Error::undefined_var(errors.join(", ")))
    }
}

/// Render a template, returning None for undefined variables instead of error
pub fn render_optional(template: &str, ctx: &TemplateContext) -> String {
    let mut result = template.to_string();

    for cap in TEMPLATE_REGEX.captures_iter(template) {
        let full_match = cap.get(0).unwrap().as_str();
        let var_path = cap.get(1).unwrap().as_str();

        if let Some(value) = ctx.get(var_path) {
            let replacement = value_to_string(value);
            result = result.replace(full_match, &replacement);
        }
        // Leave undefined variables as-is
    }

    result
}

/// Check if a string contains template variables
pub fn has_templates(s: &str) -> bool {
    TEMPLATE_REGEX.is_match(s)
}

/// Extract all variable names from a template
pub fn extract_variables(template: &str) -> Vec<String> {
    TEMPLATE_REGEX
        .captures_iter(template)
        .map(|cap| cap.get(1).unwrap().as_str().to_string())
        .collect()
}

/// Convert a JSON value to a string for template substitution
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        // For complex types, use JSON serialization
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Render all string values in a JSON object/value
pub fn render_value(value: &Value, ctx: &TemplateContext) -> Result<Value> {
    match value {
        Value::String(s) => {
            if has_templates(s) {
                Ok(Value::String(render(s, ctx)?))
            } else {
                Ok(value.clone())
            }
        }
        Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                // Also render keys
                let new_key = if has_templates(k) {
                    render(k, ctx)?
                } else {
                    k.clone()
                };
                new_map.insert(new_key, render_value(v, ctx)?);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let new_arr: Result<Vec<Value>> = arr.iter().map(|v| render_value(v, ctx)).collect();
            Ok(Value::Array(new_arr?))
        }
        _ => Ok(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_substitution() {
        let ctx = TemplateContext::with_config(json!({
            "api_key": "sk_test_123"
        }));

        let result = render("Bearer {{ config.api_key }}", &ctx).unwrap();
        assert_eq!(result, "Bearer sk_test_123");
    }

    #[test]
    fn test_multiple_substitutions() {
        let ctx = TemplateContext::with_config(json!({
            "host": "api.example.com",
            "version": "v1"
        }));

        let result = render("https://{{ config.host }}/{{ config.version }}/users", &ctx).unwrap();
        assert_eq!(result, "https://api.example.com/v1/users");
    }

    #[test]
    fn test_nested_value() {
        let ctx = TemplateContext::with_config(json!({
            "credentials": {
                "client_id": "my-client",
                "client_secret": "secret123"
            }
        }));

        let result = render("Client: {{ config.credentials.client_id }}", &ctx).unwrap();
        assert_eq!(result, "Client: my-client");
    }

    #[test]
    fn test_partition_context() {
        let mut ctx = TemplateContext::new();
        ctx.set_config(json!({"base": "https://api.example.com"}));
        ctx.set_partition(json!({"repo_id": "12345"}));

        let result = render("{{ config.base }}/repos/{{ partition.repo_id }}", &ctx).unwrap();
        assert_eq!(result, "https://api.example.com/repos/12345");
    }

    #[test]
    fn test_undefined_variable() {
        let ctx = TemplateContext::new();
        let result = render("{{ config.missing }}", &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("config.missing"));
    }

    #[test]
    fn test_no_templates() {
        let ctx = TemplateContext::new();
        let result = render("plain string without templates", &ctx).unwrap();
        assert_eq!(result, "plain string without templates");
    }

    #[test]
    fn test_has_templates() {
        assert!(has_templates("{{ config.key }}"));
        assert!(has_templates("prefix {{ var }} suffix"));
        assert!(!has_templates("no templates here"));
        assert!(!has_templates("{ not a template }"));
    }

    #[test]
    fn test_extract_variables() {
        let vars = extract_variables("{{ config.a }} and {{ partition.b }}");
        assert_eq!(vars, vec!["config.a", "partition.b"]);
    }

    #[test]
    fn test_render_value_object() {
        let ctx = TemplateContext::with_config(json!({
            "key": "value123"
        }));

        let input = json!({
            "header": "X-API-Key",
            "value": "{{ config.key }}"
        });

        let result = render_value(&input, &ctx).unwrap();
        assert_eq!(
            result,
            json!({
                "header": "X-API-Key",
                "value": "value123"
            })
        );
    }

    #[test]
    fn test_number_substitution() {
        let ctx = TemplateContext::with_config(json!({
            "limit": 100,
            "enabled": true
        }));

        let result = render(
            "limit={{ config.limit }}&enabled={{ config.enabled }}",
            &ctx,
        )
        .unwrap();
        assert_eq!(result, "limit=100&enabled=true");
    }

    #[test]
    fn test_whitespace_in_template() {
        let ctx = TemplateContext::with_config(json!({"key": "value"}));

        // Various whitespace patterns
        assert_eq!(render("{{config.key}}", &ctx).unwrap(), "value");
        assert_eq!(render("{{ config.key }}", &ctx).unwrap(), "value");
        assert_eq!(render("{{  config.key  }}", &ctx).unwrap(), "value");
    }

    #[test]
    fn test_render_optional() {
        let ctx = TemplateContext::with_config(json!({"key": "value"}));

        // Defined variable gets replaced
        assert_eq!(render_optional("test {{ config.key }}", &ctx), "test value");

        // Undefined variable stays as-is
        assert_eq!(
            render_optional("test {{ config.missing }}", &ctx),
            "test {{ config.missing }}"
        );
    }
}
