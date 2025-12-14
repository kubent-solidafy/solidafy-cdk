//! Built-in connector definitions embedded in the binary
//!
//! This module embeds all supported connector YAML files directly into the binary,
//! allowing users to use `--connector stripe` instead of specifying a file path.

use std::collections::HashMap;
use std::sync::LazyLock;

/// Built-in connector YAML definitions
pub static BUILTIN_CONNECTORS: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();

        // Payment & Billing
        m.insert("stripe", include_str!("../connectors/stripe.yaml"));

        // AI/ML Platforms
        m.insert("openai", include_str!("../connectors/openai-billing.yaml"));
        m.insert(
            "openai-billing",
            include_str!("../connectors/openai-billing.yaml"),
        );
        m.insert(
            "anthropic",
            include_str!("../connectors/anthropic-billing.yaml"),
        );
        m.insert(
            "anthropic-billing",
            include_str!("../connectors/anthropic-billing.yaml"),
        );

        // Cloud Infrastructure
        m.insert(
            "cloudflare",
            include_str!("../connectors/cloudflare-billing.yaml"),
        );
        m.insert(
            "cloudflare-billing",
            include_str!("../connectors/cloudflare-billing.yaml"),
        );

        // CRM & Sales
        m.insert("salesforce", include_str!("../connectors/salesforce.yaml"));
        m.insert(
            "salesforce-bulk",
            include_str!("../connectors/salesforce-bulk.yaml"),
        );
        m.insert("hubspot", include_str!("../connectors/hubspot.yaml"));

        // E-commerce
        m.insert("shopify", include_str!("../connectors/shopify.yaml"));

        // Support
        m.insert("zendesk", include_str!("../connectors/zendesk.yaml"));

        // Developer Tools
        m.insert("github", include_str!("../connectors/github-billing.yaml"));
        m.insert(
            "github-billing",
            include_str!("../connectors/github-billing.yaml"),
        );

        m
    });

/// Get a built-in connector by name
pub fn get_builtin(name: &str) -> Option<&'static str> {
    BUILTIN_CONNECTORS.get(name).copied()
}

/// Check if a connector name is a built-in connector
pub fn is_builtin(name: &str) -> bool {
    BUILTIN_CONNECTORS.contains_key(name)
}

/// List all built-in connector names (deduplicated, primary names only)
pub fn list_builtin() -> Vec<&'static str> {
    vec![
        "stripe",
        "openai",
        "anthropic",
        "cloudflare",
        "salesforce",
        "salesforce-bulk",
        "hubspot",
        "shopify",
        "zendesk",
        "github",
        // Database connectors
        "postgres",
        "mysql",
        "sqlite",
    ]
}

/// Connector metadata for display
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    pub name: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub aliases: &'static [&'static str],
    pub config_schema: &'static [ConfigField],
    pub streams: &'static [&'static str],
}

/// Configuration field definition
#[derive(Debug, Clone)]
pub struct ConfigField {
    pub name: &'static str,
    pub field_type: &'static str,
    pub required: bool,
    pub secret: bool,
    pub description: &'static str,
    pub default: Option<&'static str>,
}

/// Get detailed info about all built-in connectors
pub fn list_builtin_info() -> Vec<ConnectorInfo> {
    vec![
        ConnectorInfo {
            name: "stripe",
            description: "Stripe payments, customers, invoices, subscriptions",
            category: "Payments",
            aliases: &[],
            config_schema: &[ConfigField {
                name: "api_key",
                field_type: "string",
                required: true,
                secret: true,
                description: "Stripe API key (sk_live_... or sk_test_...)",
                default: None,
            }],
            streams: &[
                "customers",
                "products",
                "prices",
                "charges",
                "payment_intents",
                "refunds",
                "disputes",
                "subscriptions",
                "invoices",
                "invoice_items",
                "plans",
                "coupons",
                "balance_transactions",
                "payouts",
                "transfers",
                "events",
                "checkout_sessions",
                "payment_methods",
                "setup_intents",
            ],
        },
        ConnectorInfo {
            name: "openai",
            description: "OpenAI API usage, costs, and billing data",
            category: "AI/ML",
            aliases: &["openai-billing"],
            config_schema: &[
                ConfigField {
                    name: "admin_api_key",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "OpenAI Admin API key (sk-admin-...)",
                    default: None,
                },
                ConfigField {
                    name: "start_time",
                    field_type: "integer",
                    required: false,
                    secret: false,
                    description: "Start time as Unix timestamp (seconds). Defaults to 7 days ago.",
                    default: None,
                },
            ],
            streams: &[
                "usage_completions",
                "usage_completions_by_model",
                "usage_completions_by_project",
                "usage_embeddings",
                "usage_embeddings_by_model",
                "usage_images",
                "usage_images_by_model",
                "usage_audio_speeches",
                "usage_audio_transcriptions",
                "usage_moderations",
                "usage_vector_stores",
                "usage_code_interpreter",
                "costs",
                "costs_by_project",
                "costs_by_line_item",
            ],
        },
        ConnectorInfo {
            name: "anthropic",
            description: "Anthropic API usage and billing data",
            category: "AI/ML",
            aliases: &["anthropic-billing"],
            config_schema: &[ConfigField {
                name: "admin_api_key",
                field_type: "string",
                required: true,
                secret: true,
                description: "Anthropic Admin API key",
                default: None,
            }],
            streams: &[
                "organization",
                "usage_messages",
                "usage_messages_by_model",
                "usage_messages_by_workspace",
                "usage_messages_by_api_key",
                "usage_messages_detailed",
                "cost_report",
                "cost_report_by_workspace",
                "cost_report_by_line_item",
                "usage_claude_code",
                "usage_claude_code_by_user",
                "workspaces",
                "api_keys",
                "users",
                "invites",
            ],
        },
        ConnectorInfo {
            name: "cloudflare",
            description: "Cloudflare billing and usage data",
            category: "Infrastructure",
            aliases: &["cloudflare-billing"],
            config_schema: &[
                ConfigField {
                    name: "api_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "Cloudflare API token with billing:read permission",
                    default: None,
                },
                ConfigField {
                    name: "account_id",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description: "Cloudflare account ID",
                    default: None,
                },
            ],
            streams: &["account", "billing_profile", "subscriptions"],
        },
        ConnectorInfo {
            name: "salesforce",
            description: "Salesforce CRM objects via REST API",
            category: "CRM",
            aliases: &[],
            config_schema: &[
                ConfigField {
                    name: "instance_url",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description:
                        "Salesforce instance URL (e.g., https://yourcompany.salesforce.com)",
                    default: None,
                },
                ConfigField {
                    name: "access_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "Salesforce OAuth access token",
                    default: None,
                },
            ],
            streams: &[
                "accounts",
                "contacts",
                "leads",
                "opportunities",
                "cases",
                "tasks",
            ],
        },
        ConnectorInfo {
            name: "salesforce-bulk",
            description: "Salesforce CRM objects via Bulk API 2.0",
            category: "CRM",
            aliases: &[],
            config_schema: &[
                ConfigField {
                    name: "instance_url",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description:
                        "Salesforce instance URL (e.g., https://yourcompany.salesforce.com)",
                    default: None,
                },
                ConfigField {
                    name: "access_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "Salesforce OAuth access token",
                    default: None,
                },
            ],
            streams: &["accounts", "contacts", "leads", "opportunities"],
        },
        ConnectorInfo {
            name: "hubspot",
            description: "HubSpot CRM contacts, companies, deals",
            category: "CRM",
            aliases: &[],
            config_schema: &[ConfigField {
                name: "access_token",
                field_type: "string",
                required: true,
                secret: true,
                description: "HubSpot private app access token",
                default: None,
            }],
            streams: &[
                "contacts",
                "companies",
                "deals",
                "tickets",
                "products",
                "line_items",
                "quotes",
                "owners",
            ],
        },
        ConnectorInfo {
            name: "shopify",
            description: "Shopify orders, products, customers",
            category: "E-commerce",
            aliases: &[],
            config_schema: &[
                ConfigField {
                    name: "shop_domain",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description: "Shopify shop domain (e.g., your-store.myshopify.com)",
                    default: None,
                },
                ConfigField {
                    name: "access_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "Shopify Admin API access token",
                    default: None,
                },
                ConfigField {
                    name: "start_date",
                    field_type: "string",
                    required: false,
                    secret: false,
                    description: "Start date for orders sync (YYYY-MM-DD format)",
                    default: None,
                },
            ],
            streams: &[
                "products",
                "orders",
                "customers",
                "locations",
                "collections",
                "discounts",
            ],
        },
        ConnectorInfo {
            name: "zendesk",
            description: "Zendesk tickets, users, organizations",
            category: "Support",
            aliases: &[],
            config_schema: &[
                ConfigField {
                    name: "subdomain",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description: "Zendesk subdomain (the part before .zendesk.com)",
                    default: None,
                },
                ConfigField {
                    name: "email",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description: "Zendesk user email",
                    default: None,
                },
                ConfigField {
                    name: "api_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "Zendesk API token",
                    default: None,
                },
            ],
            streams: &[
                "tickets",
                "users",
                "organizations",
                "groups",
                "ticket_fields",
                "ticket_forms",
            ],
        },
        ConnectorInfo {
            name: "github",
            description: "GitHub Actions, Copilot, Packages billing and usage",
            category: "Developer Tools",
            aliases: &["github-billing"],
            config_schema: &[
                ConfigField {
                    name: "access_token",
                    field_type: "string",
                    required: true,
                    secret: true,
                    description: "GitHub Personal Access Token with admin:org scope",
                    default: None,
                },
                ConfigField {
                    name: "org",
                    field_type: "string",
                    required: true,
                    secret: false,
                    description: "GitHub organization name",
                    default: None,
                },
            ],
            streams: &[
                "actions_billing",
                "packages_billing",
                "shared_storage_billing",
                "copilot_billing",
                "copilot_seats",
                "actions_usage",
                "org_members",
                "org_repos",
            ],
        },
        // Database connectors
        ConnectorInfo {
            name: "postgres",
            description: "PostgreSQL database tables via DuckDB",
            category: "Database",
            aliases: &["postgresql"],
            config_schema: &[ConfigField {
                name: "connection_string",
                field_type: "string",
                required: true,
                secret: true,
                description:
                    "PostgreSQL connection string (postgresql://user:pass@host:port/database)",
                default: None,
            }],
            streams: &[], // Dynamic - discovered from database
        },
        ConnectorInfo {
            name: "mysql",
            description: "MySQL database tables via DuckDB",
            category: "Database",
            aliases: &["mariadb"],
            config_schema: &[ConfigField {
                name: "connection_string",
                field_type: "string",
                required: true,
                secret: true,
                description: "MySQL connection string (mysql://user:pass@host:port/database)",
                default: None,
            }],
            streams: &[], // Dynamic - discovered from database
        },
        ConnectorInfo {
            name: "sqlite",
            description: "SQLite database tables via DuckDB",
            category: "Database",
            aliases: &[],
            config_schema: &[ConfigField {
                name: "database_path",
                field_type: "string",
                required: true,
                secret: false,
                description: "Path to SQLite database file",
                default: None,
            }],
            streams: &[], // Dynamic - discovered from database
        },
    ]
}

/// Database connector types
pub static DATABASE_CONNECTORS: &[&str] = &["postgres", "postgresql", "mysql", "mariadb", "sqlite"];

/// Check if a connector is a database connector
pub fn is_database_connector(name: &str) -> bool {
    DATABASE_CONNECTORS.contains(&name.to_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_connectors_exist() {
        assert!(get_builtin("stripe").is_some());
        assert!(get_builtin("openai").is_some());
        assert!(get_builtin("anthropic").is_some());
        assert!(get_builtin("github").is_some());
    }

    #[test]
    fn test_aliases_work() {
        assert_eq!(get_builtin("openai"), get_builtin("openai-billing"));
        assert_eq!(get_builtin("anthropic"), get_builtin("anthropic-billing"));
        assert_eq!(get_builtin("github"), get_builtin("github-billing"));
    }

    #[test]
    fn test_unknown_connector() {
        assert!(get_builtin("unknown").is_none());
    }

    #[test]
    fn test_list_builtin() {
        let list = list_builtin();
        assert!(list.contains(&"stripe"));
        assert!(list.contains(&"openai"));
        assert!(list.contains(&"github"));
    }
}
