use regex::Regex;
use std::sync::LazyLock;

/// A reference to another dbt model via ref()
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefCall {
    /// Optional package name (for cross-project refs)
    pub package: Option<String>,
    /// Model name
    pub name: String,
}

/// A reference to a dbt source via source()
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceCall {
    /// Source name
    pub source_name: String,
    /// Table name within the source
    pub table_name: String,
}

static JINJA_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{#[\s\S]*?#\}").unwrap());

// Matches ref('name'), ref("name"), ref('pkg', 'name'), ref("pkg", "name")
// Handles {{ ref(...) }} and {{- ref(...) -}} whitespace control
static REF_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        \{\{-?\s*
        ref\s*\(\s*
        (?:
            # Two-argument form: ref('pkg', 'name') or ref("pkg", "name")
            (?:['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"])
            |
            # Single-argument form: ref('name') or ref("name")
            ['"]([^'"]+)['"]
        )
        \s*\)\s*
        -?\}\}
    "#,
    )
    .unwrap()
});

// Matches source('src_name', 'table_name')
static SOURCE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        \{\{-?\s*
        source\s*\(\s*
        ['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]
        \s*\)\s*
        -?\}\}
    "#,
    )
    .unwrap()
});

/// Strip Jinja comments from SQL content
fn strip_jinja_comments(sql: &str) -> String {
    JINJA_COMMENT.replace_all(sql, "").to_string()
}

/// Extract all ref() calls from SQL content
pub fn extract_refs(sql: &str) -> Vec<RefCall> {
    let cleaned = strip_jinja_comments(sql);
    let mut refs = Vec::new();

    for cap in REF_PATTERN.captures_iter(&cleaned) {
        if let (Some(pkg), Some(name)) = (cap.get(1), cap.get(2)) {
            // Two-argument form
            refs.push(RefCall {
                package: Some(pkg.as_str().to_string()),
                name: name.as_str().to_string(),
            });
        } else if let Some(name) = cap.get(3) {
            // Single-argument form
            refs.push(RefCall {
                package: None,
                name: name.as_str().to_string(),
            });
        }
    }

    refs
}

/// Extract all source() calls from SQL content
pub fn extract_sources(sql: &str) -> Vec<SourceCall> {
    let cleaned = strip_jinja_comments(sql);
    let mut sources = Vec::new();

    for cap in SOURCE_PATTERN.captures_iter(&cleaned) {
        sources.push(SourceCall {
            source_name: cap[1].to_string(),
            table_name: cap[2].to_string(),
        });
    }

    sources
}

/// Parsed config block from SQL
#[derive(Debug, Clone, Default)]
pub struct SqlConfig {
    pub materialized: Option<String>,
    pub tags: Vec<String>,
}

// Matches {{ config(...) }} blocks — captures the inner arguments
static CONFIG_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        \{\{-?\s*
        config\s*\(
        ([\s\S]*?)
        \)\s*
        -?\}\}
    "#,
    )
    .unwrap()
});

// Matches materialized='value' or materialized="value"
static MATERIALIZED_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"materialized\s*=\s*['"]([^'"]+)['"]"#).unwrap()
});

// Matches tags=['a', 'b'] or tags=["a", "b"]
static TAGS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"tags\s*=\s*\[([^\]]*)\]"#).unwrap()
});

// Matches individual tag values inside the tags list
static TAG_VALUE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"['"]([^'"]+)['"]"#).unwrap()
});

/// Extract config() block settings from SQL content
pub fn extract_config(sql: &str) -> SqlConfig {
    let cleaned = strip_jinja_comments(sql);
    let mut config = SqlConfig::default();

    if let Some(cap) = CONFIG_PATTERN.captures(&cleaned) {
        let inner = &cap[1];

        if let Some(mat) = MATERIALIZED_PATTERN.captures(inner) {
            config.materialized = Some(mat[1].to_string());
        }

        if let Some(tags_cap) = TAGS_PATTERN.captures(inner) {
            let tags_inner = &tags_cap[1];
            config.tags = TAG_VALUE
                .captures_iter(tags_inner)
                .map(|c| c[1].to_string())
                .collect();
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_ref() {
        let sql = "SELECT * FROM {{ ref('stg_orders') }}";
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "stg_orders");
        assert!(refs[0].package.is_none());
    }

    #[test]
    fn test_double_quoted_ref() {
        let sql = r#"SELECT * FROM {{ ref("stg_orders") }}"#;
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "stg_orders");
    }

    #[test]
    fn test_two_arg_ref() {
        let sql = "SELECT * FROM {{ ref('other_project', 'stg_orders') }}";
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].package.as_deref(), Some("other_project"));
        assert_eq!(refs[0].name, "stg_orders");
    }

    #[test]
    fn test_whitespace_control() {
        let sql = "SELECT * FROM {{- ref('stg_orders') -}}";
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "stg_orders");
    }

    #[test]
    fn test_multiple_refs() {
        let sql = r#"
            SELECT
                o.*,
                c.name
            FROM {{ ref('stg_orders') }} o
            JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id
        "#;
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].name, "stg_orders");
        assert_eq!(refs[1].name, "stg_customers");
    }

    #[test]
    fn test_source() {
        let sql = "SELECT * FROM {{ source('raw', 'orders') }}";
        let sources = extract_sources(sql);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
        assert_eq!(sources[0].table_name, "orders");
    }

    #[test]
    fn test_source_whitespace_control() {
        let sql = "SELECT * FROM {{- source('raw', 'orders') -}}";
        let sources = extract_sources(sql);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
    }

    #[test]
    fn test_strip_jinja_comments() {
        let sql = r#"
            {# This is a comment with {{ ref('should_be_ignored') }} #}
            SELECT * FROM {{ ref('actual_model') }}
        "#;
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "actual_model");
    }

    #[test]
    fn test_mixed_refs_and_sources() {
        let sql = r#"
            SELECT *
            FROM {{ source('raw', 'orders') }}
            JOIN {{ ref('stg_customers') }} ON 1=1
        "#;
        let refs = extract_refs(sql);
        let sources = extract_sources(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(sources.len(), 1);
    }

    #[test]
    fn test_no_refs() {
        let sql = "SELECT 1 as id";
        let refs = extract_refs(sql);
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extra_spaces() {
        let sql = "SELECT * FROM {{  ref(  'stg_orders'  )  }}";
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "stg_orders");
    }

    // ─── Config extraction tests ───

    #[test]
    fn test_config_materialized() {
        let sql = "{{ config(materialized='incremental') }}\nSELECT 1";
        let config = extract_config(sql);
        assert_eq!(config.materialized.as_deref(), Some("incremental"));
        assert!(config.tags.is_empty());
    }

    #[test]
    fn test_config_materialized_double_quotes() {
        let sql = r#"{{ config(materialized="table") }}"#;
        let config = extract_config(sql);
        assert_eq!(config.materialized.as_deref(), Some("table"));
    }

    #[test]
    fn test_config_tags() {
        let sql = "{{ config(tags=['nightly', 'finance']) }}\nSELECT 1";
        let config = extract_config(sql);
        assert_eq!(config.tags, vec!["nightly", "finance"]);
    }

    #[test]
    fn test_config_both() {
        let sql = "{{ config(materialized='view', tags=['daily']) }}\nSELECT 1";
        let config = extract_config(sql);
        assert_eq!(config.materialized.as_deref(), Some("view"));
        assert_eq!(config.tags, vec!["daily"]);
    }

    #[test]
    fn test_config_whitespace_control() {
        let sql = "{{- config(materialized='ephemeral') -}}\nSELECT 1";
        let config = extract_config(sql);
        assert_eq!(config.materialized.as_deref(), Some("ephemeral"));
    }

    #[test]
    fn test_config_multiline() {
        let sql = r#"{{
            config(
                materialized='incremental',
                tags=['nightly', 'warehouse']
            )
        }}
        SELECT 1"#;
        let config = extract_config(sql);
        assert_eq!(config.materialized.as_deref(), Some("incremental"));
        assert_eq!(config.tags, vec!["nightly", "warehouse"]);
    }

    #[test]
    fn test_no_config() {
        let sql = "SELECT * FROM {{ ref('orders') }}";
        let config = extract_config(sql);
        assert!(config.materialized.is_none());
        assert!(config.tags.is_empty());
    }

    #[test]
    fn test_config_in_comment_ignored() {
        let sql = r#"
            {# {{ config(materialized='table') }} #}
            SELECT 1
        "#;
        let config = extract_config(sql);
        assert!(config.materialized.is_none());
    }
}
