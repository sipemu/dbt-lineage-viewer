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

static JINJA_COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{#[\s\S]*?#\}").unwrap());

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
}
