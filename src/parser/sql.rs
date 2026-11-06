use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

/// A reference to another dbt model via ref()
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefCall {
    /// Optional package name (for cross-project refs)
    pub package: Option<String>,
    /// Model name
    pub name: String,
    /// Model version from `ref('m', v=2)` / `ref('m', version='2')`.
    #[serde(default)]
    pub version: Option<String>,
}

/// A reference to a dbt source via source()
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceCall {
    /// Source name
    pub source_name: String,
    /// Table name within the source
    pub table_name: String,
}

static JINJA_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{#[\s\S]*?#\}").unwrap());

/// A Jinja expression `{{ ... }}` or statement `{% ... %}`. ref() and
/// source() count anywhere inside one, not only as the whole expression:
/// `{% set x = ref('a') %}`, `{{ dbt_utils.star(from=ref('a')) }}`,
/// `{{ union_relations([ref('a'), ref('b')]) }}`.
pub(crate) static JINJA_BLOCK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{\{[\s\S]*?\}\}|\{%[\s\S]*?%\}").unwrap());

// ref('name'), ref('pkg', 'name'), ref('name', v=2), ref('name', version='2')
static REF_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        \bref\s*\(\s*
        ['"]([^'"]+)['"]\s*
        (?:,\s*['"]([^'"]+)['"]\s*)?
        (?:,\s*(?:v|version)\s*=\s*['"]?([^'")\s]+)['"]?\s*)?
        \)
    "#,
    )
    .unwrap()
});

// source('src_name', 'table_name')
static SOURCE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        \bsource\s*\(\s*
        ['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]
        \s*\)
    "#,
    )
    .unwrap()
});

/// Strip Jinja comments from SQL content (legacy helper, retained for the
/// macro-call extractor that doesn't yet route through the full lexer).
fn strip_jinja_comments(sql: &str) -> String {
    JINJA_COMMENT.replace_all(sql, "").to_string()
}

static VAR_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{-?\s*var\(\s*['"]([^'"]+)['"]\s*(?:,[^)]*)?\)\s*-?\}\}"#).unwrap()
});
static VAR_CALL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\bvar\(\s*['"]([^'"]+)['"]\s*(?:,[^)]*)?\)"#).unwrap());

/// Resolve `var()` the way dependencies need it: `{{ var('x') }}` whose
/// value is Jinja (`"{{ ref('orders') }}"`) becomes that Jinja, and
/// `var('x')` inside a Jinja expression becomes the value as a string
/// literal, so `ref(var('orders_model'))` is a plain ref.
pub fn expand_vars<'a>(sql: &'a str, vars: &HashMap<String, String>) -> std::borrow::Cow<'a, str> {
    if vars.is_empty() || !sql.contains("var(") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let whole = VAR_BLOCK.replace_all(sql, |c: &regex::Captures| match vars.get(&c[1]) {
        Some(v) if v.contains("{{") => v.clone(),
        _ => c[0].to_string(),
    });
    let mut out = String::with_capacity(whole.len());
    let mut last = 0;
    for block in JINJA_BLOCK.find_iter(&whole) {
        out.push_str(&whole[last..block.start()]);
        out.push_str(
            &VAR_CALL.replace_all(block.as_str(), |c: &regex::Captures| {
                match vars.get(&c[1]) {
                    Some(v) if !v.contains("{{") && !v.contains('\'') => format!("'{v}'"),
                    _ => c[0].to_string(),
                }
            }),
        );
        last = block.end();
    }
    out.push_str(&whole[last..]);
    std::borrow::Cow::Owned(out)
}

/// Extract all ref() calls from SQL content. Pipes through the SQL+Jinja
/// lexer (#25) first to blank out string literals and comments, so the regex
/// can't match `ref(` inside `'I prefer ref(x)'` or `-- ref(`.
pub fn extract_refs(sql: &str) -> Vec<RefCall> {
    let cleaned = super::lexer::jinja_code(sql);
    let mut refs = Vec::new();

    for block in JINJA_BLOCK.find_iter(&cleaned) {
        for cap in REF_PATTERN.captures_iter(block.as_str()) {
            let version = cap.get(3).map(|v| v.as_str().to_string());
            refs.push(match cap.get(2) {
                // Two positional arguments: ref('package', 'model')
                Some(name) => RefCall {
                    package: Some(cap[1].to_string()),
                    name: name.as_str().to_string(),
                    version,
                },
                None => RefCall {
                    package: None,
                    name: cap[1].to_string(),
                    version,
                },
            });
        }
    }

    refs
}

/// Extract all source() calls from SQL content. Routes through the lexer so
/// false matches inside strings and comments are eliminated.
pub fn extract_sources(sql: &str) -> Vec<SourceCall> {
    let cleaned = super::lexer::jinja_code(sql);
    let mut sources = Vec::new();

    for block in JINJA_BLOCK.find_iter(&cleaned) {
        for cap in SOURCE_PATTERN.captures_iter(block.as_str()) {
            sources.push(SourceCall {
                source_name: cap[1].to_string(),
                table_name: cap[2].to_string(),
            });
        }
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
static MATERIALIZED_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"materialized\s*=\s*['"]([^'"]+)['"]"#).unwrap());

// Matches tags=['a', 'b'] or tags=["a", "b"]
static TAGS_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"tags\s*=\s*\[([^\]]*)\]"#).unwrap());

// Matches individual tag values inside the tags list
static TAG_VALUE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"['"]([^'"]+)['"]"#).unwrap());

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
    fn test_jinja_in_sql_strings_and_comments_counts_like_dbt() {
        // dbt renders Jinja before SQL exists, so these are dependencies
        // (verified with `dbt parse`); only Jinja comments are not.
        let sql = "-- depends_on: {{ ref('a') }}\n\
                   select '{{ ref(\"b\") }}' as s /* {{ ref('c') }} */ {# {{ ref('d') }} #}\n\
                   from {{ ref('real') }}";
        let names: Vec<_> = extract_refs(sql).into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["a", "b", "c", "real"]);
    }

    #[test]
    fn test_vars_resolve_to_refs() {
        let vars: HashMap<String, String> = [
            ("enc", "{{ ref('encounter') }}"),
            ("orders_model", "stg_orders"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let sql = "select * from {{ var('enc') }} join {{ ref(var('orders_model', 'x')) }} join {{ var('unknown') }}";
        let names: Vec<_> = extract_refs(&expand_vars(sql, &vars))
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names, ["encounter", "stg_orders"]);
    }

    #[test]
    fn test_ref_text_without_jinja_is_ignored() {
        let sql = "select 1 -- TODO: ref('legacy')\nfrom {{ ref('real') }}";
        let refs = extract_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "real");
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
    fn test_ref_inside_larger_jinja() {
        let sql = r#"
            {% set orders = ref('stg_orders') %}
            select {{ dbt_utils.star(from=ref('stg_payments'), except=['x']) }}
            from {{ dbt_utils.union_relations([ref('a'), ref("b")]) }}
            join {{ source('raw', 'customers') }}
        "#;
        let names: Vec<_> = extract_refs(sql).into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["stg_orders", "stg_payments", "a", "b"]);
        assert_eq!(extract_sources(sql)[0].table_name, "customers");
    }

    #[test]
    fn test_versioned_ref() {
        let refs = extract_refs("select * from {{ ref('dim_customers', v=2) }}");
        assert_eq!(refs[0].name, "dim_customers");
        assert_eq!(refs[0].package, None);
        assert_eq!(refs[0].version.as_deref(), Some("2"));
        let refs = extract_refs("select * from {{ ref('pkg', 'm', version='3') }}");
        assert_eq!(refs[0].package.as_deref(), Some("pkg"));
        assert_eq!(refs[0].name, "m");
        assert_eq!(refs[0].version.as_deref(), Some("3"));
    }

    #[test]
    fn test_ref_outside_jinja_ignored() {
        // Plain SQL that happens to call a function named ref.
        assert!(extract_refs("select ref('x') from t").is_empty());
        assert!(extract_refs("select * from {{ xref('x') }}").is_empty());
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
