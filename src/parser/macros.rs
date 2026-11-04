//! Detection of dbt Jinja macros that simply wrap `ref()` or `source()`. Detected
//! wrappers are added to the graph builder's call recognition list so SQL-parse mode
//! follows `{{ smart_ref('orders') }}` etc. without requiring `dbt compile`.

use regex::Regex;
use std::collections::HashSet;
use std::path::Path;
use std::sync::LazyLock;

use super::sql::{RefCall, SourceCall};

/// One detected wrapper macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrapperMacro {
    pub name: String,
    pub kind: WrapperKind,
    /// Which positional parameter contains the model/table name (0-indexed).
    pub model_arg_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WrapperKind {
    /// Returns `ref(...)` — argument is treated as a model name.
    Ref,
    /// Returns `source(...)` — first arg is source_name, second is table_name.
    Source,
}

static MACRO_DEF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?xs)
        \{%-?\s*macro\s+
        (?P<name>[A-Za-z_][A-Za-z0-9_]*)
        \s*\(
            (?P<args>[^)]*)
        \)\s*-?%\}
        (?P<body>.*?)
        \{%-?\s*endmacro\s*-?%\}
        ",
    )
    .unwrap()
});

/// Scan macro source content and detect simple `ref()` / `source()` wrappers.
/// Conservative: only detects wrappers whose body contains a direct call.
pub fn detect_wrappers_in(content: &str) -> Vec<WrapperMacro> {
    let mut found = Vec::new();
    for cap in MACRO_DEF.captures_iter(content) {
        let name = cap.name("name").unwrap().as_str().to_string();
        let args_raw = cap.name("args").unwrap().as_str();
        let body = cap.name("body").unwrap().as_str();
        let arg_names: Vec<String> = args_raw
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            // Strip default values: `arg=default` → `arg`.
            .map(|s| s.split('=').next().unwrap_or(s).trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if arg_names.is_empty() {
            continue;
        }
        // ref(arg_name) — match the first argument that appears in a ref call.
        if let Some(idx) = find_call_arg_index(body, "ref", &arg_names) {
            found.push(WrapperMacro {
                name: name.clone(),
                kind: WrapperKind::Ref,
                model_arg_index: idx,
            });
            continue;
        }
        if let Some(idx) = find_call_arg_index(body, "source", &arg_names) {
            found.push(WrapperMacro {
                name,
                kind: WrapperKind::Source,
                model_arg_index: idx,
            });
        }
    }
    found
}

fn find_call_arg_index(body: &str, fn_name: &str, arg_names: &[String]) -> Option<usize> {
    // Look for `fn_name(...)`; check if any arg_name appears inside the parens.
    let pat = format!(r"{}\s*\(([^)]*)\)", regex::escape(fn_name));
    let re = Regex::new(&pat).ok()?;
    for cap in re.captures_iter(body) {
        let inside = cap.get(1)?.as_str();
        for (i, name) in arg_names.iter().enumerate() {
            // Word-boundary match.
            let arg_re = Regex::new(&format!(r"\b{}\b", regex::escape(name))).ok()?;
            if arg_re.is_match(inside) {
                return Some(i);
            }
        }
    }
    None
}

/// Detect all wrappers across multiple macro `.sql` files. Names are deduplicated.
pub fn detect_wrappers_in_files(paths: &[std::path::PathBuf]) -> Vec<WrapperMacro> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<WrapperMacro> = Vec::new();
    for p in paths {
        let content = match std::fs::read_to_string(p) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for w in detect_wrappers_in(&content) {
            if seen.insert(w.name.clone()) {
                out.push(w);
            }
        }
    }
    out
}

/// Discover macro `.sql` files under a project's `macros/` directory.
pub fn discover_macro_files(project_dir: &Path) -> Vec<std::path::PathBuf> {
    let macros_dir = project_dir.join("macros");
    if !macros_dir.exists() {
        return Vec::new();
    }
    walkdir::WalkDir::new(&macros_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.eq_ignore_ascii_case("sql"))
        })
        .map(|e| e.path().to_path_buf())
        .collect()
}

/// Extract calls to known wrapper macros from a SQL string. Yields `RefCall` for
/// ref-wrappers and `SourceCall` for source-wrappers, following the wrapper's
/// declared `model_arg_index`.
pub fn extract_wrapper_calls(
    sql: &str,
    wrappers: &[WrapperMacro],
) -> (Vec<RefCall>, Vec<SourceCall>) {
    let mut refs: Vec<RefCall> = Vec::new();
    let mut sources: Vec<SourceCall> = Vec::new();
    for w in wrappers {
        // Build a regex that matches `{{ NAME(args) }}` and captures the argument list.
        let pat = format!(
            r"\{{\{{-?\s*{}\s*\(([^)]*)\)\s*-?\}}\}}",
            regex::escape(&w.name)
        );
        let re = match Regex::new(&pat) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for cap in re.captures_iter(sql) {
            let args_raw = match cap.get(1) {
                Some(m) => m.as_str(),
                None => continue,
            };
            let args: Vec<String> = split_args(args_raw);
            match w.kind {
                WrapperKind::Ref => {
                    if let Some(name) = args.get(w.model_arg_index) {
                        if let Some(stripped) = strip_quotes(name) {
                            refs.push(RefCall {
                                package: None,
                                name: stripped.to_string(),
                            });
                        }
                    }
                }
                WrapperKind::Source => {
                    let first = args.first().and_then(|s| strip_quotes(s));
                    let second = args.get(1).and_then(|s| strip_quotes(s));
                    if let (Some(sn), Some(tn)) = (first, second) {
                        sources.push(SourceCall {
                            source_name: sn.to_string(),
                            table_name: tn.to_string(),
                        });
                    }
                }
            }
        }
    }
    (refs, sources)
}

fn split_args(raw: &str) -> Vec<String> {
    raw.split(',').map(|s| s.trim().to_string()).collect()
}

fn strip_quotes(s: &str) -> Option<&str> {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_simple_ref_wrapper() {
        let content = r"{% macro smart_ref(name) %}{{ ref(name) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(wrappers.len(), 1);
        assert_eq!(wrappers[0].name, "smart_ref");
        assert_eq!(wrappers[0].kind, WrapperKind::Ref);
        assert_eq!(wrappers[0].model_arg_index, 0);
    }

    #[test]
    fn test_detect_simple_source_wrapper() {
        let content = r"{% macro audited_source(s, t) %}{{ source(s, t) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(wrappers.len(), 1);
        assert_eq!(wrappers[0].kind, WrapperKind::Source);
    }

    #[test]
    fn test_default_value_argument_still_picked_up() {
        let content =
            r"{% macro project_ref(name, project='default') %}{{ ref(name) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(wrappers.len(), 1);
        assert_eq!(wrappers[0].model_arg_index, 0);
    }

    #[test]
    fn test_wrapper_call_emits_ref() {
        let wrappers = vec![WrapperMacro {
            name: "smart_ref".into(),
            kind: WrapperKind::Ref,
            model_arg_index: 0,
        }];
        let sql = "select * from {{ smart_ref('orders') }}";
        let (refs, _) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "orders");
    }

    #[test]
    fn test_wrapper_call_emits_source() {
        let wrappers = vec![WrapperMacro {
            name: "audited_source".into(),
            kind: WrapperKind::Source,
            model_arg_index: 0,
        }];
        let sql = r#"select * from {{ audited_source("raw", "orders") }}"#;
        let (_, sources) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
        assert_eq!(sources[0].table_name, "orders");
    }

    #[test]
    fn test_non_wrapper_macro_ignored() {
        // Macro body doesn't call ref or source.
        let content = r"{% macro fmt(x) %}{{ x | upper }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert!(wrappers.is_empty());
    }

    #[test]
    fn test_detect_in_files_dedupes() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.sql");
        let b = tmp.path().join("b.sql");
        std::fs::write(&a, "{% macro w(x) %}{{ ref(x) }}{% endmacro %}").unwrap();
        std::fs::write(&b, "{% macro w(x) %}{{ ref(x) }}{% endmacro %}").unwrap();
        let wrappers = detect_wrappers_in_files(&[a, b]);
        assert_eq!(wrappers.len(), 1);
    }
}

static SET_SOURCE_MODEL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)\{%-?\s*set\s+source_model\s*=\s*(.*?)\s*-?%\}").unwrap());
static KWARG_SOURCE_MODEL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)\bsource_model\s*=\s*("[^"]*"|'[^']*'|\[[^\]]*\]|\{[^}]*\})"#).unwrap()
});
static SET_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)\{%-?\s*set\s+\w+\s*-?%\}(.*?)\{%-?\s*endset\s*-?%\}").unwrap()
});

/// Dependencies declared through a `source_model` argument, the convention of
/// AutomateDV (dbtvault) and similar packages whose macros call `ref()` /
/// `source()` internally:
///
/// - `{% set source_model = "stg_orders" %}` then `pkg.hub(..., source_model=source_model)`
/// - a literal keyword argument: `source_model="stg_orders"` or `source_model=["a", "b"]`
/// - a YAML metadata block (`{% set yaml_metadata %} source_model: raw_orders {% endset %}`)
///
/// A string or list of strings is a model `ref`; a mapping
/// `{source_name: table}` (or a list of tables) is a `source`.
pub fn extract_source_model_refs(sql: &str) -> (Vec<RefCall>, Vec<SourceCall>) {
    let mut refs = Vec::new();
    let mut sources = Vec::new();
    if !sql.contains("source_model") {
        return (refs, sources);
    }
    let mut add = |value: &serde_yaml::Value| collect_source_model(value, &mut refs, &mut sources);

    for re in [&*SET_SOURCE_MODEL, &*KWARG_SOURCE_MODEL] {
        for cap in re.captures_iter(sql) {
            if let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(&cap[1]) {
                add(&v);
            }
        }
    }
    for cap in SET_BLOCK.captures_iter(sql) {
        if let Ok(serde_yaml::Value::Mapping(m)) = serde_yaml::from_str(&cap[1]) {
            if let Some(v) = m.get("source_model") {
                add(v);
            }
        }
    }
    refs.sort_by(|a, b| a.name.cmp(&b.name));
    refs.dedup();
    sources.sort_by(|a, b| (&a.source_name, &a.table_name).cmp(&(&b.source_name, &b.table_name)));
    sources.dedup();
    (refs, sources)
}

fn collect_source_model(
    value: &serde_yaml::Value,
    refs: &mut Vec<RefCall>,
    sources: &mut Vec<SourceCall>,
) {
    use serde_yaml::Value;
    let model = |name: &str| RefCall {
        package: None,
        name: name.to_string(),
    };
    match value {
        Value::String(name) if is_identifier(name) => refs.push(model(name)),
        Value::Sequence(items) => {
            for item in items {
                if let Value::String(name) = item {
                    if is_identifier(name) {
                        refs.push(model(name));
                    }
                }
            }
        }
        Value::Mapping(map) => {
            for (k, v) in map {
                let Value::String(source_name) = k else {
                    continue;
                };
                let tables: Vec<&str> = match v {
                    Value::String(t) => vec![t.as_str()],
                    Value::Sequence(ts) => ts.iter().filter_map(Value::as_str).collect(),
                    _ => vec![],
                };
                for t in tables.into_iter().filter(|t| is_identifier(t)) {
                    sources.push(SourceCall {
                        source_name: source_name.clone(),
                        table_name: t.to_string(),
                    });
                }
            }
        }
        _ => {}
    }
}

/// Plain dbt node names only; skips Jinja expressions and variables.
fn is_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod source_model_tests {
    use super::*;

    fn names(sql: &str) -> (Vec<String>, Vec<String>) {
        let (r, s) = extract_source_model_refs(sql);
        (
            r.into_iter().map(|r| r.name).collect(),
            s.into_iter()
                .map(|s| format!("{}.{}", s.source_name, s.table_name))
                .collect(),
        )
    }

    #[test]
    fn set_variable_form() {
        let sql = r#"{%- set source_model = "v_stg_orders" -%}
{%- set src_pk = "ORDER_PK" -%}
{{ automate_dv.hub(src_pk=src_pk, source_model=source_model) }}"#;
        assert_eq!(names(sql), (vec!["v_stg_orders".into()], vec![]));
    }

    #[test]
    fn list_and_literal_kwarg_forms() {
        let sql = r#"{%- set source_model = ["stg_a", "stg_b"] -%}
{{ automate_dv.hub(src_pk="PK", source_model=source_model) }}"#;
        assert_eq!(names(sql).0, vec!["stg_a", "stg_b"]);
        let sql = r#"{{ automate_dv.sat(src_pk="PK", source_model='stg_c') }}"#;
        assert_eq!(names(sql).0, vec!["stg_c"]);
    }

    #[test]
    fn yaml_metadata_block_with_model_or_source() {
        let sql = "{%- set yaml_metadata -%}\nsource_model: 'raw_orders'\nderived_columns:\n  A: 'B'\n{%- endset -%}\n{{ automate_dv.stage(source_model=metadata_dict['source_model']) }}";
        assert_eq!(names(sql), (vec!["raw_orders".into()], vec![]));
        let sql = "{%- set yaml_metadata -%}\nsource_model:\n  tpch: 'ORDERS'\n{%- endset -%}";
        assert_eq!(names(sql), (vec![], vec!["tpch.ORDERS".into()]));
    }

    #[test]
    fn ignores_variables_and_files_without_source_model() {
        assert_eq!(names("select * from {{ ref('x') }}"), (vec![], vec![]));
        let sql = "{{ pkg.hub(source_model=var('stg')) }}";
        assert_eq!(names(sql), (vec![], vec![]));
    }
}
