//! Detection of dbt Jinja macros that call `ref()` or `source()`. A model that
//! calls such a macro depends on what the macro references, so SQL-parse mode
//! follows these calls without requiring `dbt compile`:
//!
//! - wrappers whose argument becomes the ref: `{% macro smart_ref(name) %}{{ ref(name) }}`
//!   makes `{{ smart_ref('orders') }}` depend on `orders`;
//! - macros with fixed refs: a macro body selecting from `{{ ref('dim_feeds') }}`
//!   makes every model calling it depend on `dim_feeds`.
//!
//! Arguments are resolved when they are string literals (positional, keyword or
//! the parameter's default). Anything computed at render time is skipped.

use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use super::sql::{RefCall, SourceCall, JINJA_BLOCK};

/// One macro whose body references models or sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WrapperMacro {
    pub name: String,
    pub params: Vec<String>,
    /// String-literal default per parameter, if any.
    pub defaults: Vec<Option<String>>,
    pub deps: Vec<MacroDep>,
}

/// A `ref()` / `source()` argument inside a macro body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MacroArg {
    Literal(String),
    /// The macro parameter at this index.
    Param(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MacroDep {
    Ref(MacroArg),
    Source(MacroArg, MacroArg),
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

const ARG: &str = r#"'[^']*'|"[^"]*"|[A-Za-z_][A-Za-z0-9_]*"#;

static BODY_REF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"\bref\s*\(\s*({ARG})\s*(?:,\s*({ARG})\s*)?(?:,\s*(?:v|version)\s*=[^)]*)?\)"
    ))
    .unwrap()
});

static BODY_SOURCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"\bsource\s*\(\s*({ARG})\s*,\s*({ARG})\s*\)")).unwrap());

/// A call to some function or macro, optionally package-qualified.
static CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap()
});

/// Macro names that are dbt builtins, never treated as user macros.
const BUILTINS: &[&str] = &["ref", "source", "config", "var", "env_var", "return"];

fn macro_arg(raw: &str, params: &[String]) -> Option<MacroArg> {
    if let Some(lit) = strip_quotes(raw) {
        return Some(MacroArg::Literal(lit.to_string()));
    }
    params.iter().position(|p| p == raw).map(MacroArg::Param)
}

/// Scan macro source content for macros whose body calls `ref()` or `source()`
/// with literal or parameter arguments.
pub fn detect_wrappers_in(content: &str) -> Vec<WrapperMacro> {
    let mut found = Vec::new();
    for cap in MACRO_DEF.captures_iter(content) {
        let name = cap["name"].to_string();
        if BUILTINS.contains(&name.as_str()) {
            continue;
        }
        let mut params = Vec::new();
        let mut defaults = Vec::new();
        for raw in split_args(&cap["args"]) {
            let (param, default) = match raw.split_once('=') {
                Some((p, d)) => (p.trim(), strip_quotes(d).map(str::to_string)),
                None => (raw.trim(), None),
            };
            if !param.is_empty() {
                params.push(param.to_string());
                defaults.push(default);
            }
        }
        let body = super::lexer::jinja_code(&cap["body"]);
        let mut deps = Vec::new();
        for block in JINJA_BLOCK.find_iter(&body) {
            for c in BODY_REF.captures_iter(block.as_str()) {
                // ref('pkg', 'name'): the model is the last positional argument.
                let raw = c.get(2).unwrap_or_else(|| c.get(1).unwrap()).as_str();
                if let Some(arg) = macro_arg(raw, &params) {
                    deps.push(MacroDep::Ref(arg));
                }
            }
            for c in BODY_SOURCE.captures_iter(block.as_str()) {
                if let (Some(s), Some(t)) = (macro_arg(&c[1], &params), macro_arg(&c[2], &params)) {
                    deps.push(MacroDep::Source(s, t));
                }
            }
        }
        if !deps.is_empty() {
            found.push(WrapperMacro {
                name,
                params,
                defaults,
                deps,
            });
        }
    }
    found
}

static DISPATCH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"adapter\.dispatch\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]"#).unwrap()
});

/// Macros that only forward to an adapter-specific implementation:
/// `{% macro m(...) %}{{ adapter.dispatch('m', 'pkg')(...) }}{% endmacro %}`.
/// Returns (macro name, dispatched name) pairs.
fn detect_dispatchers_in(content: &str) -> Vec<(String, String)> {
    MACRO_DEF
        .captures_iter(content)
        .filter_map(|cap| {
            let target = DISPATCH.captures(&cap["body"])?;
            Some((cap["name"].to_string(), target[1].to_string()))
        })
        .collect()
}

/// Detect macros across multiple `.sql` files. The first definition of a name
/// wins, so project macros shadow package macros when listed first. A macro
/// that dispatches to `default__<name>` inherits that implementation's
/// references, which is what dbt runs on adapters without an override.
pub fn detect_wrappers_in_files(paths: &[PathBuf]) -> Vec<WrapperMacro> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<WrapperMacro> = Vec::new();
    let mut dispatchers: Vec<(String, String)> = Vec::new();
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
        dispatchers.extend(detect_dispatchers_in(&content));
    }
    for (name, target) in dispatchers {
        if seen.contains(&name) {
            continue;
        }
        let default_impl = format!("default__{target}");
        if let Some(w) = out.iter().find(|w| w.name == default_impl) {
            let mut alias = w.clone();
            alias.name = name.clone();
            seen.insert(name);
            out.push(alias);
        }
    }
    out
}

fn sql_files_under(dir: &Path) -> Vec<PathBuf> {
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(dir)
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
        .collect();
    files.sort();
    files
}

fn macro_dirs(project_dir: &Path) -> Vec<PathBuf> {
    let paths = super::project::DbtProject::load(project_dir)
        .map(|p| p.macro_paths)
        .unwrap_or_else(|_| vec!["macros".to_string()]);
    paths.iter().map(|p| project_dir.join(p)).collect()
}

/// Discover the project's own macro `.sql` files (its `macro-paths`).
pub fn discover_macro_files(project_dir: &Path) -> Vec<PathBuf> {
    macro_dirs(project_dir)
        .iter()
        .flat_map(|d| sql_files_under(d))
        .collect()
}

/// Macro files relevant for lineage: the project's own, then those of
/// installed packages (`dbt_packages/*`), so project macros take precedence.
pub fn discover_lineage_macro_files(project_dir: &Path) -> Vec<PathBuf> {
    let mut files = discover_macro_files(project_dir);
    if let Ok(entries) = std::fs::read_dir(project_dir.join("dbt_packages")) {
        let mut packages: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
        packages.sort();
        for pkg in packages {
            for dir in macro_dirs(&pkg) {
                files.extend(sql_files_under(&dir));
            }
        }
    }
    files
}

/// Split a call's argument list at top-level commas, respecting quotes and
/// nested brackets.
fn split_args(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for ch in raw.chars() {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                    continue;
                }
                _ => {}
            },
        }
        cur.push(ch);
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// The argument text of a call whose opening parenthesis ends at `open_end`,
/// up to the matching closing parenthesis.
pub(crate) fn call_args(text: &str, open_end: usize) -> Option<&str> {
    let mut depth = 1i32;
    let mut quote: Option<char> = None;
    for (i, ch) in text[open_end..].char_indices() {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(&text[open_end..open_end + i]);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

static KWARG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=([^=][\s\S]*)$").unwrap());

/// Extract the refs and sources contributed by calls to known macros in a
/// SQL string.
pub fn extract_wrapper_calls(
    sql: &str,
    wrappers: &[WrapperMacro],
) -> (Vec<RefCall>, Vec<SourceCall>) {
    let mut refs: Vec<RefCall> = Vec::new();
    let mut sources: Vec<SourceCall> = Vec::new();
    if wrappers.is_empty() {
        return (refs, sources);
    }
    let by_name: HashMap<&str, &WrapperMacro> =
        wrappers.iter().map(|w| (w.name.as_str(), w)).collect();
    let cleaned = super::lexer::jinja_code(sql);
    for block in JINJA_BLOCK.find_iter(&cleaned) {
        let text = block.as_str();
        if text
            .trim_start_matches(['{', '%', '-', ' '])
            .starts_with("macro ")
        {
            continue;
        }
        for cap in CALL.captures_iter(text) {
            let Some(w) = by_name.get(&cap[1]) else {
                continue;
            };
            let Some(args) = call_args(text, cap.get(0).unwrap().end()) else {
                continue;
            };
            let bound = bind_args(w, args);
            let resolve = |a: &MacroArg| match a {
                MacroArg::Literal(s) => Some(s.clone()),
                MacroArg::Param(i) => bound[*i].clone(),
            };
            for dep in &w.deps {
                match dep {
                    MacroDep::Ref(a) => {
                        if let Some(name) = resolve(a) {
                            refs.push(RefCall {
                                package: None,
                                name,
                                version: None,
                            });
                        }
                    }
                    MacroDep::Source(s, t) => {
                        if let (Some(source_name), Some(table_name)) = (resolve(s), resolve(t)) {
                            sources.push(SourceCall {
                                source_name,
                                table_name,
                            });
                        }
                    }
                }
            }
        }
    }
    (refs, sources)
}

/// String-literal value of each macro parameter at one call site.
fn bind_args(w: &WrapperMacro, args: &str) -> Vec<Option<String>> {
    let mut bound = w.defaults.clone();
    let mut positional = 0;
    for arg in split_args(args) {
        if let Some(kw) = KWARG.captures(&arg) {
            if let Some(i) = w.params.iter().position(|p| *p == kw[1]) {
                bound[i] = strip_quotes(&kw[2]).map(str::to_string);
            }
        } else {
            if positional < bound.len() {
                bound[positional] = strip_quotes(&arg).map(str::to_string);
            }
            positional += 1;
        }
    }
    bound
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

    fn names(refs: &[RefCall]) -> Vec<&str> {
        refs.iter().map(|r| r.name.as_str()).collect()
    }

    #[test]
    fn test_detect_simple_ref_wrapper() {
        let content = r"{% macro smart_ref(name) %}{{ ref(name) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(wrappers.len(), 1);
        assert_eq!(wrappers[0].name, "smart_ref");
        assert_eq!(wrappers[0].deps, vec![MacroDep::Ref(MacroArg::Param(0))]);
    }

    #[test]
    fn test_detect_simple_source_wrapper() {
        let content = r"{% macro audited_source(s, t) %}{{ source(s, t) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(
            wrappers[0].deps,
            vec![MacroDep::Source(MacroArg::Param(0), MacroArg::Param(1))]
        );
    }

    #[test]
    fn test_default_value_argument_still_picked_up() {
        let content =
            r"{% macro project_ref(name, project='default') %}{{ ref(name) }}{% endmacro %}";
        let wrappers = detect_wrappers_in(content);
        assert_eq!(wrappers[0].params, vec!["name", "project"]);
        assert_eq!(wrappers[0].defaults, vec![None, Some("default".into())]);
    }

    #[test]
    fn test_wrapper_call_emits_ref() {
        let wrappers = detect_wrappers_in("{% macro smart_ref(n) %}{{ ref(n) }}{% endmacro %}");
        let (refs, _) = extract_wrapper_calls("select * from {{ smart_ref('orders') }}", &wrappers);
        assert_eq!(names(&refs), ["orders"]);
    }

    #[test]
    fn test_wrapper_call_emits_source() {
        let wrappers =
            detect_wrappers_in("{% macro audited_source(s, t) %}{{ source(s, t) }}{% endmacro %}");
        let sql = r#"select * from {{ audited_source("raw", "orders") }}"#;
        let (_, sources) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
        assert_eq!(sources[0].table_name, "orders");
    }

    #[test]
    fn test_macro_with_fixed_ref_adds_dependency() {
        // The model passes a relation; the macro also reads a fixed model.
        let wrappers = detect_wrappers_in(
            r"{% macro latest(table_name, clean_name) %}
              select * from {{ table_name }} t
              where exists (select 1 from {{ ref('dim_feeds') }} s)
              {% endmacro %}",
        );
        let sql = "{{ latest(table_name=ref('stops'), clean_name='stops') }}";
        let (refs, _) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(names(&refs), ["dim_feeds"]);
    }

    #[test]
    fn test_keyword_and_default_arguments() {
        let wrappers = detect_wrappers_in(
            r"{% macro staged(table, schema='raw') %}{{ source(schema, table) }}{% endmacro %}",
        );
        let (_, s) = extract_wrapper_calls("{{ staged(table='orders') }}", &wrappers);
        assert_eq!(
            (s[0].source_name.as_str(), s[0].table_name.as_str()),
            ("raw", "orders")
        );
        let (_, s) = extract_wrapper_calls("{{ staged('orders', schema='erp') }}", &wrappers);
        assert_eq!(s[0].source_name, "erp");
    }

    #[test]
    fn test_package_qualified_and_nested_calls() {
        let wrappers = detect_wrappers_in("{% macro smart_ref(n) %}{{ ref(n) }}{% endmacro %}");
        let sql = "{% set rel = my_pkg.smart_ref('a') %}{{ dbt_utils.star(from=smart_ref('b')) }}";
        let (refs, _) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(names(&refs), ["a", "b"]);
    }

    #[test]
    fn test_computed_argument_is_skipped() {
        let wrappers = detect_wrappers_in("{% macro smart_ref(n) %}{{ ref(n) }}{% endmacro %}");
        let (refs, _) = extract_wrapper_calls("{{ smart_ref(var('model')) }}", &wrappers);
        assert!(refs.is_empty());
    }

    #[test]
    fn test_call_in_jinja_comment_ignored() {
        // dbt renders Jinja inside SQL comments, so `-- {{ smart_ref('a') }}`
        // counts; a Jinja comment doesn't.
        let wrappers = detect_wrappers_in("{% macro smart_ref(n) %}{{ ref(n) }}{% endmacro %}");
        let (refs, _) = extract_wrapper_calls(
            "-- {{ smart_ref('a') }}\n{# {{ smart_ref('b') }} #}",
            &wrappers,
        );
        assert_eq!(names(&refs), ["a"]);
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

    #[test]
    fn test_dispatched_macro_inherits_default_implementation() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("union.sql");
        std::fs::write(
            &f,
            r"{% macro union_connections(dict, single_source_name, single_table_name) %}
              {{ adapter.dispatch('union_connections', 'fivetran_utils')(dict, single_source_name, single_table_name) }}
              {% endmacro %}
              {% macro default__union_connections(dict, single_source_name, single_table_name) %}
              select * from {{ source(single_source_name, single_table_name) }}
              {% endmacro %}",
        )
        .unwrap();
        let wrappers = detect_wrappers_in_files(&[f]);
        let sql = "{{ fivetran_utils.union_connections(connection_dictionary='x', \
                   single_source_name='hubspot', single_table_name='company') }}";
        let (_, sources) = extract_wrapper_calls(sql, &wrappers);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "hubspot");
        assert_eq!(sources[0].table_name, "company");
    }

    #[test]
    fn test_discover_lineage_macro_files_includes_packages() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(
            root.join("dbt_project.yml"),
            "name: p\nmacro-paths: ['mac']\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("mac")).unwrap();
        std::fs::write(root.join("mac/a.sql"), "").unwrap();
        std::fs::create_dir_all(root.join("dbt_packages/utils/macros")).unwrap();
        std::fs::write(root.join("dbt_packages/utils/macros/b.sql"), "").unwrap();
        let files = discover_lineage_macro_files(root);
        assert_eq!(files.len(), 2);
        assert!(files[0].ends_with("mac/a.sql"));
        assert_eq!(discover_macro_files(root).len(), 1);
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
        version: None,
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
