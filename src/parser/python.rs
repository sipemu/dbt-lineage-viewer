//! Python model parser (#27). Detects `dbt.ref(...)` and `dbt.source(...)` calls
//! inside `.py` files. Supports both positional (`dbt.ref("orders")`) and
//! keyword (`dbt.ref(name="orders")`) styles.
//!
//! Best-effort regex extraction — no full Python parser. The patterns are
//! tight enough to avoid false positives in string literals and comments
//! (those are filtered by the lexer-style preprocessor below).

use regex::Regex;
use std::sync::LazyLock;

use super::sql::{RefCall, SourceCall};

static DBT_REF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        dbt\.ref\s*\(\s*
        (?:
            # Two-argument: dbt.ref("pkg", "name")
            (?:['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"])
            |
            # Keyword: dbt.ref(name="...")
            (?:name\s*=\s*['"]([^'"]+)['"])
            |
            # Single positional: dbt.ref("name")
            (?:['"]([^'"]+)['"])
        )
        \s*\)
    "#,
    )
    .unwrap()
});

static DBT_SOURCE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?x)
        dbt\.source\s*\(\s*
        (?:
            (?:
                source_name\s*=\s*['"]([^'"]+)['"]\s*,\s*
                table_name\s*=\s*['"]([^'"]+)['"]
            )
            |
            (?:['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"])
        )
        \s*\)
    "#,
    )
    .unwrap()
});

/// Extract `dbt.ref(...)` calls from Python source. Strips string literals and
/// comments first so a `# dbt.ref(` line doesn't false-match.
pub fn extract_refs(py: &str) -> Vec<RefCall> {
    let cleaned = strip_python_non_code(py);
    let mut refs = Vec::new();
    for cap in DBT_REF.captures_iter(&cleaned) {
        if let (Some(pkg), Some(name)) = (cap.get(1), cap.get(2)) {
            refs.push(RefCall {
                package: Some(pkg.as_str().to_string()),
                name: name.as_str().to_string(),
            });
        } else if let Some(name) = cap.get(3) {
            refs.push(RefCall {
                package: None,
                name: name.as_str().to_string(),
            });
        } else if let Some(name) = cap.get(4) {
            refs.push(RefCall {
                package: None,
                name: name.as_str().to_string(),
            });
        }
    }
    refs
}

/// Extract `dbt.source(...)` calls from Python source.
pub fn extract_sources(py: &str) -> Vec<SourceCall> {
    let cleaned = strip_python_non_code(py);
    let mut sources = Vec::new();
    for cap in DBT_SOURCE.captures_iter(&cleaned) {
        if let (Some(s), Some(t)) = (cap.get(1), cap.get(2)) {
            sources.push(SourceCall {
                source_name: s.as_str().to_string(),
                table_name: t.as_str().to_string(),
            });
        } else if let (Some(s), Some(t)) = (cap.get(3), cap.get(4)) {
            sources.push(SourceCall {
                source_name: s.as_str().to_string(),
                table_name: t.as_str().to_string(),
            });
        }
    }
    sources
}

/// Blank out Python `#` line comments and triple-quoted docstrings. Regular
/// single/double-quoted strings are left intact because they hold the actual
/// `dbt.ref("orders")` argument values we want to capture. Trade-off: a
/// `"dbt.ref('foo')"` literal in source would false-match, but that pattern
/// is rare and acceptable for v1.
fn strip_python_non_code(py: &str) -> String {
    let bytes = py.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    enum State {
        Code,
        LineComment,
        StringTripleSingle,
        StringTripleDouble,
    }
    let mut state = State::Code;

    while i < bytes.len() {
        let b = bytes[i];
        match state {
            State::Code => {
                if b == b'#' {
                    out.push(b' ');
                    state = State::LineComment;
                    i += 1;
                    continue;
                }
                if i + 2 < bytes.len()
                    && b == b'\''
                    && bytes[i + 1] == b'\''
                    && bytes[i + 2] == b'\''
                {
                    out.extend_from_slice(b"   ");
                    state = State::StringTripleSingle;
                    i += 3;
                    continue;
                }
                if i + 2 < bytes.len() && b == b'"' && bytes[i + 1] == b'"' && bytes[i + 2] == b'"'
                {
                    out.extend_from_slice(b"   ");
                    state = State::StringTripleDouble;
                    i += 3;
                    continue;
                }
                out.push(b);
                i += 1;
            }
            State::LineComment => {
                if b == b'\n' {
                    out.push(b'\n');
                    state = State::Code;
                } else {
                    out.push(b' ');
                }
                i += 1;
            }
            State::StringTripleSingle | State::StringTripleDouble => {
                let triple_bytes = if matches!(state, State::StringTripleSingle) {
                    b"'''"
                } else {
                    b"\"\"\""
                };
                if i + 2 < bytes.len() && &bytes[i..i + 3] == triple_bytes {
                    out.extend_from_slice(b"   ");
                    state = State::Code;
                    i += 3;
                } else {
                    out.push(if b == b'\n' { b'\n' } else { b' ' });
                    i += 1;
                }
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| py.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positional_ref() {
        let py = r#"def model(dbt, session):
    return dbt.ref("orders")
"#;
        let refs = extract_refs(py);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "orders");
    }

    #[test]
    fn test_keyword_ref() {
        let py = r#"df = dbt.ref(name="customers")"#;
        let refs = extract_refs(py);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "customers");
    }

    #[test]
    fn test_two_arg_ref() {
        let py = r#"df = dbt.ref("other_proj", "orders")"#;
        let refs = extract_refs(py);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].package.as_deref(), Some("other_proj"));
        assert_eq!(refs[0].name, "orders");
    }

    #[test]
    fn test_positional_source() {
        let py = r#"src = dbt.source("raw", "orders")"#;
        let sources = extract_sources(py);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
        assert_eq!(sources[0].table_name, "orders");
    }

    #[test]
    fn test_keyword_source() {
        let py = r#"src = dbt.source(source_name="raw", table_name="payments")"#;
        let sources = extract_sources(py);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_name, "raw");
        assert_eq!(sources[0].table_name, "payments");
    }

    #[test]
    fn test_strips_comments() {
        let py = r#"# dbt.ref("ignored")
df = dbt.ref("real")
"#;
        let refs = extract_refs(py);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "real");
    }

    // Note: single/double-quoted strings are NOT stripped (we need the string
    // contents inside `dbt.ref("orders")` to remain). A `"dbt.ref('phantom')"`
    // string literal would false-match. Documented trade-off.

    #[test]
    fn test_strips_triple_quoted_string() {
        let py = r#"
"""Module docstring mentioning dbt.ref('not_here')."""
df = dbt.ref("real")
"#;
        let refs = extract_refs(py);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "real");
    }
}
