//! Macro dependency analysis (#28). Walks `macros/*.sql` and builds a
//! best-effort call graph of macros → macros they invoke. Distinct from the
//! main lineage graph; users explore via `dbt-lineage macros`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use regex::Regex;
use serde::Serialize;
use std::sync::LazyLock;

use crate::parser::macros::discover_macro_files;

/// One macro definition with the names of the other macros it calls.
#[derive(Debug, Clone, Serialize)]
pub struct MacroEntry {
    pub name: String,
    pub file: String,
    pub calls: Vec<String>,
}

/// Project-wide macro call graph.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MacroGraph {
    pub macros: Vec<MacroEntry>,
}

impl MacroGraph {
    /// Macros that are defined but never called by any other macro AND never
    /// detected as a ref-wrapper in a model. Best-effort orphan signal.
    pub fn orphans(&self) -> Vec<&MacroEntry> {
        let called: HashSet<String> = self
            .macros
            .iter()
            .flat_map(|m| m.calls.iter().cloned())
            .collect();
        self.macros
            .iter()
            .filter(|m| !called.contains(&m.name))
            .collect()
    }
}

/// Regex capturing `{% macro NAME(...) %}` … `{% endmacro %}` blocks. Same
/// shape as `parser::macros::MACRO_DEF` but exported separately here so the
/// macro analyzer is self-contained.
static MACRO_DEF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?xs)
        \{%-?\s*macro\s+
        (?P<name>[A-Za-z_][A-Za-z0-9_]*)
        \s*\([^)]*\)\s*-?%\}
        (?P<body>.*?)
        \{%-?\s*endmacro\s*-?%\}
        ",
    )
    .unwrap()
});

/// Regex matching identifier callsites inside a macro body. Captures any
/// `name(args)` that isn't a reserved word. We intersect against the set of
/// defined macros to filter false positives (`upper(x)`, `sum(x)`, etc).
static CALL_SITE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(").unwrap());

/// Scan a project directory's macros and return their call graph.
pub fn analyze_macros(project_dir: &Path) -> Result<MacroGraph> {
    let files = discover_macro_files(project_dir);
    let mut raw_entries: Vec<(String, String, String)> = Vec::new(); // (name, file_path, body)

    for path in &files {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for cap in MACRO_DEF.captures_iter(&content) {
            let name = cap.name("name").unwrap().as_str().to_string();
            let body = cap.name("body").unwrap().as_str().to_string();
            raw_entries.push((name, path.to_string_lossy().to_string(), body));
        }
    }

    // Build the set of defined macro names so we can filter call-site matches.
    let defined: HashSet<&str> = raw_entries.iter().map(|(n, _, _)| n.as_str()).collect();

    let mut macros: Vec<MacroEntry> = raw_entries
        .iter()
        .map(|(name, file, body)| {
            let mut calls: Vec<String> = CALL_SITE
                .captures_iter(body)
                .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
                .filter(|n| defined.contains(n.as_str()) && n != name)
                .collect();
            calls.sort();
            calls.dedup();
            MacroEntry {
                name: name.clone(),
                file: file.clone(),
                calls,
            }
        })
        .collect();
    macros.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(MacroGraph { macros })
}

/// Filter the macro graph to only entries reachable from a starting macro,
/// useful for "what does this macro call (transitively)?" queries.
pub fn upstream_of(graph: &MacroGraph, root: &str) -> Vec<String> {
    let by_name: HashMap<&str, &MacroEntry> =
        graph.macros.iter().map(|m| (m.name.as_str(), m)).collect();
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack = vec![root.to_string()];
    while let Some(n) = stack.pop() {
        if !seen.insert(n.clone()) {
            continue;
        }
        if let Some(entry) = by_name.get(n.as_str()) {
            for c in &entry.calls {
                stack.push(c.clone());
            }
        }
    }
    let mut out: Vec<String> = seen.into_iter().filter(|n| n != root).collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_macros(project_dir: &Path, files: &[(&str, &str)]) {
        let dir = project_dir.join("macros");
        fs::create_dir_all(&dir).unwrap();
        for (name, body) in files {
            fs::write(dir.join(name), body).unwrap();
        }
    }

    #[test]
    fn test_detects_simple_call_graph() {
        let tmp = tempfile::tempdir().unwrap();
        write_macros(
            tmp.path(),
            &[
                (
                    "a.sql",
                    "{% macro a(x) %}{{ b(x) }} and {{ c(x) }}{% endmacro %}",
                ),
                ("b.sql", "{% macro b(x) %}{{ upper(x) }}{% endmacro %}"),
                ("c.sql", "{% macro c(x) %}{% endmacro %}"),
            ],
        );
        let g = analyze_macros(tmp.path()).unwrap();
        assert_eq!(g.macros.len(), 3);
        let a = g.macros.iter().find(|m| m.name == "a").unwrap();
        assert_eq!(a.calls, vec!["b".to_string(), "c".to_string()]);
        // upper(x) is not a defined macro → not captured.
        let b = g.macros.iter().find(|m| m.name == "b").unwrap();
        assert!(b.calls.is_empty());
    }

    #[test]
    fn test_orphans_are_uncalled() {
        let tmp = tempfile::tempdir().unwrap();
        write_macros(
            tmp.path(),
            &[
                ("a.sql", "{% macro a(x) %}{{ b(x) }}{% endmacro %}"),
                ("b.sql", "{% macro b(x) %}{% endmacro %}"),
                ("orphan.sql", "{% macro orphan(x) %}{% endmacro %}"),
            ],
        );
        let g = analyze_macros(tmp.path()).unwrap();
        let orphan_names: Vec<&str> = g.orphans().iter().map(|m| m.name.as_str()).collect();
        // `a` is uncalled at the macro-graph level too (no other macro calls it).
        // `orphan` is also uncalled.
        // `b` is called by `a` so not orphan.
        assert!(orphan_names.contains(&"a"));
        assert!(orphan_names.contains(&"orphan"));
        assert!(!orphan_names.contains(&"b"));
    }

    #[test]
    fn test_upstream_walks_transitively() {
        let tmp = tempfile::tempdir().unwrap();
        write_macros(
            tmp.path(),
            &[(
                "all.sql",
                "{% macro a(x) %}{{ b(x) }}{% endmacro %}\
                 {% macro b(x) %}{{ c(x) }}{% endmacro %}\
                 {% macro c(x) %}{% endmacro %}",
            )],
        );
        let g = analyze_macros(tmp.path()).unwrap();
        let chain = upstream_of(&g, "a");
        assert!(chain.contains(&"b".to_string()));
        assert!(chain.contains(&"c".to_string()));
    }

    #[test]
    fn test_recursive_self_call_not_in_calls() {
        let tmp = tempfile::tempdir().unwrap();
        write_macros(
            tmp.path(),
            &[("rec.sql", "{% macro rec(x) %}{{ rec(x) }}{% endmacro %}")],
        );
        let g = analyze_macros(tmp.path()).unwrap();
        let r = g.macros.iter().find(|m| m.name == "rec").unwrap();
        // Self-call is filtered out so we don't report a trivial cycle.
        assert!(r.calls.is_empty());
    }
}
