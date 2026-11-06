//! Changes dbt's `state:modified` sees beyond a node's own file: edited
//! macros the node calls, edited YAML properties of the node, and edited
//! `dbt_project.yml` configs that apply to it. [`mark_state_changes`] marks
//! such nodes modified in a diff, so `plan` rebuilds them.
//!
//! Like `state:modified`, description and test changes don't count: dbt
//! records them, but they don't change what a model builds.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;
use serde_yaml::Value;

use super::diff::{DiffStatus, LineageDiff};
use super::types::{LineageGraph, NodeType};

static MACRO_DEF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)\{%-?\s*macro\s+([A-Za-z_]\w*)\s*\((.*?)\{%-?\s*endmacro\s*-?%\}").unwrap()
});

/// Property keys that don't affect what a node builds.
const IGNORED_KEYS: &[&str] = &["description", "tests", "data_tests", "docs", "columns_doc"];

fn sql_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.is_dir() {
        return Vec::new();
    }
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "sql"))
        .map(|e| e.path().to_path_buf())
        .collect()
}

fn yaml_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.is_dir() {
        return Vec::new();
    }
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|x| x == "yml" || x == "yaml")
        })
        .map(|e| e.path().to_path_buf())
        .collect()
}

/// Project macro bodies by name.
fn macros(root: &Path, macro_paths: &[String]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for dir in macro_paths {
        for file in sql_files(&root.join(dir)) {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            for c in MACRO_DEF.captures_iter(&text) {
                let body: String = c[2].split_whitespace().collect::<Vec<_>>().join(" ");
                out.insert(c[1].to_string(), body);
            }
        }
    }
    out
}

fn calls(text: &str, name: &str) -> bool {
    Regex::new(&format!(r"\b{}\s*\(", regex::escape(name)))
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

/// Macros whose definition differs between the trees, plus (transitively)
/// the macros that call them.
fn changed_macros(base: &HashMap<String, String>, head: &HashMap<String, String>) -> Vec<String> {
    let mut changed: HashSet<String> = head
        .iter()
        .filter(|(name, body)| base.get(*name) != Some(*body))
        .map(|(name, _)| name.clone())
        .chain(base.keys().filter(|n| !head.contains_key(*n)).cloned())
        .collect();
    loop {
        let more: Vec<String> = head
            .iter()
            .filter(|(name, body)| {
                !changed.contains(*name) && changed.iter().any(|c| calls(body, c))
            })
            .map(|(name, _)| name.clone())
            .collect();
        if more.is_empty() {
            break;
        }
        changed.extend(more);
    }
    let mut v: Vec<String> = changed.into_iter().collect();
    v.sort();
    v
}

/// Drop keys that don't affect what a node builds, recursively. Without
/// `persist_docs` that includes descriptions and the column list, except a
/// column's contract parts (`data_type`, `constraints`); with it, dbt writes
/// descriptions to the warehouse, so they count.
fn strip_with(value: &Value, persist_docs: bool) -> Value {
    match value {
        Value::Mapping(m) => Value::Mapping(
            m.iter()
                .filter(|(k, _)| {
                    k.as_str().is_none_or(|k| {
                        !(IGNORED_KEYS.contains(&k) && !(persist_docs && k == "description"))
                    })
                })
                .map(|(k, v)| {
                    let v = match (k.as_str(), v) {
                        (Some("columns"), Value::Sequence(cols)) if !persist_docs => {
                            Value::Sequence(
                                cols.iter()
                                    .filter_map(|c| {
                                        let mut kept = serde_yaml::Mapping::new();
                                        for key in ["data_type", "constraints"] {
                                            if let Some(x) = c.get(key) {
                                                kept.insert(key.into(), x.clone());
                                            }
                                        }
                                        (!kept.is_empty()).then(|| {
                                            kept.insert(
                                                "name".into(),
                                                c.get("name").cloned().unwrap_or(Value::Null),
                                            );
                                            Value::Mapping(kept)
                                        })
                                    })
                                    .collect(),
                            )
                        }
                        _ => strip_with(v, persist_docs),
                    };
                    (k.clone(), v)
                })
                .collect(),
        ),
        Value::Sequence(s) => {
            Value::Sequence(s.iter().map(|v| strip_with(v, persist_docs)).collect())
        }
        other => other.clone(),
    }
}

fn strip(value: &Value) -> Value {
    strip_with(value, false)
}

/// YAML properties per node id: models, seeds, snapshots by name; sources as
/// `source.<source>.<table>` with the source-level properties merged in.
fn properties(root: &Path, dirs: &[String], persist_docs: bool) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for dir in dirs {
        for file in yaml_files(&root.join(dir)) {
            let Ok(doc) = std::fs::read_to_string(&file)
                .map_err(|_| ())
                .and_then(|t| serde_yaml::from_str::<Value>(&t).map_err(|_| ()))
            else {
                continue;
            };
            for (kind, prefix) in [
                ("models", "model"),
                ("seeds", "seed"),
                ("snapshots", "snapshot"),
            ] {
                for entry in doc
                    .get(kind)
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten()
                {
                    if let Some(name) = entry.get("name").and_then(Value::as_str) {
                        out.insert(format!("{prefix}.{name}"), strip_with(entry, persist_docs));
                    }
                }
            }
            for source in doc
                .get("sources")
                .and_then(Value::as_sequence)
                .into_iter()
                .flatten()
            {
                let Some(src) = source.get("name").and_then(Value::as_str) else {
                    continue;
                };
                let mut shared = strip_with(source, persist_docs);
                if let Value::Mapping(m) = &mut shared {
                    m.remove("tables");
                }
                for table in source
                    .get("tables")
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten()
                {
                    if let Some(name) = table.get("name").and_then(Value::as_str) {
                        out.insert(
                            format!("source.{src}.{name}"),
                            Value::Sequence(vec![shared.clone(), strip_with(table, persist_docs)]),
                        );
                    }
                }
            }
        }
    }
    out
}

/// `dbt_project.yml` config keys (`+materialized`, or keys of known
/// configs) per directory path below `models:` / `seeds:` / `snapshots:`.
fn project_configs(project: &Value, section: &str) -> BTreeMap<Vec<String>, Value> {
    fn walk(v: &Value, path: &mut Vec<String>, out: &mut BTreeMap<Vec<String>, Value>) {
        let Value::Mapping(m) = v else {
            return;
        };
        let mut configs = serde_yaml::Mapping::new();
        for (k, child) in m {
            let Some(key) = k.as_str() else { continue };
            if key.starts_with('+') || !matches!(child, Value::Mapping(_)) {
                configs.insert(k.clone(), strip(child));
            } else {
                path.push(key.to_string());
                walk(child, path, out);
                path.pop();
            }
        }
        if !configs.is_empty() {
            out.insert(path.clone(), Value::Mapping(configs));
        }
    }
    let mut out = BTreeMap::new();
    if let Some(v) = project.get(section) {
        walk(v, &mut Vec::new(), &mut out);
    }
    out
}

fn load_yaml(path: &Path) -> Value {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|t| serde_yaml::from_str(&t).ok())
        .unwrap_or(Value::Null)
}

/// Mark nodes modified the way `state:modified` would for changes outside
/// their own file. `head_root` is the project at head.
pub fn mark_state_changes(
    diff: &mut LineageDiff,
    head_graph: &LineageGraph,
    base_root: &Path,
    head_root: &Path,
) {
    let project = crate::parser::project::DbtProject::load(head_root).ok();
    let macro_paths = project
        .as_ref()
        .map(|p| p.macro_paths.clone())
        .unwrap_or_else(|| vec!["macros".into()]);
    let mut resource_paths: Vec<String> = project
        .as_ref()
        .map(|p| [&p.model_paths[..], &p.seed_paths[..], &p.snapshot_paths[..]].concat())
        .unwrap_or_else(|| vec!["models".into(), "seeds".into(), "snapshots".into()]);
    resource_paths.sort();
    resource_paths.dedup();

    let mut reasons: HashMap<String, String> = HashMap::new();

    // Macros: nodes whose SQL calls a changed macro.
    let changed = changed_macros(
        &macros(base_root, &macro_paths),
        &macros(head_root, &macro_paths),
    );
    if !changed.is_empty() {
        for idx in head_graph.node_indices() {
            let node = &head_graph[idx];
            let Some(rel) = node
                .file_path
                .as_deref()
                .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            else {
                continue;
            };
            let Ok(sql) = std::fs::read_to_string(head_root.join(rel)) else {
                continue;
            };
            if let Some(m) = changed.iter().find(|m| calls(&sql, m)) {
                reasons
                    .entry(node.unique_id.clone())
                    .or_insert_with(|| format!("macro `{m}` changed"));
            }
        }
    }

    // YAML properties of the node itself.
    // dbt counts description changes only when it persists docs.
    let persist_docs = std::fs::read_to_string(head_root.join("dbt_project.yml"))
        .is_ok_and(|t| t.contains("persist_docs"));
    let (base_props, head_props) = (
        properties(base_root, &resource_paths, persist_docs),
        properties(head_root, &resource_paths, persist_docs),
    );
    for (id, props) in &head_props {
        if base_props.get(id).is_some_and(|b| b != props) {
            reasons
                .entry(id.clone())
                .or_insert_with(|| "properties changed".into());
        }
    }

    // dbt_project.yml configs, applied by directory.
    let (base_project, head_project) = (
        load_yaml(&base_root.join("dbt_project.yml")),
        load_yaml(&head_root.join("dbt_project.yml")),
    );
    let name = project.as_ref().map(|p| p.name.clone()).unwrap_or_default();
    for (section, dirs, kind) in [
        (
            "models",
            project.as_ref().map(|p| p.model_paths.clone()),
            NodeType::Model,
        ),
        (
            "seeds",
            project.as_ref().map(|p| p.seed_paths.clone()),
            NodeType::Seed,
        ),
        (
            "snapshots",
            project.as_ref().map(|p| p.snapshot_paths.clone()),
            NodeType::Snapshot,
        ),
    ] {
        let (b, h) = (
            project_configs(&base_project, section),
            project_configs(&head_project, section),
        );
        let changed_paths: Vec<&Vec<String>> = h
            .keys()
            .chain(b.keys())
            .filter(|k| b.get(*k) != h.get(*k))
            .collect();
        if changed_paths.is_empty() {
            continue;
        }
        let dirs = dirs.unwrap_or_default();
        for idx in head_graph.node_indices() {
            let node = &head_graph[idx];
            if node.node_type != kind {
                continue;
            }
            let Some(rel) = node.file_path.as_deref() else {
                continue;
            };
            // Config paths are `<project>/<dir>/<subdir>…` (or a bare
            // top-level config applying to every node).
            let applies = changed_paths.iter().any(|path| {
                let Some((first, rest)) = path.split_first() else {
                    return true;
                };
                if *first != name {
                    return false;
                }
                dirs.iter().any(|d| {
                    let mut prefix = PathBuf::from(d);
                    prefix.extend(rest);
                    rel.starts_with(&prefix)
                        || rest.last().is_some_and(|last| {
                            rel.file_stem().is_some_and(|s| s == last.as_str())
                                && rel.parent().is_some_and(|p| {
                                    let mut dir = PathBuf::from(d);
                                    dir.extend(&rest[..rest.len() - 1]);
                                    p.starts_with(dir)
                                })
                        })
                })
            });
            if applies {
                reasons
                    .entry(node.unique_id.clone())
                    .or_insert_with(|| "dbt_project.yml config changed".into());
            }
        }
    }

    // Project vars: nodes whose SQL reads a var whose value changed.
    let vars_of = |p: &Value| -> BTreeMap<String, Value> {
        p.get("vars")
            .and_then(Value::as_mapping)
            .into_iter()
            .flatten()
            .flat_map(|(k, v)| match (k.as_str(), v) {
                // Package-scoped vars: `vars: {pkg: {name: value}}`.
                (Some(_), Value::Mapping(inner)) if inner.keys().all(|k| k.as_str().is_some()) => {
                    inner
                        .iter()
                        .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.clone())))
                        .chain(std::iter::once((
                            k.as_str().unwrap().to_string(),
                            v.clone(),
                        )))
                        .collect::<Vec<_>>()
                }
                (Some(key), v) => vec![(key.to_string(), v.clone())],
                _ => vec![],
            })
            .collect()
    };
    let (base_vars, head_vars) = (vars_of(&base_project), vars_of(&head_project));
    let changed_vars: Vec<&String> = head_vars
        .keys()
        .chain(base_vars.keys())
        .filter(|k| base_vars.get(*k) != head_vars.get(*k))
        .collect();
    if !changed_vars.is_empty() {
        for idx in head_graph.node_indices() {
            let node = &head_graph[idx];
            let Some(rel) = node
                .file_path
                .as_deref()
                .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            else {
                continue;
            };
            let Ok(sql) = std::fs::read_to_string(head_root.join(rel)) else {
                continue;
            };
            if let Some(v) = changed_vars.iter().find(|v| {
                Regex::new(&format!(r#"var\(\s*['"]{}['"]"#, regex::escape(v)))
                    .is_ok_and(|re| re.is_match(&sql))
            }) {
                reasons
                    .entry(node.unique_id.clone())
                    .or_insert_with(|| format!("var `{v}` changed"));
            }
        }
    }

    for node in diff.nodes.iter_mut() {
        let Some(reason) = reasons.get(&node.unique_id) else {
            continue;
        };
        match node.status {
            DiffStatus::Unchanged => {
                node.status = DiffStatus::Modified;
                diff.summary.nodes_modified += 1;
            }
            DiffStatus::Modified => {}
            _ => continue,
        }
        if !node.changes.contains(reason) {
            node.changes.push(reason.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn changed_macros_propagate_to_callers() {
        let base = map(&[("a", "x"), ("b", "{{ a() }}"), ("c", "y")]);
        let head = map(&[("a", "x2"), ("b", "{{ a() }}"), ("c", "y")]);
        assert_eq!(changed_macros(&base, &head), ["a", "b"]);
    }

    #[test]
    fn descriptions_and_tests_are_ignored() {
        let v: Value = serde_yaml::from_str(
            "name: m\ndescription: d\nconfig: {materialized: table}\ncolumns:\n  - name: id\n    description: x\n    data_tests: [unique]",
        )
        .unwrap();
        let w: Value = serde_yaml::from_str(
            "name: m\ndescription: other\nconfig: {materialized: table}\ncolumns:\n  - name: id\n    data_tests: [not_null]",
        )
        .unwrap();
        assert_eq!(strip(&v), strip(&w));
        let z: Value = serde_yaml::from_str("name: m\nconfig: {materialized: view}").unwrap();
        assert_ne!(strip(&v), strip(&z));
    }

    #[test]
    fn marks_macro_var_and_property_changes() {
        use crate::graph::diff::{DiffNode, DiffSummary};
        use crate::graph::types::NodeData;
        let write = |root: &Path, files: &[(&str, &str)]| {
            for (f, text) in files {
                let p = root.join(f);
                std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                std::fs::write(p, text).unwrap();
            }
        };
        let (base, head) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let common = [
            ("models/a.sql", "select {{ cents('x') }} as x"),
            ("models/b.sql", "select {{ var('cutoff') }} as y"),
            ("models/c.sql", "select 1 as z"),
            ("models/d.sql", "select 2 as w"),
        ];
        write(base.path(), &common);
        write(head.path(), &common);
        write(base.path(), &[
            ("dbt_project.yml", "name: p\nvars:\n  cutoff: 1\n"),
            ("macros/m.sql", "{% macro cents(c) %}{{ c }} / 100{% endmacro %}"),
            ("models/s.yml", "models:\n  - name: c\n    config: {materialized: view}\n  - name: d\n    description: old\n"),
        ]);
        write(head.path(), &[
            ("dbt_project.yml", "name: p\nvars:\n  cutoff: 2\n"),
            ("macros/m.sql", "{% macro cents(c) %}{{ c }} / 100.0{% endmacro %}"),
            ("models/s.yml", "models:\n  - name: c\n    config: {materialized: table}\n  - name: d\n    description: new\n"),
        ]);
        let mut g = LineageGraph::new();
        let mut nodes = Vec::new();
        for n in ["a", "b", "c", "d"] {
            g.add_node(NodeData {
                unique_id: format!("model.{n}"),
                label: n.into(),
                node_type: NodeType::Model,
                file_path: Some(format!("models/{n}.sql").into()),
                description: None,
                materialization: None,
                tags: vec![],
                columns: vec![],
            });
            nodes.push(DiffNode {
                unique_id: format!("model.{n}"),
                label: n.into(),
                node_type: "model".into(),
                status: DiffStatus::Unchanged,
                changes: vec![],
            });
        }
        let mut diff = LineageDiff {
            base_ref: "b".into(),
            head_ref: "h".into(),
            summary: DiffSummary::default(),
            nodes,
            edges: vec![],
            column_impacts: vec![],
            breaking_changes: vec![],
        };
        mark_state_changes(&mut diff, &g, base.path(), head.path());
        let modified: Vec<(&str, &str)> = diff
            .nodes
            .iter()
            .filter(|n| n.status == DiffStatus::Modified)
            .map(|n| (n.label.as_str(), n.changes[0].as_str()))
            .collect();
        // d: only its description changed, and the project doesn't persist docs.
        assert_eq!(
            modified,
            [
                ("a", "macro `cents` changed"),
                ("b", "var `cutoff` changed"),
                ("c", "properties changed")
            ]
        );
    }

    #[test]
    fn project_configs_by_directory() {
        let p: Value = serde_yaml::from_str(
            "models:\n  shop:\n    +materialized: view\n    marts:\n      +materialized: table\n      finance:\n        +tags: [f]",
        )
        .unwrap();
        let c = project_configs(&p, "models");
        assert!(c.contains_key(&vec!["shop".to_string()]));
        assert!(c.contains_key(&vec!["shop".to_string(), "marts".to_string()]));
        assert!(c.contains_key(&vec![
            "shop".to_string(),
            "marts".to_string(),
            "finance".to_string()
        ]));
    }
}
