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

static DOCS_DEF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)\{%-?\s*docs\s+([A-Za-z_]\w*)\s*-?%\}(.*?)\{%-?\s*enddocs\s*-?%\}").unwrap()
});

/// `doc('name')` or `doc('package', 'name')` inside a description.
static DOC_REF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\{\{-?\s*doc\(\s*(?:['"]\w+['"]\s*,\s*)?['"](\w+)['"]\s*\)\s*-?\}\}"#).unwrap()
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

/// `{% docs %}` blocks by name, from the `.md` files below `dirs`.
fn doc_blocks(root: &Path, dirs: &[String]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for dir in dirs {
        let dir = root.join(dir);
        if !dir.is_dir() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            if entry.path().extension().is_none_or(|x| x != "md") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            for c in DOCS_DEF.captures_iter(&text) {
                out.insert(c[1].to_string(), c[2].trim().to_string());
            }
        }
    }
    out
}

/// Replace `{{ doc('name') }}` in every `description` with the block's text,
/// so an edited docs block shows up as a changed description.
fn resolve_docs(value: &Value, docs: &HashMap<String, String>) -> Value {
    match value {
        Value::Mapping(m) => Value::Mapping(
            m.iter()
                .map(|(k, v)| {
                    let v = match (k.as_str(), v) {
                        (Some("description"), Value::String(s)) if s.contains("doc(") => {
                            Value::String(
                                DOC_REF
                                    .replace_all(s, |c: &regex::Captures| {
                                        docs.get(&c[1]).cloned().unwrap_or_default()
                                    })
                                    .into_owned(),
                            )
                        }
                        _ => resolve_docs(v, docs),
                    };
                    (k.clone(), v)
                })
                .collect(),
        ),
        Value::Sequence(s) => Value::Sequence(s.iter().map(|v| resolve_docs(v, docs)).collect()),
        other => other.clone(),
    }
}

/// Whether `text` calls macro `name`. A name ending in `.` stands for every
/// macro of that package: `pkg.any_macro(…)`, or a dispatch into its
/// namespace (`adapter.dispatch('x', 'pkg')`).
fn calls(text: &str, name: &str) -> bool {
    let pattern = match name.strip_suffix('.') {
        Some(pkg) => format!(
            r#"\b{}\.\w+\s*\(|dispatch\([^)]*['"]{}['"]"#,
            regex::escape(pkg),
            regex::escape(pkg)
        ),
        None => format!(r"\b{}\s*\(", regex::escape(name)),
    };
    Regex::new(&pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

/// Installed packages by name, each with its spec (version, revision, …)
/// from `packages.yml` / `dependencies.yml` and `package-lock.yml`.
fn package_specs(root: &Path) -> BTreeMap<String, Vec<Value>> {
    let mut out: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for file in ["packages.yml", "dependencies.yml", "package-lock.yml"] {
        let doc = load_yaml(&root.join(file));
        for entry in doc
            .get("packages")
            .and_then(Value::as_sequence)
            .into_iter()
            .flatten()
        {
            let Value::Mapping(m) = entry else { continue };
            let source = ["package", "git", "local", "tarball", "private"]
                .iter()
                .find_map(|k| m.get(*k).and_then(Value::as_str));
            // The lock records the project name; otherwise it's the last
            // path segment of the hub name / repo / directory.
            let name = m.get("name").and_then(Value::as_str).or_else(|| {
                source.and_then(|s| {
                    s.trim_end_matches('/')
                        .rsplit(['/', ':'])
                        .next()
                        .map(|s| s.trim_end_matches(".git"))
                })
            });
            let Some(name) = name.filter(|n| {
                !n.is_empty()
                    && n.chars()
                        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            }) else {
                continue;
            };
            let mut spec = m.clone();
            spec.remove("name");
            out.entry(name.replace('-', "_"))
                .or_default()
                .push(Value::Mapping(spec));
        }
    }
    out
}

/// Packages whose pinned version, revision or source changed (their macros
/// and models may have changed, which we cannot compare without installing
/// both), and packages new at head.
fn package_changes(base_root: &Path, head_root: &Path) -> (Vec<String>, Vec<String>) {
    let (base, head) = (package_specs(base_root), package_specs(head_root));
    let changed = head
        .iter()
        .filter(|(name, h)| base.get(*name).is_some_and(|b| b != *h))
        .map(|(name, _)| name.clone())
        .collect();
    let added = head
        .keys()
        .filter(|name| !base.contains_key(*name))
        .cloned()
        .collect();
    (changed, added)
}

/// Macros whose definition differs between the trees, plus (transitively)
/// the macros that call them.
fn changed_macros(
    base: &HashMap<String, String>,
    head: &HashMap<String, String>,
    packages: &[String],
) -> Vec<String> {
    let mut changed: HashSet<String> = head
        .iter()
        .filter(|(name, body)| base.get(*name) != Some(*body))
        .map(|(name, _)| name.clone())
        .chain(base.keys().filter(|n| !head.contains_key(*n)).cloned())
        .chain(packages.iter().map(|p| format!("{p}.")))
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
/// With `persist_docs`, `docs` resolves `doc()` references in descriptions.
fn properties(
    root: &Path,
    dirs: &[String],
    persist_docs: bool,
    docs: &HashMap<String, String>,
) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for dir in dirs {
        for file in yaml_files(&root.join(dir)) {
            let Ok(doc) = std::fs::read_to_string(&file)
                .map_err(|_| ())
                .and_then(|t| serde_yaml::from_str::<Value>(&t).map_err(|_| ()))
            else {
                continue;
            };
            let doc = if persist_docs && !docs.is_empty() {
                resolve_docs(&doc, docs)
            } else {
                doc
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

    // Macros: nodes whose SQL calls a changed macro, or any macro of a
    // package whose pinned version changed (`pkg.` in `changed`).
    let (changed_pkgs, added_pkgs) = package_changes(base_root, head_root);
    let changed = changed_macros(
        &macros(base_root, &macro_paths),
        &macros(head_root, &macro_paths),
        &changed_pkgs,
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
                reasons.entry(node.unique_id.clone()).or_insert_with(|| {
                    match m.strip_suffix('.') {
                        Some(pkg) => format!("package `{pkg}` changed"),
                        None => format!("macro `{m}` changed"),
                    }
                });
            }
        }
    }

    // YAML properties of the node itself.
    // dbt counts description changes only when it persists docs.
    let persist_docs = std::fs::read_to_string(head_root.join("dbt_project.yml"))
        .is_ok_and(|t| t.contains("persist_docs"));
    // Docs blocks live in `.md` files below `docs-paths` (default: every
    // resource and macro path).
    let docs_of = |root: &Path| {
        if !persist_docs {
            return HashMap::new();
        }
        let dirs: Vec<String> = load_yaml(&root.join("dbt_project.yml"))
            .get("docs-paths")
            .and_then(Value::as_sequence)
            .map(|s| {
                s.iter()
                    .filter_map(|d| d.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_else(|| [&resource_paths[..], &macro_paths[..]].concat());
        doc_blocks(root, &dirs)
    };
    let (base_props, head_props) = (
        properties(
            base_root,
            &resource_paths,
            persist_docs,
            &docs_of(base_root),
        ),
        properties(
            head_root,
            &resource_paths,
            persist_docs,
            &docs_of(head_root),
        ),
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

    // Installed packages: a new one brings new nodes, a re-pinned one may
    // change any of its own models. `plan` selects them as `package:<name>`.
    for (names, status) in [
        (&added_pkgs, DiffStatus::Added),
        (&changed_pkgs, DiffStatus::Modified),
    ] {
        for name in names {
            diff.nodes.push(super::diff::DiffNode {
                unique_id: format!("package.{name}"),
                label: name.clone(),
                node_type: "package".into(),
                status,
                changes: match status {
                    DiffStatus::Modified => vec!["package version changed".into()],
                    _ => vec![],
                },
            });
            match status {
                DiffStatus::Added => diff.summary.nodes_added += 1,
                _ => diff.summary.nodes_modified += 1,
            }
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
        assert_eq!(changed_macros(&base, &head, &[]), ["a", "b"]);
    }

    #[test]
    fn package_version_bumps_reach_their_callers() {
        let tree = |utils: &str, lock: &str| {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("packages.yml"),
                format!("packages:\n  - package: dbt-labs/dbt_utils\n    version: {utils}\n  - git: https://github.com/org/semantic-layer.git\n    revision: v1\n"),
            )
            .unwrap();
            std::fs::write(
                dir.path().join("package-lock.yml"),
                format!("packages:\n  - name: dbt_utils\n    package: dbt-labs/dbt_utils\n    version: {utils}\n  - git: https://github.com/org/semantic-layer.git\n    name: semantic_layer\n    revision: {lock}\nsha1_hash: x\n"),
            )
            .unwrap();
            dir
        };
        let (base, head) = (tree("1.3.0", "aaa"), tree("1.3.0", "bbb"));
        assert_eq!(
            package_changes(base.path(), head.path()).0,
            ["semantic_layer"]
        );
        let (base, head) = (tree("1.3.0", "aaa"), tree("1.4.0", "aaa"));
        let pkgs = package_changes(base.path(), head.path()).0;
        assert_eq!(pkgs, ["dbt_utils"]);
        let none = tempfile::tempdir().unwrap();
        let (changed, added) = package_changes(none.path(), head.path());
        assert!(changed.is_empty());
        assert_eq!(added, ["dbt_utils", "semantic_layer"]);

        // Project macros calling into the package change with it.
        let macros = map(&[
            ("keyed", "{{ dbt_utils.generate_surrogate_key(cols) }}"),
            ("dispatched", "{{ adapter.dispatch('x', 'dbt_utils')() }}"),
            ("caller", "{{ keyed(c) }}"),
            ("plain", "select 1"),
        ]);
        assert_eq!(
            changed_macros(&macros, &macros, &pkgs),
            ["caller", "dbt_utils.", "dispatched", "keyed"]
        );
        assert!(calls("select {{ dbt_utils.star(ref('a')) }}", "dbt_utils."));
        assert!(!calls("select {{ my_utils.star(ref('a')) }}", "dbt_utils."));
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
    fn edited_docs_blocks_count_only_with_persist_docs() {
        let tree = |block: &str| {
            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(dir.path().join("models")).unwrap();
            std::fs::write(
                dir.path().join("models/docs.md"),
                format!("{{% docs orders %}}{block}{{% enddocs %}}\n{{% docs other %}}same{{% enddocs %}}"),
            )
            .unwrap();
            std::fs::write(
                dir.path().join("models/s.yml"),
                "models:\n  - name: a\n    description: '{{ doc(\"orders\") }}'\n  - name: b\n    columns:\n      - name: id\n        description: \"{{ doc('orders') }}\"\n  - name: c\n    description: '{{ doc(\"other\") }}'\n",
            )
            .unwrap();
            dir
        };
        let (base, head) = (tree("Orders."), tree("All orders."));
        let dirs = vec!["models".to_string()];
        let changed = |persist: bool| -> Vec<String> {
            let props = |root: &Path| properties(root, &dirs, persist, &doc_blocks(root, &dirs));
            let (b, h) = (props(base.path()), props(head.path()));
            h.iter()
                .filter(|(id, v)| b.get(*id) != Some(*v))
                .map(|(id, _)| id.clone())
                .collect()
        };
        assert_eq!(changed(true), ["model.a", "model.b"]);
        assert!(changed(false).is_empty());
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
