use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::git;
use crate::graph::types::*;

/// Status of a node or edge in the diff
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffStatus {
    Added,
    Removed,
    Modified,
    Unchanged,
}

impl DiffStatus {
    pub fn label(&self) -> &'static str {
        match self {
            DiffStatus::Added => "added",
            DiffStatus::Removed => "removed",
            DiffStatus::Modified => "modified",
            DiffStatus::Unchanged => "unchanged",
        }
    }
}

/// A node in the diff with its status and changes
#[derive(Debug, Clone, Serialize)]
pub struct DiffNode {
    pub unique_id: String,
    pub label: String,
    pub node_type: String,
    pub status: DiffStatus,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<String>,
}

/// An edge in the diff with its status
#[derive(Debug, Clone, Serialize)]
pub struct DiffEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub status: DiffStatus,
}

/// Summary counts for a diff
#[derive(Debug, Clone, Default, Serialize)]
pub struct DiffSummary {
    pub nodes_added: usize,
    pub nodes_removed: usize,
    pub nodes_modified: usize,
    pub edges_added: usize,
    pub edges_removed: usize,
}

/// A column that disappeared from a model, with the downstream columns that
/// were computed from it (and so break or change meaning).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnImpact {
    /// Model the column was removed from (label).
    pub model: String,
    pub column: String,
    /// Downstream columns derived from it, as `model.column`, sorted.
    pub downstream: Vec<String>,
}

/// Full lineage diff between two refs
#[derive(Debug, Clone, Serialize)]
pub struct LineageDiff {
    pub base_ref: String,
    pub head_ref: String,
    pub summary: DiffSummary,
    pub nodes: Vec<DiffNode>,
    pub edges: Vec<DiffEdge>,
    /// Removed columns that downstream columns depend on (git diffs only).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub column_impacts: Vec<ColumnImpact>,
}

/// An edge tuple for set comparison
#[derive(Hash, Eq, PartialEq, Clone)]
struct EdgeTuple {
    source: String,
    target: String,
    edge_type: String,
}

fn edge_type_str(et: EdgeType) -> &'static str {
    match et {
        EdgeType::Ref => "ref",
        EdgeType::Source => "source",
        EdgeType::Test => "test",
        EdgeType::Exposure => "exposure",
    }
}

/// Collect edge tuples from a graph
fn collect_edge_set(graph: &LineageGraph) -> HashSet<EdgeTuple> {
    use petgraph::visit::{EdgeRef, IntoEdgeReferences};
    graph
        .edge_references()
        .map(|e| EdgeTuple {
            source: graph[e.source()].unique_id.clone(),
            target: graph[e.target()].unique_id.clone(),
            edge_type: edge_type_str(e.weight().edge_type).to_string(),
        })
        .collect()
}

/// Collect nodes into a map by unique_id
fn collect_node_map(graph: &LineageGraph) -> HashMap<String, &NodeData> {
    graph
        .node_indices()
        .map(|idx| (graph[idx].unique_id.clone(), &graph[idx]))
        .collect()
}

/// Compare two nodes and return a list of changes
fn detect_node_changes(base: &NodeData, head: &NodeData) -> Vec<String> {
    let mut changes = Vec::new();

    if base.materialization != head.materialization {
        changes.push(format!(
            "materialization: {:?} -> {:?}",
            base.materialization, head.materialization
        ));
    }

    if base.tags != head.tags {
        changes.push(format!("tags: {:?} -> {:?}", base.tags, head.tags));
    }

    if base.columns != head.columns {
        let added: Vec<&str> = head
            .columns
            .iter()
            .filter(|c| !base.columns.contains(c))
            .map(String::as_str)
            .collect();
        let removed: Vec<&str> = base
            .columns
            .iter()
            .filter(|c| !head.columns.contains(c))
            .map(String::as_str)
            .collect();
        if !added.is_empty() {
            changes.push(format!("columns added: {}", added.join(", ")));
        }
        if !removed.is_empty() {
            changes.push(format!("columns removed: {}", removed.join(", ")));
        }
        if added.is_empty() && removed.is_empty() {
            changes.push("columns reordered".to_string());
        }
    }

    if base.description != head.description {
        changes.push("description changed".to_string());
    }

    changes
}

/// Compute a diff between two graphs
pub fn compute_diff(
    base_graph: &LineageGraph,
    head_graph: &LineageGraph,
    base_ref: &str,
    head_ref: &str,
) -> LineageDiff {
    let base_nodes = collect_node_map(base_graph);
    let head_nodes = collect_node_map(head_graph);

    let base_ids: HashSet<&String> = base_nodes.keys().collect();
    let head_ids: HashSet<&String> = head_nodes.keys().collect();

    let mut diff_nodes = Vec::new();
    let mut summary = DiffSummary::default();

    // Added nodes (in head but not base)
    for id in head_ids.difference(&base_ids) {
        let node = head_nodes[*id];
        diff_nodes.push(DiffNode {
            unique_id: node.unique_id.clone(),
            label: node.label.clone(),
            node_type: node.node_type.label().to_string(),
            status: DiffStatus::Added,
            changes: vec![],
        });
        summary.nodes_added += 1;
    }

    // Removed nodes (in base but not head)
    for id in base_ids.difference(&head_ids) {
        let node = base_nodes[*id];
        diff_nodes.push(DiffNode {
            unique_id: node.unique_id.clone(),
            label: node.label.clone(),
            node_type: node.node_type.label().to_string(),
            status: DiffStatus::Removed,
            changes: vec![],
        });
        summary.nodes_removed += 1;
    }

    // Intersection: check for modifications
    for id in base_ids.intersection(&head_ids) {
        let base_node = base_nodes[*id];
        let head_node = head_nodes[*id];
        let changes = detect_node_changes(base_node, head_node);
        let status = if changes.is_empty() {
            DiffStatus::Unchanged
        } else {
            summary.nodes_modified += 1;
            DiffStatus::Modified
        };
        diff_nodes.push(DiffNode {
            unique_id: head_node.unique_id.clone(),
            label: head_node.label.clone(),
            node_type: head_node.node_type.label().to_string(),
            status,
            changes,
        });
    }

    // Sort: added first, then modified, then removed, then unchanged
    diff_nodes.sort_by_key(|n| match n.status {
        DiffStatus::Added => 0,
        DiffStatus::Modified => 1,
        DiffStatus::Removed => 2,
        DiffStatus::Unchanged => 3,
    });

    // Edge diff
    let base_edges = collect_edge_set(base_graph);
    let head_edges = collect_edge_set(head_graph);

    let mut diff_edges = Vec::new();

    for edge in head_edges.difference(&base_edges) {
        diff_edges.push(DiffEdge {
            source: edge.source.clone(),
            target: edge.target.clone(),
            edge_type: edge.edge_type.clone(),
            status: DiffStatus::Added,
        });
        summary.edges_added += 1;
    }

    for edge in base_edges.difference(&head_edges) {
        diff_edges.push(DiffEdge {
            source: edge.source.clone(),
            target: edge.target.clone(),
            edge_type: edge.edge_type.clone(),
            status: DiffStatus::Removed,
        });
        summary.edges_removed += 1;
    }

    LineageDiff {
        base_ref: base_ref.to_string(),
        head_ref: head_ref.to_string(),
        summary,
        nodes: diff_nodes,
        edges: diff_edges,
        column_impacts: Vec::new(),
    }
}

/// Which reader builds both sides of a diff. Mixing them would compare
/// manifest unique_ids (`model.<project>.x`) with SQL-parse ids (`model.x`)
/// and report every node as removed and re-added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphSource {
    Manifest,
    Sql,
}

fn manifest_in(root: &Path) -> std::path::PathBuf {
    root.join("target").join("manifest.json")
}

/// Unpack the project directory's tree at `git_ref` into a fresh temp dir.
fn checkout_tree(project_dir: &Path, git_ref: &str) -> Result<tempfile::TempDir> {
    let tmp = tempfile::tempdir().context("Failed to create temp dir for git ref")?;
    git::archive_to(project_dir, git_ref, tmp.path())?;
    Ok(tmp)
}

/// Build the graph for a project rooted at `root`. In SQL mode without a
/// `dbt_project.yml`, `git_fallback` (repo dir + ref) selects a minimal
/// models-only parse, e.g. when the project is new in this PR.
fn graph_from_dir(
    root: &Path,
    source: GraphSource,
    use_cache: bool,
    git_fallback: Option<(&Path, &str)>,
) -> Result<LineageGraph> {
    if source == GraphSource::Manifest {
        return crate::parser::manifest::build_graph_from_manifest(&manifest_in(root));
    }
    if let (false, Some((project_dir, git_ref))) =
        (root.join("dbt_project.yml").exists(), git_fallback)
    {
        return Ok(build_minimal_graph_from_git(project_dir, git_ref));
    }
    let project = crate::parser::project::DbtProject::load(root)?;
    let paths = project.resolve_paths(root);
    let files = crate::parser::discovery::discover_files(&paths)?;
    if use_cache {
        crate::graph::builder::build_graph(root, &files)
    } else {
        crate::graph::builder::build_graph_no_cache(root, &files)
    }
}

/// Build a graph from a git ref with the same pipeline as the working tree:
/// the project directory's tree at `git_ref` is unpacked into a temp dir, then
/// read via `target/manifest.json` if it was committed, else via the full
/// SQL/YAML builder (seeds, tests, exposures, descriptions, macro-aware refs).
pub fn build_graph_from_ref(project_dir: &Path, git_ref: &str) -> Result<LineageGraph> {
    let tree = checkout_tree(project_dir, git_ref)?;
    let source = if manifest_in(tree.path()).exists() {
        GraphSource::Manifest
    } else {
        GraphSource::Sql
    };
    graph_from_dir(tree.path(), source, false, Some((project_dir, git_ref)))
        .with_context(|| format!("Failed to build lineage graph at {}", git_ref))
}

/// Diff the project at `base` against `head` (or the working tree when `head`
/// is `None`). Both sides are read the same way: from `target/manifest.json`
/// only when both have one, otherwise by parsing SQL/YAML. On top of the
/// structural diff, nodes whose SQL/Python/seed file content changed are
/// marked modified, so `plan` rebuilds them.
pub fn diff_git(project_dir: &Path, base: &str, head: Option<&str>) -> Result<LineageDiff> {
    diff_git_with_head(project_dir, base, head).map(|(diff, _)| diff)
}

/// Like [`diff_git`], also returning the head graph (e.g. to draw the
/// downstream impact of the changed nodes).
pub fn diff_git_with_head(
    project_dir: &Path,
    base: &str,
    head: Option<&str>,
) -> Result<(LineageDiff, LineageGraph)> {
    let base_tree = checkout_tree(project_dir, base)?;
    let head_tree = head.map(|h| checkout_tree(project_dir, h)).transpose()?;
    let head_root = head_tree.as_ref().map_or(project_dir, |t| t.path());

    let source = if manifest_in(base_tree.path()).exists() && manifest_in(head_root).exists() {
        GraphSource::Manifest
    } else {
        GraphSource::Sql
    };
    let base_graph = graph_from_dir(base_tree.path(), source, false, Some((project_dir, base)))
        .with_context(|| format!("Failed to build lineage graph at {}", base))?;
    let (head_graph, head_label) = match head {
        Some(h) => (
            graph_from_dir(head_root, source, false, Some((project_dir, h)))
                .with_context(|| format!("Failed to build lineage graph at {}", h))?,
            h.to_string(),
        ),
        None => (
            graph_from_dir(project_dir, source, true, None)?,
            git::current_ref(project_dir).unwrap_or_else(|_| "HEAD".into()),
        ),
    };

    let mut diff = compute_diff(&base_graph, &head_graph, base, &head_label);
    mark_file_changes(&mut diff, &head_graph, base_tree.path(), head_root);
    diff.column_impacts = removed_column_impacts(&base_graph, &head_graph, base_tree.path());
    Ok((diff, head_graph))
}

/// For every column that exists in a base model but not in the head version
/// (or whose model was removed), follow the base column lineage downstream.
/// Only columns with downstream users are reported.
pub fn removed_column_impacts(
    base_graph: &LineageGraph,
    head_graph: &LineageGraph,
    base_root: &Path,
) -> Vec<ColumnImpact> {
    let head_nodes = collect_node_map(head_graph);
    let removed: Vec<(&NodeData, &String)> = base_graph
        .node_indices()
        .map(|i| &base_graph[i])
        .filter(|n| n.node_type == NodeType::Model)
        .flat_map(|n| {
            let head_cols = head_nodes.get(&n.unique_id).map(|h| &h.columns);
            n.columns
                .iter()
                .filter(move |c| head_cols.is_none_or(|hc| !hc.contains(c)))
                .map(move |c| (n, c))
        })
        .collect();
    if removed.is_empty() {
        return Vec::new();
    }

    let lineage = {
        let mut edges =
            crate::parser::ast_column_lineage::resolve_column_lineage_ast_in(base_graph, base_root)
                .edges;
        edges.extend(
            crate::parser::column_lineage::resolve_column_lineage_in(base_graph, base_root).edges,
        );
        edges
    };
    let mut children: HashMap<(&str, &str), Vec<(&str, &str)>> = HashMap::new();
    for e in &lineage {
        children
            .entry((e.source_node.as_str(), e.source_column.as_str()))
            .or_default()
            .push((e.target_node.as_str(), e.target_column.as_str()));
    }
    let labels: HashMap<&str, &str> = base_graph
        .node_indices()
        .map(|i| {
            (
                base_graph[i].unique_id.as_str(),
                base_graph[i].label.as_str(),
            )
        })
        .collect();

    let mut impacts = Vec::new();
    for (node, column) in removed {
        let start = (node.unique_id.as_str(), column.as_str());
        let mut seen: HashSet<(&str, &str)> = HashSet::from([start]);
        let mut stack = vec![start];
        while let Some(key) = stack.pop() {
            for &child in children.get(&key).into_iter().flatten() {
                if seen.insert(child) {
                    stack.push(child);
                }
            }
        }
        seen.remove(&start);
        let mut downstream: Vec<String> = seen
            .into_iter()
            .filter(|(id, _)| head_nodes.contains_key(*id))
            .map(|(id, col)| format!("{}.{}", labels.get(id).copied().unwrap_or(id), col))
            .collect();
        if downstream.is_empty() {
            continue;
        }
        downstream.sort();
        impacts.push(ColumnImpact {
            model: node.label.clone(),
            column: column.clone(),
            downstream,
        });
    }
    impacts.sort_by(|a, b| (&a.model, &a.column).cmp(&(&b.model, &b.column)));
    impacts
}

/// Mark nodes whose source file differs between the two project roots as
/// modified (dbt's `state:modified` equivalent for body changes). Trailing
/// whitespace and line endings are ignored.
fn mark_file_changes(
    diff: &mut LineageDiff,
    head_graph: &LineageGraph,
    base_root: &Path,
    head_root: &Path,
) {
    let files: HashMap<&str, &Path> = head_graph
        .node_indices()
        .filter_map(|idx| {
            let node = &head_graph[idx];
            let path = node.file_path.as_deref().filter(|p| p.is_relative())?;
            Some((node.unique_id.as_str(), path))
        })
        .collect();

    for node in diff
        .nodes
        .iter_mut()
        .filter(|n| matches!(n.status, DiffStatus::Unchanged | DiffStatus::Modified))
    {
        let Some(rel) = files.get(node.unique_id.as_str()) else {
            continue;
        };
        let what = match rel.extension().and_then(|e| e.to_str()) {
            Some("sql") => "SQL changed",
            Some("py") => "Python changed",
            Some("csv") => "seed data changed",
            _ => continue,
        };
        let (Ok(old), Ok(new)) = (
            std::fs::read_to_string(base_root.join(rel)),
            std::fs::read_to_string(head_root.join(rel)),
        ) else {
            continue;
        };
        if normalize_source(&old) != normalize_source(&new) {
            if node.status == DiffStatus::Unchanged {
                node.status = DiffStatus::Modified;
                diff.summary.nodes_modified += 1;
            }
            node.changes.insert(0, what.to_string());
        }
    }
    diff.nodes.sort_by_key(|n| match n.status {
        DiffStatus::Added => 0,
        DiffStatus::Modified => 1,
        DiffStatus::Removed => 2,
        DiffStatus::Unchanged => 3,
    });
}

fn normalize_source(s: &str) -> String {
    s.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Minimal models-only graph read straight from the git tree.
fn build_minimal_graph_from_git(project_dir: &Path, git_ref: &str) -> LineageGraph {
    let sql_files = git::git_ls_tree(project_dir, git_ref, "models")
        .unwrap_or_default()
        .into_iter()
        .filter(|f| f.ends_with(".sql"))
        .collect::<Vec<_>>();

    let yaml_files = git::git_ls_tree(project_dir, git_ref, "models")
        .unwrap_or_default()
        .into_iter()
        .filter(|f| f.ends_with(".yml") || f.ends_with(".yaml"))
        .collect::<Vec<_>>();

    let mut graph = LineageGraph::new();
    parse_sources_from_git(&mut graph, project_dir, git_ref, &yaml_files);
    parse_models_from_git(&mut graph, project_dir, git_ref, &sql_files);
    graph
}

/// Parse YAML files from a git ref and add source nodes to the graph
fn parse_sources_from_git(
    graph: &mut LineageGraph,
    project_dir: &Path,
    git_ref: &str,
    yaml_files: &[String],
) {
    for yaml_path in yaml_files {
        if let Ok(content) = git::git_show(project_dir, git_ref, yaml_path) {
            if let Ok(schema) = crate::parser::yaml_schema::parse_schema_file(&content) {
                for source_def in &schema.sources {
                    for table in &source_def.tables {
                        let unique_id = format!("source.{}.{}", source_def.name, table.name);
                        let label = format!("{}.{}", source_def.name, table.name);
                        graph.add_node(NodeData {
                            unique_id,
                            label,
                            node_type: NodeType::Source,
                            file_path: Some(yaml_path.into()),
                            description: table.description.clone(),
                            materialization: None,
                            tags: vec![],
                            columns: vec![],
                        });
                    }
                }
            }
        }
    }
}

/// Parse SQL files from a git ref and add model nodes to the graph
fn parse_models_from_git(
    graph: &mut LineageGraph,
    project_dir: &Path,
    git_ref: &str,
    sql_files: &[String],
) {
    for sql_path in sql_files {
        if let Ok(content) = git::git_show(project_dir, git_ref, sql_path) {
            let model_name = std::path::Path::new(sql_path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();
            let unique_id = format!("model.{}", model_name);
            let config = crate::parser::sql::extract_config(&content);
            let columns = crate::parser::columns::extract_select_columns(&content);

            graph.add_node(NodeData {
                unique_id,
                label: model_name,
                node_type: NodeType::Model,
                file_path: Some(sql_path.into()),
                description: None,
                materialization: config.materialized,
                tags: config.tags,
                columns,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(
        unique_id: &str,
        label: &str,
        node_type: NodeType,
        materialization: Option<&str>,
    ) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
            materialization: materialization.map(|s| s.to_string()),
            tags: vec![],
            columns: vec![],
        }
    }

    #[test]
    fn test_compute_diff_no_changes() {
        let mut base = LineageGraph::new();
        let a = base.add_node(make_node("model.orders", "orders", NodeType::Model, None));
        let b = base.add_node(make_node("model.stg", "stg", NodeType::Model, None));
        base.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let mut head = LineageGraph::new();
        let a2 = head.add_node(make_node("model.orders", "orders", NodeType::Model, None));
        let b2 = head.add_node(make_node("model.stg", "stg", NodeType::Model, None));
        head.add_edge(
            a2,
            b2,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.nodes_added, 0);
        assert_eq!(diff.summary.nodes_removed, 0);
        assert_eq!(diff.summary.nodes_modified, 0);
        assert_eq!(diff.summary.edges_added, 0);
        assert_eq!(diff.summary.edges_removed, 0);
    }

    #[test]
    fn test_compute_diff_added_node() {
        let base = LineageGraph::new();

        let mut head = LineageGraph::new();
        head.add_node(make_node("model.orders", "orders", NodeType::Model, None));

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.nodes_added, 1);
        assert_eq!(diff.summary.nodes_removed, 0);
        assert_eq!(diff.nodes.len(), 1);
        assert_eq!(diff.nodes[0].status, DiffStatus::Added);
    }

    #[test]
    fn test_compute_diff_removed_node() {
        let mut base = LineageGraph::new();
        base.add_node(make_node("model.orders", "orders", NodeType::Model, None));

        let head = LineageGraph::new();

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.nodes_removed, 1);
        assert_eq!(diff.summary.nodes_added, 0);
        assert_eq!(diff.nodes[0].status, DiffStatus::Removed);
    }

    #[test]
    fn test_compute_diff_modified_node() {
        let mut base = LineageGraph::new();
        base.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some("view"),
        ));

        let mut head = LineageGraph::new();
        head.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some("table"),
        ));

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.nodes_modified, 1);
        let modified = diff
            .nodes
            .iter()
            .find(|n| n.status == DiffStatus::Modified)
            .unwrap();
        assert!(!modified.changes.is_empty());
        assert!(modified.changes[0].contains("materialization"));
    }

    #[test]
    fn test_compute_diff_added_edge() {
        let mut base = LineageGraph::new();
        base.add_node(make_node("model.a", "a", NodeType::Model, None));
        base.add_node(make_node("model.b", "b", NodeType::Model, None));

        let mut head = LineageGraph::new();
        let a = head.add_node(make_node("model.a", "a", NodeType::Model, None));
        let b = head.add_node(make_node("model.b", "b", NodeType::Model, None));
        head.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.edges_added, 1);
        assert_eq!(diff.summary.edges_removed, 0);
    }

    #[test]
    fn test_compute_diff_removed_edge() {
        let mut base = LineageGraph::new();
        let a = base.add_node(make_node("model.a", "a", NodeType::Model, None));
        let b = base.add_node(make_node("model.b", "b", NodeType::Model, None));
        base.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let mut head = LineageGraph::new();
        head.add_node(make_node("model.a", "a", NodeType::Model, None));
        head.add_node(make_node("model.b", "b", NodeType::Model, None));

        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert_eq!(diff.summary.edges_removed, 1);
        assert_eq!(diff.summary.edges_added, 0);
    }

    #[test]
    fn test_detect_node_changes_tags() {
        let base = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec!["nightly".into()],
            columns: vec![],
        };
        let head = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec!["nightly".into(), "daily".into()],
            columns: vec![],
        };
        let changes = detect_node_changes(&base, &head);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].contains("tags"));
    }

    #[test]
    fn test_detect_node_changes_description() {
        let base = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: Some("old".into()),
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        let head = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: Some("new".into()),
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        let changes = detect_node_changes(&base, &head);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].contains("description"));
    }

    #[test]
    fn test_detect_node_changes_none() {
        let node = make_node("model.a", "a", NodeType::Model, Some("view"));
        let changes = detect_node_changes(&node, &node);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_diff_status_labels() {
        assert_eq!(DiffStatus::Added.label(), "added");
        assert_eq!(DiffStatus::Removed.label(), "removed");
        assert_eq!(DiffStatus::Modified.label(), "modified");
        assert_eq!(DiffStatus::Unchanged.label(), "unchanged");
    }

    #[test]
    fn test_compute_diff_sorting() {
        let mut base = LineageGraph::new();
        base.add_node(make_node("model.removed", "removed", NodeType::Model, None));
        base.add_node(make_node(
            "model.modified",
            "modified",
            NodeType::Model,
            Some("view"),
        ));
        base.add_node(make_node(
            "model.unchanged",
            "unchanged",
            NodeType::Model,
            None,
        ));

        let mut head = LineageGraph::new();
        head.add_node(make_node("model.added", "added", NodeType::Model, None));
        head.add_node(make_node(
            "model.modified",
            "modified",
            NodeType::Model,
            Some("table"),
        ));
        head.add_node(make_node(
            "model.unchanged",
            "unchanged",
            NodeType::Model,
            None,
        ));

        let diff = compute_diff(&base, &head, "main", "HEAD");

        // Should be sorted: added, modified, removed, unchanged
        assert_eq!(diff.nodes[0].status, DiffStatus::Added);
        assert_eq!(diff.nodes[1].status, DiffStatus::Modified);
        assert_eq!(diff.nodes[2].status, DiffStatus::Removed);
        assert_eq!(diff.nodes[3].status, DiffStatus::Unchanged);
    }

    #[test]
    fn test_compute_diff_empty_graphs() {
        let base = LineageGraph::new();
        let head = LineageGraph::new();
        let diff = compute_diff(&base, &head, "main", "HEAD");
        assert!(diff.nodes.is_empty());
        assert!(diff.edges.is_empty());
        assert_eq!(diff.summary.nodes_added, 0);
    }

    #[test]
    fn test_detect_node_changes_columns() {
        let base = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["col1".into(), "col2".into()],
        };
        let head = NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["col1".into(), "col2".into(), "col3".into()],
        };
        let changes = detect_node_changes(&base, &head);
        assert_eq!(changes, vec!["columns added: col3".to_string()]);
    }

    #[test]
    fn test_edge_type_str_all_variants() {
        assert_eq!(edge_type_str(EdgeType::Ref), "ref");
        assert_eq!(edge_type_str(EdgeType::Source), "source");
        assert_eq!(edge_type_str(EdgeType::Test), "test");
        assert_eq!(edge_type_str(EdgeType::Exposure), "exposure");
    }

    #[test]
    fn test_build_graph_from_ref_sql_fallback() {
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        // Init git repo
        Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&path)
            .output()
            .unwrap();

        // Create model directory with SQL and YAML files
        std::fs::create_dir_all(path.join("models")).unwrap();
        std::fs::write(
            path.join("models/stg_orders.sql"),
            "SELECT order_id, status FROM {{ source('raw', 'orders') }}",
        )
        .unwrap();
        std::fs::write(
            path.join("models/schema.yml"),
            r#"version: 2
sources:
  - name: raw
    tables:
      - name: orders
        description: Raw orders table
"#,
        )
        .unwrap();

        Command::new("git")
            .args(["add", "."])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&path)
            .output()
            .unwrap();

        let result = build_graph_from_ref(&path, "HEAD");
        assert!(result.is_ok());
        let graph = result.unwrap();
        // Should have at least the source node and the model node
        assert!(graph.node_count() >= 2);
    }

    #[test]
    fn test_build_graph_from_ref_empty_repo() {
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&path)
            .output()
            .unwrap();
        std::fs::write(path.join("README.md"), "# test\n").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&path)
            .output()
            .unwrap();

        let result = build_graph_from_ref(&path, "HEAD");
        assert!(result.is_ok());
        let graph = result.unwrap();
        assert_eq!(graph.node_count(), 0);
    }

    #[test]
    fn test_build_graph_from_ref_invalid_ref() {
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&path)
            .output()
            .unwrap();
        std::fs::write(path.join("README.md"), "# test\n").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "init"])
            .current_dir(&path)
            .output()
            .unwrap();

        let result = build_graph_from_ref(&path, "nonexistent_branch_abc123");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_graph_from_ref_with_manifest() {
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&path)
            .output()
            .unwrap();

        std::fs::create_dir_all(path.join("target")).unwrap();
        let manifest = r#"{
            "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json"},
            "nodes": {
                "model.project.orders": {
                    "unique_id": "model.project.orders",
                    "resource_type": "model",
                    "name": "orders",
                    "original_file_path": "models/orders.sql",
                    "description": "",
                    "config": {"materialized": "table", "tags": []},
                    "tags": [],
                    "columns": {},
                    "depends_on": {"nodes": ["source.project.raw.orders"]},
                    "refs": [{"name": "stg_orders", "package": null, "version": null}],
                    "sources": [["raw", "orders"]]
                }
            },
            "sources": {
                "source.project.raw.orders": {
                    "unique_id": "source.project.raw.orders",
                    "resource_type": "source",
                    "name": "orders",
                    "source_name": "raw",
                    "original_file_path": "models/schema.yml",
                    "description": "Raw orders",
                    "columns": {},
                    "tags": []
                }
            },
            "exposures": {},
            "metrics": {},
            "child_map": {},
            "parent_map": {}
        }"#;
        std::fs::write(path.join("target/manifest.json"), manifest).unwrap();

        Command::new("git")
            .args(["add", "."])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "with manifest"])
            .current_dir(&path)
            .output()
            .unwrap();

        let result = build_graph_from_ref(&path, "HEAD");
        assert!(result.is_ok());
        let graph = result.unwrap();
        assert!(graph.node_count() >= 2);
    }

    fn git(dir: &Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .args(["-c", "user.email=t@t", "-c", "user.name=t"])
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {:?}: {:?}", args, out);
    }

    /// Repo with a dbt project on `main`: one source, two models.
    fn dbt_repo() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path();
        std::fs::create_dir_all(p.join("models")).unwrap();
        std::fs::write(p.join("dbt_project.yml"), "name: proj\nversion: '1.0'\n").unwrap();
        std::fs::write(
            p.join("models/schema.yml"),
            "version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\nmodels:\n  - name: stg\n    description: staging\n",
        )
        .unwrap();
        std::fs::write(
            p.join("models/stg.sql"),
            "select * from {{ source('raw', 'orders') }}\n",
        )
        .unwrap();
        std::fs::write(
            p.join("models/mart.sql"),
            "select * from {{ ref('stg') }}\n",
        )
        .unwrap();
        git(p, &["init", "-q", "-b", "main"]);
        git(p, &["add", "."]);
        git(p, &["commit", "-qm", "init"]);
        tmp
    }

    #[test]
    fn test_diff_git_unchanged_working_tree_is_empty() {
        let repo = dbt_repo();
        let diff = diff_git(repo.path(), "main", None).unwrap();
        assert_eq!(diff.summary.nodes_added, 0, "{:?}", diff.nodes);
        assert_eq!(diff.summary.nodes_removed, 0);
        assert_eq!(diff.summary.nodes_modified, 0, "{:?}", diff.nodes);
        assert_eq!(diff.summary.edges_added + diff.summary.edges_removed, 0);
    }

    #[test]
    fn test_diff_git_detects_sql_body_change() {
        let repo = dbt_repo();
        std::fs::write(
            repo.path().join("models/mart.sql"),
            "select * from {{ ref('stg') }} where 1 = 1\n",
        )
        .unwrap();
        let diff = diff_git(repo.path(), "main", None).unwrap();
        assert_eq!(diff.summary.nodes_modified, 1);
        let mart = diff.nodes.iter().find(|n| n.label == "mart").unwrap();
        assert_eq!(mart.status, DiffStatus::Modified);
        assert_eq!(mart.changes, vec!["SQL changed".to_string()]);

        // Whitespace-only edits are not a change.
        std::fs::write(
            repo.path().join("models/mart.sql"),
            "select * from {{ ref('stg') }}   \r\n\n",
        )
        .unwrap();
        let diff = diff_git(repo.path(), "main", None).unwrap();
        assert_eq!(diff.summary.nodes_modified, 0, "{:?}", diff.nodes);
    }

    #[test]
    fn test_diff_git_ignores_head_only_manifest() {
        let repo = dbt_repo();
        // A compiled manifest in the working tree but not at the base ref must
        // not switch only one side to manifest mode.
        std::fs::create_dir_all(repo.path().join("target")).unwrap();
        std::fs::write(
            repo.path().join("target/manifest.json"),
            r#"{"metadata":{},"nodes":{},"sources":{},"exposures":{}}"#,
        )
        .unwrap();
        let diff = diff_git(repo.path(), "main", None).unwrap();
        assert_eq!(diff.summary.nodes_added + diff.summary.nodes_removed, 0);
    }

    #[test]
    fn test_diff_git_reports_removed_column_used_downstream() {
        let repo = dbt_repo();
        let p = repo.path();
        std::fs::write(
            p.join("models/stg.sql"),
            "select id, amount from {{ source('raw', 'orders') }}\n",
        )
        .unwrap();
        std::fs::write(
            p.join("models/mart.sql"),
            "select s.id, s.amount as total from {{ ref('stg') }} s\n",
        )
        .unwrap();
        git(p, &["commit", "-qam", "explicit columns"]);

        // Rename amount -> value in stg; mart.total still reads s.amount.
        std::fs::write(
            p.join("models/stg.sql"),
            "select id, amount as value from {{ source('raw', 'orders') }}\n",
        )
        .unwrap();
        let diff = diff_git(p, "main", None).unwrap();
        let stg = diff.nodes.iter().find(|n| n.label == "stg").unwrap();
        assert!(
            stg.changes.contains(&"columns added: value".to_string()),
            "{:?}",
            stg.changes
        );
        assert!(stg.changes.contains(&"columns removed: amount".to_string()));
        assert_eq!(
            diff.column_impacts,
            vec![ColumnImpact {
                model: "stg".into(),
                column: "amount".into(),
                downstream: vec!["mart.total".into()],
            }]
        );
    }

    #[test]
    fn test_diff_git_between_refs() {
        let repo = dbt_repo();
        git(repo.path(), &["checkout", "-qb", "feature"]);
        std::fs::write(
            repo.path().join("models/new.sql"),
            "select * from {{ ref('mart') }}\n",
        )
        .unwrap();
        git(repo.path(), &["add", "."]);
        git(repo.path(), &["commit", "-qm", "new model"]);
        git(repo.path(), &["checkout", "-q", "main"]);

        let diff = diff_git(repo.path(), "main", Some("feature")).unwrap();
        assert_eq!(diff.head_ref, "feature");
        assert_eq!(diff.summary.nodes_added, 1);
        assert_eq!(diff.summary.nodes_modified, 0, "{:?}", diff.nodes);
        assert_eq!(diff.nodes[0].label, "new");
    }

    #[test]
    fn test_collect_edge_set() {
        let mut g = LineageGraph::new();
        let a = g.add_node(make_node("model.a", "a", NodeType::Model, None));
        let b = g.add_node(make_node("model.b", "b", NodeType::Model, None));
        g.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let edges = collect_edge_set(&g);
        assert_eq!(edges.len(), 1);
        let edge = edges.iter().next().unwrap();
        assert_eq!(edge.source, "model.a");
        assert_eq!(edge.target, "model.b");
        assert_eq!(edge.edge_type, "ref");
    }

    #[test]
    fn test_collect_node_map() {
        let mut g = LineageGraph::new();
        g.add_node(make_node("model.a", "a", NodeType::Model, None));
        g.add_node(make_node("model.b", "b", NodeType::Model, None));

        let map = collect_node_map(&g);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("model.a"));
        assert!(map.contains_key("model.b"));
    }
}
