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

/// Full lineage diff between two refs
#[derive(Debug, Clone, Serialize)]
pub struct LineageDiff {
    pub base_ref: String,
    pub head_ref: String,
    pub summary: DiffSummary,
    pub nodes: Vec<DiffNode>,
    pub edges: Vec<DiffEdge>,
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
        changes.push(format!(
            "columns: {} -> {}",
            base.columns.len(),
            head.columns.len()
        ));
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
    }
}

/// Build a graph from a git ref by reading manifest.json at that ref.
/// Falls back to reading SQL/YAML files if no manifest is available.
pub fn build_graph_from_ref(project_dir: &Path, git_ref: &str) -> Result<LineageGraph> {
    // Try manifest first
    if let Ok(manifest_content) = git::git_show(project_dir, git_ref, "target/manifest.json") {
        let manifest: crate::parser::manifest::Manifest =
            serde_json::from_str(&manifest_content)
                .context("Failed to parse manifest.json from git ref")?;
        return crate::parser::manifest::build_graph_from_parsed_manifest(&manifest);
    }

    // Fallback: enumerate SQL and YAML files from the git tree
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

    // Build a minimal graph from the files we can read
    let mut graph = LineageGraph::new();

    // Parse YAML files for source definitions
    for yaml_path in &yaml_files {
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

    // Parse SQL files for model nodes
    for sql_path in &sql_files {
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

    Ok(graph)
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
        assert_eq!(changes.len(), 1);
        assert!(changes[0].contains("columns"));
        assert!(changes[0].contains("2 -> 3"));
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

        // Invalid ref returns an empty graph (git_ls_tree returns empty for bad refs)
        let result = build_graph_from_ref(&path, "nonexistent_branch_abc123");
        assert!(result.is_ok());
        let graph = result.unwrap();
        assert_eq!(graph.node_count(), 0);
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
