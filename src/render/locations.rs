//! Map graph nodes to repository-relative file paths, for findings that CI
//! platforms annotate inline (SARIF on GitHub, Code Quality on GitLab). Both
//! need a path relative to the repository root, not to the dbt project.

use std::collections::HashMap;
use std::path::Path;

use petgraph::Direction;

use crate::graph::types::{LineageGraph, NodeType};

/// Node → file lookup. Unknown nodes fall back to `dbt_project.yml`, so every
/// finding still has a location the platform accepts.
#[derive(Debug, Clone)]
pub struct Locations {
    by_id: HashMap<String, String>,
    by_model: HashMap<String, String>,
    fallback: String,
}

impl Default for Locations {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_model: HashMap::new(),
            fallback: "dbt_project.yml".into(),
        }
    }
}

impl Locations {
    /// Build from a graph whose node paths are relative to (or inside)
    /// `project_dir`. `repo_prefix` is the project's path inside the
    /// repository (`git rev-parse --show-prefix`, e.g. `"dbt/"`), or `""`.
    pub fn from_graph(graph: &LineageGraph, project_dir: &Path, repo_prefix: &str) -> Self {
        let to_repo = |p: &Path| -> Option<String> {
            let rel = if p.is_absolute() {
                p.strip_prefix(project_dir).ok()?
            } else {
                p
            };
            let rel = rel.to_string_lossy().replace('\\', "/");
            Some(format!("{}{}", repo_prefix, rel.trim_start_matches("./")))
        };

        let mut loc = Locations {
            fallback: format!("{}dbt_project.yml", repo_prefix),
            ..Default::default()
        };
        for idx in graph.node_indices() {
            let node = &graph[idx];
            if let Some(path) = node.file_path.as_deref().and_then(to_repo) {
                if node.node_type == NodeType::Model {
                    loc.by_model
                        .entry(node.label.clone())
                        .or_insert(path.clone());
                }
                loc.by_id.insert(node.unique_id.clone(), path);
            }
        }
        // Phantom nodes (undefined refs/sources) have no file: point at the
        // first model that references them.
        for idx in graph.node_indices() {
            let node = &graph[idx];
            if node.node_type != NodeType::Phantom || loc.by_id.contains_key(&node.unique_id) {
                continue;
            }
            let mut users: Vec<&String> = graph
                .neighbors_directed(idx, Direction::Outgoing)
                .filter_map(|n| loc.by_id.get(&graph[n].unique_id))
                .collect();
            users.sort();
            if let Some(path) = users.first() {
                loc.by_id.insert(node.unique_id.clone(), (*path).clone());
            }
        }
        loc
    }

    /// File for a node's unique_id.
    pub fn for_id(&self, unique_id: &str) -> &str {
        self.by_id.get(unique_id).unwrap_or(&self.fallback)
    }

    /// File for a model by name.
    pub fn for_model(&self, label: &str) -> &str {
        self.by_model.get(label).unwrap_or(&self.fallback)
    }
}

/// SARIF `locations` array pointing at line 1 of `path`.
pub fn sarif_location(path: &str) -> serde_json::Value {
    serde_json::json!([{
        "physicalLocation": {
            "artifactLocation": {"uri": path, "uriBaseId": "%SRCROOT%"},
            "region": {"startLine": 1}
        }
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{EdgeData, EdgeType, NodeData};
    use std::path::PathBuf;

    fn node(id: &str, label: &str, node_type: NodeType, path: Option<&str>) -> NodeData {
        NodeData {
            unique_id: id.into(),
            label: label.into(),
            node_type,
            file_path: path.map(PathBuf::from),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    #[test]
    fn resolves_relative_absolute_and_phantom_paths() {
        let project = Path::new("/repo/dbt");
        let mut g = LineageGraph::new();
        let ghost = g.add_node(node("phantom.ghost", "ghost", NodeType::Phantom, None));
        let mart = g.add_node(node(
            "model.mart",
            "mart",
            NodeType::Model,
            Some("models/marts/mart.sql"),
        ));
        g.add_node(node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
            Some("/repo/dbt/models/schema.yml"),
        ));
        g.add_edge(
            ghost,
            mart,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let loc = Locations::from_graph(&g, project, "dbt/");
        assert_eq!(loc.for_model("mart"), "dbt/models/marts/mart.sql");
        assert_eq!(loc.for_id("source.raw.orders"), "dbt/models/schema.yml");
        assert_eq!(loc.for_id("phantom.ghost"), "dbt/models/marts/mart.sql");
        assert_eq!(loc.for_id("model.unknown"), "dbt/dbt_project.yml");
        assert_eq!(loc.for_model("unknown"), "dbt/dbt_project.yml");

        let v = sarif_location("dbt/models/marts/mart.sql");
        assert_eq!(
            v[0]["physicalLocation"]["artifactLocation"]["uri"],
            "dbt/models/marts/mart.sql"
        );
    }
}
