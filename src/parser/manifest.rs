use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use petgraph::stable_graph::NodeIndex;
use serde::Deserialize;

use crate::graph::types::*;

/// Top-level manifest.json structure
#[derive(Debug, Deserialize)]
pub struct Manifest {
    /// Nodes keyed by unique_id (models, seeds, snapshots, tests, analyses)
    #[serde(default)]
    pub nodes: HashMap<String, ManifestNode>,
    /// Sources keyed by unique_id
    #[serde(default)]
    pub sources: HashMap<String, ManifestSource>,
    /// Exposures keyed by unique_id
    #[serde(default)]
    pub exposures: HashMap<String, ManifestExposure>,
}

/// A node entry in the manifest (model, seed, snapshot, test, analysis)
#[derive(Debug, Deserialize)]
pub struct ManifestNode {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    #[serde(default)]
    pub depends_on: DependsOn,
    #[serde(default)]
    pub config: ManifestConfig,
    pub description: Option<String>,
    pub path: Option<String>,
}

/// A source entry in the manifest
#[derive(Debug, Deserialize)]
pub struct ManifestSource {
    pub unique_id: String,
    pub name: String,
    pub source_name: String,
    #[serde(default)]
    pub resource_type: String,
    pub description: Option<String>,
    pub path: Option<String>,
}

/// An exposure entry in the manifest
#[derive(Debug, Deserialize)]
pub struct ManifestExposure {
    pub unique_id: String,
    pub name: String,
    #[serde(default)]
    pub depends_on: DependsOn,
    pub description: Option<String>,
}

/// depends_on section with a list of node unique_ids
#[derive(Debug, Default, Deserialize)]
pub struct DependsOn {
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// Config section for nodes
#[derive(Debug, Default, Deserialize)]
pub struct ManifestConfig {
    pub materialized: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Map a manifest resource_type string to our NodeType enum
fn resource_type_to_node_type(resource_type: &str) -> NodeType {
    match resource_type {
        "model" => NodeType::Model,
        "source" => NodeType::Source,
        "seed" => NodeType::Seed,
        "snapshot" => NodeType::Snapshot,
        "test" => NodeType::Test,
        "analysis" => NodeType::Model,
        "exposure" => NodeType::Exposure,
        _ => NodeType::Model,
    }
}

/// Simplify a dbt manifest unique_id (e.g. "model.my_project.stg_orders") to
/// the short form used in this tool's graph (e.g. "model.stg_orders").
/// For sources: "source.my_project.raw.orders" -> "source.raw.orders"
fn simplify_unique_id(unique_id: &str, resource_type: &str) -> String {
    let parts: Vec<&str> = unique_id.split('.').collect();
    match resource_type {
        "source" => {
            // source.project.source_name.table_name -> source.source_name.table_name
            if parts.len() >= 4 {
                format!("{}.{}.{}", parts[0], parts[2], parts[3])
            } else {
                unique_id.to_string()
            }
        }
        _ => {
            // model.project.name -> model.name
            if parts.len() >= 3 {
                format!("{}.{}", parts[0], parts[parts.len() - 1])
            } else {
                unique_id.to_string()
            }
        }
    }
}

/// Build a LineageGraph from a parsed manifest.json file.
pub fn build_graph_from_manifest(manifest_path: &Path) -> Result<LineageGraph> {
    let content = std::fs::read_to_string(manifest_path).map_err(|e| {
        crate::error::DbtLineageError::FileReadError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;

    let manifest: Manifest = serde_json::from_str(&content).map_err(|e| {
        crate::error::DbtLineageError::ArtifactParseError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;

    build_graph_from_parsed_manifest(&manifest)
}

/// Build a LineageGraph from an already-parsed Manifest struct.
/// This is separated for testability and reuse by the diff feature.
pub fn build_graph_from_parsed_manifest(manifest: &Manifest) -> Result<LineageGraph> {
    let mut graph = LineageGraph::new();
    // Map from original manifest unique_id to graph NodeIndex
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();

    // 1. Add source nodes
    add_source_nodes(&mut graph, &mut node_map, &manifest.sources);

    // 2. Add regular nodes (models, seeds, snapshots, tests, analyses)
    add_regular_nodes(&mut graph, &mut node_map, &manifest.nodes);

    // 3. Add exposure nodes
    add_exposure_nodes(&mut graph, &mut node_map, &manifest.exposures);

    // 4. Add edges from depends_on for regular nodes
    add_node_edges(&mut graph, &node_map, &manifest.nodes);

    // 5. Add edges from depends_on for exposures
    add_exposure_edges(&mut graph, &node_map, &manifest.exposures);

    Ok(graph)
}

fn add_source_nodes(
    graph: &mut LineageGraph,
    node_map: &mut HashMap<String, NodeIndex>,
    sources: &HashMap<String, ManifestSource>,
) {
    for (orig_id, source) in sources {
        let simple_id = simplify_unique_id(orig_id, "source");
        let label = format!("{}.{}", source.source_name, source.name);

        let idx = graph.add_node(NodeData {
            unique_id: simple_id.clone(),
            label,
            node_type: NodeType::Source,
            file_path: source.path.as_ref().map(|p| p.into()),
            description: non_empty_string(&source.description),
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        node_map.insert(orig_id.clone(), idx);
        // Also index by simplified id for edge resolution
        node_map.insert(simple_id, idx);
    }
}

fn add_regular_nodes(
    graph: &mut LineageGraph,
    node_map: &mut HashMap<String, NodeIndex>,
    nodes: &HashMap<String, ManifestNode>,
) {
    for (orig_id, node) in nodes {
        let node_type = resource_type_to_node_type(&node.resource_type);
        let simple_id = simplify_unique_id(orig_id, &node.resource_type);

        let idx = graph.add_node(NodeData {
            unique_id: simple_id.clone(),
            label: node.name.clone(),
            node_type,
            file_path: node.path.as_ref().map(|p| p.into()),
            description: non_empty_string(&node.description),
            materialization: node.config.materialized.clone(),
            tags: node.config.tags.clone(),
            columns: vec![],
        });
        node_map.insert(orig_id.clone(), idx);
        node_map.insert(simple_id, idx);
    }
}

fn add_exposure_nodes(
    graph: &mut LineageGraph,
    node_map: &mut HashMap<String, NodeIndex>,
    exposures: &HashMap<String, ManifestExposure>,
) {
    for (orig_id, exposure) in exposures {
        let simple_id = simplify_unique_id(orig_id, "exposure");

        let idx = graph.add_node(NodeData {
            unique_id: simple_id.clone(),
            label: exposure.name.clone(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: non_empty_string(&exposure.description),
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        node_map.insert(orig_id.clone(), idx);
        node_map.insert(simple_id, idx);
    }
}

fn add_node_edges(
    graph: &mut LineageGraph,
    node_map: &HashMap<String, NodeIndex>,
    nodes: &HashMap<String, ManifestNode>,
) {
    for (orig_id, node) in nodes {
        let current_idx = match node_map.get(orig_id) {
            Some(&idx) => idx,
            None => continue,
        };

        for dep_id in &node.depends_on.nodes {
            if let Some(&dep_idx) = node_map.get(dep_id) {
                let edge_type = infer_edge_type(dep_id);
                graph.add_edge(dep_idx, current_idx, EdgeData { edge_type });
            }
        }
    }
}

fn add_exposure_edges(
    graph: &mut LineageGraph,
    node_map: &HashMap<String, NodeIndex>,
    exposures: &HashMap<String, ManifestExposure>,
) {
    for (orig_id, exposure) in exposures {
        let current_idx = match node_map.get(orig_id) {
            Some(&idx) => idx,
            None => continue,
        };

        for dep_id in &exposure.depends_on.nodes {
            if let Some(&dep_idx) = node_map.get(dep_id) {
                graph.add_edge(
                    dep_idx,
                    current_idx,
                    EdgeData {
                        edge_type: EdgeType::Exposure,
                    },
                );
            }
        }
    }
}

/// Infer the edge type from a dependency unique_id
fn infer_edge_type(dep_unique_id: &str) -> EdgeType {
    if dep_unique_id.starts_with("source.") {
        EdgeType::Source
    } else if dep_unique_id.starts_with("test.") {
        EdgeType::Test
    } else {
        EdgeType::Ref
    }
}

/// Return None for empty or whitespace-only strings
fn non_empty_string(s: &Option<String>) -> Option<String> {
    s.as_ref().filter(|v| !v.trim().is_empty()).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_resource_type_to_node_type() {
        assert_eq!(resource_type_to_node_type("model"), NodeType::Model);
        assert_eq!(resource_type_to_node_type("source"), NodeType::Source);
        assert_eq!(resource_type_to_node_type("seed"), NodeType::Seed);
        assert_eq!(resource_type_to_node_type("snapshot"), NodeType::Snapshot);
        assert_eq!(resource_type_to_node_type("test"), NodeType::Test);
        assert_eq!(resource_type_to_node_type("analysis"), NodeType::Model);
        assert_eq!(resource_type_to_node_type("exposure"), NodeType::Exposure);
        assert_eq!(resource_type_to_node_type("unknown"), NodeType::Model);
    }

    #[test]
    fn test_simplify_unique_id_model() {
        assert_eq!(
            simplify_unique_id("model.my_project.stg_orders", "model"),
            "model.stg_orders"
        );
    }

    #[test]
    fn test_simplify_unique_id_source() {
        assert_eq!(
            simplify_unique_id("source.my_project.raw.orders", "source"),
            "source.raw.orders"
        );
    }

    #[test]
    fn test_simplify_unique_id_short() {
        assert_eq!(
            simplify_unique_id("model.stg_orders", "model"),
            "model.stg_orders"
        );
    }

    #[test]
    fn test_simplify_unique_id_source_short() {
        assert_eq!(
            simplify_unique_id("source.raw.orders", "source"),
            "source.raw.orders"
        );
    }

    #[test]
    fn test_infer_edge_type() {
        assert_eq!(
            infer_edge_type("source.my_project.raw.orders"),
            EdgeType::Source
        );
        assert_eq!(
            infer_edge_type("model.my_project.stg_orders"),
            EdgeType::Ref
        );
        assert_eq!(infer_edge_type("test.my_project.some_test"), EdgeType::Test);
        assert_eq!(infer_edge_type("seed.my_project.countries"), EdgeType::Ref);
    }

    #[test]
    fn test_non_empty_string() {
        assert_eq!(non_empty_string(&None), None);
        assert_eq!(non_empty_string(&Some("".to_string())), None);
        assert_eq!(non_empty_string(&Some("  ".to_string())), None);
        assert_eq!(
            non_empty_string(&Some("hello".to_string())),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_build_graph_from_minimal_manifest() {
        let manifest = Manifest {
            nodes: HashMap::from([(
                "model.proj.stg_orders".to_string(),
                ManifestNode {
                    unique_id: "model.proj.stg_orders".to_string(),
                    name: "stg_orders".to_string(),
                    resource_type: "model".to_string(),
                    depends_on: DependsOn {
                        nodes: vec!["source.proj.raw.orders".to_string()],
                    },
                    config: ManifestConfig {
                        materialized: Some("view".to_string()),
                        tags: vec!["staging".to_string()],
                    },
                    description: Some("Staged orders".to_string()),
                    path: Some("models/staging/stg_orders.sql".to_string()),
                },
            )]),
            sources: HashMap::from([(
                "source.proj.raw.orders".to_string(),
                ManifestSource {
                    unique_id: "source.proj.raw.orders".to_string(),
                    name: "orders".to_string(),
                    source_name: "raw".to_string(),
                    resource_type: "source".to_string(),
                    description: Some("Raw orders table".to_string()),
                    path: Some("models/staging/schema.yml".to_string()),
                },
            )]),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();

        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        // Find the model node
        let model = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Model)
            .expect("Should have a model node");
        assert_eq!(graph[model].label, "stg_orders");
        assert_eq!(graph[model].unique_id, "model.stg_orders");
        assert_eq!(graph[model].materialization.as_deref(), Some("view"));
        assert_eq!(graph[model].tags, vec!["staging"]);
        assert_eq!(graph[model].description.as_deref(), Some("Staged orders"));

        // Find the source node
        let source = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Source)
            .expect("Should have a source node");
        assert_eq!(graph[source].label, "raw.orders");
        assert_eq!(graph[source].unique_id, "source.raw.orders");
    }

    #[test]
    fn test_build_graph_with_exposures() {
        let manifest = Manifest {
            nodes: HashMap::from([(
                "model.proj.orders".to_string(),
                ManifestNode {
                    unique_id: "model.proj.orders".to_string(),
                    name: "orders".to_string(),
                    resource_type: "model".to_string(),
                    depends_on: DependsOn::default(),
                    config: ManifestConfig::default(),
                    description: None,
                    path: None,
                },
            )]),
            sources: HashMap::new(),
            exposures: HashMap::from([(
                "exposure.proj.weekly_report".to_string(),
                ManifestExposure {
                    unique_id: "exposure.proj.weekly_report".to_string(),
                    name: "weekly_report".to_string(),
                    depends_on: DependsOn {
                        nodes: vec!["model.proj.orders".to_string()],
                    },
                    description: Some("Weekly dashboard".to_string()),
                },
            )]),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        let exposure = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Exposure)
            .expect("Should have an exposure node");
        assert_eq!(graph[exposure].label, "weekly_report");
        assert_eq!(
            graph[exposure].description.as_deref(),
            Some("Weekly dashboard")
        );
    }

    #[test]
    fn test_build_graph_with_seeds_and_snapshots() {
        let manifest = Manifest {
            nodes: HashMap::from([
                (
                    "seed.proj.countries".to_string(),
                    ManifestNode {
                        unique_id: "seed.proj.countries".to_string(),
                        name: "countries".to_string(),
                        resource_type: "seed".to_string(),
                        depends_on: DependsOn::default(),
                        config: ManifestConfig::default(),
                        description: None,
                        path: Some("seeds/countries.csv".to_string()),
                    },
                ),
                (
                    "snapshot.proj.snap_orders".to_string(),
                    ManifestNode {
                        unique_id: "snapshot.proj.snap_orders".to_string(),
                        name: "snap_orders".to_string(),
                        resource_type: "snapshot".to_string(),
                        depends_on: DependsOn::default(),
                        config: ManifestConfig {
                            materialized: Some("snapshot".to_string()),
                            tags: vec![],
                        },
                        description: None,
                        path: Some("snapshots/snap_orders.sql".to_string()),
                    },
                ),
            ]),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        assert_eq!(graph.node_count(), 2);

        let seed = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Seed)
            .expect("Should have a seed node");
        assert_eq!(graph[seed].label, "countries");

        let snap = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Snapshot)
            .expect("Should have a snapshot node");
        assert_eq!(graph[snap].label, "snap_orders");
    }

    #[test]
    fn test_build_graph_with_tests() {
        let manifest = Manifest {
            nodes: HashMap::from([
                (
                    "model.proj.orders".to_string(),
                    ManifestNode {
                        unique_id: "model.proj.orders".to_string(),
                        name: "orders".to_string(),
                        resource_type: "model".to_string(),
                        depends_on: DependsOn::default(),
                        config: ManifestConfig::default(),
                        description: None,
                        path: None,
                    },
                ),
                (
                    "test.proj.assert_positive".to_string(),
                    ManifestNode {
                        unique_id: "test.proj.assert_positive".to_string(),
                        name: "assert_positive".to_string(),
                        resource_type: "test".to_string(),
                        depends_on: DependsOn {
                            nodes: vec!["model.proj.orders".to_string()],
                        },
                        config: ManifestConfig::default(),
                        description: None,
                        path: Some("tests/assert_positive.sql".to_string()),
                    },
                ),
            ]),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        let test_node = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Test)
            .expect("Should have a test node");
        assert_eq!(graph[test_node].label, "assert_positive");
    }

    #[test]
    fn test_build_graph_empty_manifest() {
        let manifest = Manifest {
            nodes: HashMap::new(),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_build_graph_missing_dependency() {
        // A node depends on something not in the manifest -- edge is skipped gracefully
        let manifest = Manifest {
            nodes: HashMap::from([(
                "model.proj.orders".to_string(),
                ManifestNode {
                    unique_id: "model.proj.orders".to_string(),
                    name: "orders".to_string(),
                    resource_type: "model".to_string(),
                    depends_on: DependsOn {
                        nodes: vec!["model.proj.nonexistent".to_string()],
                    },
                    config: ManifestConfig::default(),
                    description: None,
                    path: None,
                },
            )]),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0); // Edge to nonexistent node is skipped
    }

    #[test]
    fn test_build_graph_optional_fields() {
        let manifest = Manifest {
            nodes: HashMap::from([(
                "model.proj.bare".to_string(),
                ManifestNode {
                    unique_id: "model.proj.bare".to_string(),
                    name: "bare".to_string(),
                    resource_type: "model".to_string(),
                    depends_on: DependsOn::default(),
                    config: ManifestConfig {
                        materialized: None,
                        tags: vec![],
                    },
                    description: None,
                    path: None,
                },
            )]),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        let node = &graph[graph.node_indices().next().unwrap()];
        assert!(node.description.is_none());
        assert!(node.materialization.is_none());
        assert!(node.tags.is_empty());
        assert!(node.file_path.is_none());
    }

    #[test]
    fn test_build_graph_from_manifest_file() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest_path = tmp.path().join("manifest.json");

        let manifest_json = r#"{
            "nodes": {
                "model.proj.stg_orders": {
                    "unique_id": "model.proj.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "depends_on": { "nodes": ["source.proj.raw.orders"] },
                    "config": { "materialized": "view", "tags": [] },
                    "description": "Staged orders",
                    "path": "models/staging/stg_orders.sql"
                }
            },
            "sources": {
                "source.proj.raw.orders": {
                    "unique_id": "source.proj.raw.orders",
                    "name": "orders",
                    "source_name": "raw",
                    "resource_type": "source",
                    "description": "Raw orders",
                    "path": "models/staging/schema.yml"
                }
            },
            "exposures": {}
        }"#;

        fs::write(&manifest_path, manifest_json).unwrap();

        let graph = build_graph_from_manifest(&manifest_path).unwrap();
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_build_graph_from_manifest_file_not_found() {
        let result = build_graph_from_manifest(Path::new("/nonexistent/manifest.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_build_graph_from_manifest_invalid_json() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest_path = tmp.path().join("manifest.json");
        fs::write(&manifest_path, "not valid json").unwrap();

        let result = build_graph_from_manifest(&manifest_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_graph_analysis_maps_to_model() {
        let manifest = Manifest {
            nodes: HashMap::from([(
                "analysis.proj.my_analysis".to_string(),
                ManifestNode {
                    unique_id: "analysis.proj.my_analysis".to_string(),
                    name: "my_analysis".to_string(),
                    resource_type: "analysis".to_string(),
                    depends_on: DependsOn::default(),
                    config: ManifestConfig::default(),
                    description: None,
                    path: None,
                },
            )]),
            sources: HashMap::new(),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        let node = &graph[graph.node_indices().next().unwrap()];
        assert_eq!(node.node_type, NodeType::Model);
    }

    #[test]
    fn test_build_graph_complex_chain() {
        // source -> stg_orders -> orders (with multiple deps)
        let manifest = Manifest {
            nodes: HashMap::from([
                (
                    "model.proj.stg_orders".to_string(),
                    ManifestNode {
                        unique_id: "model.proj.stg_orders".to_string(),
                        name: "stg_orders".to_string(),
                        resource_type: "model".to_string(),
                        depends_on: DependsOn {
                            nodes: vec!["source.proj.raw.orders".to_string()],
                        },
                        config: ManifestConfig {
                            materialized: Some("view".to_string()),
                            tags: vec![],
                        },
                        description: None,
                        path: None,
                    },
                ),
                (
                    "model.proj.stg_payments".to_string(),
                    ManifestNode {
                        unique_id: "model.proj.stg_payments".to_string(),
                        name: "stg_payments".to_string(),
                        resource_type: "model".to_string(),
                        depends_on: DependsOn {
                            nodes: vec!["source.proj.raw.payments".to_string()],
                        },
                        config: ManifestConfig::default(),
                        description: None,
                        path: None,
                    },
                ),
                (
                    "model.proj.orders".to_string(),
                    ManifestNode {
                        unique_id: "model.proj.orders".to_string(),
                        name: "orders".to_string(),
                        resource_type: "model".to_string(),
                        depends_on: DependsOn {
                            nodes: vec![
                                "model.proj.stg_orders".to_string(),
                                "model.proj.stg_payments".to_string(),
                            ],
                        },
                        config: ManifestConfig {
                            materialized: Some("table".to_string()),
                            tags: vec!["marts".to_string()],
                        },
                        description: Some("Order fact table".to_string()),
                        path: None,
                    },
                ),
            ]),
            sources: HashMap::from([
                (
                    "source.proj.raw.orders".to_string(),
                    ManifestSource {
                        unique_id: "source.proj.raw.orders".to_string(),
                        name: "orders".to_string(),
                        source_name: "raw".to_string(),
                        resource_type: "source".to_string(),
                        description: None,
                        path: None,
                    },
                ),
                (
                    "source.proj.raw.payments".to_string(),
                    ManifestSource {
                        unique_id: "source.proj.raw.payments".to_string(),
                        name: "payments".to_string(),
                        source_name: "raw".to_string(),
                        resource_type: "source".to_string(),
                        description: None,
                        path: None,
                    },
                ),
            ]),
            exposures: HashMap::new(),
        };

        let graph = build_graph_from_parsed_manifest(&manifest).unwrap();
        // 2 sources + 3 models = 5 nodes
        assert_eq!(graph.node_count(), 5);
        // source.raw.orders -> stg_orders, source.raw.payments -> stg_payments,
        // stg_orders -> orders, stg_payments -> orders = 4 edges
        assert_eq!(graph.edge_count(), 4);
    }

    #[test]
    fn test_build_graph_from_fixture_manifest() {
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/simple_project/target/manifest.json");

        if !fixture_path.exists() {
            // Skip if fixture not yet created
            return;
        }

        let graph = build_graph_from_manifest(&fixture_path).unwrap();

        // The fixture has: 3 sources, 3 staging models, 2 mart models, 1 seed, 1 test, 1 exposure
        // = 11 nodes total
        assert!(
            graph.node_count() >= 10,
            "Expected at least 10 nodes, got {}",
            graph.node_count()
        );

        // Check we have all node types present
        let has_source = graph
            .node_indices()
            .any(|i| graph[i].node_type == NodeType::Source);
        let has_model = graph
            .node_indices()
            .any(|i| graph[i].node_type == NodeType::Model);
        let has_seed = graph
            .node_indices()
            .any(|i| graph[i].node_type == NodeType::Seed);
        let has_test = graph
            .node_indices()
            .any(|i| graph[i].node_type == NodeType::Test);
        let has_exposure = graph
            .node_indices()
            .any(|i| graph[i].node_type == NodeType::Exposure);

        assert!(has_source, "Should have source nodes");
        assert!(has_model, "Should have model nodes");
        assert!(has_seed, "Should have seed nodes");
        assert!(has_test, "Should have test nodes");
        assert!(has_exposure, "Should have exposure nodes");

        // Check edges exist
        assert!(graph.edge_count() > 0, "Should have edges");
    }
}
