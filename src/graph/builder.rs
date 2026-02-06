use anyhow::Result;
use petgraph::stable_graph::NodeIndex;
use std::collections::HashMap;
use std::path::Path;

use crate::parser::discovery::DiscoveredFiles;
use crate::parser::sql::{extract_refs, extract_sources};
use crate::parser::yaml_schema::{parse_schema_file, ExposureDefinition};

use super::types::*;

/// Build the lineage graph from discovered files
pub fn build_graph(project_dir: &Path, files: &DiscoveredFiles) -> Result<LineageGraph> {
    let mut graph = LineageGraph::new();
    // Map from node unique_id to NodeIndex
    let mut node_map: HashMap<String, NodeIndex> = HashMap::new();
    // Track model names to file paths for duplicate detection
    let mut model_name_paths: HashMap<String, std::path::PathBuf> = HashMap::new();

    // 1. Parse YAML files first to get source definitions and model descriptions
    let mut model_descriptions: HashMap<String, String> = HashMap::new();
    let mut exposures: Vec<ExposureDefinition> = Vec::new();

    for yaml_path in &files.yaml_files {
        let content = std::fs::read_to_string(yaml_path).map_err(|e| {
            crate::error::DbtLineageError::FileReadError {
                path: yaml_path.clone(),
                source: e,
            }
        })?;

        let schema = match parse_schema_file(&content) {
            Ok(s) => s,
            Err(_) => continue, // Skip unparseable YAML files
        };

        // Create source nodes
        for source_def in &schema.sources {
            for table in &source_def.tables {
                let unique_id = format!("source.{}.{}", source_def.name, table.name);
                let label = format!("{}.{}", source_def.name, table.name);
                let idx = graph.add_node(NodeData {
                    unique_id: unique_id.clone(),
                    label,
                    node_type: NodeType::Source,
                    file_path: Some(yaml_path.clone()),
                    description: table
                        .description
                        .clone()
                        .or_else(|| source_def.description.clone()),
                });
                node_map.insert(unique_id, idx);
            }
        }

        // Collect model descriptions
        for model_def in &schema.models {
            if let Some(desc) = &model_def.description {
                model_descriptions.insert(model_def.name.clone(), desc.clone());
            }
        }

        // Collect exposures
        exposures.extend(schema.exposures.into_iter());
    }

    // 2. Process model SQL files
    for sql_path in &files.model_sql_files {
        let model_name = sql_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Check for duplicate model names
        if let Some(existing_path) = model_name_paths.get(&model_name) {
            eprintln!(
                "Warning: duplicate model name '{}' in {} and {}",
                model_name,
                existing_path.display(),
                sql_path.display()
            );
        }
        model_name_paths.insert(model_name.clone(), sql_path.clone());

        let unique_id = format!("model.{}", model_name);
        let relative_path = sql_path
            .strip_prefix(project_dir)
            .unwrap_or(sql_path)
            .to_path_buf();

        let idx = graph.add_node(NodeData {
            unique_id: unique_id.clone(),
            label: model_name.clone(),
            node_type: NodeType::Model,
            file_path: Some(relative_path),
            description: model_descriptions.get(&model_name).cloned(),
        });
        node_map.insert(unique_id, idx);
    }

    // 3. Process seed files
    for seed_path in &files.seed_files {
        let seed_name = seed_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let unique_id = format!("seed.{}", seed_name);
        let relative_path = seed_path
            .strip_prefix(project_dir)
            .unwrap_or(seed_path)
            .to_path_buf();

        let idx = graph.add_node(NodeData {
            unique_id: unique_id.clone(),
            label: seed_name,
            node_type: NodeType::Seed,
            file_path: Some(relative_path),
            description: None,
        });
        node_map.insert(unique_id, idx);
    }

    // 4. Process snapshot SQL files
    for sql_path in &files.snapshot_sql_files {
        let snap_name = sql_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let unique_id = format!("snapshot.{}", snap_name);
        let relative_path = sql_path
            .strip_prefix(project_dir)
            .unwrap_or(sql_path)
            .to_path_buf();

        let idx = graph.add_node(NodeData {
            unique_id: unique_id.clone(),
            label: snap_name,
            node_type: NodeType::Snapshot,
            file_path: Some(relative_path),
            description: None,
        });
        node_map.insert(unique_id, idx);
    }

    // 5. Now parse SQL for refs/sources and add edges
    let all_sql_files: Vec<(&std::path::PathBuf, &str)> = files
        .model_sql_files
        .iter()
        .map(|p| (p, "model"))
        .chain(files.snapshot_sql_files.iter().map(|p| (p, "snapshot")))
        .chain(files.test_sql_files.iter().map(|p| (p, "test")))
        .collect();

    for (sql_path, file_type) in &all_sql_files {
        let content = std::fs::read_to_string(sql_path).map_err(|e| {
            crate::error::DbtLineageError::FileReadError {
                path: (*sql_path).clone(),
                source: e,
            }
        })?;

        let node_name = sql_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        let node_unique_id = format!("{}.{}", file_type, node_name);

        // Create test nodes on the fly
        if *file_type == "test" {
            let relative_path = sql_path
                .strip_prefix(project_dir)
                .unwrap_or(sql_path)
                .to_path_buf();

            let idx = graph.add_node(NodeData {
                unique_id: node_unique_id.clone(),
                label: node_name,
                node_type: NodeType::Test,
                file_path: Some(relative_path),
                description: None,
            });
            node_map.insert(node_unique_id.clone(), idx);
        }

        let current_idx = match node_map.get(&node_unique_id) {
            Some(idx) => *idx,
            None => continue,
        };

        // Process ref() calls
        let refs = extract_refs(&content);
        for ref_call in refs {
            let dep_id = resolve_ref(&ref_call.name, &node_map);
            let dep_idx = match node_map.get(&dep_id) {
                Some(idx) => *idx,
                None => {
                    // Create phantom node
                    eprintln!(
                        "Warning: unresolved ref '{}' in {}",
                        ref_call.name,
                        sql_path.display()
                    );
                    let phantom_id = format!("model.{}", ref_call.name);
                    let idx = graph.add_node(NodeData {
                        unique_id: phantom_id.clone(),
                        label: ref_call.name,
                        node_type: NodeType::Phantom,
                        file_path: None,
                        description: None,
                    });
                    node_map.insert(phantom_id, idx);
                    idx
                }
            };
            // Edge: dependency → dependent (data flows downstream)
            graph.add_edge(
                dep_idx,
                current_idx,
                EdgeData {
                    edge_type: EdgeType::Ref,
                },
            );
        }

        // Process source() calls
        let sources = extract_sources(&content);
        for source_call in sources {
            let source_id = format!(
                "source.{}.{}",
                source_call.source_name, source_call.table_name
            );
            let source_idx = match node_map.get(&source_id) {
                Some(idx) => *idx,
                None => {
                    // Create phantom source node
                    eprintln!(
                        "Warning: unresolved source '{}.{}' in {}",
                        source_call.source_name,
                        source_call.table_name,
                        sql_path.display()
                    );
                    let label = format!("{}.{}", source_call.source_name, source_call.table_name);
                    let idx = graph.add_node(NodeData {
                        unique_id: source_id.clone(),
                        label,
                        node_type: NodeType::Phantom,
                        file_path: None,
                        description: None,
                    });
                    node_map.insert(source_id, idx);
                    idx
                }
            };
            graph.add_edge(
                source_idx,
                current_idx,
                EdgeData {
                    edge_type: EdgeType::Source,
                },
            );
        }
    }

    // 6. Process exposures
    for exposure in &exposures {
        let unique_id = format!("exposure.{}", exposure.name);
        let idx = graph.add_node(NodeData {
            unique_id: unique_id.clone(),
            label: exposure.name.clone(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: exposure.description.clone(),
        });
        node_map.insert(unique_id, idx);

        for dep in &exposure.depends_on {
            // Parse ref('name') or source('src', 'table') from depends_on strings
            if let Some(model_name) = parse_exposure_ref(dep) {
                let dep_id = resolve_ref(&model_name, &node_map);
                if let Some(&dep_idx) = node_map.get(&dep_id) {
                    graph.add_edge(
                        dep_idx,
                        idx,
                        EdgeData {
                            edge_type: EdgeType::Exposure,
                        },
                    );
                }
            }
        }
    }

    Ok(graph)
}

/// Try to resolve a ref name to a node unique_id
fn resolve_ref(name: &str, node_map: &HashMap<String, NodeIndex>) -> String {
    // Try model first, then seed, then snapshot
    let model_id = format!("model.{}", name);
    if node_map.contains_key(&model_id) {
        return model_id;
    }

    let seed_id = format!("seed.{}", name);
    if node_map.contains_key(&seed_id) {
        return seed_id;
    }

    let snapshot_id = format!("snapshot.{}", name);
    if node_map.contains_key(&snapshot_id) {
        return snapshot_id;
    }

    // Default to model
    model_id
}

/// Parse a ref('name') or source('src', 'table') string from exposure depends_on
fn parse_exposure_ref(dep: &str) -> Option<String> {
    let dep = dep.trim();
    if dep.starts_with("ref(") {
        // Extract name from ref('name')
        let inner = dep.trim_start_matches("ref(").trim_end_matches(')');
        let name = inner.trim().trim_matches('\'').trim_matches('"');
        Some(name.to_string())
    } else if dep.starts_with("source(") {
        // For sources in exposures, we won't create edges here for simplicity
        None
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::discovery::DiscoveredFiles;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_resolve_ref_model() {
        let mut node_map = HashMap::new();
        let graph = &mut LineageGraph::new();
        let idx = graph.add_node(NodeData {
            unique_id: "model.orders".to_string(),
            label: "orders".to_string(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        });
        node_map.insert("model.orders".to_string(), idx);

        assert_eq!(resolve_ref("orders", &node_map), "model.orders");
    }

    #[test]
    fn test_resolve_ref_seed() {
        let mut node_map = HashMap::new();
        let graph = &mut LineageGraph::new();
        let idx = graph.add_node(NodeData {
            unique_id: "seed.countries".to_string(),
            label: "countries".to_string(),
            node_type: NodeType::Seed,
            file_path: None,
            description: None,
        });
        node_map.insert("seed.countries".to_string(), idx);

        assert_eq!(resolve_ref("countries", &node_map), "seed.countries");
    }

    #[test]
    fn test_resolve_ref_snapshot() {
        let mut node_map = HashMap::new();
        let graph = &mut LineageGraph::new();
        let idx = graph.add_node(NodeData {
            unique_id: "snapshot.snap_orders".to_string(),
            label: "snap_orders".to_string(),
            node_type: NodeType::Snapshot,
            file_path: None,
            description: None,
        });
        node_map.insert("snapshot.snap_orders".to_string(), idx);

        assert_eq!(
            resolve_ref("snap_orders", &node_map),
            "snapshot.snap_orders"
        );
    }

    #[test]
    fn test_resolve_ref_unknown_defaults_to_model() {
        let node_map = HashMap::new();
        assert_eq!(resolve_ref("unknown_ref", &node_map), "model.unknown_ref");
    }

    #[test]
    fn test_parse_exposure_ref() {
        assert_eq!(
            parse_exposure_ref("ref('orders')"),
            Some("orders".to_string())
        );
        assert_eq!(
            parse_exposure_ref("ref(\"orders\")"),
            Some("orders".to_string())
        );
        assert_eq!(parse_exposure_ref("source('raw', 'orders')"), None);
        assert_eq!(parse_exposure_ref("something_else"), None);
    }

    /// Helper to create a temporary dbt project for build_graph tests
    fn setup_temp_project() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path().to_path_buf();

        // Create model files
        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();

        fs::write(
            models_dir.join("stg_orders.sql"),
            "SELECT * FROM {{ source('raw', 'orders') }}",
        )
        .unwrap();

        fs::write(
            models_dir.join("orders.sql"),
            "SELECT * FROM {{ ref('stg_orders') }}",
        )
        .unwrap();

        // Create schema YAML with source definition
        fs::write(
            models_dir.join("schema.yml"),
            r#"
version: 2
sources:
  - name: raw
    tables:
      - name: orders
        description: "Raw orders table"
models:
  - name: stg_orders
    description: "Staged orders"
"#,
        )
        .unwrap();

        (tmp, project_dir)
    }

    #[test]
    fn test_build_graph_sources_and_models() {
        let (_tmp, project_dir) = setup_temp_project();

        let files = DiscoveredFiles {
            model_sql_files: vec![
                project_dir.join("models/stg_orders.sql"),
                project_dir.join("models/orders.sql"),
            ],
            yaml_files: vec![project_dir.join("models/schema.yml")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();

        // Should have source + 2 models = 3 nodes
        assert_eq!(graph.node_count(), 3);

        // Check node types
        let mut types: Vec<NodeType> = graph.node_indices().map(|i| graph[i].node_type).collect();
        types.sort_by_key(|t| format!("{:?}", t));
        assert!(types.contains(&NodeType::Source));
        assert!(types.iter().filter(|t| **t == NodeType::Model).count() == 2);

        // Should have 2 edges: source→stg_orders, stg_orders→orders
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_build_graph_with_seeds() {
        let (_tmp, project_dir) = setup_temp_project();

        // Add a seed
        let seeds_dir = project_dir.join("seeds");
        fs::create_dir_all(&seeds_dir).unwrap();
        fs::write(seeds_dir.join("countries.csv"), "id,name\n1,US\n").unwrap();

        let files = DiscoveredFiles {
            seed_files: vec![project_dir.join("seeds/countries.csv")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        assert_eq!(graph.node_count(), 1);
        let node = &graph[graph.node_indices().next().unwrap()];
        assert_eq!(node.node_type, NodeType::Seed);
        assert_eq!(node.label, "countries");
    }

    #[test]
    fn test_build_graph_with_snapshots() {
        let (_tmp, project_dir) = setup_temp_project();

        let snap_dir = project_dir.join("snapshots");
        fs::create_dir_all(&snap_dir).unwrap();
        fs::write(snap_dir.join("snap_orders.sql"), "SELECT 1").unwrap();

        let files = DiscoveredFiles {
            snapshot_sql_files: vec![project_dir.join("snapshots/snap_orders.sql")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        assert_eq!(graph.node_count(), 1);
        let node = &graph[graph.node_indices().next().unwrap()];
        assert_eq!(node.node_type, NodeType::Snapshot);
        assert_eq!(node.label, "snap_orders");
    }

    #[test]
    fn test_build_graph_with_tests() {
        let (_tmp, project_dir) = setup_temp_project();

        let test_dir = project_dir.join("tests");
        fs::create_dir_all(&test_dir).unwrap();
        fs::write(
            test_dir.join("assert_positive.sql"),
            "SELECT * FROM {{ ref('stg_orders') }} WHERE amount < 0",
        )
        .unwrap();

        // Need the model that the test references
        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("stg_orders.sql"), "SELECT 1").unwrap();

        let files = DiscoveredFiles {
            model_sql_files: vec![project_dir.join("models/stg_orders.sql")],
            test_sql_files: vec![project_dir.join("tests/assert_positive.sql")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        // model + test = 2 nodes
        assert_eq!(graph.node_count(), 2);
        // ref edge: stg_orders → assert_positive
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_build_graph_with_exposures() {
        let (_tmp, project_dir) = setup_temp_project();

        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("orders.sql"), "SELECT 1").unwrap();

        fs::write(
            models_dir.join("schema.yml"),
            r#"
version: 2
sources: []
models: []
exposures:
  - name: weekly_report
    description: "Weekly report dashboard"
    depends_on:
      - ref('orders')
"#,
        )
        .unwrap();

        let files = DiscoveredFiles {
            model_sql_files: vec![project_dir.join("models/orders.sql")],
            yaml_files: vec![project_dir.join("models/schema.yml")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        // model + exposure = 2 nodes
        assert_eq!(graph.node_count(), 2);
        // exposure edge: orders → weekly_report
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_build_graph_phantom_node_for_unresolved_ref() {
        let (_tmp, project_dir) = setup_temp_project();

        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("orders.sql"),
            "SELECT * FROM {{ ref('nonexistent_model') }}",
        )
        .unwrap();

        let files = DiscoveredFiles {
            model_sql_files: vec![project_dir.join("models/orders.sql")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        // model + phantom = 2 nodes
        assert_eq!(graph.node_count(), 2);
        let phantom = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Phantom)
            .expect("Should have a phantom node");
        assert_eq!(graph[phantom].label, "nonexistent_model");
    }

    #[test]
    fn test_build_graph_phantom_node_for_unresolved_source() {
        let (_tmp, project_dir) = setup_temp_project();

        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("orders.sql"),
            "SELECT * FROM {{ source('unknown_src', 'unknown_table') }}",
        )
        .unwrap();

        let files = DiscoveredFiles {
            model_sql_files: vec![project_dir.join("models/orders.sql")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        // model + phantom source = 2 nodes
        assert_eq!(graph.node_count(), 2);
        let phantom = graph
            .node_indices()
            .find(|&i| graph[i].node_type == NodeType::Phantom)
            .expect("Should have a phantom source node");
        assert_eq!(graph[phantom].label, "unknown_src.unknown_table");
    }

    #[test]
    fn test_build_graph_model_descriptions() {
        let (_tmp, project_dir) = setup_temp_project();

        let files = DiscoveredFiles {
            model_sql_files: vec![project_dir.join("models/stg_orders.sql")],
            yaml_files: vec![project_dir.join("models/schema.yml")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        let stg = graph
            .node_indices()
            .find(|&i| graph[i].label == "stg_orders")
            .unwrap();
        assert_eq!(graph[stg].description.as_deref(), Some("Staged orders"));
    }

    #[test]
    fn test_build_graph_edge_types() {
        use petgraph::visit::IntoEdgeReferences;

        let (_tmp, project_dir) = setup_temp_project();

        let files = DiscoveredFiles {
            model_sql_files: vec![
                project_dir.join("models/stg_orders.sql"),
                project_dir.join("models/orders.sql"),
            ],
            yaml_files: vec![project_dir.join("models/schema.yml")],
            ..Default::default()
        };

        let graph = build_graph(&project_dir, &files).unwrap();
        let edge_types: Vec<EdgeType> = graph
            .edge_references()
            .map(|e| e.weight().edge_type)
            .collect();
        assert!(edge_types.contains(&EdgeType::Source));
        assert!(edge_types.contains(&EdgeType::Ref));
    }

    #[test]
    fn test_build_graph_empty_files() {
        let tmp = tempfile::tempdir().unwrap();
        let files = DiscoveredFiles::default();
        let graph = build_graph(tmp.path(), &files).unwrap();
        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
}
