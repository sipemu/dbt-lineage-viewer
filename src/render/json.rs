use std::io::Write;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;

use crate::graph::types::*;

#[derive(Serialize)]
struct JsonGraph {
    nodes: Vec<JsonNode>,
    edges: Vec<JsonEdge>,
}

#[derive(Serialize)]
struct JsonNode {
    unique_id: String,
    label: String,
    node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    materialization: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    columns: Vec<String>,
}

#[derive(Serialize)]
struct JsonEdge {
    source: String,
    target: String,
    edge_type: String,
}

/// Render the lineage graph as JSON to stdout
pub fn render_json(graph: &LineageGraph) {
    render_json_to_writer(graph, &mut std::io::stdout().lock());
}

fn render_json_to_writer<W: Write>(graph: &LineageGraph, w: &mut W) {
    let nodes: Vec<JsonNode> = graph
        .node_indices()
        .map(|idx| {
            let node = &graph[idx];
            JsonNode {
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                node_type: node.node_type.label().to_string(),
                file_path: node.file_path.as_ref().map(|p| p.to_string_lossy().into()),
                description: node.description.clone(),
                materialization: node.materialization.clone(),
                tags: node.tags.clone(),
                columns: node.columns.clone(),
            }
        })
        .collect();

    let edges: Vec<JsonEdge> = graph
        .edge_references()
        .map(|edge| {
            let source = &graph[edge.source()];
            let target = &graph[edge.target()];
            JsonEdge {
                source: source.unique_id.clone(),
                target: target.unique_id.clone(),
                edge_type: edge_type_label(edge.weight().edge_type),
            }
        })
        .collect();

    let json_graph = JsonGraph { nodes, edges };
    serde_json::to_writer_pretty(&mut *w, &json_graph).unwrap();
    writeln!(w).unwrap();
}

fn edge_type_label(edge_type: EdgeType) -> String {
    match edge_type {
        EdgeType::Ref => "ref",
        EdgeType::Source => "source",
        EdgeType::Test => "test",
        EdgeType::Exposure => "exposure",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_node(unique_id: &str, label: &str, node_type: NodeType) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn render_to_string(graph: &LineageGraph) -> String {
        let mut buf = Vec::new();
        render_json_to_writer(graph, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["edges"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_single_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["unique_id"], "model.orders");
        assert_eq!(nodes[0]["label"], "orders");
        assert_eq!(nodes[0]["node_type"], "model");
        assert!(nodes[0].get("file_path").is_none());
        assert!(nodes[0].get("description").is_none());
    }

    #[test]
    fn test_node_with_file_path_and_description() {
        let mut graph = LineageGraph::new();
        graph.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: Some(PathBuf::from("models/orders.sql")),
            description: Some("Orders mart model".into()),
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let output = render_to_string(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        assert_eq!(nodes[0]["file_path"], "models/orders.sql");
        assert_eq!(nodes[0]["description"], "Orders mart model");
    }

    #[test]
    fn test_edges() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
        ));
        let b = graph.add_node(make_node("model.stg_orders", "stg_orders", NodeType::Model));
        graph.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );

        let output = render_to_string(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let edges = parsed["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["source"], "source.raw.orders");
        assert_eq!(edges[0]["target"], "model.stg_orders");
        assert_eq!(edges[0]["edge_type"], "source");
    }

    #[test]
    fn test_all_edge_types() {
        assert_eq!(edge_type_label(EdgeType::Ref), "ref");
        assert_eq!(edge_type_label(EdgeType::Source), "source");
        assert_eq!(edge_type_label(EdgeType::Test), "test");
        assert_eq!(edge_type_label(EdgeType::Exposure), "exposure");
    }

    #[test]
    fn test_all_node_types() {
        let mut graph = LineageGraph::new();
        let types = [
            ("model.a", NodeType::Model, "model"),
            ("source.a.b", NodeType::Source, "source"),
            ("seed.a", NodeType::Seed, "seed"),
            ("snapshot.a", NodeType::Snapshot, "snapshot"),
            ("test.a", NodeType::Test, "test"),
            ("exposure.a", NodeType::Exposure, "exposure"),
            ("model.unknown", NodeType::Phantom, "phantom"),
        ];
        for (id, nt, _) in &types {
            graph.add_node(make_node(id, "a", *nt));
        }
        let output = render_to_string(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let nodes = parsed["nodes"].as_array().unwrap();
        for (i, (_, _, expected_type)) in types.iter().enumerate() {
            assert_eq!(nodes[i]["node_type"], *expected_type);
        }
    }

    #[test]
    fn test_valid_json() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.a", "a", NodeType::Model));
        let b = graph.add_node(make_node("model.b", "b", NodeType::Model));
        graph.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let output = render_to_string(&graph);
        // Should parse as valid JSON
        let _: serde_json::Value = serde_json::from_str(&output).unwrap();
    }
}
