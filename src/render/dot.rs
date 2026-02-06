use std::io::Write;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;

/// Render the lineage graph as Graphviz DOT format to stdout
pub fn render_dot(graph: &LineageGraph) {
    render_dot_to_writer(graph, &mut std::io::stdout().lock());
}

fn render_dot_to_writer<W: Write>(graph: &LineageGraph, w: &mut W) {
    writeln!(w, "digraph dbt_lineage {{").unwrap();
    writeln!(w, "  rankdir=LR;").unwrap();
    writeln!(
        w,
        "  node [shape=box, style=filled, fontname=\"Helvetica\"];"
    )
    .unwrap();
    writeln!(w).unwrap();

    // Render nodes
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let (color, fontcolor) = node_colors(node.node_type);
        let label = node.display_name();
        writeln!(
            w,
            "  \"{}\" [label=\"{}\", fillcolor=\"{}\", fontcolor=\"{}\"];",
            node.unique_id, label, color, fontcolor
        )
        .unwrap();
    }

    writeln!(w).unwrap();

    // Render edges
    for edge in graph.edge_references() {
        let source = &graph[edge.source()];
        let target = &graph[edge.target()];
        let style = match edge.weight().edge_type {
            EdgeType::Ref => "",
            EdgeType::Source => ", style=dashed",
            EdgeType::Test => ", style=dotted",
            EdgeType::Exposure => ", style=bold",
        };
        writeln!(
            w,
            "  \"{}\" -> \"{}\" [label=\"{}\"{style}];",
            source.unique_id,
            target.unique_id,
            edge.weight().edge_type_label(),
        )
        .unwrap();
    }

    writeln!(w, "}}").unwrap();
}

impl EdgeData {
    fn edge_type_label(&self) -> &'static str {
        match self.edge_type {
            EdgeType::Ref => "ref",
            EdgeType::Source => "source",
            EdgeType::Test => "test",
            EdgeType::Exposure => "exposure",
        }
    }
}

fn node_colors(node_type: NodeType) -> (&'static str, &'static str) {
    match node_type {
        NodeType::Model => ("#4A90D9", "white"),
        NodeType::Source => ("#27AE60", "white"),
        NodeType::Seed => ("#F39C12", "white"),
        NodeType::Snapshot => ("#8E44AD", "white"),
        NodeType::Test => ("#1ABC9C", "white"),
        NodeType::Exposure => ("#E74C3C", "white"),
        NodeType::Phantom => ("#BDC3C7", "black"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(unique_id: &str, label: &str, node_type: NodeType) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
        }
    }

    fn render_to_string(graph: &LineageGraph) -> String {
        let mut buf = Vec::new();
        render_dot_to_writer(graph, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        assert!(output.contains("digraph dbt_lineage {"));
        assert!(output.contains("}"));
    }

    #[test]
    fn test_single_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("\"model.orders\""));
        assert!(output.contains("label=\"orders\""));
        assert!(output.contains("fillcolor=\"#4A90D9\""));
    }

    #[test]
    fn test_edge_styles() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("source.raw.orders", "raw.orders", NodeType::Source));
        let b = graph.add_node(make_node("model.stg_orders", "stg_orders", NodeType::Model));
        graph.add_edge(a, b, EdgeData { edge_type: EdgeType::Source });

        let output = render_to_string(&graph);
        assert!(output.contains("style=dashed"));
        assert!(output.contains("label=\"source\""));
    }

    #[test]
    fn test_all_edge_type_labels() {
        let types = [
            (EdgeType::Ref, "ref"),
            (EdgeType::Source, "source"),
            (EdgeType::Test, "test"),
            (EdgeType::Exposure, "exposure"),
        ];
        for (et, expected) in types {
            let ed = EdgeData { edge_type: et };
            assert_eq!(ed.edge_type_label(), expected);
        }
    }

    #[test]
    fn test_node_colors_all_types() {
        let types = [
            NodeType::Model,
            NodeType::Source,
            NodeType::Seed,
            NodeType::Snapshot,
            NodeType::Test,
            NodeType::Exposure,
            NodeType::Phantom,
        ];
        for nt in types {
            let (color, fontcolor) = node_colors(nt);
            assert!(color.starts_with('#'), "Color for {:?} should start with #", nt);
            assert!(!fontcolor.is_empty());
        }
    }

    #[test]
    fn test_multiple_edges_different_styles() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.a", "a", NodeType::Model));
        let b = graph.add_node(make_node("model.b", "b", NodeType::Model));
        let c = graph.add_node(make_node("test.t", "t", NodeType::Test));
        let d = graph.add_node(make_node("exposure.e", "e", NodeType::Exposure));

        graph.add_edge(a, b, EdgeData { edge_type: EdgeType::Ref });
        graph.add_edge(b, c, EdgeData { edge_type: EdgeType::Test });
        graph.add_edge(b, d, EdgeData { edge_type: EdgeType::Exposure });

        let output = render_to_string(&graph);
        // Ref edges have no extra style
        assert!(output.contains("label=\"ref\""));
        assert!(output.contains("style=dotted"));
        assert!(output.contains("style=bold"));
    }
}
