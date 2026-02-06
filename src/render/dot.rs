use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;

/// Render the lineage graph as Graphviz DOT format to stdout
pub fn render_dot(graph: &LineageGraph) {
    println!("digraph dbt_lineage {{");
    println!("  rankdir=LR;");
    println!("  node [shape=box, style=filled, fontname=\"Helvetica\"];");
    println!();

    // Render nodes
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let (color, fontcolor) = node_colors(node.node_type);
        let label = node.display_name();
        println!(
            "  \"{}\" [label=\"{}\", fillcolor=\"{}\", fontcolor=\"{}\"];",
            node.unique_id, label, color, fontcolor
        );
    }

    println!();

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
        println!(
            "  \"{}\" -> \"{}\" [{}];",
            source.unique_id,
            target.unique_id,
            format!("label=\"{}\"{}", edge.weight().edge_type_label(), style),
        );
    }

    println!("}}");
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
