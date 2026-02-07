use std::io::Write;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;
use crate::render::layout::{sugiyama_layout, LayoutResult};

const NODE_WIDTH: f64 = 160.0;
const NODE_HEIGHT: f64 = 40.0;
const LAYER_SPACING: f64 = 220.0;
const NODE_SPACING: f64 = 60.0;
const PADDING: f64 = 40.0;

fn node_fill(node_type: NodeType) -> &'static str {
    match node_type {
        NodeType::Model => "#4A90D9",
        NodeType::Source => "#27AE60",
        NodeType::Seed => "#F39C12",
        NodeType::Snapshot => "#8E44AD",
        NodeType::Test => "#1ABC9C",
        NodeType::Exposure => "#E74C3C",
        NodeType::Phantom => "#BDC3C7",
    }
}

fn node_font_color(node_type: NodeType) -> &'static str {
    match node_type {
        NodeType::Phantom => "#000000",
        _ => "#ffffff",
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn edge_style(edge_type: EdgeType) -> &'static str {
    match edge_type {
        EdgeType::Ref => "stroke:#555;stroke-width:1.5",
        EdgeType::Source => "stroke:#555;stroke-width:1.5;stroke-dasharray:5,3",
        EdgeType::Test => "stroke:#555;stroke-width:1;stroke-dasharray:2,2",
        EdgeType::Exposure => "stroke:#555;stroke-width:2.5",
    }
}

fn node_center(layer: usize, pos: usize) -> (f64, f64) {
    let x = PADDING + layer as f64 * LAYER_SPACING + NODE_WIDTH / 2.0;
    let y = PADDING + pos as f64 * (NODE_HEIGHT + NODE_SPACING) + NODE_HEIGHT / 2.0;
    (x, y)
}

/// Render SVG to stdout
pub fn render_svg(graph: &LineageGraph) {
    render_svg_to_writer(graph, &mut std::io::stdout().lock());
}

/// Render SVG to a string (used by HTML renderer)
pub fn render_svg_to_string(graph: &LineageGraph) -> String {
    let mut buf = Vec::new();
    render_svg_to_writer(graph, &mut buf);
    String::from_utf8(buf).unwrap()
}

pub fn render_svg_to_writer<W: Write>(graph: &LineageGraph, w: &mut W) {
    let layout = sugiyama_layout(graph);

    let total_width = if layout.num_layers == 0 {
        200.0
    } else {
        PADDING * 2.0 + layout.num_layers as f64 * LAYER_SPACING
    };
    let total_height = if layout.max_layer_width == 0 {
        100.0
    } else {
        PADDING * 2.0 + layout.max_layer_width as f64 * (NODE_HEIGHT + NODE_SPACING)
    };

    writeln!(
        w,
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {} {}" width="{}" height="{}">"#,
        total_width, total_height, total_width, total_height
    )
    .unwrap();

    // Defs for arrowhead marker
    writeln!(w, "  <defs>").unwrap();
    writeln!(
        w,
        r#"    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">"#
    )
    .unwrap();
    writeln!(
        w,
        r##"      <polygon points="0 0, 10 3.5, 0 7" fill="#555" />"##
    )
    .unwrap();
    writeln!(w, "    </marker>").unwrap();
    writeln!(w, "  </defs>").unwrap();

    // Background
    writeln!(
        w,
        r##"  <rect width="100%" height="100%" fill="#1a1a2e" />"##
    )
    .unwrap();

    // Render edges first (behind nodes)
    render_svg_edges(w, graph, &layout);

    // Render nodes
    render_svg_nodes(w, graph, &layout);

    // Legend
    render_svg_legend(w, total_height);

    writeln!(w, "</svg>").unwrap();
}

fn render_svg_edges<W: Write>(w: &mut W, graph: &LineageGraph, layout: &LayoutResult) {
    for edge in graph.edge_references() {
        let source_pos = layout.positions.get(&edge.source());
        let target_pos = layout.positions.get(&edge.target());

        if let (Some(&(sl, sp)), Some(&(tl, tp))) = (source_pos, target_pos) {
            let (sx, sy) = node_center(sl, sp);
            let (tx, ty) = node_center(tl, tp);

            // Start from right edge of source, end at left edge of target
            let x1 = sx + NODE_WIDTH / 2.0;
            let y1 = sy;
            let x2 = tx - NODE_WIDTH / 2.0;
            let y2 = ty;

            let cx1 = x1 + (x2 - x1) * 0.4;
            let cx2 = x1 + (x2 - x1) * 0.6;

            let source_node = &graph[edge.source()];
            let target_node = &graph[edge.target()];
            let style = edge_style(edge.weight().edge_type);

            writeln!(
                w,
                r#"  <path d="M{},{} C{},{} {},{} {},{}" fill="none" style="{}" marker-end="url(#arrowhead)" data-source="{}" data-target="{}" />"#,
                x1, y1, cx1, y1, cx2, y2, x2, y2, style,
                xml_escape(&source_node.unique_id),
                xml_escape(&target_node.unique_id)
            )
            .unwrap();
        }
    }
}

fn render_svg_nodes<W: Write>(w: &mut W, graph: &LineageGraph, layout: &LayoutResult) {
    for idx in graph.node_indices() {
        let Some(&(layer, pos)) = layout.positions.get(&idx) else {
            continue;
        };
        let node = &graph[idx];
        let (cx, cy) = node_center(layer, pos);
        let x = cx - NODE_WIDTH / 2.0;
        let y = cy - NODE_HEIGHT / 2.0;

        let fill = node_fill(node.node_type);
        let font_color = node_font_color(node.node_type);
        let label = xml_escape(&node.display_name());

        writeln!(
            w,
            r#"  <g data-id="{}" class="node">"#,
            xml_escape(&node.unique_id)
        )
        .unwrap();
        writeln!(
            w,
            r#"    <rect x="{}" y="{}" width="{}" height="{}" rx="8" fill="{}" />"#,
            x, y, NODE_WIDTH, NODE_HEIGHT, fill
        )
        .unwrap();
        writeln!(
            w,
            r#"    <text x="{}" y="{}" text-anchor="middle" dominant-baseline="central" fill="{}" font-family="Helvetica,Arial,sans-serif" font-size="12">{}</text>"#,
            cx, cy, font_color, label
        )
        .unwrap();
        writeln!(w, "  </g>").unwrap();
    }
}

fn render_svg_legend<W: Write>(w: &mut W, total_height: f64) {
    let legend_y = total_height - 30.0;
    let types: &[(&str, &str)] = &[
        ("model", "#4A90D9"),
        ("source", "#27AE60"),
        ("seed", "#F39C12"),
        ("snapshot", "#8E44AD"),
        ("test", "#1ABC9C"),
        ("exposure", "#E74C3C"),
        ("phantom", "#BDC3C7"),
    ];

    let mut x = PADDING;
    for (label, color) in types {
        writeln!(
            w,
            r#"  <rect x="{}" y="{}" width="12" height="12" rx="2" fill="{}" />"#,
            x, legend_y, color
        )
        .unwrap();
        writeln!(
            w,
            r##"  <text x="{}" y="{}" fill="#ccc" font-family="Helvetica,Arial,sans-serif" font-size="10">{}</text>"##,
            x + 16.0,
            legend_y + 10.0,
            label
        )
        .unwrap();
        x += 80.0;
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
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn render_to_string(graph: &LineageGraph) -> String {
        let mut buf = Vec::new();
        render_svg_to_writer(graph, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        assert!(output.contains("<svg"));
        assert!(output.contains("</svg>"));
    }

    #[test]
    fn test_single_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("data-id=\"model.orders\""));
        assert!(output.contains(">orders</text>"));
        assert!(output.contains("#4A90D9"));
    }

    #[test]
    fn test_edge_rendering() {
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
        assert!(output.contains("<path"));
        assert!(output.contains("marker-end"));
        assert!(output.contains("data-source=\"source.raw.orders\""));
        assert!(output.contains("data-target=\"model.stg_orders\""));
    }

    #[test]
    fn test_all_node_colors() {
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
            let fill = node_fill(nt);
            assert!(fill.starts_with('#'));
        }
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("a<b>c"), "a&lt;b&gt;c");
        assert_eq!(xml_escape("a&b"), "a&amp;b");
        assert_eq!(xml_escape("a\"b"), "a&quot;b");
    }

    #[test]
    fn test_legend_present() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.a", "a", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains(">model</text>"));
        assert!(output.contains(">source</text>"));
    }

    #[test]
    fn test_render_svg_to_string() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.a", "a", NodeType::Model));
        let s = super::render_svg_to_string(&graph);
        assert!(s.contains("<svg"));
    }
}
