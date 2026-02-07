use std::io::Write;

use colored::Colorize;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;

use super::layout::{sugiyama_layout, LayoutResult};

/// Warn if the graph layout is wider than the terminal
#[cfg(not(tarpaulin_include))]
fn warn_if_too_wide(graph: &LineageGraph) {
    if graph.node_count() == 0 {
        return;
    }
    let layout = sugiyama_layout(graph);
    if layout.num_layers == 0 {
        return;
    }
    let col_widths = calculate_column_widths(graph, &layout);
    let col_spacing = 4;
    let total_width: usize =
        col_widths.iter().sum::<usize>() + col_spacing * col_widths.len().saturating_sub(1);
    if let Some((term_width, _)) = term_size() {
        if total_width > term_width {
            eprintln!(
                "Warning: graph width ({}) exceeds terminal width ({}). Consider using --output dot or filtering with -u/-d.",
                total_width, term_width
            );
        }
    }
}

/// Render the lineage graph as ASCII art to stdout
#[cfg(not(tarpaulin_include))]
pub fn render_ascii(graph: &LineageGraph) {
    warn_if_too_wide(graph);
    render_ascii_to_writer(graph, &mut std::io::stdout().lock());
}

/// Compute column x-offsets from column widths and spacing
fn compute_col_offsets(col_widths: &[usize], spacing: usize) -> Vec<usize> {
    let mut offsets = vec![0usize; col_widths.len()];
    for i in 1..col_widths.len() {
        offsets[i] = offsets[i - 1] + col_widths[i - 1] + spacing;
    }
    offsets
}

/// Render a single row of the ASCII layout into a line string
fn render_row(
    graph: &LineageGraph,
    layout: &LayoutResult,
    row: usize,
    col_widths: &[usize],
    col_offsets: &[usize],
) -> String {
    let mut line = String::new();
    let mut cursor = 0;

    for (layer_idx, layer) in layout.layers.iter().enumerate() {
        let col_start = col_offsets[layer_idx];
        let col_width = col_widths[layer_idx];

        // Pad to column start
        while cursor < col_start {
            line.push(' ');
            cursor += 1;
        }

        if row < layer.len() {
            let node = &graph[layer[row]];
            let display = node.display_name();
            let box_str = format!("[ {} ]", display);
            let colored_box = colorize_node(&box_str, node.node_type);

            let padding = col_width.saturating_sub(box_str.len()) / 2;
            for _ in 0..padding {
                line.push(' ');
                cursor += 1;
            }
            line.push_str(&colored_box);
            cursor += box_str.len();

            let remaining = col_start + col_width - cursor;
            for _ in 0..remaining {
                line.push(' ');
                cursor += 1;
            }
        } else {
            for _ in 0..col_width {
                line.push(' ');
                cursor += 1;
            }
        }
    }

    line
}

/// Format a single edge as a display string
fn format_edge_arrow(edge_type: EdgeType) -> &'static str {
    match edge_type {
        EdgeType::Ref => "──ref──>",
        EdgeType::Source => "──src──>",
        EdgeType::Test => "──test─>",
        EdgeType::Exposure => "──exp──>",
    }
}

fn render_ascii_to_writer<W: Write>(graph: &LineageGraph, w: &mut W) {
    if graph.node_count() == 0 {
        writeln!(w, "(empty graph — no nodes to display)").unwrap();
        return;
    }

    let layout = sugiyama_layout(graph);
    if layout.num_layers == 0 {
        return;
    }

    let col_widths = calculate_column_widths(graph, &layout);
    let col_offsets = compute_col_offsets(&col_widths, 4);

    for row in 0..layout.max_layer_width {
        let line = render_row(graph, &layout, row, &col_widths, &col_offsets);
        writeln!(w, "{}", line.trim_end()).unwrap();
    }

    writeln!(w).unwrap();
    writeln!(w, "{}", "Edges:".bold()).unwrap();
    for edge in graph.edge_references() {
        let source = &graph[edge.source()];
        let target = &graph[edge.target()];
        writeln!(
            w,
            "  {} {} {}",
            colorize_node(&source.display_name(), source.node_type),
            format_edge_arrow(edge.weight().edge_type),
            colorize_node(&target.display_name(), target.node_type),
        )
        .unwrap();
    }

    writeln!(w).unwrap();
    print_legend_to_writer(w);
}

/// Calculate the width needed for each column (layer)
fn calculate_column_widths(graph: &LineageGraph, layout: &LayoutResult) -> Vec<usize> {
    layout
        .layers
        .iter()
        .map(|layer| {
            layer
                .iter()
                .map(|&idx| {
                    let node = &graph[idx];
                    // "[ display_name ]" = display_name.len() + 4
                    node.display_name().len() + 4
                })
                .max()
                .unwrap_or(0)
        })
        .collect()
}

/// Apply color to a node string based on its type
fn colorize_node(text: &str, node_type: NodeType) -> String {
    match node_type {
        NodeType::Model => text.blue().bold().to_string(),
        NodeType::Source => text.green().to_string(),
        NodeType::Seed => text.yellow().to_string(),
        NodeType::Snapshot => text.magenta().to_string(),
        NodeType::Test => text.cyan().to_string(),
        NodeType::Exposure => text.red().to_string(),
        NodeType::Phantom => text.white().dimmed().to_string(),
    }
}

fn print_legend_to_writer<W: Write>(w: &mut W) {
    writeln!(w, "{}", "Legend:".bold()).unwrap();
    writeln!(
        w,
        "  {} {} {} {} {} {} {}",
        "model".blue().bold(),
        "source".green(),
        "seed".yellow(),
        "snapshot".magenta(),
        "test".cyan(),
        "exposure".red(),
        "phantom".dimmed(),
    )
    .unwrap();
}

#[cfg(not(tarpaulin_include))]
fn term_size() -> Option<(usize, usize)> {
    // Try to get terminal size from environment
    #[cfg(unix)]
    {
        use std::mem;
        unsafe {
            let mut size: libc_winsize = mem::zeroed();
            if libc_ioctl(1, TIOCGWINSZ, &mut size) == 0 && size.ws_col > 0 {
                return Some((size.ws_col as usize, size.ws_row as usize));
            }
        }
    }

    None
}

#[cfg(not(tarpaulin_include))]
#[cfg(unix)]
#[repr(C)]
struct libc_winsize {
    ws_row: u16,
    ws_col: u16,
    ws_xpixel: u16,
    ws_ypixel: u16,
}

#[cfg(unix)]
const TIOCGWINSZ: u64 = 0x5413;

#[cfg(not(tarpaulin_include))]
#[cfg(unix)]
unsafe fn libc_ioctl(fd: i32, request: u64, arg: *mut libc_winsize) -> i32 {
    extern "C" {
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }
    unsafe { ioctl(fd, request, arg) }
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
        render_ascii_to_writer(graph, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        assert!(output.contains("empty graph"));
    }

    #[test]
    fn test_single_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("orders"));
        assert!(output.contains("Legend:"));
    }

    #[test]
    fn test_edges_section() {
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
        assert!(output.contains("Edges:"));
        // Should contain arrow
        assert!(
            output.contains("──src──>"),
            "Output should contain src arrow: {}",
            output
        );
    }

    #[test]
    fn test_legend() {
        let mut buf = Vec::new();
        print_legend_to_writer(&mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Legend:"));
    }

    #[test]
    fn test_colorize_all_types() {
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
            let result = colorize_node("test", nt);
            // colorize_node always returns a non-empty string
            assert!(!result.is_empty(), "colorize_node failed for {:?}", nt);
        }
    }

    #[test]
    fn test_column_widths() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.short", "short", NodeType::Model));
        let b = graph.add_node(make_node(
            "model.very_long_name",
            "very_long_name",
            NodeType::Model,
        ));
        graph.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let layout = sugiyama_layout(&graph);
        let widths = calculate_column_widths(&graph, &layout);
        // Each column width should be at least label.len() + 4
        assert!(widths[0] >= 9); // "short" + 4
        assert!(widths[1] >= 18); // "very_long_name" + 4
    }

    #[test]
    fn test_two_nodes_with_edge() {
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
        assert!(output.contains("[ a ]"), "Output:\n{}", output);
        assert!(output.contains("[ b ]"), "Output:\n{}", output);
        assert!(output.contains("──ref──>"));
    }

    #[test]
    fn test_format_edge_arrow_all_types() {
        assert_eq!(format_edge_arrow(EdgeType::Ref), "──ref──>");
        assert_eq!(format_edge_arrow(EdgeType::Source), "──src──>");
        assert_eq!(format_edge_arrow(EdgeType::Test), "──test─>");
        assert_eq!(format_edge_arrow(EdgeType::Exposure), "──exp──>");
    }

    #[test]
    fn test_uneven_layers_padding() {
        // Create a graph where layers have different numbers of nodes
        // to cover the else branch in render_row (row >= layer.len())
        let mut graph = LineageGraph::new();
        let src1 = graph.add_node(make_node("source.raw.a", "raw.a", NodeType::Source));
        let src2 = graph.add_node(make_node("source.raw.b", "raw.b", NodeType::Source));
        let model = graph.add_node(make_node("model.combined", "combined", NodeType::Model));
        graph.add_edge(
            src1,
            model,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            src2,
            model,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );

        let output = render_to_string(&graph);
        // First layer has 2 nodes, second has 1 — should render without panic
        assert!(output.contains("raw.a"));
        assert!(output.contains("raw.b"));
        assert!(output.contains("combined"));
        assert!(output.contains("Edges:"));
    }

    #[test]
    fn test_compute_col_offsets() {
        let widths = vec![10, 20, 15];
        let offsets = compute_col_offsets(&widths, 4);
        assert_eq!(offsets, vec![0, 14, 38]);
    }

    #[test]
    fn test_all_edge_arrows_in_output() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.a", "a", NodeType::Model));
        let b = graph.add_node(make_node("model.b", "b", NodeType::Model));
        let t = graph.add_node(make_node("test.t", "t", NodeType::Test));
        let e = graph.add_node(make_node("exposure.e", "e", NodeType::Exposure));
        let s = graph.add_node(make_node("source.raw.s", "raw.s", NodeType::Source));

        graph.add_edge(
            s,
            a,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        graph.add_edge(
            b,
            t,
            EdgeData {
                edge_type: EdgeType::Test,
            },
        );
        graph.add_edge(
            b,
            e,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );

        let output = render_to_string(&graph);
        assert!(output.contains("──src──>"));
        assert!(output.contains("──ref──>"));
        assert!(output.contains("──test─>"));
        assert!(output.contains("──exp──>"));
    }
}
