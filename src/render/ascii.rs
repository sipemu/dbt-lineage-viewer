use colored::Colorize;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;

use super::layout::{sugiyama_layout, LayoutResult};

/// Render the lineage graph as ASCII art to stdout
pub fn render_ascii(graph: &LineageGraph) {
    if graph.node_count() == 0 {
        println!("(empty graph — no nodes to display)");
        return;
    }

    let layout = sugiyama_layout(graph);

    if layout.num_layers == 0 {
        return;
    }

    // Calculate column widths based on node labels
    let col_widths = calculate_column_widths(graph, &layout);
    let col_spacing = 4; // spacing between columns

    // Render layer by layer (left-to-right, so layers are columns)
    // We'll transpose: each layer is a column, each position is a row
    let total_rows = layout.max_layer_width;

    // Pre-compute column x offsets
    let col_offsets: Vec<usize> = {
        let mut offsets = vec![0usize; layout.num_layers];
        for i in 1..layout.num_layers {
            offsets[i] = offsets[i - 1] + col_widths[i - 1] + col_spacing;
        }
        offsets
    };

    let total_width = if layout.num_layers > 0 {
        col_offsets[layout.num_layers - 1] + col_widths[layout.num_layers - 1]
    } else {
        0
    };

    // Check terminal width
    if let Some((term_width, _)) = term_size() {
        if total_width > term_width {
            eprintln!(
                "Warning: graph width ({}) exceeds terminal width ({}). Consider using --output dot or filtering with -u/-d.",
                total_width, term_width
            );
        }
    }

    // Build a 2D grid of strings (row x cols as characters)
    // For simplicity, render line by line

    for row in 0..total_rows {
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

                // Center the box in the column
                let padding = col_width.saturating_sub(box_str.len()) / 2;
                for _ in 0..padding {
                    line.push(' ');
                    cursor += 1;
                }
                line.push_str(&colored_box);
                cursor += box_str.len();

                // Fill remaining column width
                let remaining = col_start + col_width - cursor;
                for _ in 0..remaining {
                    line.push(' ');
                    cursor += 1;
                }
            } else {
                // Empty cell
                for _ in 0..col_width {
                    line.push(' ');
                    cursor += 1;
                }
            }
        }

        println!("{}", line.trim_end());
    }

    // Print edges below the graph as a summary
    println!();
    println!("{}", "Edges:".bold());
    for edge in graph.edge_references() {
        let source = &graph[edge.source()];
        let target = &graph[edge.target()];
        let arrow = match edge.weight().edge_type {
            EdgeType::Ref => "──ref──>",
            EdgeType::Source => "──src──>",
            EdgeType::Test => "──test─>",
            EdgeType::Exposure => "──exp──>",
        };
        println!(
            "  {} {} {}",
            colorize_node(&source.display_name(), source.node_type),
            arrow,
            colorize_node(&target.display_name(), target.node_type),
        );
    }

    // Print legend
    println!();
    print_legend();
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

fn print_legend() {
    println!("{}", "Legend:".bold());
    println!(
        "  {} {} {} {} {} {} {}",
        "model".blue().bold(),
        "source".green(),
        "seed".yellow(),
        "snapshot".magenta(),
        "test".cyan(),
        "exposure".red(),
        "phantom".dimmed(),
    );
}

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

#[cfg(unix)]
unsafe fn libc_ioctl(fd: i32, request: u64, arg: *mut libc_winsize) -> i32 {
    extern "C" {
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }
    unsafe { ioctl(fd, request, arg) }
}
