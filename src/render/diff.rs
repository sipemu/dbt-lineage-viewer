use std::io::Write;

use colored::Colorize;

use crate::graph::diff::{DiffStatus, LineageDiff};

/// Render diff report as colored text to stdout
pub fn render_diff_text(diff: &LineageDiff) {
    render_diff_text_to_writer(diff, &mut std::io::stdout().lock());
}

pub fn render_diff_text_to_writer<W: Write>(diff: &LineageDiff, w: &mut W) {
    writeln!(w).unwrap();
    writeln!(
        w,
        "{}",
        format!("Lineage Diff: {} → {}", diff.base_ref, diff.head_ref).bold()
    )
    .unwrap();
    writeln!(w, "{}", "=".repeat(50)).unwrap();
    writeln!(w).unwrap();

    writeln!(w, "{}", "Summary:".bold()).unwrap();
    writeln!(
        w,
        "  Nodes added:    {}",
        format!("{}", diff.summary.nodes_added).green()
    )
    .unwrap();
    writeln!(
        w,
        "  Nodes removed:  {}",
        format!("{}", diff.summary.nodes_removed).red()
    )
    .unwrap();
    writeln!(
        w,
        "  Nodes modified: {}",
        format!("{}", diff.summary.nodes_modified).yellow()
    )
    .unwrap();
    writeln!(
        w,
        "  Edges added:    {}",
        format!("{}", diff.summary.edges_added).green()
    )
    .unwrap();
    writeln!(
        w,
        "  Edges removed:  {}",
        format!("{}", diff.summary.edges_removed).red()
    )
    .unwrap();
    writeln!(w).unwrap();

    // Only show non-unchanged nodes
    let changed_nodes: Vec<_> = diff
        .nodes
        .iter()
        .filter(|n| n.status != DiffStatus::Unchanged)
        .collect();

    if !changed_nodes.is_empty() {
        writeln!(w, "{}", "Changed Nodes:".bold()).unwrap();
        for node in &changed_nodes {
            let (symbol, color) = match node.status {
                DiffStatus::Added => ("+", colored::Color::Green),
                DiffStatus::Removed => ("-", colored::Color::Red),
                DiffStatus::Modified => ("~", colored::Color::Yellow),
                DiffStatus::Unchanged => (" ", colored::Color::White),
            };
            writeln!(
                w,
                "  {} {} ({}) [{}]",
                symbol.color(color),
                node.label.color(color),
                node.node_type,
                node.status.label()
            )
            .unwrap();
            for change in &node.changes {
                writeln!(w, "      {}", change).unwrap();
            }
        }
        writeln!(w).unwrap();
    }

    if !diff.edges.is_empty() {
        writeln!(w, "{}", "Changed Edges:".bold()).unwrap();
        for edge in &diff.edges {
            let (symbol, color) = match edge.status {
                DiffStatus::Added => ("+", colored::Color::Green),
                DiffStatus::Removed => ("-", colored::Color::Red),
                _ => (" ", colored::Color::White),
            };
            writeln!(
                w,
                "  {} {} → {} ({})",
                symbol.color(color),
                edge.source.color(color),
                edge.target.color(color),
                edge.edge_type
            )
            .unwrap();
        }
        writeln!(w).unwrap();
    }
}

/// Render diff report as JSON to stdout
pub fn render_diff_json(diff: &LineageDiff) {
    render_diff_json_to_writer(diff, &mut std::io::stdout().lock());
}

pub fn render_diff_json_to_writer<W: Write>(diff: &LineageDiff, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, diff).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::diff::{DiffEdge, DiffNode, DiffSummary, LineageDiff};

    fn make_diff() -> LineageDiff {
        LineageDiff {
            base_ref: "main".to_string(),
            head_ref: "feature".to_string(),
            summary: DiffSummary {
                nodes_added: 1,
                nodes_removed: 1,
                nodes_modified: 1,
                edges_added: 1,
                edges_removed: 0,
            },
            nodes: vec![
                DiffNode {
                    unique_id: "model.new_model".to_string(),
                    label: "new_model".to_string(),
                    node_type: "model".to_string(),
                    status: DiffStatus::Added,
                    changes: vec![],
                },
                DiffNode {
                    unique_id: "model.orders".to_string(),
                    label: "orders".to_string(),
                    node_type: "model".to_string(),
                    status: DiffStatus::Modified,
                    changes: vec!["materialization: view -> table".to_string()],
                },
                DiffNode {
                    unique_id: "model.old_model".to_string(),
                    label: "old_model".to_string(),
                    node_type: "model".to_string(),
                    status: DiffStatus::Removed,
                    changes: vec![],
                },
            ],
            edges: vec![DiffEdge {
                source: "model.stg_orders".to_string(),
                target: "model.new_model".to_string(),
                edge_type: "ref".to_string(),
                status: DiffStatus::Added,
            }],
        }
    }

    #[test]
    fn test_render_diff_text() {
        let diff = make_diff();
        let mut buf = Vec::new();
        render_diff_text_to_writer(&diff, &mut buf);
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("Lineage Diff: main → feature"));
        assert!(output.contains("Nodes added:"));
        assert!(output.contains("Changed Nodes:"));
        assert!(output.contains("new_model"));
        assert!(output.contains("orders"));
        assert!(output.contains("old_model"));
        assert!(output.contains("Changed Edges:"));
    }

    #[test]
    fn test_render_diff_json() {
        let diff = make_diff();
        let mut buf = Vec::new();
        render_diff_json_to_writer(&diff, &mut buf);
        let output = String::from_utf8(buf).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["base_ref"], "main");
        assert_eq!(parsed["head_ref"], "feature");
        assert_eq!(parsed["summary"]["nodes_added"], 1);
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_render_diff_text_empty() {
        let diff = LineageDiff {
            base_ref: "main".to_string(),
            head_ref: "HEAD".to_string(),
            summary: DiffSummary::default(),
            nodes: vec![],
            edges: vec![],
        };
        let mut buf = Vec::new();
        render_diff_text_to_writer(&diff, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Lineage Diff"));
        assert!(output.contains("Nodes added:"));
    }

    #[test]
    fn test_render_diff_text_with_removed_edges() {
        let diff = LineageDiff {
            base_ref: "main".to_string(),
            head_ref: "feature".to_string(),
            summary: DiffSummary {
                nodes_added: 0,
                nodes_removed: 0,
                nodes_modified: 0,
                edges_added: 1,
                edges_removed: 1,
            },
            nodes: vec![],
            edges: vec![
                DiffEdge {
                    source: "model.a".to_string(),
                    target: "model.b".to_string(),
                    edge_type: "ref".to_string(),
                    status: DiffStatus::Added,
                },
                DiffEdge {
                    source: "model.c".to_string(),
                    target: "model.d".to_string(),
                    edge_type: "ref".to_string(),
                    status: DiffStatus::Removed,
                },
            ],
        };
        let mut buf = Vec::new();
        render_diff_text_to_writer(&diff, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Changed Edges:"));
        assert!(output.contains("model.a"));
        assert!(output.contains("model.b"));
        assert!(output.contains("model.c"));
        assert!(output.contains("model.d"));
        assert!(output.contains("Edges removed:"));
    }

    #[test]
    fn test_render_diff_text_with_change_details() {
        let diff = LineageDiff {
            base_ref: "v1".to_string(),
            head_ref: "v2".to_string(),
            summary: DiffSummary {
                nodes_added: 0,
                nodes_removed: 0,
                nodes_modified: 1,
                edges_added: 0,
                edges_removed: 0,
            },
            nodes: vec![DiffNode {
                unique_id: "model.orders".to_string(),
                label: "orders".to_string(),
                node_type: "model".to_string(),
                status: DiffStatus::Modified,
                changes: vec![
                    "materialization: view -> table".to_string(),
                    "columns: 3 -> 5".to_string(),
                ],
            }],
            edges: vec![],
        };
        let mut buf = Vec::new();
        render_diff_text_to_writer(&diff, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("orders"));
        assert!(output.contains("modified"));
        assert!(output.contains("materialization: view -> table"));
        assert!(output.contains("columns: 3 -> 5"));
    }
}
