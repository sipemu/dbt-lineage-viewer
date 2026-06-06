use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;

use petgraph::stable_graph::{EdgeIndex, NodeIndex};
use petgraph::visit::{EdgeRef, IntoEdgeReferences};

use crate::graph::types::*;

/// Options controlling Mermaid output.
#[derive(Debug, Clone, Default)]
pub struct MermaidOptions<'a> {
    /// Inline column names inside each node's label.
    pub show_columns: bool,
    /// Group nodes by source-file directory using Mermaid subgraphs.
    pub group_by_directory: bool,
    /// Per-edge intermediate-node counts from collapse. When set, edge labels
    /// are suffixed with `(via N)` for entries with `N > 0`.
    pub via_hops: Option<&'a HashMap<EdgeIndex, usize>>,
}

/// Render the lineage graph as a Mermaid flowchart to stdout with default options.
pub fn render_mermaid(graph: &LineageGraph) {
    render_mermaid_with_options(graph, MermaidOptions::default());
}

/// Render the lineage graph as a Mermaid flowchart to stdout with options.
pub fn render_mermaid_with_options(graph: &LineageGraph, options: MermaidOptions<'_>) {
    render_mermaid_to_writer(graph, &options, &mut std::io::stdout().lock());
}

fn render_mermaid_to_writer<W: Write>(
    graph: &LineageGraph,
    options: &MermaidOptions<'_>,
    w: &mut W,
) {
    writeln!(w, "flowchart LR").unwrap();

    if graph.node_count() == 0 {
        return;
    }

    if options.group_by_directory {
        write_grouped_nodes(graph, options, w);
    } else {
        for idx in graph.node_indices() {
            let node = &graph[idx];
            write_node(node, options, w, "    ");
        }
    }

    writeln!(w).unwrap();

    // Render edges
    for edge in graph.edge_references() {
        let source = &graph[edge.source()];
        let target = &graph[edge.target()];
        let src_id = mermaid_id(&source.unique_id);
        let tgt_id = mermaid_id(&target.unique_id);
        let base = match edge.weight().edge_type {
            EdgeType::Ref => "ref",
            EdgeType::Source => "source",
            EdgeType::Test => "test",
            EdgeType::Exposure => "exposure",
        };
        let arrow_style = match edge.weight().edge_type {
            EdgeType::Ref => "-->",
            EdgeType::Source => "-.->",
            EdgeType::Test => "-.->",
            EdgeType::Exposure => "==>",
        };
        let via = options
            .via_hops
            .and_then(|m| m.get(&edge.id()).copied())
            .filter(|&n| n > 0);
        let label = match via {
            Some(n) => format!("{} (via {})", base, n),
            None => base.to_string(),
        };
        writeln!(w, "    {} {}|{}| {}", src_id, arrow_style, label, tgt_id).unwrap();
    }

    writeln!(w).unwrap();

    // Style classes for node types
    writeln!(w, "    classDef model fill:#4A90D9,stroke:#333,color:#fff").unwrap();
    writeln!(w, "    classDef source fill:#27AE60,stroke:#333,color:#fff").unwrap();
    writeln!(w, "    classDef seed fill:#F39C12,stroke:#333,color:#fff").unwrap();
    writeln!(
        w,
        "    classDef snapshot fill:#8E44AD,stroke:#333,color:#fff"
    )
    .unwrap();
    writeln!(w, "    classDef test fill:#1ABC9C,stroke:#333,color:#fff").unwrap();
    writeln!(
        w,
        "    classDef exposure fill:#E74C3C,stroke:#333,color:#fff"
    )
    .unwrap();
    writeln!(
        w,
        "    classDef phantom fill:#BDC3C7,stroke:#333,color:#000"
    )
    .unwrap();

    // Apply classes
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let id = mermaid_id(&node.unique_id);
        let class = node.node_type.label();
        writeln!(w, "    class {} {}", id, class).unwrap();
    }
}

/// Write the per-node Mermaid declaration at the given indent. Honors `show_columns`.
fn write_node<W: Write>(node: &NodeData, options: &MermaidOptions<'_>, w: &mut W, indent: &str) {
    let id = mermaid_id(&node.unique_id);
    let label = node_label(node, options);
    let shape = match node.node_type {
        NodeType::Model => format!("{}[\"{}\"]\n", id, label),
        NodeType::Source => format!("{}([\"{}\"])\n", id, label),
        NodeType::Seed => format!("{}[/\"{}\"\\]\n", id, label),
        NodeType::Snapshot => format!("{}{{{{\"{}\"}}}}\n", id, label),
        NodeType::Test => format!("{}{{\"{}\"}}\n", id, label),
        NodeType::Exposure => format!("{}>\"{}\"]\n", id, label),
        NodeType::Phantom => format!("{}(\"{}\")\n", id, label),
    };
    write!(w, "{}{}", indent, shape).unwrap();
}

/// Build the label inside a node's shape. When `show_columns` is set and the node has
/// columns, append them after a `<br/>---<br/>` separator.
fn node_label(node: &NodeData, options: &MermaidOptions<'_>) -> String {
    if options.show_columns && !node.columns.is_empty() {
        format!("{}<br/>---<br/>{}", node.label, node.columns.join(", "))
    } else {
        node.label.clone()
    }
}

/// Emit nodes grouped by source-file directory. Nodes without a `file_path` are written
/// at top-level, outside any subgraph (matches the layout used by sources, exposures, etc.).
fn write_grouped_nodes<W: Write>(graph: &LineageGraph, options: &MermaidOptions<'_>, w: &mut W) {
    let mut by_dir: BTreeMap<String, Vec<NodeIndex>> = BTreeMap::new();
    let mut ungrouped: Vec<NodeIndex> = Vec::new();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        match node.file_path.as_deref().and_then(directory_key) {
            Some(dir) => by_dir.entry(dir).or_default().push(idx),
            None => ungrouped.push(idx),
        }
    }

    for (dir, nodes) in &by_dir {
        let sg_id = mermaid_id(dir);
        writeln!(w, "    subgraph {}[\"{}\"]", sg_id, dir).unwrap();
        for &idx in nodes {
            write_node(&graph[idx], options, w, "        ");
        }
        writeln!(w, "    end").unwrap();
    }

    for idx in ungrouped {
        write_node(&graph[idx], options, w, "    ");
    }
}

/// Compute the directory key for a node's `file_path`. Uses parent dir as a forward-slash
/// string. When the path is absolute (e.g., from SQL-parse-mode file discovery), trims
/// everything before the first dbt subdirectory (`models/`, `seeds/`, `snapshots/`,
/// `tests/`, `analyses/`, `macros/`) so the label stays portable across machines.
fn directory_key(path: &Path) -> Option<String> {
    let parent = path.parent()?;
    if parent.as_os_str().is_empty() {
        return None;
    }
    let parts: Vec<String> = parent
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect();
    const DBT_ROOTS: &[&str] = &[
        "models",
        "seeds",
        "snapshots",
        "tests",
        "analyses",
        "macros",
    ];
    // Pick the LAST occurrence; the dbt subdir always sits just above the leaf file,
    // so this is robust against paths whose ancestors happen to share a name (e.g. a
    // monorepo where the project lives under `tests/fixtures/...`).
    let start = parts.iter().rposition(|c| DBT_ROOTS.contains(&c.as_str()));
    let trimmed: Vec<String> = match start {
        Some(i) => parts[i..].to_vec(),
        None => parts,
    };
    let s = trimmed.join("/");
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Convert a unique_id (or any string with dots/slashes) to a valid Mermaid identifier.
fn mermaid_id(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
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
        render_mermaid_to_writer(graph, &MermaidOptions::default(), &mut buf);
        String::from_utf8(buf).unwrap()
    }

    fn render_with(graph: &LineageGraph, options: MermaidOptions<'_>) -> String {
        let mut buf = Vec::new();
        render_mermaid_to_writer(graph, &options, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        assert!(output.contains("flowchart LR"));
        assert!(!output.contains("classDef"));
    }

    #[test]
    fn test_single_model_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("flowchart LR"));
        assert!(output.contains("model_orders[\"orders\"]"));
        assert!(output.contains("class model_orders model"));
    }

    #[test]
    fn test_source_node_shape() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
        ));
        let output = render_to_string(&graph);
        assert!(output.contains("source_raw_orders([\"raw.orders\"])"));
    }

    #[test]
    fn test_edge_styles() {
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
        assert!(output.contains("-.->|source|"));
    }

    #[test]
    fn test_ref_edge() {
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
        assert!(output.contains("-->|ref|"));
    }

    #[test]
    fn test_exposure_edge() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.a", "a", NodeType::Model));
        let b = graph.add_node(make_node("exposure.dash", "dash", NodeType::Exposure));
        graph.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );

        let output = render_to_string(&graph);
        assert!(output.contains("==>|exposure|"));
    }

    #[test]
    fn test_mermaid_id_alphanumeric_only() {
        assert_eq!(mermaid_id("model.orders"), "model_orders");
        assert_eq!(mermaid_id("source.raw.orders"), "source_raw_orders");
        assert_eq!(mermaid_id("models/staging"), "models_staging");
    }

    #[test]
    fn test_test_edge() {
        let mut graph = LineageGraph::new();
        let a = graph.add_node(make_node("model.a", "a", NodeType::Model));
        let t = graph.add_node(make_node("test.t", "t", NodeType::Test));
        graph.add_edge(
            a,
            t,
            EdgeData {
                edge_type: EdgeType::Test,
            },
        );

        let output = render_to_string(&graph);
        assert!(output.contains("-.->|test|"));
    }

    #[test]
    fn test_style_classes() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.a", "a", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("classDef model fill:#4A90D9"));
    }

    #[test]
    fn test_all_node_shapes() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.a", "a", NodeType::Model));
        graph.add_node(make_node("source.a.b", "a.b", NodeType::Source));
        graph.add_node(make_node("seed.a", "a", NodeType::Seed));
        graph.add_node(make_node("snapshot.a", "a", NodeType::Snapshot));
        graph.add_node(make_node("test.a", "a", NodeType::Test));
        graph.add_node(make_node("exposure.a", "a", NodeType::Exposure));
        graph.add_node(make_node("model.unknown", "unknown", NodeType::Phantom));

        let output = render_to_string(&graph);
        assert!(output.contains("model_a[\"a\"]"));
        assert!(output.contains("source_a_b([\"a.b\"])"));
        assert!(output.contains("seed_a[/\"a\"\\]"));
        assert!(output.contains("snapshot_a{{\"a\"}}"));
        assert!(output.contains("test_a{\"a\"}"));
        assert!(output.contains("exposure_a>\"a\"]"));
        assert!(output.contains("model_unknown(\"unknown\")"));
    }

    #[test]
    fn test_show_columns_appends_column_list() {
        let mut graph = LineageGraph::new();
        let n = NodeData {
            columns: vec!["id".into(), "name".into(), "email".into()],
            ..make_node("model.users", "users", NodeType::Model)
        };
        graph.add_node(n);
        let out = render_with(
            &graph,
            MermaidOptions {
                show_columns: true,
                group_by_directory: false,
                via_hops: None,
            },
        );
        assert!(out.contains("users<br/>---<br/>id, name, email"));
    }

    #[test]
    fn test_show_columns_skipped_when_no_columns() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.users", "users", NodeType::Model));
        let out = render_with(
            &graph,
            MermaidOptions {
                show_columns: true,
                group_by_directory: false,
                via_hops: None,
            },
        );
        // Plain label, no separator emitted.
        assert!(out.contains("model_users[\"users\"]"));
        assert!(!out.contains("---"));
    }

    #[test]
    fn test_group_by_directory_emits_subgraphs() {
        let mut graph = LineageGraph::new();
        let stg = NodeData {
            file_path: Some(PathBuf::from("models/staging/stg_orders.sql")),
            ..make_node("model.stg_orders", "stg_orders", NodeType::Model)
        };
        let mart = NodeData {
            file_path: Some(PathBuf::from("models/marts/orders.sql")),
            ..make_node("model.orders", "orders", NodeType::Model)
        };
        let src = make_node("source.raw.orders", "raw.orders", NodeType::Source);
        graph.add_node(stg);
        graph.add_node(mart);
        graph.add_node(src);

        let out = render_with(
            &graph,
            MermaidOptions {
                show_columns: false,
                group_by_directory: true,
                via_hops: None,
            },
        );
        // BTreeMap iteration order is alphabetical by directory.
        assert!(out.contains("subgraph models_marts[\"models/marts\"]"));
        assert!(out.contains("subgraph models_staging[\"models/staging\"]"));
        assert!(out.contains("    end"));
        // Source (no file_path) stays outside any subgraph.
        assert!(out.contains("source_raw_orders([\"raw.orders\"])"));
    }

    #[test]
    fn test_group_by_directory_trims_absolute_prefix() {
        let mut graph = LineageGraph::new();
        let abs = NodeData {
            file_path: Some(PathBuf::from(
                "/abs/path/to/project/models/marts/orders.sql",
            )),
            ..make_node("model.orders", "orders", NodeType::Model)
        };
        graph.add_node(abs);
        let out = render_with(
            &graph,
            MermaidOptions {
                show_columns: false,
                group_by_directory: true,
                via_hops: None,
            },
        );
        // Absolute prefix is trimmed to first dbt root.
        assert!(out.contains("subgraph models_marts[\"models/marts\"]"));
        assert!(!out.contains("/abs/path/to/project"));
    }

    #[test]
    fn test_group_and_show_columns_compose() {
        let mut graph = LineageGraph::new();
        let n = NodeData {
            file_path: Some(PathBuf::from("models/marts/orders.sql")),
            columns: vec!["order_id".into()],
            ..make_node("model.orders", "orders", NodeType::Model)
        };
        graph.add_node(n);
        let out = render_with(
            &graph,
            MermaidOptions {
                show_columns: true,
                group_by_directory: true,
                via_hops: None,
            },
        );
        assert!(out.contains("subgraph models_marts[\"models/marts\"]"));
        assert!(out.contains("orders<br/>---<br/>order_id"));
    }
}
