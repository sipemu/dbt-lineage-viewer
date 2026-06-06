use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use crate::graph::types::*;
use crate::render::mermaid::{render_mermaid_with_options, MermaidOptions};

/// Generate a Markdown document for a single model.
pub fn render_model_md<W: Write>(graph: &LineageGraph, idx: NodeIndex, w: &mut W) -> Result<()> {
    let node = &graph[idx];

    writeln!(w, "# {}", node.label)?;
    writeln!(w)?;

    // Header line: file path · materialization · tags
    let mut meta_parts: Vec<String> = Vec::new();
    if let Some(p) = &node.file_path {
        meta_parts.push(format!("`{}`", p.display()));
    }
    if let Some(m) = &node.materialization {
        meta_parts.push(format!("materialized=`{}`", m));
    }
    if !node.tags.is_empty() {
        meta_parts.push(format!(
            "tags: {}",
            node.tags
                .iter()
                .map(|t| format!("`{}`", t))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !meta_parts.is_empty() {
        writeln!(w, "{}", meta_parts.join(" · "))?;
        writeln!(w)?;
    }

    if let Some(desc) = node.description.as_deref() {
        if !desc.trim().is_empty() {
            writeln!(w, "{}", desc.trim())?;
            writeln!(w)?;
        }
    }

    let upstream: Vec<&NodeData> = graph
        .edges_directed(idx, Direction::Incoming)
        .map(|e| &graph[e.source()])
        .collect();
    if !upstream.is_empty() {
        writeln!(w, "## Upstream")?;
        writeln!(w)?;
        for n in &upstream {
            writeln!(w, "- `{}` ({})", n.label, n.node_type.label())?;
        }
        writeln!(w)?;
    }

    let downstream: Vec<&NodeData> = graph
        .edges_directed(idx, Direction::Outgoing)
        .map(|e| &graph[e.target()])
        .collect();
    if !downstream.is_empty() {
        writeln!(w, "## Downstream")?;
        writeln!(w)?;
        for n in &downstream {
            writeln!(w, "- `{}` ({})", n.label, n.node_type.label())?;
        }
        writeln!(w)?;
    }

    if !node.columns.is_empty() {
        writeln!(w, "## Columns")?;
        writeln!(w)?;
        writeln!(w, "| Column |")?;
        writeln!(w, "|---|")?;
        for c in &node.columns {
            writeln!(w, "| {} |", c)?;
        }
        writeln!(w)?;
    }

    // Mermaid lineage: 1 hop upstream + 1 hop downstream for a focused view.
    let neighbors = focused_subgraph(graph, idx);
    if neighbors.node_count() > 1 {
        writeln!(w, "## Lineage")?;
        writeln!(w)?;
        writeln!(w, "```mermaid")?;
        let mut buf: Vec<u8> = Vec::new();
        // Capture Mermaid into a buffer using a wrapper since the public API writes to stdout.
        // For simplicity we use the renderer's write to stdout path indirectly via to_writer below.
        crate::render::mermaid::render_mermaid_with_options_to_writer(
            &neighbors,
            MermaidOptions::default(),
            &mut buf,
        );
        let _ = render_mermaid_with_options;
        w.write_all(&buf)?;
        writeln!(w, "```")?;
        writeln!(w)?;
    }

    Ok(())
}

/// Build a small graph containing `idx` plus its direct upstream and downstream
/// neighbors. Used for the Mermaid block in per-model docs.
fn focused_subgraph(graph: &LineageGraph, idx: NodeIndex) -> LineageGraph {
    let mut keep: HashSet<NodeIndex> = HashSet::new();
    keep.insert(idx);
    for e in graph.edges_directed(idx, Direction::Incoming) {
        keep.insert(e.source());
    }
    for e in graph.edges_directed(idx, Direction::Outgoing) {
        keep.insert(e.target());
    }

    let mut new_graph = LineageGraph::new();
    let mut old_to_new = std::collections::HashMap::new();
    for &k in &keep {
        let new_idx = new_graph.add_node(graph[k].clone());
        old_to_new.insert(k, new_idx);
    }
    for &k in &keep {
        for e in graph.edges_directed(k, Direction::Outgoing) {
            if let Some(&tgt) = old_to_new.get(&e.target()) {
                new_graph.add_edge(old_to_new[&k], tgt, e.weight().clone());
            }
        }
    }
    new_graph
}

/// Iterate over all models and call `f` with (label, content). Useful for writing
/// to files or stdout from the caller.
pub fn for_each_model_doc<F>(graph: &LineageGraph, mut f: F) -> Result<()>
where
    F: FnMut(&str, String) -> Result<()>,
{
    for idx in graph.node_indices() {
        if graph[idx].node_type != NodeType::Model {
            continue;
        }
        let mut buf: Vec<u8> = Vec::new();
        render_model_md(graph, idx, &mut buf)?;
        let label = graph[idx].label.clone();
        f(&label, String::from_utf8(buf)?)?;
    }
    Ok(())
}

/// Write per-model Markdown files into `out_dir/<model>.md`. Returns the count.
pub fn write_models_to_dir(graph: &LineageGraph, out_dir: &Path) -> Result<usize> {
    std::fs::create_dir_all(out_dir)?;
    let mut count = 0;
    for_each_model_doc(graph, |label, content| {
        std::fs::write(out_dir.join(format!("{}.md", label)), content)?;
        count += 1;
        Ok(())
    })?;
    Ok(count)
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

    fn build_graph() -> (LineageGraph, NodeIndex) {
        let mut g = LineageGraph::new();
        let src = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
        ));
        let stg = g.add_node(NodeData {
            description: Some("Staging table for raw orders.".into()),
            materialization: Some("view".into()),
            tags: vec!["staging".into()],
            columns: vec!["order_id".into(), "customer_id".into()],
            ..make_node("model.stg_orders", "stg_orders", NodeType::Model)
        });
        let mart = g.add_node(make_node("model.orders", "orders", NodeType::Model));
        g.add_edge(
            src,
            stg,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        g.add_edge(
            stg,
            mart,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        (g, stg)
    }

    #[test]
    fn test_md_has_header_and_sections() {
        let (g, stg) = build_graph();
        let mut buf = Vec::new();
        render_model_md(&g, stg, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("# stg_orders"));
        assert!(s.contains("materialized=`view`"));
        assert!(s.contains("tags: `staging`"));
        assert!(s.contains("Staging table for raw orders."));
        assert!(s.contains("## Upstream"));
        assert!(s.contains("`raw.orders` (source)"));
        assert!(s.contains("## Downstream"));
        assert!(s.contains("`orders` (model)"));
        assert!(s.contains("## Columns"));
        assert!(s.contains("| order_id |"));
        assert!(s.contains("## Lineage"));
        assert!(s.contains("flowchart LR"));
    }

    #[test]
    fn test_for_each_visits_every_model() {
        let (g, _) = build_graph();
        let mut seen: Vec<String> = Vec::new();
        for_each_model_doc(&g, |label, _md| {
            seen.push(label.into());
            Ok(())
        })
        .unwrap();
        seen.sort();
        assert_eq!(seen, vec!["orders", "stg_orders"]);
    }

    #[test]
    fn test_write_models_to_dir_creates_files() {
        let (g, _) = build_graph();
        let tmp = tempfile::tempdir().unwrap();
        let count = write_models_to_dir(&g, tmp.path()).unwrap();
        assert_eq!(count, 2);
        assert!(tmp.path().join("stg_orders.md").exists());
        assert!(tmp.path().join("orders.md").exists());
    }
}
