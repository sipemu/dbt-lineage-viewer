//! MCP `resources/list` and `resources/read` support. Exposes models, sources,
//! and exposures as URI-addressable content for MCP clients (Claude Code,
//! Claude Desktop) that surface resources as `@`-mention pickers.

use anyhow::{anyhow, Result};

use crate::graph::types::*;
use crate::mcp::server::McpContext;

#[derive(Debug, Clone)]
pub struct ResourceEntry {
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
    pub mime_type: &'static str,
}

#[derive(Debug, Clone)]
pub struct ResourceContent {
    pub text: String,
    pub mime_type: &'static str,
}

/// Build the static resource list from the loaded graph.
pub fn list(ctx: &McpContext) -> Vec<ResourceEntry> {
    let mut out: Vec<ResourceEntry> = Vec::new();

    // Always-fresh project summary as a synthetic resource.
    out.push(ResourceEntry {
        uri: "dbt-lineage://summary".into(),
        name: "Project summary".into(),
        description: Some("Counts, tags, top fan-out models, orphans.".into()),
        mime_type: "application/json",
    });

    for idx in ctx.graph.node_indices() {
        let node = &ctx.graph[idx];
        match node.node_type {
            NodeType::Model | NodeType::Seed | NodeType::Snapshot => {
                out.push(ResourceEntry {
                    uri: format!("dbt-lineage://model/{}/sql", node.label),
                    name: format!("{} SQL", node.label),
                    description: node.description.clone(),
                    mime_type: "text/plain",
                });
                out.push(ResourceEntry {
                    uri: format!("dbt-lineage://model/{}/lineage.mermaid", node.label),
                    name: format!("{} lineage (Mermaid)", node.label),
                    description: Some("Direct upstream + downstream of this model.".into()),
                    mime_type: "text/markdown",
                });
            }
            NodeType::Source => {
                out.push(ResourceEntry {
                    uri: format!("dbt-lineage://source/{}", node.label),
                    name: format!("source: {}", node.label),
                    description: node.description.clone(),
                    mime_type: "application/json",
                });
            }
            NodeType::Exposure => {
                out.push(ResourceEntry {
                    uri: format!("dbt-lineage://exposure/{}", node.label),
                    name: format!("exposure: {}", node.label),
                    description: node.description.clone(),
                    mime_type: "application/json",
                });
            }
            _ => {}
        }
    }
    out
}

/// Resolve a URI to its content.
pub fn read(ctx: &McpContext, uri: &str) -> Result<ResourceContent> {
    let stripped = uri
        .strip_prefix("dbt-lineage://")
        .ok_or_else(|| anyhow!("unsupported URI scheme: {}", uri))?;

    if stripped == "summary" {
        let report = crate::graph::summary::compute_summary(&ctx.graph, None);
        return Ok(ResourceContent {
            text: serde_json::to_string_pretty(&report)?,
            mime_type: "application/json",
        });
    }

    let parts: Vec<&str> = stripped.split('/').collect();
    match parts.as_slice() {
        ["model", label, "sql"] => read_model_sql(ctx, label),
        ["model", label, "lineage.mermaid"] => read_lineage_mermaid(ctx, label),
        ["source", label] => read_node_json(ctx, label, NodeType::Source),
        ["exposure", label] => read_node_json(ctx, label, NodeType::Exposure),
        _ => Err(anyhow!("unknown resource URI: {}", uri)),
    }
}

fn find_node_by_label(ctx: &McpContext, label: &str) -> Option<petgraph::stable_graph::NodeIndex> {
    ctx.graph
        .node_indices()
        .find(|&idx| ctx.graph[idx].label == label)
}

fn read_model_sql(ctx: &McpContext, label: &str) -> Result<ResourceContent> {
    let idx =
        find_node_by_label(ctx, label).ok_or_else(|| anyhow!("model '{}' not found", label))?;
    let node = &ctx.graph[idx];

    if let Some(rel) = node.file_path.as_ref() {
        let abs = if rel.is_absolute() {
            rel.clone()
        } else {
            ctx.project_dir.join(rel)
        };
        if let Ok(text) = std::fs::read_to_string(&abs) {
            return Ok(ResourceContent {
                text,
                mime_type: "text/plain",
            });
        }
    }

    if let Some(sql) = ctx.manifest_sql.get(&node.unique_id) {
        return Ok(ResourceContent {
            text: sql.clone(),
            mime_type: "text/plain",
        });
    }

    Err(anyhow!(
        "no SQL available for model '{}' (no readable file_path, no manifest fallback)",
        label
    ))
}

fn read_lineage_mermaid(ctx: &McpContext, label: &str) -> Result<ResourceContent> {
    use petgraph::visit::EdgeRef;
    let idx =
        find_node_by_label(ctx, label).ok_or_else(|| anyhow!("model '{}' not found", label))?;
    // Build a focused subgraph: the model + direct upstream + direct downstream.
    let mut keep: std::collections::HashSet<petgraph::stable_graph::NodeIndex> =
        std::collections::HashSet::new();
    keep.insert(idx);
    for e in ctx.graph.edges_directed(idx, petgraph::Direction::Incoming) {
        keep.insert(e.source());
    }
    for e in ctx.graph.edges_directed(idx, petgraph::Direction::Outgoing) {
        keep.insert(e.target());
    }
    let mut new_graph = LineageGraph::new();
    let mut map: std::collections::HashMap<
        petgraph::stable_graph::NodeIndex,
        petgraph::stable_graph::NodeIndex,
    > = std::collections::HashMap::new();
    for &k in &keep {
        let new_idx = new_graph.add_node(ctx.graph[k].clone());
        map.insert(k, new_idx);
    }
    for &k in &keep {
        for e in ctx.graph.edges_directed(k, petgraph::Direction::Outgoing) {
            if let Some(&tgt) = map.get(&e.target()) {
                new_graph.add_edge(map[&k], tgt, e.weight().clone());
            }
        }
    }
    let mut buf: Vec<u8> = Vec::new();
    crate::render::mermaid::render_mermaid_with_options_to_writer(
        &new_graph,
        crate::render::mermaid::MermaidOptions::default(),
        &mut buf,
    );
    Ok(ResourceContent {
        text: String::from_utf8(buf).unwrap_or_default(),
        mime_type: "text/markdown",
    })
}

fn read_node_json(ctx: &McpContext, label: &str, expected: NodeType) -> Result<ResourceContent> {
    let idx = find_node_by_label(ctx, label)
        .ok_or_else(|| anyhow!("{} '{}' not found", expected.label(), label))?;
    let node = &ctx.graph[idx];
    if node.node_type != expected {
        return Err(anyhow!(
            "label '{}' resolved to {} (expected {})",
            label,
            node.node_type.label(),
            expected.label()
        ));
    }
    let value = serde_json::json!({
        "unique_id": node.unique_id,
        "label": node.label,
        "node_type": node.node_type.label(),
        "description": node.description,
        "tags": node.tags,
    });
    Ok(ResourceContent {
        text: serde_json::to_string_pretty(&value)?,
        mime_type: "application/json",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn ctx() -> McpContext {
        let mut g = LineageGraph::new();
        let s = g.add_node(NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: Some("Raw orders feed.".into()),
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let m = g.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: Some("table".into()),
            tags: vec![],
            columns: vec![],
        });
        g.add_edge(
            s,
            m,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        McpContext {
            graph: g,
            project_dir: PathBuf::from("/tmp"),
            manifest_sql: std::collections::HashMap::new(),
            column_lineage: crate::parser::column_lineage::ColumnLineage::default(),
        }
    }

    #[test]
    fn test_list_contains_summary_and_model_sql() {
        let entries = list(&ctx());
        let uris: Vec<&str> = entries.iter().map(|e| e.uri.as_str()).collect();
        assert!(uris.contains(&"dbt-lineage://summary"));
        assert!(uris.contains(&"dbt-lineage://model/orders/sql"));
        assert!(uris.contains(&"dbt-lineage://model/orders/lineage.mermaid"));
    }

    #[test]
    fn test_read_summary_returns_json() {
        let c = read(&ctx(), "dbt-lineage://summary").unwrap();
        assert_eq!(c.mime_type, "application/json");
        let v: serde_json::Value = serde_json::from_str(&c.text).unwrap();
        assert_eq!(v["models"], 1);
    }

    #[test]
    fn test_read_source_json() {
        let c = read(&ctx(), "dbt-lineage://source/raw.orders").unwrap();
        let v: serde_json::Value = serde_json::from_str(&c.text).unwrap();
        assert_eq!(v["label"], "raw.orders");
        assert_eq!(v["node_type"], "source");
    }

    #[test]
    fn test_read_lineage_mermaid_renders() {
        let c = read(&ctx(), "dbt-lineage://model/orders/lineage.mermaid").unwrap();
        assert!(c.text.contains("flowchart LR"));
        assert!(c.text.contains("orders"));
    }

    #[test]
    fn test_unsupported_uri_errors() {
        let err = read(&ctx(), "https://example.com/x").unwrap_err();
        assert!(err.to_string().contains("unsupported URI scheme"));
    }

    #[test]
    fn test_unknown_model_errors() {
        let err = read(&ctx(), "dbt-lineage://model/ghost/sql").unwrap_err();
        assert!(err.to_string().contains("ghost"));
    }
}
