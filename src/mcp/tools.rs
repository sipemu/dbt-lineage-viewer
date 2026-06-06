//! Tool implementations exposed by the MCP server. Each tool takes a JSON-shaped
//! `arguments` object and returns a JSON value that the server wraps in MCP's
//! `{type: "text", text: <serialized json>}` content envelope.

use std::collections::HashSet;
use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;
use serde_json::{json, Value};

use crate::graph::filter::{self, NodeTypeFilter};
use crate::graph::impact;
use crate::graph::summary;
use crate::graph::types::*;

/// Metadata for a single MCP tool — used to populate `tools/list`.
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

/// Static registry of tools the server exposes.
pub fn registry() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "summary",
            description: "Project overview: node counts, tags, top-fan-out models, orphans.",
            input_schema: json!({"type": "object", "properties": {}, "required": []}),
        },
        ToolSpec {
            name: "search_models",
            description: "Find nodes by selector expression (tag:X, path:Y, or model name; comma-separated) and/or node_type.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "selector": {"type": "string", "description": "Optional selector (e.g. 'tag:finance,path:marts')"},
                    "node_type": {
                        "type": "string",
                        "enum": ["model", "source", "seed", "snapshot", "test", "exposure"],
                        "description": "Optional restriction to a single node type"
                    },
                    "limit": {"type": "integer", "default": 100}
                },
                "required": []
            }),
        },
        ToolSpec {
            name: "lineage",
            description: "Upstream and/or downstream lineage for a model. Returns nodes + edges as JSON.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string", "description": "Model name (e.g. 'orders')"},
                    "upstream": {"type": "integer", "description": "Max upstream hops; omit for unlimited"},
                    "downstream": {"type": "integer", "description": "Max downstream hops; omit for unlimited"}
                },
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "impact",
            description: "Downstream impact analysis for a model, with per-node severity classification.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string", "description": "Model name (e.g. 'orders')"}
                },
                "required": ["model"]
            }),
        },
    ]
}

/// Dispatch a tool call by name. Returns the JSON value that becomes the tool result.
pub fn call_tool(graph: &LineageGraph, name: &str, args: &Value) -> Result<Value> {
    match name {
        "summary" => tool_summary(graph, args),
        "search_models" => tool_search_models(graph, args),
        "lineage" => tool_lineage(graph, args),
        "impact" => tool_impact(graph, args),
        _ => Err(anyhow!("unknown tool: {}", name)),
    }
}

fn tool_summary(graph: &LineageGraph, _args: &Value) -> Result<Value> {
    let report = summary::compute_summary(graph, None);
    Ok(serde_json::to_value(report)?)
}

#[derive(Serialize)]
struct SearchHit {
    unique_id: String,
    label: String,
    node_type: String,
    file_path: Option<String>,
    tags: Vec<String>,
    materialization: Option<String>,
}

fn tool_search_models(graph: &LineageGraph, args: &Value) -> Result<Value> {
    let selector_str = args.get("selector").and_then(|v| v.as_str()).unwrap_or("");
    let node_type_str = args.get("node_type").and_then(|v| v.as_str());
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;

    let selectors = if selector_str.is_empty() {
        vec![]
    } else {
        filter::parse_selectors(selector_str)
    };

    let nt_filter = node_type_str.map(parse_node_type).transpose()?;

    let mut hits: Vec<SearchHit> = Vec::new();
    for idx in graph.node_indices() {
        let node = &graph[idx];
        if let Some(nt) = nt_filter {
            if node.node_type != nt {
                continue;
            }
        }
        if !selectors.is_empty() && !selectors_match(node, &selectors) {
            continue;
        }
        hits.push(SearchHit {
            unique_id: node.unique_id.clone(),
            label: node.label.clone(),
            node_type: node.node_type.label().to_string(),
            file_path: node
                .file_path
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            tags: node.tags.clone(),
            materialization: node.materialization.clone(),
        });
        if hits.len() >= limit {
            break;
        }
    }

    Ok(json!({"count": hits.len(), "matches": hits}))
}

fn selectors_match(node: &NodeData, selectors: &[filter::Selector]) -> bool {
    // Union semantics, matching the CLI: any selector matching is a match.
    selectors.iter().any(|sel| match sel {
        filter::Selector::Tag(t) => node.tags.iter().any(|nt| nt == t),
        filter::Selector::Path(p) => node
            .file_path
            .as_ref()
            .is_some_and(|fp| fp.to_string_lossy().contains(p)),
        filter::Selector::ModelName(m) => &node.label == m,
    })
}

fn parse_node_type(s: &str) -> Result<NodeType> {
    match s {
        "model" => Ok(NodeType::Model),
        "source" => Ok(NodeType::Source),
        "seed" => Ok(NodeType::Seed),
        "snapshot" => Ok(NodeType::Snapshot),
        "test" => Ok(NodeType::Test),
        "exposure" => Ok(NodeType::Exposure),
        other => Err(anyhow!("unknown node_type: {}", other)),
    }
}

#[derive(Serialize)]
struct LineageNode {
    unique_id: String,
    label: String,
    node_type: String,
}

#[derive(Serialize)]
struct LineageEdge {
    source: String,
    target: String,
    edge_type: String,
}

fn tool_lineage(graph: &LineageGraph, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let upstream = args
        .get("upstream")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    let downstream = args
        .get("downstream")
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);

    // Use the existing filter; allow all node types so the agent sees the full picture.
    let filtered = filter::filter_graph(
        graph,
        Some(model),
        upstream,
        downstream,
        &NodeTypeFilter {
            include_tests: true,
            include_seeds: true,
            include_snapshots: true,
            include_exposures: true,
        },
        &[],
    )?;

    let nodes: Vec<LineageNode> = filtered
        .node_indices()
        .map(|idx| {
            let n = &filtered[idx];
            LineageNode {
                unique_id: n.unique_id.clone(),
                label: n.label.clone(),
                node_type: n.node_type.label().to_string(),
            }
        })
        .collect();
    let edges: Vec<LineageEdge> = filtered
        .edge_references()
        .map(|e| LineageEdge {
            source: filtered[e.source()].unique_id.clone(),
            target: filtered[e.target()].unique_id.clone(),
            edge_type: match e.weight().edge_type {
                EdgeType::Ref => "ref",
                EdgeType::Source => "source",
                EdgeType::Test => "test",
                EdgeType::Exposure => "exposure",
            }
            .to_string(),
        })
        .collect();

    Ok(json!({"focus": model, "nodes": nodes, "edges": edges}))
}

fn tool_impact(graph: &LineageGraph, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let idx = find_node(graph, model)
        .ok_or_else(|| anyhow!("model '{}' not found in project graph", model))?;
    let report = impact::compute_impact(graph, idx);
    Ok(serde_json::to_value(report)?)
}

/// Locate a node by exact label, falling back to unique_id suffix match.
fn find_node(graph: &LineageGraph, label: &str) -> Option<NodeIndex> {
    if let Some(idx) = graph.node_indices().find(|&idx| graph[idx].label == label) {
        return Some(idx);
    }
    let suffix = format!(".{}", label);
    graph
        .node_indices()
        .find(|&idx| graph[idx].unique_id.ends_with(&suffix))
}

// Currently unused but kept to suppress an "unused VecDeque/HashSet" warning when
// the lineage tool's traversal path is reused; pinned here so future tools can
// reuse the BFS helpers without re-deriving them. Will tree-shake if unused.
#[allow(dead_code)]
fn _unused() -> (VecDeque<()>, HashSet<()>) {
    (VecDeque::new(), HashSet::new())
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

    fn make_graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        let s = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
        ));
        let stg = g.add_node(NodeData {
            tags: vec!["staging".into()],
            ..make_node("model.stg_orders", "stg_orders", NodeType::Model)
        });
        let mart = g.add_node(NodeData {
            tags: vec!["marts".into()],
            ..make_node("model.orders", "orders", NodeType::Model)
        });
        g.add_edge(
            s,
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
        g
    }

    #[test]
    fn test_registry_lists_all_tools() {
        let names: Vec<&str> = registry().iter().map(|t| t.name).collect();
        assert!(names.contains(&"summary"));
        assert!(names.contains(&"search_models"));
        assert!(names.contains(&"lineage"));
        assert!(names.contains(&"impact"));
    }

    #[test]
    fn test_call_summary() {
        let g = make_graph();
        let v = call_tool(&g, "summary", &json!({})).unwrap();
        assert_eq!(v["models"], 2);
        assert_eq!(v["sources"], 1);
    }

    #[test]
    fn test_call_search_by_tag() {
        let g = make_graph();
        let v = call_tool(&g, "search_models", &json!({"selector": "tag:marts"})).unwrap();
        assert_eq!(v["count"], 1);
        assert_eq!(v["matches"][0]["label"], "orders");
    }

    #[test]
    fn test_call_search_by_node_type() {
        let g = make_graph();
        let v = call_tool(&g, "search_models", &json!({"node_type": "source"})).unwrap();
        assert_eq!(v["count"], 1);
        assert_eq!(v["matches"][0]["label"], "raw.orders");
    }

    #[test]
    fn test_call_lineage() {
        let g = make_graph();
        let v = call_tool(&g, "lineage", &json!({"model": "orders"})).unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        assert!(!nodes.is_empty());
        assert_eq!(v["focus"], "orders");
    }

    #[test]
    fn test_call_impact() {
        let g = make_graph();
        let v = call_tool(&g, "impact", &json!({"model": "stg_orders"})).unwrap();
        assert_eq!(v["affected_models"], 1);
    }

    #[test]
    fn test_call_impact_missing_model() {
        let g = make_graph();
        let err = call_tool(&g, "impact", &json!({"model": "nonexistent"})).unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn test_call_unknown_tool() {
        let g = make_graph();
        let err = call_tool(&g, "bogus", &json!({})).unwrap_err();
        assert!(err.to_string().contains("unknown tool"));
    }
}
