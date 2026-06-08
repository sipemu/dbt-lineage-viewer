//! Tool implementations exposed by the MCP server.

use anyhow::{anyhow, Result};
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;
use serde_json::{json, Value};

use crate::graph::filter::{self, NodeTypeFilter};
use crate::graph::impact;
use crate::graph::summary;
use crate::graph::types::*;
use crate::mcp::server::McpContext;

pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

pub fn registry() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "summary",
            description: "Project overview: node counts, tags, top fan-out, orphans.",
            input_schema: json!({"type": "object", "properties": {}, "required": []}),
        },
        ToolSpec {
            name: "search_models",
            description: "Find nodes by selector (tag:X, path:Y, model name) and/or node_type.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "selector": {"type": "string"},
                    "node_type": {"type": "string", "enum": ["model","source","seed","snapshot","test","exposure"]},
                    "limit": {"type": "integer", "default": 100}
                }
            }),
        },
        ToolSpec {
            name: "lineage",
            description: "Upstream/downstream lineage for a model. Returns nodes + edges as JSON.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "upstream": {"type": "integer"},
                    "downstream": {"type": "integer"}
                },
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "impact",
            description: "Downstream impact analysis with per-node severity classification.",
            input_schema: json!({
                "type": "object",
                "properties": {"model": {"type": "string"}},
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "get_model_details",
            description: "Full NodeData for a model: description, materialization, tags, columns, upstream + downstream neighbors.",
            input_schema: json!({
                "type": "object",
                "properties": {"model": {"type": "string"}},
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "read_model_sql",
            description: "Read the raw SQL contents of a model (requires SQL-parse mode with file_path on the node).",
            input_schema: json!({
                "type": "object",
                "properties": {"model": {"type": "string"}},
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "column_upstream",
            description: "Trace each output column of a model back to its source columns by walking the lineage graph upstream.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "column": {"type": "string", "description": "Optional; if omitted, returns mapping for all columns."}
                },
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "column_downstream",
            description: "Trace a column forward to every downstream model that references the source model.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "column": {"type": "string"}
                },
                "required": ["model", "column"]
            }),
        },
        ToolSpec {
            name: "lineage_bundle",
            description: "One-shot composition: lineage graph + SQL + descriptions + columns for all nodes within `upstream` / `downstream` hops of `model`.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "upstream": {"type": "integer", "default": 2},
                    "downstream": {"type": "integer", "default": 1}
                },
                "required": ["model"]
            }),
        },
        ToolSpec {
            name: "propose_test",
            description: "Draft a YAML diff adding a generic test (not_null|unique|accepted_values|relationships) to a model's column. The agent emits a diff; the user reviews and applies.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "column": {"type": "string"},
                    "kind": {"type": "string", "enum": ["not_null", "unique", "accepted_values", "relationships"]}
                },
                "required": ["model", "column", "kind"]
            }),
        },
    ]
}

pub fn call_tool(ctx: &McpContext, name: &str, args: &Value) -> Result<Value> {
    match name {
        "summary" => tool_summary(&ctx.graph, args),
        "search_models" => tool_search_models(&ctx.graph, args),
        "lineage" => tool_lineage(&ctx.graph, args),
        "impact" => tool_impact(&ctx.graph, args),
        "get_model_details" => tool_get_model_details(&ctx.graph, args),
        "read_model_sql" => tool_read_model_sql(ctx, args),
        "column_upstream" => tool_column_upstream(ctx, args),
        "column_downstream" => tool_column_downstream(ctx, args),
        "lineage_bundle" => tool_lineage_bundle(ctx, args),
        "propose_test" => tool_propose_test(ctx, args),
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
            edge_type: edge_type_label(e.weight().edge_type).into(),
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

fn tool_get_model_details(graph: &LineageGraph, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let idx = find_node(graph, model).ok_or_else(|| anyhow!("model '{}' not found", model))?;
    let node = &graph[idx];
    let upstream: Vec<String> = graph
        .edges_directed(idx, petgraph::Direction::Incoming)
        .map(|e| graph[e.source()].label.clone())
        .collect();
    let downstream: Vec<String> = graph
        .edges_directed(idx, petgraph::Direction::Outgoing)
        .map(|e| graph[e.target()].label.clone())
        .collect();
    Ok(json!({
        "unique_id": node.unique_id,
        "label": node.label,
        "node_type": node.node_type.label(),
        "file_path": node.file_path.as_ref().map(|p| p.to_string_lossy().to_string()),
        "description": node.description,
        "materialization": node.materialization,
        "tags": node.tags,
        "columns": node.columns,
        "upstream": upstream,
        "downstream": downstream,
    }))
}

fn tool_read_model_sql(ctx: &McpContext, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let idx = find_node(&ctx.graph, model).ok_or_else(|| anyhow!("model '{}' not found", model))?;
    let node = &ctx.graph[idx];

    // Disk first — closer to what the developer sees.
    if let Some(rel) = node.file_path.as_ref() {
        let abs = if rel.is_absolute() {
            rel.clone()
        } else {
            ctx.project_dir.join(rel)
        };
        if let Ok(content) = std::fs::read_to_string(&abs) {
            return Ok(json!({
                "source": "disk",
                "path": abs.to_string_lossy(),
                "content": content,
            }));
        }
    }

    // Manifest fallback. Useful for analysts running from a distributed
    // `manifest.json` with no SQL files on disk.
    if let Some(sql) = ctx.manifest_sql.get(&node.unique_id) {
        return Ok(json!({
            "source": "manifest",
            "content": sql,
        }));
    }

    Err(anyhow!(
        "no SQL available for model '{}' (no readable file_path on disk, no raw_code/compiled_code in manifest)",
        model
    ))
}

/// Column upstream tracing using the precomputed `ColumnLineage` from
/// `parser::column_lineage`. Each emitted hop carries the actual transformation
/// kind (direct / aliased / derived / star) and the source column name even when
/// it differs from the target column. Falls back to the name-matching heuristic
/// for projects where the analyzer can't produce edges (e.g. unparseable SQL).
fn tool_column_upstream(ctx: &McpContext, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let column = args.get("column").and_then(|v| v.as_str());
    let idx = find_node(&ctx.graph, model).ok_or_else(|| anyhow!("model '{}' not found", model))?;
    let focus_unique_id = ctx.graph[idx].unique_id.clone();
    let target_cols: Vec<String> = match column {
        Some(c) => vec![c.to_string()],
        None => ctx.graph[idx].columns.clone(),
    };

    // Real lineage path: BFS through ColumnEdges where target_node==current.
    let mut hops: Vec<Value> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let mut frontier: std::collections::VecDeque<(String, String)> =
        std::collections::VecDeque::new();
    for c in &target_cols {
        frontier.push_back((focus_unique_id.clone(), c.clone()));
        seen.insert((focus_unique_id.clone(), c.clone()));
    }
    while let Some((node, col)) = frontier.pop_front() {
        for edge in ctx
            .column_lineage
            .edges
            .iter()
            .filter(|e| e.target_node == node && e.target_column == col)
        {
            hops.push(json!({
                "from": {"model": edge.source_node, "column": edge.source_column},
                "to": {"model": edge.target_node, "column": edge.target_column},
                "kind": edge.confidence.label().to_lowercase(),
            }));
            let key = (edge.source_node.clone(), edge.source_column.clone());
            if seen.insert(key.clone()) {
                frontier.push_back(key);
            }
        }
    }

    // Heuristic fallback (preserves prior behavior for projects with no parseable SQL).
    let traces = heuristic_upstream(&ctx.graph, idx, &target_cols);

    Ok(json!({
        "focus": {"model": model, "columns": target_cols},
        "hops": hops,
        "traces": traces,
        "source": if hops.is_empty() { "heuristic" } else { "analyzer" },
    }))
}

fn heuristic_upstream(graph: &LineageGraph, idx: NodeIndex, target_cols: &[String]) -> Vec<Value> {
    let mut traces: Vec<Value> = Vec::new();
    let mut visited: std::collections::HashSet<NodeIndex> = std::collections::HashSet::new();
    visited.insert(idx);
    let mut queue: std::collections::VecDeque<NodeIndex> = std::collections::VecDeque::new();
    queue.push_back(idx);
    while let Some(current) = queue.pop_front() {
        for e in graph.edges_directed(current, petgraph::Direction::Incoming) {
            let n = e.source();
            if !visited.insert(n) {
                continue;
            }
            let up = &graph[n];
            for col in target_cols {
                if up.columns.iter().any(|c| c == col) {
                    traces.push(json!({
                        "column": col,
                        "source_model": up.label,
                        "source_unique_id": up.unique_id,
                        "node_type": up.node_type.label(),
                    }));
                }
            }
            queue.push_back(n);
        }
    }
    traces
}

fn tool_column_downstream(ctx: &McpContext, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let column = args
        .get("column")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'column' is required"))?;
    let idx = find_node(&ctx.graph, model).ok_or_else(|| anyhow!("model '{}' not found", model))?;
    let focus_unique_id = ctx.graph[idx].unique_id.clone();

    // Real lineage path: BFS through ColumnEdges where source_node==current.
    let mut hops: Vec<Value> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let mut frontier: std::collections::VecDeque<(String, String)> =
        std::collections::VecDeque::new();
    frontier.push_back((focus_unique_id.clone(), column.to_string()));
    seen.insert((focus_unique_id, column.to_string()));
    while let Some((node, col)) = frontier.pop_front() {
        for edge in ctx
            .column_lineage
            .edges
            .iter()
            .filter(|e| e.source_node == node && e.source_column == col)
        {
            hops.push(json!({
                "from": {"model": edge.source_node, "column": edge.source_column},
                "to": {"model": edge.target_node, "column": edge.target_column},
                "kind": edge.confidence.label().to_lowercase(),
            }));
            let key = (edge.target_node.clone(), edge.target_column.clone());
            if seen.insert(key.clone()) {
                frontier.push_back(key);
            }
        }
    }

    // Heuristic fallback (existing behavior).
    let traces = heuristic_downstream(&ctx.graph, idx, column);

    Ok(json!({
        "focus": {"model": model, "column": column},
        "hops": hops,
        "traces": traces,
        "source": if hops.is_empty() { "heuristic" } else { "analyzer" },
    }))
}

fn heuristic_downstream(graph: &LineageGraph, idx: NodeIndex, column: &str) -> Vec<Value> {
    let mut traces: Vec<Value> = Vec::new();
    let mut visited: std::collections::HashSet<NodeIndex> = std::collections::HashSet::new();
    visited.insert(idx);
    let mut queue: std::collections::VecDeque<NodeIndex> = std::collections::VecDeque::new();
    queue.push_back(idx);
    while let Some(current) = queue.pop_front() {
        for e in graph.edges_directed(current, petgraph::Direction::Outgoing) {
            let n = e.target();
            if !visited.insert(n) {
                continue;
            }
            let down = &graph[n];
            if down.columns.iter().any(|c| c == column) {
                traces.push(json!({
                    "column": column,
                    "target_model": down.label,
                    "target_unique_id": down.unique_id,
                    "node_type": down.node_type.label(),
                }));
            }
            queue.push_back(n);
        }
    }
    traces
}

fn tool_lineage_bundle(ctx: &McpContext, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let upstream = args.get("upstream").and_then(|v| v.as_u64()).unwrap_or(2) as usize;
    let downstream = args.get("downstream").and_then(|v| v.as_u64()).unwrap_or(1) as usize;

    let filtered = filter::filter_graph(
        &ctx.graph,
        Some(model),
        Some(upstream),
        Some(downstream),
        &NodeTypeFilter {
            include_tests: true,
            include_seeds: true,
            include_snapshots: true,
            include_exposures: true,
        },
        &[],
    )?;

    let mut bundled_nodes: Vec<Value> = Vec::new();
    for idx in filtered.node_indices() {
        let node = &filtered[idx];
        let sql = node.file_path.as_ref().and_then(|rel| {
            let abs = if rel.is_absolute() {
                rel.clone()
            } else {
                ctx.project_dir.join(rel)
            };
            std::fs::read_to_string(abs).ok()
        });
        bundled_nodes.push(json!({
            "unique_id": node.unique_id,
            "label": node.label,
            "node_type": node.node_type.label(),
            "description": node.description,
            "materialization": node.materialization,
            "tags": node.tags,
            "columns": node.columns,
            "sql": sql,
        }));
    }
    let edges: Vec<Value> = filtered
        .edge_references()
        .map(|e| {
            json!({
                "source": filtered[e.source()].unique_id,
                "target": filtered[e.target()].unique_id,
                "edge_type": edge_type_label(e.weight().edge_type),
            })
        })
        .collect();

    Ok(json!({
        "focus": model,
        "nodes": bundled_nodes,
        "edges": edges,
    }))
}

fn tool_propose_test(ctx: &McpContext, args: &Value) -> Result<Value> {
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'model' is required"))?;
    let column = args
        .get("column")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'column' is required"))?;
    let kind = args
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'kind' is required"))?;
    crate::mcp::propose::validate_kind(kind)?;

    let idx = find_node(&ctx.graph, model).ok_or_else(|| anyhow!("model '{}' not found", model))?;
    let node = &ctx.graph[idx];
    if !node.columns.is_empty() && !node.columns.iter().any(|c| c == column) {
        return Err(anyhow!(
            "column '{}' not found on model '{}'",
            column,
            model
        ));
    }

    let req = crate::mcp::propose::ProposeTestRequest {
        project_dir: &ctx.project_dir,
        model,
        column,
        kind,
        yaml_files: vec![],
    };
    let result = crate::mcp::propose::build_proposal(&req)?;

    let yaml_snippet = format!(
        "  - name: {}\n    columns:\n      - name: {}\n        tests:\n          - {}\n",
        model, column, kind
    );
    let rationale = rationale_for(kind, column);
    Ok(json!({
        "model": model,
        "column": column,
        "kind": kind,
        "file": result.file.to_string_lossy(),
        "diff": result.diff,
        "already_present": result.already_present,
        "new_file": result.new_file,
        "yaml_snippet": yaml_snippet,
        "rationale": rationale,
    }))
}

fn rationale_for(kind: &str, column: &str) -> String {
    match kind {
        "not_null" => format!(
            "Column '{}' is referenced in downstream queries; a not_null test catches upstream data quality regressions early.",
            column
        ),
        "unique" => format!(
            "Column '{}' looks like a key candidate based on its name and position; a unique test prevents accidental fan-out.",
            column
        ),
        "accepted_values" => format!(
            "Column '{}' is a candidate enum/categorical; constrain to known values to catch upstream drift.",
            column
        ),
        "relationships" => format!(
            "Column '{}' likely references another model's primary key; a relationships test enforces referential integrity.",
            column
        ),
        _ => String::new(),
    }
}

fn find_node(graph: &LineageGraph, label: &str) -> Option<NodeIndex> {
    if let Some(idx) = graph.node_indices().find(|&idx| graph[idx].label == label) {
        return Some(idx);
    }
    let suffix = format!(".{}", label);
    graph
        .node_indices()
        .find(|&idx| graph[idx].unique_id.ends_with(&suffix))
}

fn edge_type_label(t: EdgeType) -> &'static str {
    match t {
        EdgeType::Ref => "ref",
        EdgeType::Source => "source",
        EdgeType::Test => "test",
        EdgeType::Exposure => "exposure",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_node(unique_id: &str, label: &str, nt: NodeType) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type: nt,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn ctx_with_graph() -> McpContext {
        let mut g = LineageGraph::new();
        let s = g.add_node(NodeData {
            columns: vec!["order_id".into()],
            ..make_node("source.raw.orders", "raw.orders", NodeType::Source)
        });
        let stg = g.add_node(NodeData {
            columns: vec!["order_id".into()],
            ..make_node("model.stg_orders", "stg_orders", NodeType::Model)
        });
        let m = g.add_node(NodeData {
            columns: vec!["order_id".into()],
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
            m,
            EdgeData {
                edge_type: EdgeType::Ref,
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
    fn test_registry_includes_new_tools() {
        let names: Vec<&str> = registry().iter().map(|t| t.name).collect();
        for required in [
            "get_model_details",
            "read_model_sql",
            "column_upstream",
            "column_downstream",
            "lineage_bundle",
            "propose_test",
        ] {
            assert!(names.contains(&required), "missing tool {}", required);
        }
    }

    #[test]
    fn test_get_model_details_returns_neighbors() {
        let ctx = ctx_with_graph();
        let v = call_tool(&ctx, "get_model_details", &json!({"model": "stg_orders"})).unwrap();
        assert_eq!(v["upstream"][0], "raw.orders");
        assert_eq!(v["downstream"][0], "orders");
    }

    #[test]
    fn test_column_upstream_finds_source() {
        let ctx = ctx_with_graph();
        let v = call_tool(
            &ctx,
            "column_upstream",
            &json!({"model": "orders", "column": "order_id"}),
        )
        .unwrap();
        let traces = v["traces"].as_array().unwrap();
        assert!(traces.iter().any(|t| t["source_model"] == "raw.orders"));
    }

    #[test]
    fn test_column_downstream_finds_consumer() {
        let ctx = ctx_with_graph();
        let v = call_tool(
            &ctx,
            "column_downstream",
            &json!({"model": "stg_orders", "column": "order_id"}),
        )
        .unwrap();
        let traces = v["traces"].as_array().unwrap();
        assert!(traces.iter().any(|t| t["target_model"] == "orders"));
    }

    #[test]
    fn test_column_upstream_uses_analyzer_when_edges_present() {
        use crate::parser::column_lineage::{ColumnConfidence, ColumnEdge, ColumnLineage};
        let mut ctx = ctx_with_graph();
        // Inject a precomputed column edge: stg_orders.order_id → orders.order_id (aliased).
        ctx.column_lineage = ColumnLineage {
            edges: vec![ColumnEdge {
                source_node: "model.stg_orders".into(),
                source_column: "order_id".into(),
                target_node: "model.orders".into(),
                target_column: "order_id".into(),
                confidence: ColumnConfidence::Aliased,
            }],
        };
        let v = call_tool(
            &ctx,
            "column_upstream",
            &json!({"model": "orders", "column": "order_id"}),
        )
        .unwrap();
        assert_eq!(v["source"], "analyzer");
        let hops = v["hops"].as_array().unwrap();
        assert_eq!(hops.len(), 1);
        assert_eq!(hops[0]["from"]["model"], "model.stg_orders");
        assert_eq!(hops[0]["kind"], "aliased");
    }

    #[test]
    fn test_column_upstream_falls_back_to_heuristic() {
        let ctx = ctx_with_graph(); // empty column_lineage
        let v = call_tool(
            &ctx,
            "column_upstream",
            &json!({"model": "orders", "column": "order_id"}),
        )
        .unwrap();
        assert_eq!(v["source"], "heuristic");
        assert!(v["hops"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_propose_test_emits_yaml_snippet() {
        let ctx = ctx_with_graph();
        let v = call_tool(
            &ctx,
            "propose_test",
            &json!({"model": "orders", "column": "order_id", "kind": "not_null"}),
        )
        .unwrap();
        let yaml = v["yaml_snippet"].as_str().unwrap();
        assert!(yaml.contains("not_null"));
        assert!(yaml.contains("order_id"));
        assert!(yaml.contains("- name: orders"));
    }

    #[test]
    fn test_propose_test_rejects_unknown_column() {
        let ctx = ctx_with_graph();
        let err = call_tool(
            &ctx,
            "propose_test",
            &json!({"model": "orders", "column": "ghost", "kind": "not_null"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_read_model_sql_errors_when_no_source_at_all() {
        // No file_path on disk AND empty manifest_sql → clean error.
        let ctx = ctx_with_graph();
        let err = call_tool(&ctx, "read_model_sql", &json!({"model": "orders"})).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no SQL available"));
    }

    #[test]
    fn test_read_model_sql_falls_back_to_manifest() {
        let mut ctx = ctx_with_graph();
        ctx.manifest_sql
            .insert("model.orders".into(), "select 1 as order_id".into());
        let v = call_tool(&ctx, "read_model_sql", &json!({"model": "orders"})).unwrap();
        assert_eq!(v["source"], "manifest");
        assert_eq!(v["content"], "select 1 as order_id");
    }

    #[test]
    fn test_read_model_sql_disk_preferred_over_manifest() {
        let mut ctx = ctx_with_graph();
        // Put a temp file on disk and set file_path; manifest also has a different value.
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("orders.sql");
        std::fs::write(&p, "from-disk").unwrap();
        ctx.project_dir = tmp.path().to_path_buf();
        let orders_idx: NodeIndex = ctx
            .graph
            .node_indices()
            .find(|&i| ctx.graph[i].label == "orders")
            .unwrap();
        ctx.graph[orders_idx].file_path = Some(std::path::PathBuf::from("orders.sql"));
        ctx.manifest_sql
            .insert("model.orders".into(), "from-manifest".into());
        let v = call_tool(&ctx, "read_model_sql", &json!({"model": "orders"})).unwrap();
        assert_eq!(v["source"], "disk");
        assert_eq!(v["content"], "from-disk");
    }

    #[test]
    fn test_lineage_bundle_includes_neighbors_and_edges() {
        let ctx = ctx_with_graph();
        let v = call_tool(
            &ctx,
            "lineage_bundle",
            &json!({"model": "stg_orders", "upstream": 1, "downstream": 1}),
        )
        .unwrap();
        let nodes = v["nodes"].as_array().unwrap();
        let labels: Vec<&str> = nodes.iter().map(|n| n["label"].as_str().unwrap()).collect();
        assert!(labels.contains(&"stg_orders"));
        // SQL will be null because file_path is None in test fixtures.
        assert!(nodes[0]["sql"].is_null());
    }
}
