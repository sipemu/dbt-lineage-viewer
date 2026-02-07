use std::io::Write;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;

use crate::graph::types::*;

#[derive(Serialize)]
struct HtmlJsonNode {
    unique_id: String,
    label: String,
    node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    materialization: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    columns: Vec<String>,
}

#[derive(Serialize)]
struct HtmlJsonEdge {
    source: String,
    target: String,
    edge_type: String,
}

#[derive(Serialize)]
struct HtmlJsonGraph {
    nodes: Vec<HtmlJsonNode>,
    edges: Vec<HtmlJsonEdge>,
}

fn build_html_json(graph: &LineageGraph) -> String {
    let nodes: Vec<HtmlJsonNode> = graph
        .node_indices()
        .map(|idx| {
            let node = &graph[idx];
            HtmlJsonNode {
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                node_type: node.node_type.label().to_string(),
                description: node.description.clone(),
                materialization: node.materialization.clone(),
                tags: node.tags.clone(),
                columns: node.columns.clone(),
            }
        })
        .collect();

    let edges: Vec<HtmlJsonEdge> = graph
        .edge_references()
        .map(|edge| {
            let source = &graph[edge.source()];
            let target = &graph[edge.target()];
            HtmlJsonEdge {
                source: source.unique_id.clone(),
                target: target.unique_id.clone(),
                edge_type: match edge.weight().edge_type {
                    EdgeType::Ref => "ref",
                    EdgeType::Source => "source",
                    EdgeType::Test => "test",
                    EdgeType::Exposure => "exposure",
                }
                .to_string(),
            }
        })
        .collect();

    let json_graph = HtmlJsonGraph { nodes, edges };
    serde_json::to_string(&json_graph).unwrap()
}

/// Render HTML to stdout
pub fn render_html(graph: &LineageGraph) {
    render_html_to_writer(graph, &mut std::io::stdout().lock());
}

pub fn render_html_to_writer<W: Write>(graph: &LineageGraph, w: &mut W) {
    let svg_content = crate::render::svg::render_svg_to_string(graph);
    let json_data = build_html_json(graph);

    write!(
        w,
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dbt Lineage Graph</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ background: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; overflow: hidden; }}
#container {{ display: flex; width: 100vw; height: 100vh; }}
#graph-area {{ flex: 1; overflow: hidden; position: relative; cursor: grab; }}
#graph-area.dragging {{ cursor: grabbing; }}
#svg-wrap {{ transform-origin: 0 0; }}
#detail-panel {{ width: 300px; background: #161b22; border-left: 1px solid #30363d; padding: 16px; overflow-y: auto; }}
#detail-panel h2 {{ font-size: 14px; color: #58a6ff; margin-bottom: 8px; }}
#detail-panel .field {{ margin-bottom: 6px; font-size: 13px; }}
#detail-panel .label {{ color: #8b949e; }}
#search-bar {{ position: absolute; top: 10px; left: 10px; z-index: 10; }}
#search-bar input {{ background: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 6px 12px; border-radius: 6px; font-size: 13px; width: 220px; }}
#toolbar {{ position: absolute; bottom: 10px; left: 10px; z-index: 10; display: flex; gap: 6px; }}
#toolbar button {{ background: #21262d; color: #c9d1d9; border: 1px solid #30363d; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; }}
#toolbar button:hover {{ background: #30363d; }}
.node {{ cursor: pointer; }}
.node:hover rect {{ stroke: #58a6ff; stroke-width: 2; }}
.node.selected rect {{ stroke: #f0e68c; stroke-width: 2.5; }}
.node.dimmed {{ opacity: 0.3; }}
</style>
</head>
<body>
<div id="container">
  <div id="graph-area">
    <div id="search-bar"><input type="text" id="search" placeholder="Search nodes..." /></div>
    <div id="toolbar">
      <button id="fit-btn">Fit to View</button>
      <button id="zoom-in">+</button>
      <button id="zoom-out">-</button>
    </div>
    <div id="svg-wrap">
{svg_content}
    </div>
  </div>
  <div id="detail-panel">
    <h2>Node Details</h2>
    <div id="detail-content"><div class="field">Click a node to inspect</div></div>
  </div>
</div>
<script>
(function() {{
  const data = {json_data};
  const nodeMap = {{}};
  data.nodes.forEach(n => nodeMap[n.unique_id] = n);

  const svgWrap = document.getElementById('svg-wrap');
  const graphArea = document.getElementById('graph-area');
  let scale = 1, tx = 0, ty = 0;
  let dragging = false, startX = 0, startY = 0, startTx = 0, startTy = 0;

  function applyTransform() {{
    svgWrap.style.transform = `translate(${{tx}}px,${{ty}}px) scale(${{scale}})`;
  }}

  graphArea.addEventListener('mousedown', e => {{
    if (e.target.closest('.node')) return;
    dragging = true;
    startX = e.clientX; startY = e.clientY;
    startTx = tx; startTy = ty;
    graphArea.classList.add('dragging');
  }});
  window.addEventListener('mousemove', e => {{
    if (!dragging) return;
    tx = startTx + (e.clientX - startX);
    ty = startTy + (e.clientY - startY);
    applyTransform();
  }});
  window.addEventListener('mouseup', () => {{
    dragging = false;
    graphArea.classList.remove('dragging');
  }});
  graphArea.addEventListener('wheel', e => {{
    e.preventDefault();
    const delta = e.deltaY > 0 ? 0.9 : 1.1;
    scale = Math.max(0.1, Math.min(5, scale * delta));
    applyTransform();
  }});

  document.getElementById('zoom-in').onclick = () => {{ scale = Math.min(5, scale * 1.2); applyTransform(); }};
  document.getElementById('zoom-out').onclick = () => {{ scale = Math.max(0.1, scale / 1.2); applyTransform(); }};
  document.getElementById('fit-btn').onclick = () => {{
    scale = 1; tx = 0; ty = 0; applyTransform();
  }};

  // Node click
  document.querySelectorAll('.node').forEach(g => {{
    g.addEventListener('click', () => {{
      document.querySelectorAll('.node.selected').forEach(n => n.classList.remove('selected'));
      g.classList.add('selected');
      const id = g.getAttribute('data-id');
      const node = nodeMap[id];
      if (!node) return;
      let html = `<div class="field"><span class="label">Name:</span> ${{node.label}}</div>`;
      html += `<div class="field"><span class="label">Type:</span> ${{node.node_type}}</div>`;
      html += `<div class="field"><span class="label">ID:</span> ${{node.unique_id}}</div>`;
      if (node.materialization) html += `<div class="field"><span class="label">Materialization:</span> ${{node.materialization}}</div>`;
      if (node.description) html += `<div class="field"><span class="label">Description:</span> ${{node.description}}</div>`;
      if (node.tags && node.tags.length) html += `<div class="field"><span class="label">Tags:</span> ${{node.tags.join(', ')}}</div>`;
      if (node.columns && node.columns.length) {{
        html += `<div class="field"><span class="label">Columns (${{node.columns.length}}):</span></div>`;
        node.columns.forEach(c => html += `<div class="field">&nbsp;&nbsp;${{c}}</div>`);
      }}
      // Find upstream/downstream
      const upstream = data.edges.filter(e => e.target === id).map(e => nodeMap[e.source]).filter(Boolean);
      const downstream = data.edges.filter(e => e.source === id).map(e => nodeMap[e.target]).filter(Boolean);
      if (upstream.length) {{
        html += `<div class="field"><span class="label">Upstream:</span></div>`;
        upstream.forEach(n => html += `<div class="field">&nbsp;&nbsp;${{n.label}} (${{n.node_type}})</div>`);
      }}
      if (downstream.length) {{
        html += `<div class="field"><span class="label">Downstream:</span></div>`;
        downstream.forEach(n => html += `<div class="field">&nbsp;&nbsp;${{n.label}} (${{n.node_type}})</div>`);
      }}
      document.getElementById('detail-content').innerHTML = html;
    }});
  }});

  // Search
  const searchInput = document.getElementById('search');
  searchInput.addEventListener('input', () => {{
    const q = searchInput.value.toLowerCase();
    document.querySelectorAll('.node').forEach(g => {{
      const id = g.getAttribute('data-id') || '';
      const node = nodeMap[id];
      const match = !q || (node && (node.label.toLowerCase().includes(q) || node.unique_id.toLowerCase().includes(q)));
      g.classList.toggle('dimmed', !match);
    }});
  }});
}})();
</script>
</body>
</html>"#,
        svg_content = svg_content,
        json_data = json_data
    )
    .unwrap();
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
        render_html_to_writer(graph, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_empty_graph() {
        let graph = LineageGraph::new();
        let output = render_to_string(&graph);
        assert!(output.contains("<!DOCTYPE html>"));
        assert!(output.contains("dbt Lineage Graph"));
        assert!(output.contains("<svg"));
    }

    #[test]
    fn test_single_node() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let output = render_to_string(&graph);
        assert!(output.contains("model.orders"));
        assert!(output.contains("orders"));
        assert!(output.contains("const data ="));
    }

    #[test]
    fn test_with_edges() {
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
        assert!(output.contains("source.raw.orders"));
        assert!(output.contains("model.stg_orders"));
        assert!(output.contains("Fit to View"));
    }

    #[test]
    fn test_json_data_embedded() {
        let mut graph = LineageGraph::new();
        graph.add_node(make_node("model.a", "a", NodeType::Model));
        let json = build_html_json(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_node_with_full_metadata() {
        let mut graph = LineageGraph::new();
        graph.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: Some("All completed orders".into()),
            materialization: Some("table".into()),
            tags: vec!["nightly".into(), "finance".into()],
            columns: vec!["order_id".into(), "customer_id".into(), "amount".into()],
        });

        let json = build_html_json(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let node = &parsed["nodes"][0];
        assert_eq!(node["unique_id"], "model.orders");
        assert_eq!(node["label"], "orders");
        assert_eq!(node["node_type"], "model");
        assert_eq!(node["description"], "All completed orders");
        assert_eq!(node["materialization"], "table");
        assert_eq!(node["tags"].as_array().unwrap().len(), 2);
        assert_eq!(node["tags"][0], "nightly");
        assert_eq!(node["tags"][1], "finance");
        assert_eq!(node["columns"].as_array().unwrap().len(), 3);
        assert_eq!(node["columns"][0], "order_id");
    }

    #[test]
    fn test_all_edge_types_in_json() {
        let mut graph = LineageGraph::new();
        let src = graph.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
        ));
        let model = graph.add_node(make_node("model.orders", "orders", NodeType::Model));
        let test = graph.add_node(make_node("test.t", "t", NodeType::Test));
        let exp = graph.add_node(make_node("exposure.dash", "dash", NodeType::Exposure));

        graph.add_edge(
            src,
            model,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            model,
            model,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        graph.add_edge(
            model,
            test,
            EdgeData {
                edge_type: EdgeType::Test,
            },
        );
        graph.add_edge(
            model,
            exp,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );

        let json = build_html_json(&graph);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let edges = parsed["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 4);

        let edge_types: Vec<&str> = edges
            .iter()
            .map(|e| e["edge_type"].as_str().unwrap())
            .collect();
        assert!(edge_types.contains(&"ref"));
        assert!(edge_types.contains(&"source"));
        assert!(edge_types.contains(&"test"));
        assert!(edge_types.contains(&"exposure"));
    }

    #[test]
    fn test_html_output_contains_interactive_elements() {
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
        assert!(output.contains("search-bar"));
        assert!(output.contains("detail-panel"));
        assert!(output.contains("zoom-in"));
        assert!(output.contains("zoom-out"));
        assert!(output.contains("fit-btn"));
        assert!(output.contains("const data ="));
    }
}
