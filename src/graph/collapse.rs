use std::collections::{HashMap, HashSet, VecDeque};

use petgraph::stable_graph::{EdgeIndex, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use super::types::*;

/// Selection rule for which nodes to keep during collapse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollapseMode {
    /// Keep endpoints (nodes with no predecessors OR no successors) plus the focus
    /// model. This is the default `--collapse` behavior.
    Auto,
    /// Keep ONLY sources, exposures, and the focus model. BFS-window
    /// pseudo-endpoints introduced by `-u`/`-d` truncation are dropped.
    Focal,
}

/// Output of [`collapse_graph`]. The `graph` is structurally equivalent (same node
/// set, transitive edges) and `via_hops` carries the per-edge intermediate-node
/// count for renderers that want to show a `(via N)` label.
#[derive(Debug)]
pub struct CollapsedGraph {
    pub graph: LineageGraph,
    pub via_hops: HashMap<EdgeIndex, usize>,
}

/// Collapse the lineage graph by dropping intermediate nodes. The returned graph
/// contains only "kept" nodes; transitive paths through dropped nodes become single
/// edges whose `via_hops` value records how many intermediate nodes were folded.
///
/// `focus_label` corresponds to the positional model argument (`dbt-lineage graph <name>`)
/// and is always preserved when present.
pub fn collapse_graph(
    graph: &LineageGraph,
    mode: CollapseMode,
    focus_label: Option<&str>,
) -> CollapsedGraph {
    let focus_idx = focus_label.and_then(|label| find_node_by_label(graph, label));
    let keep = compute_keep_set(graph, mode, focus_idx);
    build_collapsed(graph, &keep)
}

/// Locate a node by `label`. Exact label match wins; only when none is found do we
/// fall back to a unique_id suffix match. (Without this preference, `"orders"` could
/// resolve to `source.raw.orders` because that unique_id also ends with `.orders`.)
fn find_node_by_label(graph: &LineageGraph, label: &str) -> Option<NodeIndex> {
    if let Some(idx) = graph.node_indices().find(|&idx| graph[idx].label == label) {
        return Some(idx);
    }
    let suffix = format!(".{}", label);
    graph
        .node_indices()
        .find(|&idx| graph[idx].unique_id.ends_with(&suffix))
}

fn compute_keep_set(
    graph: &LineageGraph,
    mode: CollapseMode,
    focus_idx: Option<NodeIndex>,
) -> HashSet<NodeIndex> {
    let mut keep: HashSet<NodeIndex> = HashSet::new();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let has_pred = graph
            .edges_directed(idx, Direction::Incoming)
            .next()
            .is_some();
        let has_succ = graph
            .edges_directed(idx, Direction::Outgoing)
            .next()
            .is_some();

        let is_endpoint = !has_pred || !has_succ;
        let is_source = node.node_type == NodeType::Source;
        let is_exposure = node.node_type == NodeType::Exposure;

        let kept = match mode {
            CollapseMode::Auto => is_endpoint || is_source || is_exposure,
            CollapseMode::Focal => is_source || is_exposure,
        };
        if kept {
            keep.insert(idx);
        }
    }

    if let Some(f) = focus_idx {
        keep.insert(f);
    }

    keep
}

fn build_collapsed(graph: &LineageGraph, keep: &HashSet<NodeIndex>) -> CollapsedGraph {
    let mut new_graph = LineageGraph::new();
    let mut via_hops: HashMap<EdgeIndex, usize> = HashMap::new();
    let mut old_to_new: HashMap<NodeIndex, NodeIndex> = HashMap::new();

    for &idx in keep {
        let new_idx = new_graph.add_node(graph[idx].clone());
        old_to_new.insert(idx, new_idx);
    }

    for &src_old in keep {
        for (dst_old, edge_type, hops) in transitive_targets(graph, keep, src_old) {
            let new_src = old_to_new[&src_old];
            let new_dst = old_to_new[&dst_old];
            let eid = new_graph.add_edge(new_src, new_dst, EdgeData { edge_type });
            if hops > 0 {
                via_hops.insert(eid, hops);
            }
        }
    }

    CollapsedGraph {
        graph: new_graph,
        via_hops,
    }
}

/// BFS forward from `src` traversing only dropped (non-kept) nodes. Returns one
/// entry per reachable kept node with the edge type of the LAST hop (the edge that
/// ENTERS the kept node) and the number of intermediate dropped nodes.
fn transitive_targets(
    graph: &LineageGraph,
    keep: &HashSet<NodeIndex>,
    src: NodeIndex,
) -> Vec<(NodeIndex, EdgeType, usize)> {
    let mut out: Vec<(NodeIndex, EdgeType, usize)> = Vec::new();
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    visited.insert(src);

    let mut queue: VecDeque<(NodeIndex, usize, EdgeType)> = VecDeque::new();
    for edge in graph.edges_directed(src, Direction::Outgoing) {
        let n = edge.target();
        if visited.insert(n) {
            queue.push_back((n, 1, edge.weight().edge_type));
        }
    }

    while let Some((node, depth, last_etype)) = queue.pop_front() {
        if keep.contains(&node) {
            // Found a kept target; record the (last-hop edge type, intermediate count).
            out.push((node, last_etype, depth - 1));
            // Don't expand past kept nodes.
            continue;
        }
        for edge in graph.edges_directed(node, Direction::Outgoing) {
            let n = edge.target();
            if visited.insert(n) {
                queue.push_back((n, depth + 1, edge.weight().edge_type));
            }
        }
    }

    out
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

    fn er() -> EdgeData {
        EdgeData {
            edge_type: EdgeType::Ref,
        }
    }

    fn es(t: EdgeType) -> EdgeData {
        EdgeData { edge_type: t }
    }

    /// Build: source -> stg -> mart -> exposure
    ///                          mart -> test
    fn linear_with_branches() -> (LineageGraph, [NodeIndex; 5]) {
        let mut g = LineageGraph::new();
        let s = g.add_node(make_node("source.raw.x", "raw.x", NodeType::Source));
        let stg = g.add_node(make_node("model.stg", "stg", NodeType::Model));
        let mart = g.add_node(make_node("model.mart", "mart", NodeType::Model));
        let exp = g.add_node(make_node("exposure.dash", "dash", NodeType::Exposure));
        let tst = g.add_node(make_node("test.t", "t", NodeType::Test));

        g.add_edge(s, stg, es(EdgeType::Source));
        g.add_edge(stg, mart, er());
        g.add_edge(mart, exp, es(EdgeType::Exposure));
        g.add_edge(mart, tst, es(EdgeType::Test));
        (g, [s, stg, mart, exp, tst])
    }

    #[test]
    fn test_auto_drops_intermediate_mart() {
        // mart has predecessors AND successors, but is NOT a source/exposure → drop.
        // stg also has both, so also dropped. Endpoints kept: source, exposure, test.
        let (g, [s, _stg, _mart, exp, tst]) = linear_with_branches();
        let c = collapse_graph(&g, CollapseMode::Auto, None);
        assert_eq!(c.graph.node_count(), 3);
        let labels: Vec<&str> = c
            .graph
            .node_indices()
            .map(|i| c.graph[i].label.as_str())
            .collect();
        assert!(labels.contains(&"raw.x"));
        assert!(labels.contains(&"dash"));
        assert!(labels.contains(&"t"));
        // We expect 2 edges: source -> exposure (via 2 intermediates: stg, mart),
        // and source -> test (via 2: stg, mart).
        assert_eq!(c.graph.edge_count(), 2);
        let via_values: Vec<usize> = c.via_hops.values().copied().collect();
        assert!(via_values.iter().all(|&v| v == 2));
        let _ = s;
        let _ = exp;
        let _ = tst;
    }

    #[test]
    fn test_auto_preserves_focus_model() {
        // With focus on "mart", mart stays in spite of being intermediate.
        let (g, _) = linear_with_branches();
        let c = collapse_graph(&g, CollapseMode::Auto, Some("mart"));
        let labels: Vec<&str> = c
            .graph
            .node_indices()
            .map(|i| c.graph[i].label.as_str())
            .collect();
        assert!(labels.contains(&"mart"));
        // Now mart is kept, so source -> mart (via 1 intermediate: stg).
        // And mart -> exposure and mart -> test are direct edges (via 0).
        let mart_idx = c
            .graph
            .node_indices()
            .find(|&i| c.graph[i].label == "mart")
            .unwrap();
        let incoming: Vec<EdgeIndex> = c
            .graph
            .edges_directed(mart_idx, Direction::Incoming)
            .map(|e| e.id())
            .collect();
        assert_eq!(incoming.len(), 1);
        assert_eq!(c.via_hops.get(&incoming[0]).copied().unwrap_or(0), 1);
    }

    #[test]
    fn test_focal_drops_endpoint_test() {
        // Focal: only sources/exposures/focus. The test node is an endpoint
        // but NOT a source/exposure → dropped under focal mode.
        let (g, _) = linear_with_branches();
        let c = collapse_graph(&g, CollapseMode::Focal, None);
        let labels: Vec<&str> = c
            .graph
            .node_indices()
            .map(|i| c.graph[i].label.as_str())
            .collect();
        assert!(labels.contains(&"raw.x"));
        assert!(labels.contains(&"dash"));
        assert!(!labels.contains(&"t"));
        // Only 1 edge: source -> exposure, via 2 intermediates (stg, mart).
        assert_eq!(c.graph.edge_count(), 1);
    }

    #[test]
    fn test_edge_type_uses_last_hop() {
        // source -> stg (Source) -> mart (Ref) -> exposure (Exposure).
        // Collapsed source -> exposure should carry the LAST hop type (Exposure).
        let (g, _) = linear_with_branches();
        let c = collapse_graph(&g, CollapseMode::Auto, None);
        let src_idx = c
            .graph
            .node_indices()
            .find(|&i| c.graph[i].label == "raw.x")
            .unwrap();
        let exp_idx = c
            .graph
            .node_indices()
            .find(|&i| c.graph[i].label == "dash")
            .unwrap();
        let e = c
            .graph
            .edges_directed(src_idx, Direction::Outgoing)
            .find(|e| e.target() == exp_idx)
            .unwrap();
        assert_eq!(e.weight().edge_type, EdgeType::Exposure);
    }

    #[test]
    fn test_direct_edge_between_kept_nodes_has_zero_hops() {
        // Two endpoints with a direct edge between them.
        let mut g = LineageGraph::new();
        let a = g.add_node(make_node("source.a", "a", NodeType::Source));
        let b = g.add_node(make_node("model.b", "b", NodeType::Model));
        g.add_edge(a, b, es(EdgeType::Source));
        // b has no successors so it's an endpoint in Auto mode.
        let c = collapse_graph(&g, CollapseMode::Auto, None);
        assert_eq!(c.graph.node_count(), 2);
        assert_eq!(c.graph.edge_count(), 1);
        // No via_hops recorded (or recorded as 0).
        assert!(c.via_hops.values().all(|&v| v == 0));
    }

    #[test]
    fn test_empty_graph() {
        let g = LineageGraph::new();
        let c = collapse_graph(&g, CollapseMode::Auto, None);
        assert_eq!(c.graph.node_count(), 0);
        assert_eq!(c.graph.edge_count(), 0);
    }
}
