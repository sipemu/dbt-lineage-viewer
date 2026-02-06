use anyhow::Result;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use petgraph::Direction;
use std::collections::{HashSet, VecDeque};

use crate::error::DbtLineageError;

use super::types::*;

/// Configuration for which node types to include
pub struct NodeTypeFilter {
    pub include_tests: bool,
    pub include_seeds: bool,
    pub include_snapshots: bool,
    pub include_exposures: bool,
}

/// Filter the graph based on focus model, distance, and node types
pub fn filter_graph(
    graph: &LineageGraph,
    focus_model: Option<&str>,
    upstream: Option<usize>,
    downstream: Option<usize>,
    type_filter: &NodeTypeFilter,
) -> Result<LineageGraph> {
    // Check for cycles
    if petgraph::algo::is_cyclic_directed(graph) {
        return Err(DbtLineageError::CycleDetected.into());
    }

    let mut keep_nodes: HashSet<NodeIndex> = HashSet::new();

    if let Some(model_name) = focus_model {
        // Find the focus node
        let focus_idx = graph
            .node_indices()
            .find(|&idx| {
                let node = &graph[idx];
                node.label == model_name || node.unique_id == format!("model.{}", model_name)
            })
            .ok_or_else(|| DbtLineageError::ModelNotFound(model_name.to_string()))?;

        keep_nodes.insert(focus_idx);

        // BFS upstream (predecessors)
        bfs_collect(
            graph,
            focus_idx,
            Direction::Incoming,
            upstream,
            &mut keep_nodes,
        );

        // BFS downstream (successors)
        bfs_collect(
            graph,
            focus_idx,
            Direction::Outgoing,
            downstream,
            &mut keep_nodes,
        );
    } else {
        // No focus model — keep all nodes
        keep_nodes.extend(graph.node_indices());
    }

    // Apply node type filter
    let keep_nodes: HashSet<NodeIndex> = keep_nodes
        .into_iter()
        .filter(|&idx| {
            let node = &graph[idx];
            match node.node_type {
                NodeType::Test => type_filter.include_tests,
                NodeType::Seed => type_filter.include_seeds,
                NodeType::Snapshot => type_filter.include_snapshots,
                NodeType::Exposure => type_filter.include_exposures,
                // Models, Sources, and Phantoms are always included
                NodeType::Model | NodeType::Source | NodeType::Phantom => true,
            }
        })
        .collect();

    // Build filtered subgraph
    let mut new_graph = LineageGraph::new();
    let mut index_map: std::collections::HashMap<NodeIndex, NodeIndex> =
        std::collections::HashMap::new();

    for &old_idx in &keep_nodes {
        let node = graph[old_idx].clone();
        let new_idx = new_graph.add_node(node);
        index_map.insert(old_idx, new_idx);
    }

    for edge in graph.edge_references() {
        let source = edge.source();
        let target = edge.target();
        if let (Some(&new_source), Some(&new_target)) =
            (index_map.get(&source), index_map.get(&target))
        {
            new_graph.add_edge(new_source, new_target, edge.weight().clone());
        }
    }

    Ok(new_graph)
}

/// BFS traversal collecting nodes up to max_depth levels away
fn bfs_collect(
    graph: &LineageGraph,
    start: NodeIndex,
    direction: Direction,
    max_depth: Option<usize>,
    collected: &mut HashSet<NodeIndex>,
) {
    let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::new();
    queue.push_back((start, 0));
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    visited.insert(start);

    while let Some((node, depth)) = queue.pop_front() {
        // Skip expansion if at max depth
        if max_depth.is_some_and(|max| depth >= max) {
            continue;
        }

        for e in graph.edges_directed(node, direction) {
            let neighbor = match direction {
                Direction::Incoming => e.source(),
                Direction::Outgoing => e.target(),
            };
            if visited.insert(neighbor) {
                collected.insert(neighbor);
                queue.push_back((neighbor, depth + 1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        // A -> B -> C -> D
        let a = g.add_node(NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
        });
        let b = g.add_node(NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        });
        let c = g.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        });
        let d = g.add_node(NodeData {
            unique_id: "exposure.dashboard".into(),
            label: "dashboard".into(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: None,
        });

        g.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        g.add_edge(
            b,
            c,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        g.add_edge(
            c,
            d,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );
        g
    }

    #[test]
    fn test_filter_no_focus() {
        let g = make_test_graph();
        let filter = NodeTypeFilter {
            include_tests: false,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: true,
        };
        let filtered = filter_graph(&g, None, None, None, &filter).unwrap();
        assert_eq!(filtered.node_count(), 4);
    }

    #[test]
    fn test_filter_focus_upstream_1() {
        let g = make_test_graph();
        let filter = NodeTypeFilter {
            include_tests: false,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: true,
        };
        // Focus on "orders" with 1 upstream, 0 downstream
        let filtered = filter_graph(&g, Some("orders"), Some(1), Some(0), &filter).unwrap();
        // Should have: orders + stg_orders (1 upstream)
        assert_eq!(filtered.node_count(), 2);
    }

    #[test]
    fn test_filter_excludes_exposures() {
        let g = make_test_graph();
        let filter = NodeTypeFilter {
            include_tests: false,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: false,
        };
        let filtered = filter_graph(&g, None, None, None, &filter).unwrap();
        // Exposure should be excluded
        assert_eq!(filtered.node_count(), 3);
    }

    #[test]
    fn test_filter_model_not_found() {
        let g = make_test_graph();
        let filter = NodeTypeFilter {
            include_tests: false,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: true,
        };
        let result = filter_graph(&g, Some("nonexistent"), None, None, &filter);
        assert!(result.is_err());
    }
}
