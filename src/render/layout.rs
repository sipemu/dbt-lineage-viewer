use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use std::collections::HashMap;

use crate::graph::types::LineageGraph;

/// Layout result: each node gets a (layer, position_within_layer) coordinate
#[derive(Debug, Clone)]
pub struct LayoutResult {
    /// Map from NodeIndex to (layer, position)
    pub positions: HashMap<NodeIndex, (usize, usize)>,
    /// Number of layers
    pub num_layers: usize,
    /// Max nodes in any layer
    pub max_layer_width: usize,
    /// Nodes in each layer, ordered by position
    pub layers: Vec<Vec<NodeIndex>>,
}

/// Perform simplified Sugiyama layout
pub fn sugiyama_layout(graph: &LineageGraph) -> LayoutResult {
    if graph.node_count() == 0 {
        return LayoutResult {
            positions: HashMap::new(),
            num_layers: 0,
            max_layer_width: 0,
            layers: Vec::new(),
        };
    }

    // Step 1: Assign layers using longest path from roots
    let layers = assign_layers(graph);

    // Step 2: Order nodes within layers to minimize crossings (barycenter method)
    let ordered_layers = reduce_crossings(graph, &layers);

    // Step 3: Build position map
    let mut positions = HashMap::new();
    let mut max_width = 0;

    for (layer_idx, layer) in ordered_layers.iter().enumerate() {
        max_width = max_width.max(layer.len());
        for (pos, &node) in layer.iter().enumerate() {
            positions.insert(node, (layer_idx, pos));
        }
    }

    LayoutResult {
        positions,
        num_layers: ordered_layers.len(),
        max_layer_width: max_width,
        layers: ordered_layers,
    }
}

/// Assign layers using longest path from roots (nodes with no incoming edges)
fn assign_layers(graph: &LineageGraph) -> Vec<Vec<NodeIndex>> {
    let mut layer_of: HashMap<NodeIndex, usize> = HashMap::new();

    // Use topological order for longest-path layer assignment
    if let Ok(topo) = petgraph::algo::toposort(graph, None) {
        for node in &topo {
            let predecessors: Vec<usize> = graph
                .edges_directed(*node, Direction::Incoming)
                .filter_map(|e| layer_of.get(&e.source()).copied())
                .collect();

            let layer = if predecessors.is_empty() {
                0
            } else {
                predecessors.iter().max().unwrap() + 1
            };
            layer_of.insert(*node, layer);
        }
    } else {
        // Fallback for cyclic graphs (shouldn't happen after filter)
        for (i, node) in graph.node_indices().enumerate() {
            layer_of.insert(node, i);
        }
    }

    // Group by layer
    let max_layer = layer_of.values().copied().max().unwrap_or(0);
    let mut layers: Vec<Vec<NodeIndex>> = vec![Vec::new(); max_layer + 1];
    for (node, layer) in &layer_of {
        layers[*layer].push(*node);
    }

    // Remove empty layers
    layers.retain(|l| !l.is_empty());

    layers
}

/// Reduce edge crossings using barycenter heuristic
fn reduce_crossings(graph: &LineageGraph, initial_layers: &[Vec<NodeIndex>]) -> Vec<Vec<NodeIndex>> {
    let mut layers = initial_layers.to_vec();

    // Run 3 passes of barycenter ordering
    for _ in 0..3 {
        // Forward pass
        for i in 1..layers.len() {
            let prev_layer = layers[i - 1].clone();
            sort_by_barycenter(graph, &mut layers[i], &prev_layer, Direction::Incoming);
        }

        // Backward pass
        for i in (0..layers.len().saturating_sub(1)).rev() {
            let next_layer = layers[i + 1].clone();
            sort_by_barycenter(graph, &mut layers[i], &next_layer, Direction::Outgoing);
        }
    }

    layers
}

/// Sort nodes in a layer based on barycenter of connected nodes in adjacent layer
fn sort_by_barycenter(
    graph: &LineageGraph,
    layer: &mut [NodeIndex],
    adjacent: &[NodeIndex],
    direction: Direction,
) {
    let adj_positions: HashMap<NodeIndex, usize> = adjacent
        .iter()
        .enumerate()
        .map(|(i, &n)| (n, i))
        .collect();

    let mut barycenters: HashMap<NodeIndex, f64> = HashMap::new();

    for &node in layer.iter() {
        let neighbors: Vec<usize> = graph
            .edges_directed(node, direction)
            .filter_map(|e| {
                let other = match direction {
                    Direction::Incoming => e.source(),
                    Direction::Outgoing => e.target(),
                };
                adj_positions.get(&other).copied()
            })
            .collect();

        let bc = if neighbors.is_empty() {
            f64::MAX // Keep at current position
        } else {
            neighbors.iter().sum::<usize>() as f64 / neighbors.len() as f64
        };
        barycenters.insert(node, bc);
    }

    layer.sort_by(|a, b| {
        let ba = barycenters.get(a).unwrap_or(&f64::MAX);
        let bb = barycenters.get(b).unwrap_or(&f64::MAX);
        ba.partial_cmp(bb).unwrap_or(std::cmp::Ordering::Equal)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::*;

    #[test]
    fn test_empty_graph() {
        let g = LineageGraph::new();
        let layout = sugiyama_layout(&g);
        assert_eq!(layout.num_layers, 0);
    }

    #[test]
    fn test_linear_graph() {
        let mut g = LineageGraph::new();
        let a = g.add_node(NodeData {
            unique_id: "a".into(),
            label: "a".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
        });
        let b = g.add_node(NodeData {
            unique_id: "b".into(),
            label: "b".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        });
        let c = g.add_node(NodeData {
            unique_id: "c".into(),
            label: "c".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        });
        g.add_edge(a, b, EdgeData { edge_type: EdgeType::Source });
        g.add_edge(b, c, EdgeData { edge_type: EdgeType::Ref });

        let layout = sugiyama_layout(&g);
        assert_eq!(layout.num_layers, 3);

        let (la, _) = layout.positions[&a];
        let (lb, _) = layout.positions[&b];
        let (lc, _) = layout.positions[&c];
        assert!(la < lb);
        assert!(lb < lc);
    }
}
