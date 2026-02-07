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

/// A parsed selector expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    /// Match nodes whose tags contain the given value
    Tag(String),
    /// Match nodes whose file_path starts with the given path prefix
    Path(String),
    /// Match nodes whose label equals the given model name
    ModelName(String),
}

/// Parse a comma-separated selector string into a list of `Selector` values.
///
/// Syntax:
/// - `tag:nightly` -> `Selector::Tag("nightly")`
/// - `path:models/staging` -> `Selector::Path("models/staging")`
/// - `orders` -> `Selector::ModelName("orders")`
pub fn parse_selectors(input: &str) -> Vec<Selector> {
    input
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            if let Some(tag) = s.strip_prefix("tag:") {
                Selector::Tag(tag.to_string())
            } else if let Some(path) = s.strip_prefix("path:") {
                Selector::Path(path.to_string())
            } else {
                Selector::ModelName(s.to_string())
            }
        })
        .collect()
}

/// Check if a single node matches any of the given selectors (union / OR logic).
fn node_matches_any_selector(node: &NodeData, selectors: &[Selector]) -> bool {
    selectors.iter().any(|sel| match sel {
        Selector::Tag(tag) => node.tags.contains(tag),
        Selector::Path(prefix) => node
            .file_path
            .as_ref()
            .map(|fp| fp.to_string_lossy().starts_with(prefix.as_str()))
            .unwrap_or(false),
        Selector::ModelName(name) => node.label == *name,
    })
}

/// Return the set of node indices that match any of the given selectors.
pub fn apply_selectors(graph: &LineageGraph, selectors: &[Selector]) -> HashSet<NodeIndex> {
    graph
        .node_indices()
        .filter(|&idx| node_matches_any_selector(&graph[idx], selectors))
        .collect()
}

/// Filter the graph based on focus model, distance, selectors, and node types
pub fn filter_graph(
    graph: &LineageGraph,
    focus_model: Option<&str>,
    upstream: Option<usize>,
    downstream: Option<usize>,
    type_filter: &NodeTypeFilter,
    selectors: &[Selector],
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
        // No focus model -- keep all nodes
        keep_nodes.extend(graph.node_indices());
    }

    // Apply selector filter: intersect with BFS results (or use as base set)
    if !selectors.is_empty() {
        let selector_matches = apply_selectors(graph, selectors);
        if focus_model.is_some() {
            // Intersect: keep only nodes that match both BFS and selectors
            keep_nodes = keep_nodes
                .intersection(&selector_matches)
                .copied()
                .collect();
        } else {
            // No focus model: use selectors as the base set
            keep_nodes = selector_matches;
        }
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
    use std::path::PathBuf;

    fn make_node(
        unique_id: &str,
        label: &str,
        node_type: NodeType,
        file_path: Option<PathBuf>,
        tags: Vec<String>,
    ) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path,
            description: None,
            materialization: None,
            tags,
            columns: vec![],
        }
    }

    fn make_test_graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        // A -> B -> C -> D
        let a = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
            None,
            vec![],
        ));
        let b = g.add_node(make_node(
            "model.stg_orders",
            "stg_orders",
            NodeType::Model,
            None,
            vec![],
        ));
        let c = g.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            None,
            vec![],
        ));
        let d = g.add_node(make_node(
            "exposure.dashboard",
            "dashboard",
            NodeType::Exposure,
            None,
            vec![],
        ));

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
        let filtered = filter_graph(&g, None, None, None, &filter, &[]).unwrap();
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
        let filtered = filter_graph(&g, Some("orders"), Some(1), Some(0), &filter, &[]).unwrap();
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
        let filtered = filter_graph(&g, None, None, None, &filter, &[]).unwrap();
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
        let result = filter_graph(&g, Some("nonexistent"), None, None, &filter, &[]);
        assert!(result.is_err());
    }

    // -- Selector parsing tests -----------------------------------------------

    #[test]
    fn test_parse_selectors_tag() {
        let selectors = parse_selectors("tag:nightly");
        assert_eq!(selectors, vec![Selector::Tag("nightly".into())]);
    }

    #[test]
    fn test_parse_selectors_path() {
        let selectors = parse_selectors("path:models/staging");
        assert_eq!(selectors, vec![Selector::Path("models/staging".into())]);
    }

    #[test]
    fn test_parse_selectors_model_name() {
        let selectors = parse_selectors("orders");
        assert_eq!(selectors, vec![Selector::ModelName("orders".into())]);
    }

    #[test]
    fn test_parse_selectors_multiple() {
        let selectors = parse_selectors("tag:nightly,path:models/staging,orders");
        assert_eq!(
            selectors,
            vec![
                Selector::Tag("nightly".into()),
                Selector::Path("models/staging".into()),
                Selector::ModelName("orders".into()),
            ]
        );
    }

    #[test]
    fn test_parse_selectors_whitespace_handling() {
        let selectors = parse_selectors(" tag:nightly , path:models/staging , orders ");
        assert_eq!(
            selectors,
            vec![
                Selector::Tag("nightly".into()),
                Selector::Path("models/staging".into()),
                Selector::ModelName("orders".into()),
            ]
        );
    }

    #[test]
    fn test_parse_selectors_empty_string() {
        let selectors = parse_selectors("");
        assert!(selectors.is_empty());
    }

    #[test]
    fn test_parse_selectors_trailing_comma() {
        let selectors = parse_selectors("orders,");
        assert_eq!(selectors, vec![Selector::ModelName("orders".into())]);
    }

    // -- Selector-based graph filtering tests ---------------------------------

    fn make_tagged_graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        // A: source, no tags, path schema.yml
        let a = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
            Some(PathBuf::from("models/staging/schema.yml")),
            vec![],
        ));
        // B: model, tag:nightly, path models/staging/stg_orders.sql
        let b = g.add_node(make_node(
            "model.stg_orders",
            "stg_orders",
            NodeType::Model,
            Some(PathBuf::from("models/staging/stg_orders.sql")),
            vec!["nightly".into()],
        ));
        // C: model, tag:daily, path models/marts/orders.sql
        let c = g.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some(PathBuf::from("models/marts/orders.sql")),
            vec!["daily".into()],
        ));
        // D: exposure, no tags, no path
        let d = g.add_node(make_node(
            "exposure.dashboard",
            "dashboard",
            NodeType::Exposure,
            None,
            vec![],
        ));

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

    fn default_type_filter() -> NodeTypeFilter {
        NodeTypeFilter {
            include_tests: true,
            include_seeds: true,
            include_snapshots: true,
            include_exposures: true,
        }
    }

    #[test]
    fn test_selector_by_tag() {
        let g = make_tagged_graph();
        let selectors = parse_selectors("tag:nightly");
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &selectors).unwrap();
        assert_eq!(filtered.node_count(), 1);
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"stg_orders".to_string()));
    }

    #[test]
    fn test_selector_by_path() {
        let g = make_tagged_graph();
        let selectors = parse_selectors("path:models/staging");
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &selectors).unwrap();
        // Should match: raw.orders (schema.yml in models/staging) and stg_orders
        assert_eq!(filtered.node_count(), 2);
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"raw.orders".to_string()));
        assert!(labels.contains(&"stg_orders".to_string()));
    }

    #[test]
    fn test_selector_by_model_name() {
        let g = make_tagged_graph();
        let selectors = parse_selectors("orders");
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &selectors).unwrap();
        assert_eq!(filtered.node_count(), 1);
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"orders".to_string()));
    }

    #[test]
    fn test_selector_union_multiple() {
        let g = make_tagged_graph();
        // tag:nightly matches stg_orders, model name "orders" matches orders
        let selectors = parse_selectors("tag:nightly,orders");
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &selectors).unwrap();
        assert_eq!(filtered.node_count(), 2);
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"stg_orders".to_string()));
        assert!(labels.contains(&"orders".to_string()));
    }

    #[test]
    fn test_selector_no_matches() {
        let g = make_tagged_graph();
        let selectors = parse_selectors("tag:nonexistent");
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &selectors).unwrap();
        assert_eq!(filtered.node_count(), 0);
    }

    #[test]
    fn test_selector_with_focus_intersects() {
        let g = make_tagged_graph();
        // Focus on "orders" with full upstream, then select only tag:nightly
        // BFS from "orders" upstream: raw.orders, stg_orders, orders
        // BFS downstream: dashboard
        // Selector tag:nightly matches only stg_orders
        // Intersection: stg_orders
        let selectors = parse_selectors("tag:nightly");
        let filtered = filter_graph(
            &g,
            Some("orders"),
            None,
            None,
            &default_type_filter(),
            &selectors,
        )
        .unwrap();
        assert_eq!(filtered.node_count(), 1);
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"stg_orders".to_string()));
    }

    #[test]
    fn test_selector_empty_does_not_filter() {
        let g = make_tagged_graph();
        let no_selectors: Vec<Selector> = vec![];
        let filtered =
            filter_graph(&g, None, None, None, &default_type_filter(), &no_selectors).unwrap();
        assert_eq!(filtered.node_count(), 4);
    }

    #[test]
    fn test_apply_selectors_directly() {
        let g = make_tagged_graph();
        let selectors = parse_selectors("tag:daily,stg_orders");
        let matched = apply_selectors(&g, &selectors);
        // tag:daily matches orders, stg_orders matches stg_orders
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn test_node_matches_any_selector_tag() {
        let node = make_node(
            "model.x",
            "x",
            NodeType::Model,
            Some(PathBuf::from("models/x.sql")),
            vec!["nightly".into(), "daily".into()],
        );
        assert!(node_matches_any_selector(
            &node,
            &[Selector::Tag("nightly".into())]
        ));
        assert!(node_matches_any_selector(
            &node,
            &[Selector::Tag("daily".into())]
        ));
        assert!(!node_matches_any_selector(
            &node,
            &[Selector::Tag("weekly".into())]
        ));
    }

    #[test]
    fn test_node_matches_any_selector_path() {
        let node = make_node(
            "model.x",
            "x",
            NodeType::Model,
            Some(PathBuf::from("models/staging/x.sql")),
            vec![],
        );
        assert!(node_matches_any_selector(
            &node,
            &[Selector::Path("models/staging".into())]
        ));
        assert!(node_matches_any_selector(
            &node,
            &[Selector::Path("models".into())]
        ));
        assert!(!node_matches_any_selector(
            &node,
            &[Selector::Path("tests".into())]
        ));
    }

    #[test]
    fn test_node_matches_any_selector_path_none() {
        let node = make_node("exposure.x", "x", NodeType::Exposure, None, vec![]);
        assert!(!node_matches_any_selector(
            &node,
            &[Selector::Path("models".into())]
        ));
    }

    #[test]
    fn test_node_matches_any_selector_model_name() {
        let node = make_node("model.orders", "orders", NodeType::Model, None, vec![]);
        assert!(node_matches_any_selector(
            &node,
            &[Selector::ModelName("orders".into())]
        ));
        assert!(!node_matches_any_selector(
            &node,
            &[Selector::ModelName("customers".into())]
        ));
    }

    #[test]
    fn test_type_filter_excludes_test_seed_snapshot() {
        let mut g = LineageGraph::new();
        let model = g.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            None,
            vec![],
        ));
        let test = g.add_node(make_node(
            "test.orders_positive",
            "orders_positive",
            NodeType::Test,
            None,
            vec![],
        ));
        let seed = g.add_node(make_node(
            "seed.countries",
            "countries",
            NodeType::Seed,
            None,
            vec![],
        ));
        let snap = g.add_node(make_node(
            "snapshot.orders_hist",
            "orders_hist",
            NodeType::Snapshot,
            None,
            vec![],
        ));
        g.add_edge(
            model,
            test,
            EdgeData {
                edge_type: EdgeType::Test,
            },
        );
        g.add_edge(
            seed,
            model,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        g.add_edge(
            model,
            snap,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        // Exclude all optional types
        let filter = NodeTypeFilter {
            include_tests: false,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: false,
        };
        let filtered = filter_graph(&g, None, None, None, &filter, &[]).unwrap();
        assert_eq!(filtered.node_count(), 1); // Only the model remains
        let labels: Vec<String> = filtered
            .node_indices()
            .map(|i| filtered[i].label.clone())
            .collect();
        assert!(labels.contains(&"orders".to_string()));

        // Include tests only
        let filter2 = NodeTypeFilter {
            include_tests: true,
            include_seeds: false,
            include_snapshots: false,
            include_exposures: false,
        };
        let filtered2 = filter_graph(&g, None, None, None, &filter2, &[]).unwrap();
        assert_eq!(filtered2.node_count(), 2); // model + test
    }

    #[test]
    fn test_filter_graph_rejects_cycle() {
        // Covers line 85: CycleDetected error
        let mut g = LineageGraph::new();
        let a = g.add_node(make_node("model.a", "a", NodeType::Model, None, vec![]));
        let b = g.add_node(make_node("model.b", "b", NodeType::Model, None, vec![]));
        g.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        g.add_edge(
            b,
            a,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let result = filter_graph(&g, None, None, None, &default_type_filter(), &[]);
        assert!(result.is_err());
    }
}
