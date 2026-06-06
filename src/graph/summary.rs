use std::collections::{HashMap, HashSet, VecDeque};

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::Serialize;

use super::types::*;

/// A tag and how many nodes carry it.
#[derive(Debug, Clone, Serialize)]
pub struct TagCount {
    pub tag: String,
    pub count: usize,
}

/// A model and the size of its downstream reach (transitive descendants).
#[derive(Debug, Clone, Serialize)]
pub struct ModelReach {
    pub unique_id: String,
    pub label: String,
    pub downstream_count: usize,
}

/// Project-wide overview.
#[derive(Debug, Clone, Serialize)]
pub struct SummaryReport {
    pub project_name: Option<String>,
    pub total_nodes: usize,
    pub models: usize,
    pub sources: usize,
    pub seeds: usize,
    pub snapshots: usize,
    pub tests: usize,
    pub exposures: usize,
    pub tags: Vec<TagCount>,
    pub top_downstream: Vec<ModelReach>,
    pub orphans: Vec<String>,
}

/// Count how many distinct nodes are transitively reachable downstream from `start`,
/// excluding `start` itself.
fn downstream_count(graph: &LineageGraph, start: NodeIndex) -> usize {
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    visited.insert(start);
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let n = edge.target();
            if visited.insert(n) {
                queue.push_back(n);
            }
        }
    }

    visited.len().saturating_sub(1)
}

/// Build a project overview from the lineage graph.
pub fn compute_summary(graph: &LineageGraph, project_name: Option<String>) -> SummaryReport {
    let mut counts: HashMap<NodeType, usize> = HashMap::new();
    let mut tag_counts: HashMap<String, usize> = HashMap::new();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        *counts.entry(node.node_type).or_insert(0) += 1;
        for tag in &node.tags {
            *tag_counts.entry(tag.clone()).or_insert(0) += 1;
        }
    }

    let mut tags: Vec<TagCount> = tag_counts
        .into_iter()
        .map(|(tag, count)| TagCount { tag, count })
        .collect();
    tags.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.tag.cmp(&b.tag)));

    // Compute downstream reach for every model.
    let mut reach: Vec<ModelReach> = Vec::new();
    let mut orphans: Vec<String> = Vec::new();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        if node.node_type != NodeType::Model {
            continue;
        }
        let count = downstream_count(graph, idx);
        if count == 0 {
            orphans.push(node.label.clone());
        }
        reach.push(ModelReach {
            unique_id: node.unique_id.clone(),
            label: node.label.clone(),
            downstream_count: count,
        });
    }

    // Sort orphans alphabetically; sort reach descending and keep the top 5.
    orphans.sort();
    reach.sort_by(|a, b| {
        b.downstream_count
            .cmp(&a.downstream_count)
            .then_with(|| a.label.cmp(&b.label))
    });
    let top_downstream: Vec<ModelReach> = reach
        .into_iter()
        .filter(|r| r.downstream_count > 0)
        .take(5)
        .collect();

    SummaryReport {
        project_name,
        total_nodes: graph.node_count(),
        models: *counts.get(&NodeType::Model).unwrap_or(&0),
        sources: *counts.get(&NodeType::Source).unwrap_or(&0),
        seeds: *counts.get(&NodeType::Seed).unwrap_or(&0),
        snapshots: *counts.get(&NodeType::Snapshot).unwrap_or(&0),
        tests: *counts.get(&NodeType::Test).unwrap_or(&0),
        exposures: *counts.get(&NodeType::Exposure).unwrap_or(&0),
        tags,
        top_downstream,
        orphans,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(unique_id: &str, label: &str, node_type: NodeType, tags: Vec<&str>) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
            materialization: None,
            tags: tags.into_iter().map(String::from).collect(),
            columns: vec![],
        }
    }

    fn edge_ref() -> EdgeData {
        EdgeData {
            edge_type: EdgeType::Ref,
        }
    }

    fn make_graph() -> LineageGraph {
        // src -> stg -> mart -> exp
        //               mart -> test
        //         lone (orphan model with no downstream)
        let mut g = LineageGraph::new();
        let src = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
            vec![],
        ));
        let stg = g.add_node(make_node(
            "model.stg_orders",
            "stg_orders",
            NodeType::Model,
            vec!["staging"],
        ));
        let mart = g.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            vec!["marts", "finance"],
        ));
        let test = g.add_node(make_node("test.t", "t", NodeType::Test, vec![]));
        let exp = g.add_node(make_node("exposure.d", "d", NodeType::Exposure, vec![]));
        let lone = g.add_node(make_node(
            "model.lone",
            "lone",
            NodeType::Model,
            vec!["marts"],
        ));

        g.add_edge(
            src,
            stg,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        g.add_edge(stg, mart, edge_ref());
        g.add_edge(
            mart,
            test,
            EdgeData {
                edge_type: EdgeType::Test,
            },
        );
        g.add_edge(
            mart,
            exp,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );
        let _ = lone;
        g
    }

    #[test]
    fn test_counts() {
        let g = make_graph();
        let r = compute_summary(&g, Some("jaffle".into()));
        assert_eq!(r.project_name.as_deref(), Some("jaffle"));
        assert_eq!(r.models, 3);
        assert_eq!(r.sources, 1);
        assert_eq!(r.tests, 1);
        assert_eq!(r.exposures, 1);
        assert_eq!(r.seeds, 0);
        assert_eq!(r.snapshots, 0);
        assert_eq!(r.total_nodes, 6);
    }

    #[test]
    fn test_tags_sorted_by_count() {
        let g = make_graph();
        let r = compute_summary(&g, None);
        // "marts" appears on 2 models, others appear once
        assert_eq!(r.tags[0].tag, "marts");
        assert_eq!(r.tags[0].count, 2);
    }

    #[test]
    fn test_orphans_include_lone_model() {
        let g = make_graph();
        let r = compute_summary(&g, None);
        // `orders` reaches a test + exposure downstream so it is NOT orphan in our definition;
        // but the orphan check counts ANY downstream node. So "orders" has downstream and is not orphan.
        // `lone` has nothing downstream → orphan. `stg_orders` reaches `orders` → not orphan.
        assert!(r.orphans.contains(&"lone".to_string()));
        assert!(!r.orphans.contains(&"stg_orders".to_string()));
        assert!(!r.orphans.contains(&"orders".to_string()));
    }

    #[test]
    fn test_top_downstream_ranks_by_reach() {
        let g = make_graph();
        let r = compute_summary(&g, None);
        // stg_orders reaches: orders, test, exp = 3
        // orders reaches: test, exp = 2
        // lone reaches: 0 (excluded)
        assert_eq!(r.top_downstream[0].label, "stg_orders");
        assert_eq!(r.top_downstream[0].downstream_count, 3);
        assert_eq!(r.top_downstream[1].label, "orders");
        assert_eq!(r.top_downstream[1].downstream_count, 2);
    }

    #[test]
    fn test_empty_graph() {
        let g = LineageGraph::new();
        let r = compute_summary(&g, None);
        assert_eq!(r.total_nodes, 0);
        assert_eq!(r.models, 0);
        assert!(r.tags.is_empty());
        assert!(r.top_downstream.is_empty());
        assert!(r.orphans.is_empty());
    }
}
