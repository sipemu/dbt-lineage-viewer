use std::collections::{HashSet, VecDeque};

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::Serialize;

use super::types::*;

/// Severity level of impact
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ImpactSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl ImpactSeverity {
    pub fn label(&self) -> &'static str {
        match self {
            ImpactSeverity::Low => "low",
            ImpactSeverity::Medium => "medium",
            ImpactSeverity::High => "high",
            ImpactSeverity::Critical => "critical",
        }
    }
}

/// A single impacted node with its severity
#[derive(Debug, Clone, Serialize)]
pub struct ImpactedNode {
    pub unique_id: String,
    pub label: String,
    pub node_type: String,
    pub severity: ImpactSeverity,
    pub distance: usize,
}

/// Full impact analysis report
#[derive(Debug, Clone, Serialize)]
pub struct ImpactReport {
    pub source_model: String,
    pub overall_severity: ImpactSeverity,
    pub affected_models: usize,
    pub affected_tests: usize,
    pub affected_exposures: usize,
    pub longest_path_length: usize,
    pub longest_path: Vec<String>,
    pub impacted_nodes: Vec<ImpactedNode>,
}

/// Classify the severity of a single node
pub fn classify_severity(node: &NodeData) -> ImpactSeverity {
    match node.node_type {
        NodeType::Exposure => ImpactSeverity::Critical,
        NodeType::Test => ImpactSeverity::Low,
        NodeType::Model => {
            // Check for mart-like indicators
            let is_mart = node
                .materialization
                .as_deref()
                .is_some_and(|m| m == "table" || m == "incremental")
                || node
                    .file_path
                    .as_ref()
                    .is_some_and(|p| p.to_string_lossy().contains("mart"));

            if is_mart {
                return ImpactSeverity::High;
            }

            ImpactSeverity::Medium
        }
        _ => ImpactSeverity::Medium,
    }
}

/// Find the longest path from the source node going downstream using BFS/DFS
pub fn find_longest_path(graph: &LineageGraph, start: NodeIndex) -> Vec<String> {
    let mut best_path: Vec<NodeIndex> = vec![start];
    let mut stack: Vec<(NodeIndex, Vec<NodeIndex>)> = vec![(start, vec![start])];

    while let Some((current, path)) = stack.pop() {
        let neighbors: Vec<NodeIndex> = graph
            .edges_directed(current, Direction::Outgoing)
            .map(|e| e.target())
            .collect();

        if neighbors.is_empty() {
            if path.len() > best_path.len() {
                best_path = path;
            }
        } else {
            for neighbor in neighbors {
                if !path.contains(&neighbor) {
                    let mut new_path = path.clone();
                    new_path.push(neighbor);
                    stack.push((neighbor, new_path));
                }
            }
        }
    }

    best_path
        .iter()
        .map(|&idx| graph[idx].label.clone())
        .collect()
}

/// Compute the full impact report for a given model
pub fn compute_impact(graph: &LineageGraph, source_idx: NodeIndex) -> ImpactReport {
    let source_node = &graph[source_idx];
    let source_model = source_node.label.clone();

    // BFS downstream to find all impacted nodes with distances
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<(NodeIndex, usize)> = VecDeque::new();
    visited.insert(source_idx);
    queue.push_back((source_idx, 0));

    let mut impacted_nodes: Vec<ImpactedNode> = Vec::new();
    let mut affected_models = 0usize;
    let mut affected_tests = 0usize;
    let mut affected_exposures = 0usize;

    while let Some((current, distance)) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let neighbor = edge.target();
            if visited.insert(neighbor) {
                let node = &graph[neighbor];
                let severity = classify_severity(node);
                let next_distance = distance + 1;

                match node.node_type {
                    NodeType::Model => affected_models += 1,
                    NodeType::Test => affected_tests += 1,
                    NodeType::Exposure => affected_exposures += 1,
                    _ => {}
                }

                impacted_nodes.push(ImpactedNode {
                    unique_id: node.unique_id.clone(),
                    label: node.label.clone(),
                    node_type: node.node_type.label().to_string(),
                    severity,
                    distance: next_distance,
                });

                queue.push_back((neighbor, next_distance));
            }
        }
    }

    // Sort by severity (descending), then distance
    impacted_nodes.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then(a.distance.cmp(&b.distance))
    });

    let overall_severity = impacted_nodes
        .iter()
        .map(|n| n.severity)
        .max()
        .unwrap_or(ImpactSeverity::Low);

    let longest_path = find_longest_path(graph, source_idx);
    let longest_path_length = longest_path.len().saturating_sub(1);

    ImpactReport {
        source_model,
        overall_severity,
        affected_models,
        affected_tests,
        affected_exposures,
        longest_path_length,
        longest_path,
        impacted_nodes,
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
        materialization: Option<&str>,
        file_path: Option<&str>,
    ) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type,
            file_path: file_path.map(PathBuf::from),
            description: None,
            materialization: materialization.map(|s| s.to_string()),
            tags: vec![],
            columns: vec![],
        }
    }

    fn make_test_graph() -> (LineageGraph, NodeIndex) {
        let mut g = LineageGraph::new();
        let src = g.add_node(make_node(
            "source.raw.orders",
            "raw.orders",
            NodeType::Source,
            None,
            None,
        ));
        let stg = g.add_node(make_node(
            "model.stg_orders",
            "stg_orders",
            NodeType::Model,
            Some("view"),
            Some("models/staging/stg_orders.sql"),
        ));
        let mart = g.add_node(make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some("table"),
            Some("models/marts/orders.sql"),
        ));
        let test = g.add_node(make_node(
            "test.orders_positive",
            "orders_positive",
            NodeType::Test,
            None,
            None,
        ));
        let exp = g.add_node(make_node(
            "exposure.dashboard",
            "dashboard",
            NodeType::Exposure,
            None,
            None,
        ));

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

        (g, stg)
    }

    #[test]
    fn test_classify_severity_exposure() {
        let node = make_node("exposure.x", "x", NodeType::Exposure, None, None);
        assert_eq!(classify_severity(&node), ImpactSeverity::Critical);
    }

    #[test]
    fn test_classify_severity_test() {
        let node = make_node("test.x", "x", NodeType::Test, None, None);
        assert_eq!(classify_severity(&node), ImpactSeverity::Low);
    }

    #[test]
    fn test_classify_severity_mart_table() {
        let node = make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some("table"),
            None,
        );
        assert_eq!(classify_severity(&node), ImpactSeverity::High);
    }

    #[test]
    fn test_classify_severity_mart_incremental() {
        let node = make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            Some("incremental"),
            None,
        );
        assert_eq!(classify_severity(&node), ImpactSeverity::High);
    }

    #[test]
    fn test_classify_severity_mart_path() {
        let node = make_node(
            "model.orders",
            "orders",
            NodeType::Model,
            None,
            Some("models/marts/orders.sql"),
        );
        assert_eq!(classify_severity(&node), ImpactSeverity::High);
    }

    #[test]
    fn test_classify_severity_staging() {
        let node = make_node(
            "model.stg_orders",
            "stg_orders",
            NodeType::Model,
            Some("view"),
            Some("models/staging/stg_orders.sql"),
        );
        assert_eq!(classify_severity(&node), ImpactSeverity::Medium);
    }

    #[test]
    fn test_compute_impact() {
        let (g, stg) = make_test_graph();
        let report = compute_impact(&g, stg);

        assert_eq!(report.source_model, "stg_orders");
        assert_eq!(report.affected_models, 1); // orders
        assert_eq!(report.affected_tests, 1); // orders_positive
        assert_eq!(report.affected_exposures, 1); // dashboard
        assert_eq!(report.overall_severity, ImpactSeverity::Critical);
        assert!(report.longest_path_length >= 2);
        assert_eq!(report.impacted_nodes.len(), 3);
    }

    #[test]
    fn test_compute_impact_leaf_node() {
        let (g, _) = make_test_graph();
        let exp = g
            .node_indices()
            .find(|&i| g[i].label == "dashboard")
            .unwrap();
        let report = compute_impact(&g, exp);

        assert_eq!(report.source_model, "dashboard");
        assert_eq!(report.affected_models, 0);
        assert_eq!(report.affected_tests, 0);
        assert_eq!(report.affected_exposures, 0);
        assert!(report.impacted_nodes.is_empty());
    }

    #[test]
    fn test_find_longest_path() {
        let (g, _) = make_test_graph();
        let src = g
            .node_indices()
            .find(|&i| g[i].label == "raw.orders")
            .unwrap();
        let path = find_longest_path(&g, src);
        // src -> stg -> mart -> (test or exp) = 4 nodes
        assert!(path.len() >= 4);
        assert_eq!(path[0], "raw.orders");
    }

    #[test]
    fn test_impact_severity_ordering() {
        assert!(ImpactSeverity::Low < ImpactSeverity::Medium);
        assert!(ImpactSeverity::Medium < ImpactSeverity::High);
        assert!(ImpactSeverity::High < ImpactSeverity::Critical);
    }

    #[test]
    fn test_impact_isolated_node() {
        let mut g = LineageGraph::new();
        let n = g.add_node(make_node("model.x", "x", NodeType::Model, None, None));
        let report = compute_impact(&g, n);
        assert_eq!(report.affected_models, 0);
        assert_eq!(report.affected_tests, 0);
        assert_eq!(report.affected_exposures, 0);
        assert!(report.impacted_nodes.is_empty());
        assert_eq!(report.longest_path_length, 0);
    }
}
