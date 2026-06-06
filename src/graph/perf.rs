use std::collections::HashMap;

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::Serialize;

use crate::parser::artifacts::RunResults;

use super::types::*;

/// One model's runtime (own + cumulative critical path through upstream).
#[derive(Debug, Clone, Serialize)]
pub struct ModelTiming {
    pub unique_id: String,
    pub label: String,
    pub seconds: f64,
    /// Critical-path runtime to reach this model, summing along the slowest upstream chain.
    pub critical_path_seconds: f64,
    /// Number of downstream nodes (transitive); higher = more impact when slow.
    pub downstream_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PerfReport {
    pub total_runtime_seconds: f64,
    pub measured_models: usize,
    /// Models sorted by `seconds` descending.
    pub slowest: Vec<ModelTiming>,
    /// Models sorted by `critical_path_seconds` descending.
    pub critical_paths: Vec<ModelTiming>,
}

/// Build a perf report by joining the graph with run_results.
pub fn compute_perf(graph: &LineageGraph, run_results: &RunResults) -> PerfReport {
    // Map dbt's full unique_id (model.project.name) to seconds.
    let by_simplified: HashMap<String, f64> = run_results
        .results
        .iter()
        .filter_map(|r| {
            let simple = simplify(&r.unique_id)?;
            let secs = r.execute_seconds()?;
            Some((simple, secs))
        })
        .collect();

    // Collect per-node timing.
    let mut timings: HashMap<NodeIndex, f64> = HashMap::new();
    let mut total: f64 = 0.0;
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let simple = simplify_node_id(&node.unique_id);
        if let Some(&s) = by_simplified.get(&simple) {
            timings.insert(idx, s);
            total += s;
        }
    }

    // Critical path: for each kept node, find the max upstream critical path + own runtime.
    // DAGs only; we don't guard against cycles here (the upstream builder rejects them).
    let mut critical: HashMap<NodeIndex, f64> = HashMap::new();
    for idx in topo_order(graph) {
        let own = timings.get(&idx).copied().unwrap_or(0.0);
        let upstream_max = graph
            .edges_directed(idx, Direction::Incoming)
            .map(|e| critical.get(&e.source()).copied().unwrap_or(0.0))
            .fold(0.0_f64, f64::max);
        critical.insert(idx, upstream_max + own);
    }

    let mut entries: Vec<ModelTiming> = timings
        .iter()
        .map(|(&idx, &secs)| {
            let node = &graph[idx];
            ModelTiming {
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                seconds: secs,
                critical_path_seconds: critical.get(&idx).copied().unwrap_or(0.0),
                downstream_count: count_downstream(graph, idx),
            }
        })
        .collect();
    entries.sort_by(|a, b| {
        b.seconds
            .partial_cmp(&a.seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let slowest = entries.clone();

    entries.sort_by(|a, b| {
        b.critical_path_seconds
            .partial_cmp(&a.critical_path_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let critical_paths = entries;

    PerfReport {
        total_runtime_seconds: total,
        measured_models: timings.len(),
        slowest,
        critical_paths,
    }
}

fn simplify(unique_id: &str) -> Option<String> {
    let parts: Vec<&str> = unique_id.split('.').collect();
    if parts.len() >= 3 {
        Some(format!("{}.{}", parts[0], parts[parts.len() - 1]))
    } else if parts.len() == 2 {
        Some(unique_id.to_string())
    } else {
        None
    }
}

fn simplify_node_id(uid: &str) -> String {
    let parts: Vec<&str> = uid.split('.').collect();
    if parts.len() >= 3 {
        format!("{}.{}", parts[0], parts[parts.len() - 1])
    } else {
        uid.to_string()
    }
}

fn topo_order(graph: &LineageGraph) -> Vec<NodeIndex> {
    petgraph::algo::toposort(graph, None).unwrap_or_else(|_| graph.node_indices().collect())
}

fn count_downstream(graph: &LineageGraph, start: NodeIndex) -> usize {
    use std::collections::{HashSet, VecDeque};
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    visited.insert(start);
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    queue.push_back(start);
    while let Some(n) = queue.pop_front() {
        for e in graph.edges_directed(n, Direction::Outgoing) {
            if visited.insert(e.target()) {
                queue.push_back(e.target());
            }
        }
    }
    visited.len().saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::artifacts::{RunResult, RunResults, TimingEntry};
    use chrono::{Duration, Utc};

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

    fn run_result(unique_id: &str, seconds: i64) -> RunResult {
        let start = Utc::now();
        let end = start + Duration::seconds(seconds);
        RunResult {
            unique_id: unique_id.into(),
            status: "success".into(),
            message: None,
            timing: Some(vec![TimingEntry {
                name: "execute".into(),
                started_at: Some(start),
                completed_at: Some(end),
            }]),
        }
    }

    #[test]
    fn test_measures_and_orders_slowest() {
        let mut g = LineageGraph::new();
        let a = g.add_node(make_node("model.a", "a", NodeType::Model));
        let b = g.add_node(make_node("model.b", "b", NodeType::Model));
        g.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let rr = RunResults {
            results: vec![
                run_result("model.project.a", 5),
                run_result("model.project.b", 20),
            ],
        };
        let report = compute_perf(&g, &rr);
        assert_eq!(report.measured_models, 2);
        assert!((report.total_runtime_seconds - 25.0).abs() < 1e-6);
        assert_eq!(report.slowest[0].label, "b");
        assert_eq!(report.slowest[1].label, "a");
    }

    #[test]
    fn test_critical_path_sums_upstream() {
        // a (5s) → b (20s) ⇒ b's critical path = 5 + 20 = 25
        let mut g = LineageGraph::new();
        let a = g.add_node(make_node("model.a", "a", NodeType::Model));
        let b = g.add_node(make_node("model.b", "b", NodeType::Model));
        g.add_edge(
            a,
            b,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let rr = RunResults {
            results: vec![
                run_result("model.project.a", 5),
                run_result("model.project.b", 20),
            ],
        };
        let report = compute_perf(&g, &rr);
        let b = report
            .critical_paths
            .iter()
            .find(|m| m.label == "b")
            .unwrap();
        assert!((b.critical_path_seconds - 25.0).abs() < 1e-6);
    }

    #[test]
    fn test_empty_results() {
        let g = LineageGraph::new();
        let rr = RunResults { results: vec![] };
        let report = compute_perf(&g, &rr);
        assert_eq!(report.measured_models, 0);
        assert!(report.slowest.is_empty());
    }
}
