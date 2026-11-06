//! `verify`: measure how closely SQL-parse lineage matches dbt's own.
//!
//! Compares the graph built by parsing the project's files against the graph
//! from a `manifest.json` produced by `dbt parse` / `dbt compile`. For every
//! model and snapshot present in both, the set of upstream models, seeds,
//! snapshots and sources is compared. Edges dbt knows about but parsing missed
//! are the costly kind (impact reports would under-count); edges parsing found
//! but dbt doesn't usually come from refs in disabled Jinja branches.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Serialize;

use super::types::{LineageGraph, NodeType};

/// One upstream dependency present on one side only.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct EdgeMismatch {
    /// The dependent node, e.g. `model.fct_orders`.
    pub child: String,
    /// The upstream node, e.g. `model.stg_payments` or `source.raw.orders`.
    pub parent: String,
    /// The child's file, relative to the project, when known.
    pub file: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifyReport {
    /// Models and snapshots present in both graphs.
    pub nodes_compared: usize,
    /// Upstream edges of the compared nodes according to the manifest.
    pub manifest_edges: usize,
    /// Upstream edges of the compared nodes found by parsing.
    pub parsed_edges: usize,
    /// Edges on both sides.
    pub matched_edges: usize,
    /// In the manifest, not found by parsing.
    pub missing: Vec<EdgeMismatch>,
    /// Found by parsing, not in the manifest.
    pub extra: Vec<EdgeMismatch>,
    /// Project models/snapshots dbt knows about that parsing did not find.
    pub only_in_manifest: Vec<String>,
    /// Models/snapshots parsing found that the manifest lacks (disabled
    /// models, or a stale manifest).
    pub only_in_parse: Vec<String>,
}

impl VerifyReport {
    /// Share of the manifest's edges that parsing found, in percent.
    pub fn recall(&self) -> f64 {
        percent(self.matched_edges, self.manifest_edges)
    }

    /// Share of parsed edges that the manifest confirms, in percent.
    pub fn precision(&self) -> f64 {
        percent(self.matched_edges, self.parsed_edges)
    }

    /// True when every dependency and model dbt knows about was found. Extra
    /// edges and models are reported but not failures: they come from Jinja
    /// branches or models the current vars switch off.
    pub fn found_everything(&self) -> bool {
        self.missing.is_empty() && self.only_in_manifest.is_empty()
    }

    pub fn is_exact(&self) -> bool {
        self.missing.is_empty()
            && self.extra.is_empty()
            && self.only_in_manifest.is_empty()
            && self.only_in_parse.is_empty()
    }
}

fn percent(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        100.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

fn is_compared(t: NodeType) -> bool {
    matches!(t, NodeType::Model | NodeType::Snapshot)
}

fn is_upstream(t: NodeType) -> bool {
    matches!(
        t,
        NodeType::Model
            | NodeType::Seed
            | NodeType::Snapshot
            | NodeType::Source
            | NodeType::Phantom
    )
}

struct Side {
    /// unique_id -> (file, upstream unique_ids)
    nodes: BTreeMap<String, (Option<String>, BTreeSet<String>)>,
}

fn collect(graph: &LineageGraph) -> Side {
    let mut nodes = BTreeMap::new();
    for idx in graph.node_indices() {
        let node = &graph[idx];
        // The manifest maps analyses to models; they are not part of the DAG
        // that SQL-parse mode builds.
        if !is_compared(node.node_type) || node.unique_id.starts_with("analysis.") {
            continue;
        }
        let parents = graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .map(|p| &graph[p])
            .filter(|p| is_upstream(p.node_type))
            .map(|p| p.unique_id.clone())
            .collect();
        let file = node
            .file_path
            .as_ref()
            .map(|p| p.to_string_lossy().replace('\\', "/"));
        nodes.insert(node.unique_id.clone(), (file, parents));
    }
    Side { nodes }
}

/// Compare parsed lineage against manifest lineage. `in_project` decides
/// whether a manifest node belongs to the project being parsed (manifests
/// also hold installed packages' models, which parsing never sees); it gets
/// the node's file path as recorded in the manifest.
pub fn verify(
    parsed: &LineageGraph,
    manifest: &LineageGraph,
    in_project: impl Fn(&str) -> bool,
) -> VerifyReport {
    let parsed = collect(parsed);
    let manifest = collect(manifest);
    let mut report = VerifyReport::default();

    for (id, (file, m_parents)) in &manifest.nodes {
        let Some((p_file, p_parents)) = parsed.nodes.get(id) else {
            if file.as_deref().is_some_and(&in_project) {
                report.only_in_manifest.push(id.clone());
            }
            continue;
        };
        report.nodes_compared += 1;
        report.manifest_edges += m_parents.len();
        report.parsed_edges += p_parents.len();
        report.matched_edges += m_parents.intersection(p_parents).count();
        let file = p_file.clone().or_else(|| file.clone());
        for parent in m_parents.difference(p_parents) {
            report.missing.push(EdgeMismatch {
                child: id.clone(),
                parent: parent.clone(),
                file: file.clone(),
            });
        }
        for parent in p_parents.difference(m_parents) {
            report.extra.push(EdgeMismatch {
                child: id.clone(),
                parent: parent.clone(),
                file: file.clone(),
            });
        }
    }
    report.only_in_parse = parsed
        .nodes
        .keys()
        .filter(|id| !manifest.nodes.contains_key(*id))
        .cloned()
        .collect();
    report
}

/// `in_project` predicate for [`verify`]: a manifest path counts as part of
/// the project when it is a model file (`.sql` / `.py`) under `project_dir`.
/// Hooks, whose path is `dbt_project.yml`, are not models.
pub fn file_in_project(project_dir: &Path) -> impl Fn(&str) -> bool + '_ {
    move |file| {
        (file.ends_with(".sql") || file.ends_with(".py")) && project_dir.join(file).is_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{EdgeData, EdgeType, NodeData};
    use petgraph::stable_graph::NodeIndex;
    use std::collections::HashMap;

    fn graph(nodes: &[(&str, NodeType)], edges: &[(&str, &str)]) -> LineageGraph {
        let mut g = LineageGraph::new();
        let mut idx: HashMap<&str, NodeIndex> = HashMap::new();
        for (id, t) in nodes {
            let label = id.rsplit('.').next().unwrap().to_string();
            let i = g.add_node(NodeData {
                unique_id: id.to_string(),
                label: label.clone(),
                node_type: *t,
                file_path: Some(format!("models/{label}.sql").into()),
                description: None,
                materialization: None,
                tags: vec![],
                columns: vec![],
            });
            idx.insert(id, i);
        }
        for (from, to) in edges {
            g.add_edge(
                idx[from],
                idx[to],
                EdgeData {
                    edge_type: EdgeType::Ref,
                },
            );
        }
        g
    }

    const BASE: &[(&str, NodeType)] = &[
        ("source.raw.orders", NodeType::Source),
        ("model.stg_orders", NodeType::Model),
        ("model.stg_payments", NodeType::Model),
        ("model.fct_orders", NodeType::Model),
    ];

    #[test]
    fn identical_graphs_are_exact() {
        let edges = [
            ("source.raw.orders", "model.stg_orders"),
            ("model.stg_orders", "model.fct_orders"),
        ];
        let r = verify(&graph(BASE, &edges), &graph(BASE, &edges), |_| true);
        assert!(r.is_exact());
        assert_eq!(r.nodes_compared, 3);
        assert_eq!(r.matched_edges, 2);
        assert_eq!(r.recall(), 100.0);
    }

    #[test]
    fn reports_missing_and_extra_edges() {
        let parsed = graph(
            BASE,
            &[
                ("model.stg_orders", "model.fct_orders"),
                ("source.raw.orders", "model.stg_payments"),
            ],
        );
        let manifest = graph(
            BASE,
            &[
                ("model.stg_orders", "model.fct_orders"),
                ("model.stg_payments", "model.fct_orders"),
            ],
        );
        let r = verify(&parsed, &manifest, |_| true);
        assert_eq!(
            r.missing,
            vec![EdgeMismatch {
                child: "model.fct_orders".into(),
                parent: "model.stg_payments".into(),
                file: Some("models/fct_orders.sql".into()),
            }]
        );
        assert_eq!(r.extra.len(), 1);
        assert_eq!(r.extra[0].parent, "source.raw.orders");
        assert_eq!(r.recall(), 50.0);
        assert_eq!(r.precision(), 50.0);
    }

    #[test]
    fn package_models_are_not_reported_as_missing() {
        let parsed = graph(&BASE[..2], &[]);
        let manifest = graph(BASE, &[]);
        let r = verify(&parsed, &manifest, |f| f.ends_with("stg_payments.sql"));
        assert_eq!(r.only_in_manifest, vec!["model.stg_payments".to_string()]);
    }

    #[test]
    fn tests_and_exposures_are_ignored() {
        let mut nodes = BASE.to_vec();
        nodes.push(("test.not_null_x", NodeType::Test));
        nodes.push(("exposure.dash", NodeType::Exposure));
        let parsed = graph(BASE, &[]);
        let manifest = graph(
            &nodes,
            &[
                ("model.fct_orders", "test.not_null_x"),
                ("model.fct_orders", "exposure.dash"),
            ],
        );
        assert!(verify(&parsed, &manifest, |_| true).is_exact());
    }

    #[test]
    fn hooks_are_not_project_models() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), "").unwrap();
        std::fs::write(tmp.path().join("a.sql"), "").unwrap();
        let in_project = file_in_project(tmp.path());
        assert!(in_project("a.sql"));
        assert!(!in_project("./dbt_project.yml"));
        assert!(!in_project("missing.sql"));
    }

    #[test]
    fn extra_edges_still_count_as_found_everything() {
        let parsed = graph(BASE, &[("model.stg_orders", "model.fct_orders")]);
        let manifest = graph(BASE, &[]);
        let r = verify(&parsed, &manifest, |_| true);
        assert!(r.found_everything());
        assert!(!r.is_exact());
    }

    #[test]
    fn analyses_are_ignored() {
        let parsed = graph(BASE, &[]);
        let mut nodes = BASE.to_vec();
        nodes.push(("analysis.flow", NodeType::Model));
        let manifest = graph(&nodes, &[("model.fct_orders", "analysis.flow")]);
        assert!(verify(&parsed, &manifest, |_| true).is_exact());
    }

    #[test]
    fn only_in_parse_lists_unknown_models() {
        let parsed = graph(BASE, &[]);
        let manifest = graph(&BASE[..3], &[]);
        let r = verify(&parsed, &manifest, |_| true);
        assert_eq!(r.only_in_parse, vec!["model.fct_orders".to_string()]);
    }
}
