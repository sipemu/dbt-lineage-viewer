use serde::Serialize;

use petgraph::visit::EdgeRef;
use petgraph::Direction;

use super::types::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LintSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize)]
pub struct LintFinding {
    pub rule: &'static str,
    pub severity: LintSeverity,
    pub unique_id: String,
    pub label: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LintReport {
    pub findings: Vec<LintFinding>,
}

impl LintReport {
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
    pub fn count_by_severity(&self, sev: LintSeverity) -> usize {
        self.findings.iter().filter(|f| f.severity == sev).count()
    }
}

/// Set of opt-in/out flags for individual rules.
#[derive(Debug, Clone)]
pub struct LintConfig {
    pub unused_source: bool,
    pub undefined_source: bool,
    pub dead_end_model: bool,
    pub missing_description: bool,
    pub model_without_source: bool,
    pub generic_only: bool,
    pub circular_ref: bool,
}

impl Default for LintConfig {
    fn default() -> Self {
        Self {
            unused_source: true,
            undefined_source: true,
            dead_end_model: true,
            missing_description: true,
            model_without_source: true,
            generic_only: true,
            circular_ref: true,
        }
    }
}

/// Run all configured rules over the lineage graph.
pub fn lint(graph: &LineageGraph, config: &LintConfig) -> LintReport {
    let mut findings: Vec<LintFinding> = Vec::new();

    // Whole-graph rules.
    if config.circular_ref && petgraph::algo::is_cyclic_directed(graph) {
        findings.push(LintFinding {
            rule: "circular-ref",
            severity: LintSeverity::Error,
            unique_id: String::new(),
            label: String::new(),
            message:
                "Lineage graph contains a cycle. Models can't depend (transitively) on themselves."
                    .to_string(),
        });
    }

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let downstream = graph
            .edges_directed(idx, Direction::Outgoing)
            .next()
            .is_some();

        if config.unused_source && node.node_type == NodeType::Source && !downstream {
            findings.push(LintFinding {
                rule: "unused-source",
                severity: LintSeverity::Warning,
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                message: format!(
                    "Source '{}' is declared but no model references it via source().",
                    node.label
                ),
            });
        }

        if config.undefined_source && node.node_type == NodeType::Phantom {
            findings.push(LintFinding {
                rule: "undefined-source",
                severity: LintSeverity::Error,
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                message: format!(
                    "Reference '{}' is used but no matching model or source is defined.",
                    node.label
                ),
            });
        }

        if config.dead_end_model && node.node_type == NodeType::Model && !downstream {
            findings.push(LintFinding {
                rule: "dead-end-model",
                severity: LintSeverity::Info,
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                message: format!(
                    "Model '{}' has no downstream consumers (no models, tests, or exposures depend on it).",
                    node.label
                ),
            });
        }

        if config.missing_description
            && (node.node_type == NodeType::Model || node.node_type == NodeType::Source)
            && node.description.as_deref().unwrap_or("").trim().is_empty()
        {
            findings.push(LintFinding {
                rule: "missing-description",
                severity: LintSeverity::Info,
                unique_id: node.unique_id.clone(),
                label: node.label.clone(),
                message: format!(
                    "{} '{}' has no description in its schema YAML.",
                    node.node_type.label(),
                    node.label
                ),
            });
        }

        // model-without-source: a model whose upstream references are all phantoms
        // (unresolved). Usually means a source was forgotten or a ref typo'd.
        if config.model_without_source && node.node_type == NodeType::Model {
            let upstream: Vec<_> = graph
                .edges_directed(idx, Direction::Incoming)
                .map(|e| e.source())
                .collect();
            if !upstream.is_empty()
                && upstream
                    .iter()
                    .all(|&u| graph[u].node_type == NodeType::Phantom)
            {
                findings.push(LintFinding {
                    rule: "model-without-source",
                    severity: LintSeverity::Warning,
                    unique_id: node.unique_id.clone(),
                    label: node.label.clone(),
                    message: format!(
                        "Model '{}' has only unresolved upstream references. Likely a missing source declaration or a typo'd ref.",
                        node.label
                    ),
                });
            }
        }
    }

    findings.sort_by(|a, b| a.rule.cmp(b.rule).then(a.label.cmp(&b.label)));
    LintReport { findings }
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

    #[test]
    fn test_unused_source_flagged() {
        let mut g = LineageGraph::new();
        g.add_node(make_node(
            "source.raw.orphan",
            "raw.orphan",
            NodeType::Source,
        ));
        let report = lint(&g, &LintConfig::default());
        assert!(report.findings.iter().any(|f| f.rule == "unused-source"));
    }

    #[test]
    fn test_used_source_not_flagged() {
        let mut g = LineageGraph::new();
        let src = g.add_node(make_node("source.raw.x", "raw.x", NodeType::Source));
        let model = g.add_node(make_node("model.stg_x", "stg_x", NodeType::Model));
        g.add_edge(
            src,
            model,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        let report = lint(&g, &LintConfig::default());
        assert!(!report.findings.iter().any(|f| f.rule == "unused-source"));
    }

    #[test]
    fn test_phantom_flagged_as_undefined_source() {
        let mut g = LineageGraph::new();
        g.add_node(make_node("phantom.x", "x", NodeType::Phantom));
        let report = lint(&g, &LintConfig::default());
        assert!(report
            .findings
            .iter()
            .any(|f| f.rule == "undefined-source" && f.severity == LintSeverity::Error));
    }

    #[test]
    fn test_dead_end_model_flagged() {
        let mut g = LineageGraph::new();
        g.add_node(make_node("model.dead", "dead", NodeType::Model));
        let report = lint(&g, &LintConfig::default());
        assert!(report.findings.iter().any(|f| f.rule == "dead-end-model"));
    }

    #[test]
    fn test_missing_description_flagged() {
        let mut g = LineageGraph::new();
        let n = NodeData {
            description: None,
            ..make_node("model.a", "a", NodeType::Model)
        };
        g.add_node(n);
        let report = lint(&g, &LintConfig::default());
        assert!(report
            .findings
            .iter()
            .any(|f| f.rule == "missing-description"));
    }

    #[test]
    fn test_present_description_not_flagged() {
        let mut g = LineageGraph::new();
        let m = NodeData {
            description: Some("does X".into()),
            ..make_node("model.a", "a", NodeType::Model)
        };
        let downstream = g.add_node(make_node("model.b", "b", NodeType::Model));
        let _ = downstream;
        let idx = g.add_node(m);
        g.add_edge(
            idx,
            downstream,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let report = lint(&g, &LintConfig::default());
        let missing_for_a = report
            .findings
            .iter()
            .any(|f| f.rule == "missing-description" && f.label == "a");
        assert!(!missing_for_a);
    }

    #[test]
    fn test_model_without_source_flagged_for_all_phantom_upstream() {
        let mut g = LineageGraph::new();
        let phantom = g.add_node(make_node("phantom.x", "x", NodeType::Phantom));
        let model = g.add_node(make_node("model.mart", "mart", NodeType::Model));
        g.add_edge(
            phantom,
            model,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let report = lint(&g, &LintConfig::default());
        assert!(report
            .findings
            .iter()
            .any(|f| f.rule == "model-without-source" && f.label == "mart"));
    }

    #[test]
    fn test_model_without_source_not_flagged_when_one_real_source() {
        let mut g = LineageGraph::new();
        let real = g.add_node(make_node("source.raw.x", "raw.x", NodeType::Source));
        let phantom = g.add_node(make_node("phantom.y", "y", NodeType::Phantom));
        let model = g.add_node(make_node("model.mart", "mart", NodeType::Model));
        g.add_edge(
            real,
            model,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        g.add_edge(
            phantom,
            model,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let report = lint(&g, &LintConfig::default());
        assert!(!report
            .findings
            .iter()
            .any(|f| f.rule == "model-without-source"));
    }

    #[test]
    fn test_circular_ref_flagged() {
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
        g.add_edge(
            b,
            a,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        let report = lint(&g, &LintConfig::default());
        assert!(report
            .findings
            .iter()
            .any(|f| f.rule == "circular-ref" && f.severity == LintSeverity::Error));
    }

    #[test]
    fn test_disable_rule() {
        let mut g = LineageGraph::new();
        g.add_node(make_node("source.unused", "u", NodeType::Source));
        let config = LintConfig {
            unused_source: false,
            ..LintConfig::default()
        };
        let report = lint(&g, &config);
        assert!(!report.findings.iter().any(|f| f.rule == "unused-source"));
    }
}
