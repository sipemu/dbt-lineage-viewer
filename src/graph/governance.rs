//! dbt model governance: access levels, groups, contracts and versions.
//!
//! Read from schema YAML (`access`, `group`, `config.contract.enforced`,
//! column `data_type`, `versions`) and used for two checks:
//! - lint: refs to `access: private` models from outside their group, which
//!   dbt rejects at parse time;
//! - diff: changes that break consumers of a governed model (contract
//!   columns removed or retyped, contract dropped, access narrowed, versions
//!   or governed models removed).

use std::collections::BTreeMap;
use std::path::Path;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde::Serialize;

use super::lint::{LintFinding, LintSeverity};
use super::types::{EdgeType, LineageGraph, NodeType};

/// Governance settings of one model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelGovernance {
    pub access: Option<String>,
    pub group: Option<String>,
    pub contract_enforced: bool,
    /// Columns declared in YAML with their `data_type`, in file order.
    pub columns: Vec<(String, Option<String>)>,
    pub versions: Vec<String>,
}

impl ModelGovernance {
    /// Governed models are the ones other teams may depend on.
    fn is_governed(&self) -> bool {
        self.contract_enforced
            || matches!(self.access.as_deref(), Some("public" | "protected"))
            || !self.versions.is_empty()
    }
}

/// Model name → governance settings.
pub type Governance = BTreeMap<String, ModelGovernance>;

/// Collect governance settings from every schema YAML of the project at `root`.
/// Unreadable projects yield an empty map.
pub fn load_governance(root: &Path) -> Governance {
    let Ok(project) = crate::parser::project::DbtProject::load(root) else {
        return Governance::new();
    };
    let paths = project.resolve_paths(root);
    let Ok(files) = crate::parser::discovery::discover_files(&paths) else {
        return Governance::new();
    };
    let mut gov = Governance::new();
    for path in &files.yaml_files {
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        let Ok(schema) = crate::parser::yaml_schema::parse_schema_file(&content) else {
            continue;
        };
        for m in schema.models {
            let config = m.config.unwrap_or_default();
            let versions = m
                .versions
                .iter()
                .map(|v| match &v.v {
                    serde_yaml::Value::String(s) => s.clone(),
                    other => serde_yaml::to_string(other)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                })
                .collect();
            gov.insert(
                m.name,
                ModelGovernance {
                    access: m.access.or(config.access).map(|a| a.to_lowercase()),
                    group: m.group.or(config.group),
                    contract_enforced: config.contract.is_some_and(|c| c.enforced),
                    columns: m
                        .columns
                        .into_iter()
                        .map(|c| (c.name.to_lowercase(), c.data_type))
                        .collect(),
                    versions,
                },
            );
        }
    }
    gov
}

/// Lint findings for refs to private models from outside their group.
pub fn private_ref_findings(graph: &LineageGraph, gov: &Governance) -> Vec<LintFinding> {
    let mut findings = Vec::new();
    for edge in graph.edge_references() {
        if edge.weight().edge_type != EdgeType::Ref {
            continue;
        }
        let (upstream, downstream) = (&graph[edge.source()], &graph[edge.target()]);
        if downstream.node_type != NodeType::Model {
            continue;
        }
        let Some(target) = gov.get(&upstream.label) else {
            continue;
        };
        if target.access.as_deref() != Some("private") {
            continue;
        }
        let caller_group = gov.get(&downstream.label).and_then(|g| g.group.as_deref());
        if caller_group.is_some() && caller_group == target.group.as_deref() {
            continue;
        }
        findings.push(LintFinding {
            rule: "private-ref",
            severity: LintSeverity::Error,
            unique_id: downstream.unique_id.clone(),
            label: downstream.label.clone(),
            message: format!(
                "Model '{}' refs private model '{}' (group '{}') from outside that group.",
                downstream.label,
                upstream.label,
                target.group.as_deref().unwrap_or("none")
            ),
        });
    }
    findings.sort_by(|a, b| (&a.label, &a.message).cmp(&(&b.label, &b.message)));
    findings
}

/// A change that can break consumers of a governed model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BreakingChange {
    pub model: String,
    /// `model-removed`, `contract-removed`, `contract-column-removed`,
    /// `contract-type-changed`, `access-narrowed` or `version-removed`.
    pub kind: &'static str,
    pub detail: String,
}

fn access_rank(access: Option<&str>) -> u8 {
    match access {
        Some("public") => 2,
        Some("protected") | None => 1, // dbt's default access is protected
        _ => 0,
    }
}

/// Breaking changes between the base and head governance of a project.
/// `head_models` are the model names that exist in the head graph.
pub fn breaking_changes(
    base: &Governance,
    head: &Governance,
    head_graph: &LineageGraph,
) -> Vec<BreakingChange> {
    let head_models: std::collections::HashSet<&str> = head_graph
        .node_indices()
        .filter(|&i| head_graph[i].node_type == NodeType::Model)
        .map(|i| head_graph[i].label.as_str())
        .collect();
    let mut out = Vec::new();
    for (name, b) in base {
        if !b.is_governed() {
            continue;
        }
        let change = |kind, detail: String| BreakingChange {
            model: name.clone(),
            kind,
            detail,
        };
        let Some(h) = head
            .get(name)
            .filter(|_| head_models.contains(name.as_str()))
        else {
            if !head_models.contains(name.as_str()) {
                out.push(change("model-removed", "governed model was removed".into()));
            }
            continue;
        };
        if b.contract_enforced && !h.contract_enforced {
            out.push(change(
                "contract-removed",
                "contract is no longer enforced".into(),
            ));
        }
        if b.contract_enforced && h.contract_enforced {
            for (col, base_type) in &b.columns {
                match h.columns.iter().find(|(c, _)| c == col) {
                    None => out.push(change(
                        "contract-column-removed",
                        format!("column `{col}` removed from the contract"),
                    )),
                    Some((_, head_type)) => {
                        let norm = |t: &Option<String>| t.as_ref().map(|t| t.trim().to_lowercase());
                        if base_type.is_some() && norm(base_type) != norm(head_type) {
                            out.push(change(
                                "contract-type-changed",
                                format!(
                                    "column `{col}`: {} -> {}",
                                    base_type.as_deref().unwrap_or("?"),
                                    head_type.as_deref().unwrap_or("none")
                                ),
                            ));
                        }
                    }
                }
            }
        }
        if access_rank(h.access.as_deref()) < access_rank(b.access.as_deref()) {
            out.push(change(
                "access-narrowed",
                format!(
                    "access {} -> {}",
                    b.access.as_deref().unwrap_or("protected"),
                    h.access.as_deref().unwrap_or("protected")
                ),
            ));
        }
        for v in b.versions.iter().filter(|v| !h.versions.contains(v)) {
            out.push(change("version-removed", format!("version {v} removed")));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::{EdgeData, NodeData};

    fn node(label: &str) -> NodeData {
        NodeData {
            unique_id: format!("model.{label}"),
            label: label.into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn gov(entries: &[(&str, ModelGovernance)]) -> Governance {
        entries
            .iter()
            .map(|(n, g)| (n.to_string(), g.clone()))
            .collect()
    }

    fn graph_of(labels: &[&str], refs: &[(&str, &str)]) -> LineageGraph {
        let mut g = LineageGraph::new();
        let idx: BTreeMap<&str, _> = labels.iter().map(|l| (*l, g.add_node(node(l)))).collect();
        for (up, down) in refs {
            g.add_edge(
                idx[up],
                idx[down],
                EdgeData {
                    edge_type: EdgeType::Ref,
                },
            );
        }
        g
    }

    #[test]
    fn private_refs_outside_group_are_errors() {
        let g = graph_of(
            &["core", "same_team", "other_team"],
            &[("core", "same_team"), ("core", "other_team")],
        );
        let private = ModelGovernance {
            access: Some("private".into()),
            group: Some("finance".into()),
            ..Default::default()
        };
        let finance = ModelGovernance {
            group: Some("finance".into()),
            ..Default::default()
        };
        let findings = private_ref_findings(&g, &gov(&[("core", private), ("same_team", finance)]));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].label, "other_team");
        assert_eq!(findings[0].rule, "private-ref");
        assert_eq!(findings[0].severity, LintSeverity::Error);
    }

    #[test]
    fn contract_access_and_version_changes() {
        let contracted = |cols: &[(&str, &str)], access: &str, versions: &[&str]| ModelGovernance {
            access: Some(access.into()),
            group: None,
            contract_enforced: true,
            columns: cols
                .iter()
                .map(|(c, t)| (c.to_string(), Some(t.to_string())))
                .collect(),
            versions: versions.iter().map(|v| v.to_string()).collect(),
        };
        let base = gov(&[
            (
                "orders",
                contracted(
                    &[("id", "int"), ("amount", "numeric"), ("note", "text")],
                    "public",
                    &["1", "2"],
                ),
            ),
            ("gone", contracted(&[("id", "int")], "public", &[])),
            ("loose", ModelGovernance::default()),
        ]);
        let head = gov(&[(
            "orders",
            contracted(&[("id", "INT"), ("amount", "float")], "protected", &["2"]),
        )]);
        let g = graph_of(&["orders", "loose"], &[]);
        let changes = breaking_changes(&base, &head, &g);
        let kinds: Vec<(&str, &str)> = changes.iter().map(|c| (c.model.as_str(), c.kind)).collect();
        assert_eq!(
            kinds,
            vec![
                ("gone", "model-removed"),
                ("orders", "contract-type-changed"),
                ("orders", "contract-column-removed"),
                ("orders", "access-narrowed"),
                ("orders", "version-removed"),
            ]
        );
    }

    #[test]
    fn dropping_a_contract_is_breaking() {
        let base = gov(&[(
            "m",
            ModelGovernance {
                contract_enforced: true,
                ..Default::default()
            },
        )]);
        let head = gov(&[("m", ModelGovernance::default())]);
        let changes = breaking_changes(&base, &head, &graph_of(&["m"], &[]));
        assert_eq!(changes[0].kind, "contract-removed");
    }

    #[test]
    fn loads_governance_from_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        std::fs::create_dir_all(p.join("models")).unwrap();
        std::fs::write(p.join("dbt_project.yml"), "name: p\nversion: '1'\n").unwrap();
        std::fs::write(
            p.join("models/schema.yml"),
            r#"version: 2
models:
  - name: orders
    access: public
    group: finance
    config:
      contract: {enforced: true}
    columns:
      - name: ID
        data_type: int
    versions:
      - v: 1
      - v: 2
"#,
        )
        .unwrap();
        let g = load_governance(p);
        let o = &g["orders"];
        assert_eq!(o.access.as_deref(), Some("public"));
        assert_eq!(o.group.as_deref(), Some("finance"));
        assert!(o.contract_enforced);
        assert_eq!(o.columns, vec![("id".to_string(), Some("int".to_string()))]);
        assert_eq!(o.versions, vec!["1", "2"]);
    }
}
