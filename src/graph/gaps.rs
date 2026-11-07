//! Where SQL-parse lineage may be incomplete: models whose dependencies are
//! computed at render time, or whose SELECT is written by Jinja. Reported in
//! the CI comment so a short impact list isn't mistaken for a complete one.

use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use petgraph::stable_graph::NodeIndex;

use super::types::{LineageGraph, NodeType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapReason {
    /// A `ref()` / `source()` with computed arguments, e.g. `ref(var('x'))`.
    DynamicRef(String),
    /// A loop or macro writes part of the SELECT: column lineage may miss
    /// columns.
    GeneratedSql,
    /// The SQL depends on warehouse data (`dbt_utils.get_column_values()`,
    /// `run_query()`, …), e.g. a pivot with a column per value: columns it
    /// writes from query results are unknown without dbt.
    DataDependent(String),
}

static DATA_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b((?:dbt_utils\.)?(?:get_column_values|get_query_results_as_dict|get_single_value)|run_query)\s*\(",
    )
    .unwrap()
});

/// The first call in `sql` that reads warehouse data at render time.
fn data_dependent_call(sql: &str) -> Option<String> {
    let sql = JINJA_COMMENT.replace_all(sql, "");
    DATA_CALL.captures(&sql).map(|c| format!("{}()", &c[1]))
}

static JINJA_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{#.*?#\}").unwrap());

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageGap {
    pub model: String,
    pub reasons: Vec<GapReason>,
}

/// Check the SQL models among `nodes`, reading files relative to `root`.
pub fn lineage_gaps(
    graph: &LineageGraph,
    root: &Path,
    nodes: impl IntoIterator<Item = NodeIndex>,
) -> Vec<LineageGap> {
    let mut gaps: Vec<LineageGap> = nodes
        .into_iter()
        .filter_map(|idx| {
            let node = &graph[idx];
            if !matches!(node.node_type, NodeType::Model | NodeType::Snapshot) {
                return None;
            }
            let path = node
                .file_path
                .as_ref()
                .filter(|p| p.extension().is_some_and(|e| e == "sql"))?;
            let sql = std::fs::read_to_string(root.join(path)).ok()?;
            let mut reasons: Vec<GapReason> = crate::parser::loops::unresolved_dynamic_calls(&sql)
                .into_iter()
                .map(GapReason::DynamicRef)
                .collect();
            if crate::parser::ast_column_lineage::sql_generated_by_jinja(&sql) {
                reasons.push(GapReason::GeneratedSql);
            }
            if let Some(call) = data_dependent_call(&sql) {
                reasons.push(GapReason::DataDependent(call));
            }
            (!reasons.is_empty()).then(|| LineageGap {
                model: node.label.clone(),
                reasons,
            })
        })
        .collect();
    gaps.sort_by(|a, b| a.model.cmp(&b.model));
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::NodeData;

    #[test]
    fn reports_dynamic_refs_and_generated_sql() {
        let tmp = tempfile::tempdir().unwrap();
        let files = [
            ("plain.sql", "select id from {{ ref('a') }}"),
            ("dynamic.sql", "select id from {{ ref(var('orders')) }}"),
            (
                "looped.sql",
                "select {% for c in cols %}{{ c }},{% endfor %} id from {{ ref('a') }}",
            ),
            (
                "pivot.sql",
                "{# run_query() in a comment #}{% set vals = dbt_utils.get_column_values(ref('a'), 'k') %}select {% for v in vals %}{{ v }},{% endfor %} id from {{ ref('a') }}",
            ),
        ];
        let mut g = LineageGraph::new();
        let mut idx = Vec::new();
        for (file, sql) in files {
            std::fs::write(tmp.path().join(file), sql).unwrap();
            idx.push(g.add_node(NodeData {
                unique_id: format!("model.{}", file.trim_end_matches(".sql")),
                label: file.trim_end_matches(".sql").into(),
                node_type: NodeType::Model,
                file_path: Some(file.into()),
                description: None,
                materialization: None,
                tags: vec![],
                columns: vec![],
            }));
        }
        let gaps = lineage_gaps(&g, tmp.path(), idx);
        assert_eq!(
            gaps,
            vec![
                LineageGap {
                    model: "dynamic".into(),
                    reasons: vec![GapReason::DynamicRef("ref(var('orders'))".into())],
                },
                LineageGap {
                    model: "looped".into(),
                    reasons: vec![GapReason::GeneratedSql],
                },
                LineageGap {
                    model: "pivot".into(),
                    reasons: vec![
                        GapReason::GeneratedSql,
                        GapReason::DataDependent("dbt_utils.get_column_values()".into()),
                    ],
                },
            ]
        );
    }
}
