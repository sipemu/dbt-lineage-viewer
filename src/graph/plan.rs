use serde::Serialize;

use super::diff::{DiffStatus, LineageDiff};

/// Style of dbt selector expression to emit.
#[derive(Debug, Clone, Copy)]
pub enum PlanStyle {
    /// `+model` form: include the changed model AND its downstream. Default.
    Downstream,
    /// `model` form: only the changed models themselves, no downstream rebuild.
    LeafOnly,
}

/// Output of [`build_plan`]: the dbt selector tokens plus a count of affected nodes.
#[derive(Debug, Clone, Serialize)]
pub struct PlanReport {
    pub base_ref: String,
    pub head_ref: String,
    pub selectors: Vec<String>,
    pub affected_count: usize,
}

impl PlanReport {
    /// Render as a space-separated selector string suitable for `dbt run -s "..."`.
    pub fn selector_string(&self) -> String {
        self.selectors.join(" ")
    }
}

/// Build a rebuild plan from a precomputed `LineageDiff`. Only Added/Modified models
/// trigger rebuild work; removed models can't be rebuilt and tests are excluded by
/// default since dbt rebuilds them automatically with their parents.
pub fn build_plan(diff: &LineageDiff, style: PlanStyle, exclude_tests: bool) -> PlanReport {
    let mut selectors: Vec<String> = Vec::new();
    let mut seen_labels: std::collections::HashSet<String> = std::collections::HashSet::new();

    for node in &diff.nodes {
        if !matches!(node.status, DiffStatus::Added | DiffStatus::Modified) {
            continue;
        }
        if exclude_tests && node.node_type == "test" {
            continue;
        }
        // dbt selector syntax uses bare model names, not unique_ids.
        if !seen_labels.insert(node.label.clone()) {
            continue;
        }
        let prefix = match style {
            PlanStyle::Downstream => "+",
            PlanStyle::LeafOnly => "",
        };
        selectors.push(format!("{}{}", prefix, node.label));
    }

    selectors.sort();

    PlanReport {
        base_ref: diff.base_ref.clone(),
        head_ref: diff.head_ref.clone(),
        affected_count: selectors.len(),
        selectors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::diff::{DiffEdge, DiffNode, DiffStatus, DiffSummary, LineageDiff};

    fn node(label: &str, node_type: &str, status: DiffStatus) -> DiffNode {
        DiffNode {
            unique_id: format!("model.{}", label),
            label: label.into(),
            node_type: node_type.into(),
            status,
            changes: vec![],
        }
    }

    fn diff_with(nodes: Vec<DiffNode>) -> LineageDiff {
        LineageDiff {
            base_ref: "main".into(),
            head_ref: "HEAD".into(),
            summary: DiffSummary::default(),
            nodes,
            edges: Vec::<DiffEdge>::new(),
        }
    }

    #[test]
    fn test_added_and_modified_nodes_become_selectors() {
        let diff = diff_with(vec![
            node("a", "model", DiffStatus::Added),
            node("b", "model", DiffStatus::Modified),
            node("c", "model", DiffStatus::Unchanged),
            node("d", "model", DiffStatus::Removed),
        ]);
        let plan = build_plan(&diff, PlanStyle::Downstream, true);
        assert_eq!(plan.selectors, vec!["+a", "+b"]);
        assert_eq!(plan.affected_count, 2);
    }

    #[test]
    fn test_leaf_only_omits_plus_prefix() {
        let diff = diff_with(vec![node("a", "model", DiffStatus::Modified)]);
        let plan = build_plan(&diff, PlanStyle::LeafOnly, true);
        assert_eq!(plan.selectors, vec!["a"]);
    }

    #[test]
    fn test_tests_excluded_by_default() {
        let diff = diff_with(vec![
            node("a", "model", DiffStatus::Modified),
            node("t", "test", DiffStatus::Added),
        ]);
        let plan = build_plan(&diff, PlanStyle::Downstream, true);
        assert_eq!(plan.selectors, vec!["+a"]);
    }

    #[test]
    fn test_tests_kept_when_requested() {
        let diff = diff_with(vec![
            node("a", "model", DiffStatus::Modified),
            node("t", "test", DiffStatus::Added),
        ]);
        let plan = build_plan(&diff, PlanStyle::Downstream, false);
        assert_eq!(plan.selectors, vec!["+a", "+t"]);
    }

    #[test]
    fn test_no_duplicates() {
        let diff = diff_with(vec![
            node("a", "model", DiffStatus::Added),
            node("a", "model", DiffStatus::Modified),
        ]);
        let plan = build_plan(&diff, PlanStyle::Downstream, true);
        assert_eq!(plan.selectors, vec!["+a"]);
    }

    #[test]
    fn test_empty_diff_yields_empty_plan() {
        let diff = diff_with(vec![]);
        let plan = build_plan(&diff, PlanStyle::Downstream, true);
        assert!(plan.selectors.is_empty());
        assert_eq!(plan.affected_count, 0);
        assert_eq!(plan.selector_string(), "");
    }

    #[test]
    fn test_selector_string_joins_with_space() {
        let diff = diff_with(vec![
            node("a", "model", DiffStatus::Added),
            node("b", "model", DiffStatus::Modified),
        ]);
        let plan = build_plan(&diff, PlanStyle::Downstream, true);
        assert_eq!(plan.selector_string(), "+a +b");
    }
}
