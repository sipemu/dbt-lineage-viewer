//! Platform-agnostic CI support for `dbt-lineage ci`.
//!
//! Everything a PR/MR integration needs lives here so that the per-platform
//! wrappers (GitHub Action, GitLab component, Bitbucket Pipe, Azure task) stay
//! thin: detect the CI platform and target branch from the environment, run
//! the analyses, render one Markdown report, and decide whether the job fails.

pub mod annotations;
pub mod comment;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;

use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::Serialize;

use crate::graph::diff::{DiffStatus, LineageDiff};
use crate::graph::lint::{LintReport, LintSeverity};
use crate::graph::plan::PlanReport;
use crate::graph::summary::SummaryReport;
use crate::graph::types::{LineageGraph, NodeType};
use crate::parser::manifest_check::ManifestCheckReport;

/// Hidden marker so wrappers can find and update their previous comment.
pub const COMMENT_MARKER: &str = "<!-- dbt-lineage -->";

/// Max lint findings / changed nodes listed individually before truncating.
const MAX_ROWS: usize = 25;

/// Max nodes drawn in the impact diagram before truncating.
const MAX_DIAGRAM_NODES: usize = 40;

/// CI platform the process is running on, detected from well-known env vars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Platform {
    GitHub,
    GitLab,
    Bitbucket,
    AzureDevOps,
    Unknown,
}

impl Platform {
    pub fn detect(env: &HashMap<String, String>) -> Self {
        let set = |k: &str| env.get(k).is_some_and(|v| !v.is_empty());
        if set("GITHUB_ACTIONS") {
            Platform::GitHub
        } else if set("GITLAB_CI") {
            Platform::GitLab
        } else if set("BITBUCKET_BUILD_NUMBER") {
            Platform::Bitbucket
        } else if set("TF_BUILD") {
            Platform::AzureDevOps
        } else {
            Platform::Unknown
        }
    }

    /// Target branch of the current PR/MR, if the platform exposes one.
    pub fn target_branch(self, env: &HashMap<String, String>) -> Option<String> {
        let key = match self {
            Platform::GitHub => "GITHUB_BASE_REF",
            Platform::GitLab => "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
            Platform::Bitbucket => "BITBUCKET_PR_DESTINATION_BRANCH",
            Platform::AzureDevOps => "SYSTEM_PULLREQUEST_TARGETBRANCH",
            Platform::Unknown => return None,
        };
        env.get(key)
            .map(|v| v.trim().trim_start_matches("refs/heads/").to_string())
            .filter(|v| !v.is_empty())
    }
}

/// Conditions that make `dbt-lineage ci report` exit non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum FailOn {
    /// Any error-severity lint finding.
    LintErrors,
    /// A base ref was given but the rebuild plan is empty.
    PlanEmpty,
    /// `target/manifest.json` (or `--manifest`) is out of sync with the SQL files.
    ManifestStale,
    /// A governed model (contract, public/protected access, versions) changed
    /// in a way that breaks its consumers.
    BreakingChanges,
    /// Never fail (report only).
    Never,
}

/// One section that could not be computed, kept so the report can say so
/// instead of silently dropping it.
#[derive(Debug, Clone, Serialize)]
pub struct SectionError {
    pub section: &'static str,
    pub message: String,
}

/// Everything the CI report renders. Each analysis is optional: `None` means
/// it was not requested (or failed, see `errors`).
#[derive(Debug, Default)]
pub struct CiReport {
    pub summary: Option<SummaryReport>,
    pub diff: Option<LineageDiff>,
    pub plan: Option<PlanReport>,
    pub lint: Option<LintReport>,
    pub manifest_check: Option<ManifestCheckReport>,
    /// Mermaid diagram of changed nodes and their downstream (see [`impact_mermaid`]).
    pub impact_diagram: Option<String>,
    /// Models in the impact path whose lineage may be incomplete.
    pub lineage_gaps: Vec<crate::graph::gaps::LineageGap>,
    pub errors: Vec<SectionError>,
}

impl CiReport {
    /// Human-readable reasons the job should fail, given the `--fail-on` set.
    pub fn failures(&self, fail_on: &[FailOn]) -> Vec<String> {
        let mut out = Vec::new();
        for cond in fail_on {
            match cond {
                FailOn::LintErrors => {
                    if let Some(lint) = &self.lint {
                        let n = lint.count_by_severity(LintSeverity::Error);
                        if n > 0 {
                            out.push(format!("lint reported {n} error-severity finding(s)"));
                        }
                    }
                }
                FailOn::PlanEmpty => {
                    if let Some(plan) = &self.plan {
                        if plan.selectors.is_empty() {
                            out.push("rebuild plan is empty".to_string());
                        }
                    }
                }
                FailOn::Never => {}
                FailOn::BreakingChanges => {
                    if let Some(diff) = &self.diff {
                        let n = diff.breaking_changes.len();
                        if n > 0 {
                            out.push(format!("{n} breaking change(s) to governed models"));
                        }
                    }
                }
                FailOn::ManifestStale => {
                    if let Some(check) = &self.manifest_check {
                        if check.is_stale {
                            out.push("manifest.json is stale".to_string());
                        }
                    }
                }
            }
        }
        out
    }

    /// Selector for `dbt run -s`, empty when there is no plan.
    pub fn selector(&self) -> String {
        self.plan
            .as_ref()
            .map(|p| p.selector_string())
            .unwrap_or_default()
    }
}

/// Render the report as GitHub/GitLab/Bitbucket-compatible Markdown.
pub fn render_markdown(report: &CiReport) -> String {
    let mut md = String::new();
    let _ = writeln!(md, "{COMMENT_MARKER}");
    let _ = writeln!(md, "## dbt-lineage report\n");

    if let Some(diff) = report
        .diff
        .as_ref()
        .filter(|d| !d.breaking_changes.is_empty())
    {
        let _ = writeln!(md, "### Breaking changes\n");
        let _ = writeln!(
            md,
            "Changes to governed models (contracts, access, versions) that can break their consumers.\n"
        );
        let _ = writeln!(md, "| Model | Change | Detail |");
        let _ = writeln!(md, "| --- | --- | --- |");
        for c in &diff.breaking_changes {
            let _ = writeln!(md, "| `{}` | {} | {} |", c.model, c.kind, escape(&c.detail));
        }
        md.push('\n');
    }
    if let Some(diff) = &report.diff {
        render_diff_section(&mut md, diff);
        render_column_impact_section(&mut md, diff);
    }
    render_gaps_section(&mut md, &report.lineage_gaps);
    if let Some(diagram) = &report.impact_diagram {
        let _ = writeln!(md, "### Impact\n");
        let _ = writeln!(
            md,
            "Changed nodes (green: added, yellow: modified) and everything downstream of them.\n"
        );
        let _ = writeln!(md, "{diagram}");
    }
    if let Some(plan) = &report.plan {
        render_plan_section(&mut md, plan);
    }
    if let Some(check) = &report.manifest_check {
        render_manifest_section(&mut md, check);
    }
    if let Some(lint) = &report.lint {
        render_lint_section(&mut md, lint);
    }
    if let Some(summary) = &report.summary {
        render_summary_section(&mut md, summary);
    }
    if !report.errors.is_empty() {
        let _ = writeln!(md, "### Skipped sections\n");
        for e in &report.errors {
            let _ = writeln!(md, "- **{}**: {}", e.section, escape(&e.message));
        }
        md.push('\n');
    }
    md.trim_end().to_string() + "\n"
}

fn render_diff_section(md: &mut String, diff: &LineageDiff) {
    let s = &diff.summary;
    let _ = writeln!(md, "### Changes vs. `{}`\n", diff.base_ref);
    if s.nodes_added + s.nodes_removed + s.nodes_modified + s.edges_added + s.edges_removed == 0 {
        let _ = writeln!(md, "No lineage changes.\n");
        return;
    }
    let _ = writeln!(
        md,
        "**{}** added · **{}** modified · **{}** removed nodes · **{}** added / **{}** removed edges\n",
        s.nodes_added, s.nodes_modified, s.nodes_removed, s.edges_added, s.edges_removed
    );
    let changed: Vec<_> = diff
        .nodes
        .iter()
        .filter(|n| n.status != DiffStatus::Unchanged)
        .collect();
    if changed.is_empty() {
        return;
    }
    let _ = writeln!(md, "| Status | Node | Type | Details |");
    let _ = writeln!(md, "| --- | --- | --- | --- |");
    for n in changed.iter().take(MAX_ROWS) {
        let _ = writeln!(
            md,
            "| {} | `{}` | {} | {} |",
            n.status.label(),
            n.label,
            n.node_type,
            escape(&n.changes.join("; "))
        );
    }
    if changed.len() > MAX_ROWS {
        let _ = writeln!(md, "\n…and {} more.", changed.len() - MAX_ROWS);
    }
    md.push('\n');
}

fn render_column_impact_section(md: &mut String, diff: &LineageDiff) {
    if diff.column_impacts.is_empty() {
        return;
    }
    let _ = writeln!(md, "### Column impact\n");
    let _ = writeln!(
        md,
        "These columns were removed (or renamed) but downstream columns are computed from them.\n"
    );
    let _ = writeln!(md, "| Removed column | Used by |");
    let _ = writeln!(md, "| --- | --- |");
    for i in diff.column_impacts.iter().take(MAX_ROWS) {
        let used_by: Vec<String> = i.downstream.iter().map(|d| format!("`{d}`")).collect();
        let _ = writeln!(
            md,
            "| `{}.{}` | {} |",
            i.model,
            i.column,
            used_by.join(", ")
        );
    }
    if diff.column_impacts.len() > MAX_ROWS {
        let _ = writeln!(md, "\n…and {} more.", diff.column_impacts.len() - MAX_ROWS);
    }
    md.push('\n');
}

fn render_gaps_section(md: &mut String, gaps: &[crate::graph::gaps::LineageGap]) {
    use crate::graph::gaps::GapReason;
    if gaps.is_empty() {
        return;
    }
    let _ = writeln!(
        md,
        "### ⚠️ Lineage may be incomplete
"
    );
    let _ = writeln!(
        md,
        "dbt-lineage reads SQL without running dbt. In these models on the impact path, Jinja \
         decides part of the lineage at render time, so the impact above may miss some \
         dependencies or columns.\n"
    );
    let _ = writeln!(md, "| Model | Why |");
    let _ = writeln!(md, "| --- | --- |");
    for gap in gaps.iter().take(MAX_ROWS) {
        let why: Vec<String> = gap
            .reasons
            .iter()
            .map(|r| match r {
                GapReason::DynamicRef(call) => {
                    format!("dependency computed at render time: `{}`", escape(call))
                }
                GapReason::GeneratedSql => "columns written by a Jinja loop or macro".to_string(),
            })
            .collect();
        let _ = writeln!(md, "| `{}` | {} |", gap.model, why.join("; "));
    }
    if gaps.len() > MAX_ROWS {
        let _ = writeln!(md, "\n…and {} more.", gaps.len() - MAX_ROWS);
    }
    md.push('\n');
}

fn render_plan_section(md: &mut String, plan: &PlanReport) {
    let _ = writeln!(md, "### Rebuild selector\n");
    if plan.selectors.is_empty() {
        let _ = writeln!(md, "Nothing to rebuild.\n");
        return;
    }
    let _ = writeln!(
        md,
        "```sh\ndbt build -s \"{}\"\n```\n",
        plan.selector_string()
    );
}

fn render_manifest_section(md: &mut String, check: &ManifestCheckReport) {
    let _ = writeln!(md, "### Manifest freshness\n");
    if !check.is_stale {
        let _ = writeln!(md, "`manifest.json` is in sync with the SQL files.\n");
        return;
    }
    let _ = writeln!(
        md,
        "`manifest.json` is **stale**: {} modified, {} missing, {} untracked file(s). Run `dbt compile`.\n",
        check.modified.len(),
        check.missing.len(),
        check.untracked.len()
    );
}

fn render_lint_section(md: &mut String, lint: &LintReport) {
    let errors = lint.count_by_severity(LintSeverity::Error);
    let warnings = lint.count_by_severity(LintSeverity::Warning);
    let infos = lint.count_by_severity(LintSeverity::Info);
    let _ = writeln!(md, "### Lint\n");
    if lint.is_clean() {
        let _ = writeln!(md, "No findings.\n");
        return;
    }
    let _ = writeln!(
        md,
        "**{errors}** error(s) · **{warnings}** warning(s) · **{infos}** info\n"
    );
    // Errors first, then warnings, then info.
    let mut findings: Vec<_> = lint.findings.iter().collect();
    findings.sort_by_key(|f| match f.severity {
        LintSeverity::Error => 0,
        LintSeverity::Warning => 1,
        LintSeverity::Info => 2,
    });
    let _ = writeln!(md, "<details><summary>Findings</summary>\n");
    let _ = writeln!(md, "| Severity | Rule | Node | Message |");
    let _ = writeln!(md, "| --- | --- | --- | --- |");
    for f in findings.iter().take(MAX_ROWS) {
        let sev = match f.severity {
            LintSeverity::Error => "error",
            LintSeverity::Warning => "warning",
            LintSeverity::Info => "info",
        };
        let _ = writeln!(
            md,
            "| {sev} | `{}` | `{}` | {} |",
            f.rule,
            f.label,
            escape(&f.message)
        );
    }
    if findings.len() > MAX_ROWS {
        let _ = writeln!(md, "\n…and {} more.", findings.len() - MAX_ROWS);
    }
    let _ = writeln!(md, "\n</details>\n");
}

fn render_summary_section(md: &mut String, summary: &SummaryReport) {
    let mut text = Vec::new();
    crate::render::summary::render_summary_text_to_writer(summary, &mut text);
    let text = strip_ansi(&String::from_utf8_lossy(&text));
    let _ = writeln!(md, "<details><summary>Project summary</summary>\n");
    let _ = writeln!(md, "```text\n{}\n```\n", text.trim());
    let _ = writeln!(md, "</details>\n");
}

/// Mermaid flowchart of added/modified nodes plus everything downstream of
/// them in `head`, styled by status. Tests and phantom nodes are left out to
/// keep it readable. `None` when no node in `head` changed.
/// Changed nodes and everything downstream of them in `head`, breadth-first
/// in a stable order (tests and phantom nodes left out).
pub fn impact_scope(diff: &LineageDiff, head: &LineageGraph) -> Vec<NodeIndex> {
    let changed: HashSet<&str> = diff
        .nodes
        .iter()
        .filter(|n| matches!(n.status, DiffStatus::Added | DiffStatus::Modified))
        .map(|n| n.unique_id.as_str())
        .collect();
    let drawable =
        |idx: NodeIndex| !matches!(head[idx].node_type, NodeType::Test | NodeType::Phantom);
    let by_label = |a: &NodeIndex, b: &NodeIndex| head[*a].label.cmp(&head[*b].label);
    let mut seeds: Vec<NodeIndex> = head
        .node_indices()
        .filter(|&i| drawable(i) && changed.contains(head[i].unique_id.as_str()))
        .collect();
    seeds.sort_by(by_label);
    let mut seen: HashSet<NodeIndex> = seeds.iter().copied().collect();
    let mut queue: VecDeque<NodeIndex> = seeds.into_iter().collect();
    let mut order = Vec::new();
    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        let mut next: Vec<NodeIndex> = head
            .neighbors_directed(idx, Direction::Outgoing)
            .filter(|&n| drawable(n) && !seen.contains(&n))
            .collect();
        next.sort_by(by_label);
        next.dedup();
        for n in next {
            if seen.insert(n) {
                queue.push_back(n);
            }
        }
    }
    order
}

pub fn impact_mermaid(diff: &LineageDiff, head: &LineageGraph) -> Option<String> {
    let changed: HashMap<&str, DiffStatus> = diff
        .nodes
        .iter()
        .filter(|n| matches!(n.status, DiffStatus::Added | DiffStatus::Modified))
        .map(|n| (n.unique_id.as_str(), n.status))
        .collect();
    let mut order = impact_scope(diff, head);
    if order.is_empty() {
        return None;
    }
    let total = order.len();
    order.truncate(MAX_DIAGRAM_NODES);
    let ids: HashMap<NodeIndex, usize> = order.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    let mut out = String::from("```mermaid\nflowchart LR\n");
    for (i, &idx) in order.iter().enumerate() {
        let node = &head[idx];
        let class = match changed.get(node.unique_id.as_str()) {
            Some(DiffStatus::Added) => "added",
            Some(_) => "modified",
            None => "impacted",
        };
        let label = node.display_name().replace('"', "#quot;");
        let _ = writeln!(out, "  n{i}[\"{label}\"]:::{class}");
    }
    let mut edges = HashSet::new();
    for &idx in &order {
        for e in head.edges_directed(idx, Direction::Outgoing) {
            if let (Some(&a), Some(&b)) = (ids.get(&idx), ids.get(&e.target())) {
                if edges.insert((a, b)) {
                    let _ = writeln!(out, "  n{a} --> n{b}");
                }
            }
        }
    }
    out.push_str("  classDef added fill:#dafbe1,stroke:#1a7f37,color:#1f2328\n");
    out.push_str("  classDef modified fill:#fff8c5,stroke:#9a6700,color:#1f2328\n");
    out.push_str("  classDef impacted fill:#f6f8fa,stroke:#8c959f,color:#1f2328\n");
    out.push_str("```\n");
    if total > MAX_DIAGRAM_NODES {
        let _ = writeln!(
            out,
            "\nShowing {MAX_DIAGRAM_NODES} of {total} changed and downstream nodes."
        );
    }
    Some(out)
}

/// Keep table cells on one line and stop `|` from splitting columns.
fn escape(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

fn strip_ansi(s: &str) -> String {
    let re = regex::Regex::new(r"\x1b\[[0-9;]*m").unwrap();
    re.replace_all(s, "").into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::diff::{DiffNode, DiffSummary};
    use crate::graph::lint::LintFinding;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn detects_platforms() {
        assert_eq!(
            Platform::detect(&env(&[("GITHUB_ACTIONS", "true")])),
            Platform::GitHub
        );
        assert_eq!(
            Platform::detect(&env(&[("GITLAB_CI", "true")])),
            Platform::GitLab
        );
        assert_eq!(
            Platform::detect(&env(&[("BITBUCKET_BUILD_NUMBER", "7")])),
            Platform::Bitbucket
        );
        assert_eq!(
            Platform::detect(&env(&[("TF_BUILD", "True")])),
            Platform::AzureDevOps
        );
        assert_eq!(Platform::detect(&env(&[])), Platform::Unknown);
        assert_eq!(
            Platform::detect(&env(&[("GITHUB_ACTIONS", "")])),
            Platform::Unknown
        );
    }

    #[test]
    fn target_branch_per_platform() {
        let e = env(&[
            ("GITHUB_BASE_REF", "main"),
            ("CI_MERGE_REQUEST_TARGET_BRANCH_NAME", "develop"),
            ("BITBUCKET_PR_DESTINATION_BRANCH", "master"),
            ("SYSTEM_PULLREQUEST_TARGETBRANCH", "refs/heads/release"),
        ]);
        assert_eq!(Platform::GitHub.target_branch(&e).as_deref(), Some("main"));
        assert_eq!(
            Platform::GitLab.target_branch(&e).as_deref(),
            Some("develop")
        );
        assert_eq!(
            Platform::Bitbucket.target_branch(&e).as_deref(),
            Some("master")
        );
        assert_eq!(
            Platform::AzureDevOps.target_branch(&e).as_deref(),
            Some("release")
        );
        assert_eq!(Platform::Unknown.target_branch(&e), None);
        // Push builds on GitHub set GITHUB_BASE_REF to an empty string.
        assert_eq!(
            Platform::GitHub.target_branch(&env(&[("GITHUB_BASE_REF", "")])),
            None
        );
    }

    fn lint_with(sev: LintSeverity) -> LintReport {
        LintReport {
            findings: vec![LintFinding {
                rule: "dead-end-model",
                severity: sev,
                unique_id: "model.p.a".into(),
                label: "a".into(),
                message: "no | consumers".into(),
            }],
        }
    }

    fn plan(selectors: &[&str]) -> PlanReport {
        PlanReport {
            base_ref: "main".into(),
            head_ref: "feature".into(),
            selectors: selectors.iter().map(|s| s.to_string()).collect(),
            affected_count: selectors.len(),
        }
    }

    #[test]
    fn failures_respect_fail_on() {
        let report = CiReport {
            lint: Some(lint_with(LintSeverity::Error)),
            plan: Some(plan(&[])),
            ..Default::default()
        };
        assert!(report.failures(&[]).is_empty());
        assert_eq!(report.failures(&[FailOn::LintErrors]).len(), 1);
        assert_eq!(
            report
                .failures(&[FailOn::LintErrors, FailOn::PlanEmpty])
                .len(),
            2
        );
        // Not computed → not a failure.
        assert!(report.failures(&[FailOn::ManifestStale]).is_empty());

        let warn_only = CiReport {
            lint: Some(lint_with(LintSeverity::Warning)),
            plan: Some(plan(&["+a"])),
            ..Default::default()
        };
        assert!(warn_only
            .failures(&[FailOn::LintErrors, FailOn::PlanEmpty])
            .is_empty());
    }

    #[test]
    fn markdown_contains_sections_and_marker() {
        let report = CiReport {
            diff: Some(LineageDiff {
                base_ref: "origin/main".into(),
                head_ref: "HEAD".into(),
                summary: DiffSummary {
                    nodes_added: 1,
                    ..Default::default()
                },
                nodes: vec![DiffNode {
                    unique_id: "model.p.new".into(),
                    label: "new".into(),
                    node_type: "model".into(),
                    status: DiffStatus::Added,
                    changes: vec![],
                }],
                edges: vec![],
                column_impacts: vec![],
                breaking_changes: vec![],
            }),
            plan: Some(plan(&["+new"])),
            lint: Some(lint_with(LintSeverity::Warning)),
            errors: vec![SectionError {
                section: "summary",
                message: "boom".into(),
            }],
            ..Default::default()
        };
        let md = render_markdown(&report);
        assert!(md.starts_with(COMMENT_MARKER));
        assert!(md.contains("### Changes vs. `origin/main`"));
        assert!(md.contains("| added | `new` | model |"));
        assert!(md.contains("dbt build -s \"+new\""));
        assert!(md.contains("no \\| consumers"));
        assert!(md.contains("- **summary**: boom"));
        assert_eq!(report.selector(), "+new");
    }

    #[test]
    fn impact_mermaid_draws_changed_and_downstream() {
        use crate::graph::types::{EdgeData, EdgeType, NodeData};
        let node = |id: &str, label: &str, node_type| NodeData {
            unique_id: id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        let mut g = LineageGraph::new();
        let stg = g.add_node(node("model.stg", "stg", NodeType::Model));
        let mart = g.add_node(node("model.mart", "mart", NodeType::Model));
        let test = g.add_node(node("test.t", "t", NodeType::Test));
        let other = g.add_node(node("model.other", "other", NodeType::Model));
        let edge = |edge_type| EdgeData { edge_type };
        g.add_edge(stg, mart, edge(EdgeType::Ref));
        g.add_edge(stg, test, edge(EdgeType::Test));
        g.add_edge(other, mart, edge(EdgeType::Ref));

        let diff = LineageDiff {
            base_ref: "main".into(),
            head_ref: "HEAD".into(),
            summary: DiffSummary {
                nodes_modified: 1,
                ..Default::default()
            },
            nodes: vec![DiffNode {
                unique_id: "model.stg".into(),
                label: "stg".into(),
                node_type: "model".into(),
                status: DiffStatus::Modified,
                changes: vec!["SQL changed".into()],
            }],
            edges: vec![],
            column_impacts: vec![],
            breaking_changes: vec![],
        };
        let md = impact_mermaid(&diff, &g).unwrap();
        assert!(md.starts_with("```mermaid\nflowchart LR\n"));
        assert!(md.contains("n0[\"stg\"]:::modified"));
        assert!(md.contains("n1[\"mart\"]:::impacted"));
        assert!(md.contains("n0 --> n1"));
        // Tests and unrelated upstream nodes are not drawn.
        assert!(!md.contains("\"t\""));
        assert!(!md.contains("other"));

        let unchanged = LineageDiff {
            nodes: vec![],
            ..diff
        };
        assert!(impact_mermaid(&unchanged, &g).is_none());
    }

    #[test]
    fn markdown_empty_diff_and_plan() {
        let report = CiReport {
            diff: Some(LineageDiff {
                base_ref: "main".into(),
                head_ref: "HEAD".into(),
                summary: DiffSummary::default(),
                nodes: vec![],
                edges: vec![],
                column_impacts: vec![],
                breaking_changes: vec![],
            }),
            plan: Some(plan(&[])),
            lint: Some(LintReport { findings: vec![] }),
            ..Default::default()
        };
        let md = render_markdown(&report);
        assert!(md.contains("No lineage changes."));
        assert!(md.contains("Nothing to rebuild."));
        assert!(md.contains("No findings."));
    }
}
