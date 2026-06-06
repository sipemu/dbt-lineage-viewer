use std::io::Write;

use colored::Colorize;

use crate::graph::summary::SummaryReport;

pub fn render_summary_text(report: &SummaryReport) {
    render_summary_text_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_summary_text_to_writer<W: Write>(report: &SummaryReport, w: &mut W) {
    let header = match &report.project_name {
        Some(name) => format!("Project: {}", name),
        None => "Project Summary".to_string(),
    };
    writeln!(w, "{}", header.bold()).unwrap();
    writeln!(w, "{}", "=".repeat(50)).unwrap();

    writeln!(
        w,
        "Models:    {:<4} Sources:   {:<4} Exposures: {}",
        report.models, report.sources, report.exposures
    )
    .unwrap();
    writeln!(
        w,
        "Seeds:     {:<4} Snapshots: {:<4} Tests:     {}",
        report.seeds, report.snapshots, report.tests
    )
    .unwrap();
    writeln!(w).unwrap();

    if !report.tags.is_empty() {
        let tag_list: Vec<String> = report
            .tags
            .iter()
            .map(|t| format!("{} ({})", t.tag, t.count))
            .collect();
        writeln!(w, "{} {}", "Tags:".bold(), tag_list.join(", ")).unwrap();
        writeln!(w).unwrap();
    }

    if !report.top_downstream.is_empty() {
        writeln!(w, "{}", "Top downstream impact:".bold()).unwrap();
        let pad = report
            .top_downstream
            .iter()
            .map(|r| r.label.len())
            .max()
            .unwrap_or(0);
        for r in &report.top_downstream {
            writeln!(
                w,
                "  {:<width$} → {} downstream",
                r.label,
                r.downstream_count,
                width = pad
            )
            .unwrap();
        }
        writeln!(w).unwrap();
    }

    if !report.orphans.is_empty() {
        writeln!(
            w,
            "{} {}",
            "Orphan models (no downstream):".bold(),
            report.orphans.len()
        )
        .unwrap();
        writeln!(w, "  {}", report.orphans.join(", ")).unwrap();
    }
}

pub fn render_summary_json(report: &SummaryReport) {
    render_summary_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_summary_json_to_writer<W: Write>(report: &SummaryReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::summary::{ModelReach, SummaryReport, TagCount};

    fn report() -> SummaryReport {
        SummaryReport {
            project_name: Some("jaffle_shop".into()),
            total_nodes: 12,
            models: 6,
            sources: 3,
            seeds: 1,
            snapshots: 0,
            tests: 1,
            exposures: 1,
            tags: vec![
                TagCount {
                    tag: "finance".into(),
                    count: 3,
                },
                TagCount {
                    tag: "staging".into(),
                    count: 2,
                },
            ],
            top_downstream: vec![
                ModelReach {
                    unique_id: "model.stg_orders".into(),
                    label: "stg_orders".into(),
                    downstream_count: 6,
                },
                ModelReach {
                    unique_id: "model.stg_payments".into(),
                    label: "stg_payments".into(),
                    downstream_count: 5,
                },
            ],
            orphans: vec!["customer_lifetime_value".into(), "daily_revenue".into()],
        }
    }

    #[test]
    fn test_render_text_includes_all_sections() {
        let mut buf = Vec::new();
        render_summary_text_to_writer(&report(), &mut buf);
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("Project: jaffle_shop"));
        assert!(out.contains("Models:    6"));
        assert!(out.contains("Sources:   3"));
        assert!(out.contains("Exposures: 1"));
        assert!(out.contains("finance (3)"));
        assert!(out.contains("staging (2)"));
        assert!(out.contains("stg_orders"));
        assert!(out.contains("→ 6 downstream"));
        assert!(out.contains("Orphan models"));
        assert!(out.contains("customer_lifetime_value, daily_revenue"));
    }

    #[test]
    fn test_render_text_no_project_name() {
        let mut r = report();
        r.project_name = None;
        let mut buf = Vec::new();
        render_summary_text_to_writer(&r, &mut buf);
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("Project Summary"));
    }

    #[test]
    fn test_render_text_empty_report() {
        let r = SummaryReport {
            project_name: None,
            total_nodes: 0,
            models: 0,
            sources: 0,
            seeds: 0,
            snapshots: 0,
            tests: 0,
            exposures: 0,
            tags: vec![],
            top_downstream: vec![],
            orphans: vec![],
        };
        let mut buf = Vec::new();
        render_summary_text_to_writer(&r, &mut buf);
        let out = String::from_utf8(buf).unwrap();
        // Should not crash and should not include tag/top/orphan sections.
        assert!(!out.contains("Tags:"));
        assert!(!out.contains("Top downstream"));
        assert!(!out.contains("Orphan models"));
    }

    #[test]
    fn test_render_json_round_trips() {
        let mut buf = Vec::new();
        render_summary_json_to_writer(&report(), &mut buf);
        let out = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["project_name"], "jaffle_shop");
        assert_eq!(parsed["models"], 6);
        assert_eq!(parsed["tags"][0]["tag"], "finance");
        assert_eq!(parsed["top_downstream"][0]["label"], "stg_orders");
        assert_eq!(parsed["orphans"][0], "customer_lifetime_value");
    }
}
