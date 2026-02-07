use std::io::Write;

use colored::Colorize;

use crate::graph::impact::{ImpactReport, ImpactSeverity};

/// Render impact report as colored text to stdout
pub fn render_impact_text(report: &ImpactReport) {
    render_impact_text_to_writer(report, &mut std::io::stdout().lock());
}

fn severity_color(severity: ImpactSeverity) -> colored::Color {
    match severity {
        ImpactSeverity::Low => colored::Color::Green,
        ImpactSeverity::Medium => colored::Color::Yellow,
        ImpactSeverity::High => colored::Color::Red,
        ImpactSeverity::Critical => colored::Color::BrightRed,
    }
}

pub fn render_impact_text_to_writer<W: Write>(report: &ImpactReport, w: &mut W) {
    writeln!(w).unwrap();
    writeln!(
        w,
        "{}",
        format!("Impact Analysis: {}", report.source_model).bold()
    )
    .unwrap();
    writeln!(w, "{}", "=".repeat(50)).unwrap();

    let severity_str = report
        .overall_severity
        .label()
        .to_uppercase()
        .color(severity_color(report.overall_severity))
        .bold();
    writeln!(w, "Overall Severity: {}", severity_str).unwrap();
    writeln!(w).unwrap();

    writeln!(w, "{}", "Summary:".bold()).unwrap();
    writeln!(w, "  Affected models:    {}", report.affected_models).unwrap();
    writeln!(w, "  Affected tests:     {}", report.affected_tests).unwrap();
    writeln!(w, "  Affected exposures: {}", report.affected_exposures).unwrap();
    writeln!(
        w,
        "  Longest path:       {} hops",
        report.longest_path_length
    )
    .unwrap();
    writeln!(w).unwrap();

    if !report.longest_path.is_empty() {
        writeln!(w, "{}", "Longest Path:".bold()).unwrap();
        writeln!(w, "  {}", report.longest_path.join(" -> ")).unwrap();
        writeln!(w).unwrap();
    }

    if !report.impacted_nodes.is_empty() {
        writeln!(w, "{}", "Impacted Nodes:".bold()).unwrap();
        for node in &report.impacted_nodes {
            let sev = node.severity.label().color(severity_color(node.severity));
            writeln!(
                w,
                "  [{:<8}] {} ({}, {} hops)",
                sev, node.label, node.node_type, node.distance
            )
            .unwrap();
        }
    }

    writeln!(w).unwrap();
}

/// Render impact report as JSON to stdout
pub fn render_impact_json(report: &ImpactReport) {
    render_impact_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_impact_json_to_writer<W: Write>(report: &ImpactReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::impact::{ImpactReport, ImpactSeverity, ImpactedNode};

    fn make_report() -> ImpactReport {
        ImpactReport {
            source_model: "stg_orders".to_string(),
            overall_severity: ImpactSeverity::Critical,
            affected_models: 1,
            affected_tests: 1,
            affected_exposures: 1,
            longest_path_length: 3,
            longest_path: vec![
                "stg_orders".to_string(),
                "orders".to_string(),
                "dashboard".to_string(),
            ],
            impacted_nodes: vec![
                ImpactedNode {
                    unique_id: "exposure.dashboard".to_string(),
                    label: "dashboard".to_string(),
                    node_type: "exposure".to_string(),
                    severity: ImpactSeverity::Critical,
                    distance: 2,
                },
                ImpactedNode {
                    unique_id: "model.orders".to_string(),
                    label: "orders".to_string(),
                    node_type: "model".to_string(),
                    severity: ImpactSeverity::High,
                    distance: 1,
                },
                ImpactedNode {
                    unique_id: "test.orders_positive".to_string(),
                    label: "orders_positive".to_string(),
                    node_type: "test".to_string(),
                    severity: ImpactSeverity::Low,
                    distance: 2,
                },
            ],
        }
    }

    #[test]
    fn test_render_impact_text() {
        let report = make_report();
        let mut buf = Vec::new();
        render_impact_text_to_writer(&report, &mut buf);
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("Impact Analysis: stg_orders"));
        assert!(output.contains("Affected models:    1"));
        assert!(output.contains("Affected tests:     1"));
        assert!(output.contains("Affected exposures: 1"));
        assert!(output.contains("Longest Path:"));
        assert!(output.contains("stg_orders -> orders -> dashboard"));
        assert!(output.contains("Impacted Nodes:"));
    }

    #[test]
    fn test_render_impact_json() {
        let report = make_report();
        let mut buf = Vec::new();
        render_impact_json_to_writer(&report, &mut buf);
        let output = String::from_utf8(buf).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["source_model"], "stg_orders");
        assert_eq!(parsed["overall_severity"], "critical");
        assert_eq!(parsed["affected_models"], 1);
        assert_eq!(parsed["impacted_nodes"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_render_impact_text_empty() {
        let report = ImpactReport {
            source_model: "isolated".to_string(),
            overall_severity: ImpactSeverity::Low,
            affected_models: 0,
            affected_tests: 0,
            affected_exposures: 0,
            longest_path_length: 0,
            longest_path: vec![],
            impacted_nodes: vec![],
        };
        let mut buf = Vec::new();
        render_impact_text_to_writer(&report, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Impact Analysis: isolated"));
        assert!(output.contains("Affected models:    0"));
    }

    #[test]
    fn test_severity_color_all_levels() {
        assert_eq!(severity_color(ImpactSeverity::Low), colored::Color::Green);
        assert_eq!(
            severity_color(ImpactSeverity::Medium),
            colored::Color::Yellow
        );
        assert_eq!(severity_color(ImpactSeverity::High), colored::Color::Red);
        assert_eq!(
            severity_color(ImpactSeverity::Critical),
            colored::Color::BrightRed
        );
    }

    #[test]
    fn test_render_impact_text_medium_severity() {
        let report = ImpactReport {
            source_model: "stg_payments".to_string(),
            overall_severity: ImpactSeverity::Medium,
            affected_models: 2,
            affected_tests: 0,
            affected_exposures: 0,
            longest_path_length: 2,
            longest_path: vec!["stg_payments".to_string(), "payments".to_string()],
            impacted_nodes: vec![ImpactedNode {
                unique_id: "model.payments".to_string(),
                label: "payments".to_string(),
                node_type: "model".to_string(),
                severity: ImpactSeverity::Medium,
                distance: 1,
            }],
        };
        let mut buf = Vec::new();
        render_impact_text_to_writer(&report, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Impact Analysis: stg_payments"));
        assert!(output.contains("MEDIUM"));
        assert!(output.contains("Affected models:    2"));
        assert!(output.contains("Longest Path:"));
        assert!(output.contains("stg_payments -> payments"));
        assert!(output.contains("Impacted Nodes:"));
        assert!(output.contains("payments"));
    }
}
