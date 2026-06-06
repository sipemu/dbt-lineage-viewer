use std::io::Write;

use colored::Colorize;
use serde_json::json;

use crate::graph::lint::{LintReport, LintSeverity};

pub fn render_lint_text(report: &LintReport) {
    render_lint_text_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_lint_text_to_writer<W: Write>(report: &LintReport, w: &mut W) {
    if report.is_clean() {
        writeln!(w, "{} no findings", "lint:".bold()).unwrap();
        return;
    }
    let errors = report.count_by_severity(LintSeverity::Error);
    let warnings = report.count_by_severity(LintSeverity::Warning);
    let infos = report.count_by_severity(LintSeverity::Info);
    writeln!(
        w,
        "{} {} error · {} warning · {} info",
        "lint:".bold(),
        errors,
        warnings,
        infos
    )
    .unwrap();
    for f in &report.findings {
        let tag = match f.severity {
            LintSeverity::Error => "ERROR".red().bold(),
            LintSeverity::Warning => "WARN ".yellow().bold(),
            LintSeverity::Info => "INFO ".dimmed(),
        };
        writeln!(w, "  [{}] {:<22} {} — {}", tag, f.rule, f.label, f.message).unwrap();
    }
}

pub fn render_lint_json(report: &LintReport) {
    render_lint_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_lint_json_to_writer<W: Write>(report: &LintReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

pub fn render_lint_sarif(report: &LintReport) {
    render_lint_sarif_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_lint_sarif_to_writer<W: Write>(report: &LintReport, w: &mut W) {
    let results: Vec<serde_json::Value> = report
        .findings
        .iter()
        .map(|f| {
            let level = match f.severity {
                LintSeverity::Error => "error",
                LintSeverity::Warning => "warning",
                LintSeverity::Info => "note",
            };
            json!({
                "ruleId": f.rule,
                "level": level,
                "message": {"text": f.message},
            })
        })
        .collect();
    let sarif = json!({
        "version": "2.1.0",
        "$schema": "https://docs.oasis-open.org/sarif/sarif/v2.1.0/cs01/schemas/sarif-schema-2.1.0.json",
        "runs": [{
            "tool": {"driver": {"name": "dbt-lineage"}},
            "results": results
        }]
    });
    serde_json::to_writer_pretty(&mut *w, &sarif).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::lint::{LintFinding, LintReport};

    fn report() -> LintReport {
        LintReport {
            findings: vec![
                LintFinding {
                    rule: "unused-source",
                    severity: LintSeverity::Warning,
                    unique_id: "source.raw.x".into(),
                    label: "raw.x".into(),
                    message: "Source 'raw.x' is unused.".into(),
                },
                LintFinding {
                    rule: "undefined-source",
                    severity: LintSeverity::Error,
                    unique_id: "phantom.y".into(),
                    label: "y".into(),
                    message: "Reference 'y' is undefined.".into(),
                },
            ],
        }
    }

    #[test]
    fn test_text_clean_report() {
        let mut buf = Vec::new();
        render_lint_text_to_writer(&LintReport { findings: vec![] }, &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("no findings"));
    }

    #[test]
    fn test_text_includes_each_rule() {
        let mut buf = Vec::new();
        render_lint_text_to_writer(&report(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("unused-source"));
        assert!(s.contains("undefined-source"));
    }

    #[test]
    fn test_json_round_trips() {
        let mut buf = Vec::new();
        render_lint_json_to_writer(&report(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["findings"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_sarif_maps_severity() {
        let mut buf = Vec::new();
        render_lint_sarif_to_writer(&report(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let results = v["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results[0]["level"], "warning");
        assert_eq!(results[1]["level"], "error");
    }
}
