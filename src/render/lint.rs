use std::io::Write;

use colored::Colorize;
use serde_json::json;

use crate::graph::lint::{LintReport, LintSeverity};
use crate::render::locations::{sarif_location, Locations};

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

pub fn render_lint_sarif(report: &LintReport, locations: &Locations) {
    render_lint_sarif_to_writer(report, locations, &mut std::io::stdout().lock());
}

/// SARIF 2.1.0 for GitHub code scanning. Every result carries a file location
/// (GitHub drops results without one).
pub fn render_lint_sarif_to_writer<W: Write>(
    report: &LintReport,
    locations: &Locations,
    w: &mut W,
) {
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
                "locations": sarif_location(locations.for_id(&f.unique_id)),
            })
        })
        .collect();
    let sarif = json!({
        "version": "2.1.0",
        "$schema": "https://docs.oasis-open.org/sarif/sarif/v2.1.0/cs01/schemas/sarif-schema-2.1.0.json",
        "runs": [{
            "tool": {"driver": {
                "name": "dbt-lineage",
                "informationUri": "https://github.com/sipemu/dbt-lineage-viewer"
            }},
            "results": results
        }]
    });
    serde_json::to_writer_pretty(&mut *w, &sarif).unwrap();
    writeln!(w).unwrap();
}

pub fn render_lint_codeclimate(report: &LintReport, locations: &Locations) {
    render_lint_codeclimate_to_writer(report, locations, &mut std::io::stdout().lock());
}

/// GitLab Code Quality report (CodeClimate subset), for
/// `artifacts:reports:codequality`.
pub fn render_lint_codeclimate_to_writer<W: Write>(
    report: &LintReport,
    locations: &Locations,
    w: &mut W,
) {
    let issues: Vec<serde_json::Value> = report
        .findings
        .iter()
        .map(|f| {
            let severity = match f.severity {
                LintSeverity::Error => "major",
                LintSeverity::Warning => "minor",
                LintSeverity::Info => "info",
            };
            let fingerprint = blake3::hash(format!("{}:{}", f.rule, f.unique_id).as_bytes());
            json!({
                "description": f.message,
                "check_name": f.rule,
                "fingerprint": fingerprint.to_hex().to_string(),
                "severity": severity,
                "location": {
                    "path": locations.for_id(&f.unique_id),
                    "lines": {"begin": 1}
                }
            })
        })
        .collect();
    serde_json::to_writer_pretty(&mut *w, &issues).unwrap();
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
        render_lint_sarif_to_writer(&report(), &Locations::default(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let results = v["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results[0]["level"], "warning");
        assert_eq!(results[1]["level"], "error");
        // Every result needs a location for GitHub to show it.
        for r in results {
            assert_eq!(
                r["locations"][0]["physicalLocation"]["artifactLocation"]["uri"],
                "dbt_project.yml"
            );
        }
    }

    #[test]
    fn test_codeclimate_shape() {
        let mut buf = Vec::new();
        render_lint_codeclimate_to_writer(&report(), &Locations::default(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let issues = v.as_array().unwrap();
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0]["check_name"], "unused-source");
        assert_eq!(issues[0]["severity"], "minor");
        assert_eq!(issues[1]["severity"], "major");
        assert_eq!(issues[0]["location"]["path"], "dbt_project.yml");
        assert_eq!(issues[0]["location"]["lines"]["begin"], 1);
        assert_eq!(issues[0]["fingerprint"].as_str().unwrap().len(), 64);
        assert_ne!(issues[0]["fingerprint"], issues[1]["fingerprint"]);
    }
}
