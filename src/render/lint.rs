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
///
/// GitLab requires `description`, `check_name`, `fingerprint`, `severity`
/// (`info`, `minor`, `major`, `critical`, `blocker`), `location.path`
/// relative to the repository root and `location.lines.begin`. GitLab keys
/// findings by fingerprint to tell an MR's new findings from existing ones,
/// so the fingerprint hashes rule and node (stable across commits and
/// message rewording) and is made unique when a node repeats a finding.
pub fn render_lint_codeclimate_to_writer<W: Write>(
    report: &LintReport,
    locations: &Locations,
    w: &mut W,
) {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let issues: Vec<serde_json::Value> = report
        .findings
        .iter()
        .map(|f| {
            let (severity, category) = match f.severity {
                LintSeverity::Error => ("major", "Bug Risk"),
                LintSeverity::Warning => ("minor", "Style"),
                LintSeverity::Info => ("info", "Style"),
            };
            let key = format!("{}:{}", f.rule, f.unique_id);
            let n = seen.entry(key.clone()).or_insert(0);
            let key = if *n == 0 {
                key
            } else {
                format!("{key}:{n}:{}", f.message)
            };
            *n += 1;
            json!({
                "type": "issue",
                "description": f.message,
                "check_name": f.rule,
                "categories": [category],
                "fingerprint": blake3::hash(key.as_bytes()).to_hex().to_string(),
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

    /// Check a report against GitLab's Code Quality schema: required fields
    /// and their types, the severity enum, repository-relative paths and
    /// unique fingerprints.
    fn assert_gitlab_code_quality(v: &serde_json::Value) {
        let issues = v.as_array().expect("top level is an array");
        let mut fingerprints = std::collections::HashSet::new();
        for i in issues {
            for field in ["description", "check_name", "fingerprint", "severity"] {
                assert!(i[field].is_string(), "{field} missing in {i}");
            }
            assert!(matches!(
                i["severity"].as_str().unwrap(),
                "info" | "minor" | "major" | "critical" | "blocker"
            ));
            let path = i["location"]["path"].as_str().expect("location.path");
            assert!(!path.is_empty());
            assert!(!path.starts_with('/') && !path.starts_with("./"), "{path}");
            assert!(!path.contains('\\') && !path.contains("/./"), "{path}");
            assert!(i["location"]["lines"]["begin"].as_u64().unwrap() >= 1);
            assert!(
                fingerprints.insert(i["fingerprint"].as_str().unwrap().to_string()),
                "duplicate fingerprint in {i}"
            );
        }
    }

    fn codeclimate(r: &LintReport, loc: &Locations) -> serde_json::Value {
        let mut buf = Vec::new();
        render_lint_codeclimate_to_writer(r, loc, &mut buf);
        serde_json::from_slice(&buf).unwrap()
    }

    #[test]
    fn test_codeclimate_shape() {
        let v = codeclimate(&report(), &Locations::default());
        assert_gitlab_code_quality(&v);
        let issues = v.as_array().unwrap();
        assert_eq!(issues.len(), 2);
        assert_eq!(issues[0]["type"], "issue");
        assert_eq!(issues[0]["check_name"], "unused-source");
        assert_eq!(issues[0]["description"], "Source 'raw.x' is unused.");
        assert_eq!(issues[0]["severity"], "minor");
        assert_eq!(issues[1]["severity"], "major");
        assert_eq!(issues[0]["location"]["path"], "dbt_project.yml");
        assert_eq!(issues[0]["location"]["lines"]["begin"], 1);
        assert_eq!(issues[0]["fingerprint"].as_str().unwrap().len(), 64);
    }

    #[test]
    fn test_codeclimate_empty_report_is_empty_array() {
        let v = codeclimate(&LintReport { findings: vec![] }, &Locations::default());
        assert_eq!(v, serde_json::json!([]));
    }

    #[test]
    fn test_codeclimate_fingerprints_stable_and_unique() {
        // The same rule on the same node twice: still distinct fingerprints.
        let mut r = report();
        let mut dup = r.findings[0].clone();
        dup.message = "Source 'raw.x' is unused (again).".into();
        r.findings.push(dup);
        assert_gitlab_code_quality(&codeclimate(&r, &Locations::default()));

        // Stable across runs and independent of the message wording, so an
        // MR that rewords a message doesn't show the finding as new.
        let mut reworded = report();
        reworded.findings[0].message = "Unused source.".into();
        let base = codeclimate(&report(), &Locations::default());
        assert_eq!(
            base[0]["fingerprint"],
            codeclimate(&reworded, &Locations::default())[0]["fingerprint"]
        );
        assert_eq!(base, codeclimate(&report(), &Locations::default()));
    }

    #[test]
    fn test_codeclimate_paths_relative_to_repo_root() {
        use crate::graph::types::{LineageGraph, NodeData, NodeType};
        let mut g = LineageGraph::new();
        g.add_node(NodeData {
            unique_id: "source.raw.x".into(),
            label: "raw.x".into(),
            node_type: NodeType::Source,
            file_path: Some("/repo/dbt/./models/sources.yml".into()),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let loc = Locations::from_graph(&g, std::path::Path::new("/repo/dbt"), "dbt/");
        let v = codeclimate(&report(), &loc);
        assert_gitlab_code_quality(&v);
        assert_eq!(v[0]["location"]["path"], "dbt/models/sources.yml");
        // Undefined node no model references: the project file.
        assert_eq!(v[1]["location"]["path"], "dbt/dbt_project.yml");
    }
}
