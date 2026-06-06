use std::io::Write;

use colored::Colorize;
use serde_json::json;

use crate::graph::coverage::CoverageReport;

pub fn render_coverage_text(report: &CoverageReport) {
    render_coverage_text_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_coverage_text_to_writer<W: Write>(report: &CoverageReport, w: &mut W) {
    writeln!(
        w,
        "{} ({} models, {} with at least one test = {:.1}%)",
        "Coverage".bold(),
        report.total_models,
        report.models_with_any_test,
        report.coverage_pct
    )
    .unwrap();

    let untested: Vec<&str> = report
        .models
        .iter()
        .filter(|m| !m.has_any_test())
        .map(|m| m.model.as_str())
        .collect();
    if !untested.is_empty() {
        writeln!(w).unwrap();
        writeln!(w, "{} {}", "Untested models:".bold(), untested.len()).unwrap();
        writeln!(w, "  {}", untested.join(", ")).unwrap();
    }

    let low_col_coverage: Vec<&_> = report
        .models
        .iter()
        .filter(|m| m.columns_total > 0 && m.column_coverage_pct() < 50.0)
        .collect();
    if !low_col_coverage.is_empty() {
        writeln!(w).unwrap();
        writeln!(
            w,
            "{} {}",
            "Models with column coverage < 50%:".bold(),
            low_col_coverage.len()
        )
        .unwrap();
        for m in low_col_coverage {
            writeln!(
                w,
                "  {:<30} {:>3}/{:<3} columns tested ({:.0}%)",
                m.model,
                m.columns_tested,
                m.columns_total,
                m.column_coverage_pct()
            )
            .unwrap();
        }
    }
}

pub fn render_coverage_json(report: &CoverageReport) {
    render_coverage_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_coverage_json_to_writer<W: Write>(report: &CoverageReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

/// Emit minimal SARIF for models with no tests, so CI can surface them as PR
/// annotations via GitHub's code-scanning integration.
pub fn render_coverage_sarif(report: &CoverageReport) {
    render_coverage_sarif_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_coverage_sarif_to_writer<W: Write>(report: &CoverageReport, w: &mut W) {
    let results: Vec<serde_json::Value> = report
        .models
        .iter()
        .filter(|m| !m.has_any_test())
        .map(|m| {
            json!({
                "ruleId": "untested-model",
                "level": "warning",
                "message": {"text": format!("Model '{}' has no tests in its schema YAML.", m.model)},
                "locations": [{
                    "physicalLocation": {
                        "artifactLocation": {"uri": format!("models/{}.sql", m.model)}
                    }
                }]
            })
        })
        .collect();

    let sarif = json!({
        "version": "2.1.0",
        "$schema": "https://docs.oasis-open.org/sarif/sarif/v2.1.0/cs01/schemas/sarif-schema-2.1.0.json",
        "runs": [{
            "tool": {
                "driver": {
                    "name": "dbt-lineage",
                    "informationUri": "https://github.com/sipemu/dbt-lineage-viewer",
                    "rules": [{
                        "id": "untested-model",
                        "shortDescription": {"text": "Model has no tests"},
                        "helpUri": "https://docs.getdbt.com/docs/build/tests"
                    }]
                }
            },
            "results": results
        }]
    });

    serde_json::to_writer_pretty(&mut *w, &sarif).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::coverage::{CoverageReport, ModelCoverage};

    fn fixture() -> CoverageReport {
        CoverageReport {
            total_models: 3,
            models_with_any_test: 1,
            coverage_pct: 33.33,
            models: vec![
                ModelCoverage {
                    model: "orders".into(),
                    generic_tests: 2,
                    custom_tests: 0,
                    columns_total: 4,
                    columns_tested: 1,
                },
                ModelCoverage {
                    model: "customers".into(),
                    generic_tests: 0,
                    custom_tests: 0,
                    columns_total: 0,
                    columns_tested: 0,
                },
                ModelCoverage {
                    model: "products".into(),
                    generic_tests: 0,
                    custom_tests: 0,
                    columns_total: 2,
                    columns_tested: 0,
                },
            ],
        }
    }

    #[test]
    fn test_text_includes_summary_and_untested_list() {
        let mut buf = Vec::new();
        render_coverage_text_to_writer(&fixture(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Coverage"));
        assert!(s.contains("3 models"));
        assert!(s.contains("Untested"));
        assert!(s.contains("customers"));
        assert!(s.contains("products"));
        // orders has tests → not in untested list
        assert!(s.contains("orders")); // appears in low-coverage section
    }

    #[test]
    fn test_json_round_trips() {
        let mut buf = Vec::new();
        render_coverage_json_to_writer(&fixture(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["total_models"], 3);
        assert_eq!(v["models_with_any_test"], 1);
    }

    #[test]
    fn test_sarif_flags_untested_models() {
        let mut buf = Vec::new();
        render_coverage_sarif_to_writer(&fixture(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let results = v["runs"][0]["results"].as_array().unwrap();
        // 2 untested models (customers, products). orders has tests so excluded.
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["ruleId"], "untested-model");
    }
}
