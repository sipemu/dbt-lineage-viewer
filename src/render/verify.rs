use std::io::Write;

use colored::Colorize;

use crate::graph::verify::{EdgeMismatch, VerifyReport};

/// How many mismatches per list the text report prints before summarising.
const SHOWN: usize = 50;

pub fn render_verify_text(report: &VerifyReport) {
    render_verify_text_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_verify_text_to_writer<W: Write>(report: &VerifyReport, w: &mut W) {
    writeln!(
        w,
        "Compared {} models/snapshots against manifest.json",
        report.nodes_compared
    )
    .unwrap();
    writeln!(
        w,
        "  dependencies found: {}/{} ({:.1}%), not confirmed by dbt: {}",
        report.matched_edges,
        report.manifest_edges,
        report.recall(),
        report.extra.len()
    )
    .unwrap();
    if report.is_exact() {
        writeln!(w, "{}", "Lineage matches dbt exactly.".green().bold()).unwrap();
        return;
    }
    if report.found_everything() {
        writeln!(
            w,
            "{}",
            "Every dependency dbt knows about was found.".green().bold()
        )
        .unwrap();
    }
    edge_list(
        w,
        "Missed (in manifest, not found by parsing)",
        &report.missing,
    );
    edge_list(
        w,
        "Extra (found by parsing, not in manifest)",
        &report.extra,
    );
    id_list(
        w,
        "Models dbt knows that parsing did not find",
        &report.only_in_manifest,
    );
    id_list(
        w,
        "Models parsing found that the manifest lacks (disabled, or stale manifest)",
        &report.only_in_parse,
    );
    if !report.missing.is_empty() {
        writeln!(
            w,
            "\nMissed dependencies come from refs computed at render time (variables, macro \
             return values). Use --manifest with other commands for exact lineage on these models."
        )
        .unwrap();
    }
    if !report.extra.is_empty() {
        writeln!(
            w,
            "\nExtra dependencies usually sit in Jinja branches switched off by the current \
             vars or target ({{% if var(...) %}}). dbt records only the branches it renders; \
             dbt-lineage keeps them so impact analysis does not miss them."
        )
        .unwrap();
    }
}

fn edge_list<W: Write>(w: &mut W, title: &str, edges: &[EdgeMismatch]) {
    if edges.is_empty() {
        return;
    }
    writeln!(w, "\n{} ({}):", title.bold(), edges.len()).unwrap();
    for e in edges.iter().take(SHOWN) {
        let file = e.file.as_deref().unwrap_or("");
        writeln!(w, "  {} <- {}  {}", e.child, e.parent, file.dimmed()).unwrap();
    }
    if edges.len() > SHOWN {
        writeln!(
            w,
            "  … and {} more (use -o json for all)",
            edges.len() - SHOWN
        )
        .unwrap();
    }
}

fn id_list<W: Write>(w: &mut W, title: &str, ids: &[String]) {
    if ids.is_empty() {
        return;
    }
    writeln!(w, "\n{} ({}):", title.bold(), ids.len()).unwrap();
    for id in ids.iter().take(SHOWN) {
        writeln!(w, "  {id}").unwrap();
    }
    if ids.len() > SHOWN {
        writeln!(
            w,
            "  … and {} more (use -o json for all)",
            ids.len() - SHOWN
        )
        .unwrap();
    }
}

pub fn render_verify_json(report: &VerifyReport) {
    render_verify_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_verify_json_to_writer<W: Write>(report: &VerifyReport, w: &mut W) {
    let mut value = serde_json::to_value(report).unwrap();
    value["recall"] = serde_json::json!((report.recall() * 10.0).round() / 10.0);
    value["precision"] = serde_json::json!((report.precision() * 10.0).round() / 10.0);
    serde_json::to_writer_pretty(&mut *w, &value).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> VerifyReport {
        VerifyReport {
            nodes_compared: 2,
            manifest_edges: 2,
            parsed_edges: 1,
            matched_edges: 1,
            missing: vec![EdgeMismatch {
                child: "model.fct".into(),
                parent: "model.stg".into(),
                file: Some("models/fct.sql".into()),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn text_lists_missing_edges() {
        colored::control::set_override(false);
        let mut out = Vec::new();
        render_verify_text_to_writer(&report(), &mut out);
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("dependencies found: 1/2 (50.0%)"));
        assert!(s.contains("model.fct <- model.stg  models/fct.sql"));
    }

    #[test]
    fn text_exact_match() {
        colored::control::set_override(false);
        let mut out = Vec::new();
        render_verify_text_to_writer(&VerifyReport::default(), &mut out);
        assert!(String::from_utf8(out)
            .unwrap()
            .contains("matches dbt exactly"));
    }

    #[test]
    fn json_includes_rates() {
        let mut out = Vec::new();
        render_verify_json_to_writer(&report(), &mut out);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["recall"], 50.0);
        assert_eq!(v["precision"], 100.0);
        assert_eq!(v["missing"][0]["parent"], "model.stg");
    }
}
