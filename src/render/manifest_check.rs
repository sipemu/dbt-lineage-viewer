use std::io::Write;

use colored::Colorize;

use crate::parser::manifest_check::ManifestCheckReport;

pub fn render_check_text(report: &ManifestCheckReport) {
    render_check_text_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_check_text_to_writer<W: Write>(report: &ManifestCheckReport, w: &mut W) {
    if !report.is_stale {
        writeln!(
            w,
            "manifest.json: {} ({} nodes)",
            "in sync".green().bold(),
            report.manifest_node_count
        )
        .unwrap();
        return;
    }
    writeln!(w, "manifest.json: {}", "STALE".red().bold()).unwrap();
    for f in &report.modified {
        writeln!(w, "  modified since manifest: {}", f).unwrap();
    }
    for f in &report.missing {
        writeln!(w, "  referenced but missing:  {}", f).unwrap();
    }
    for f in &report.untracked {
        writeln!(w, "  not in manifest:          {}", f).unwrap();
    }
}

pub fn render_check_json(report: &ManifestCheckReport) {
    render_check_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_check_json_to_writer<W: Write>(report: &ManifestCheckReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> ManifestCheckReport {
        ManifestCheckReport {
            is_stale: false,
            missing: vec![],
            modified: vec![],
            untracked: vec![],
            manifest_node_count: 12,
        }
    }

    fn stale() -> ManifestCheckReport {
        ManifestCheckReport {
            is_stale: true,
            missing: vec!["models/marts/gone.sql".into()],
            modified: vec!["models/marts/orders.sql".into()],
            untracked: vec!["models/marts/new.sql".into()],
            manifest_node_count: 12,
        }
    }

    #[test]
    fn test_render_fresh_text() {
        let mut buf = Vec::new();
        render_check_text_to_writer(&fresh(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("in sync"));
        assert!(s.contains("12 nodes"));
    }

    #[test]
    fn test_render_stale_text() {
        let mut buf = Vec::new();
        render_check_text_to_writer(&stale(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("STALE"));
        assert!(s.contains("models/marts/orders.sql"));
        assert!(s.contains("models/marts/gone.sql"));
        assert!(s.contains("models/marts/new.sql"));
    }

    #[test]
    fn test_render_json_round_trips() {
        let mut buf = Vec::new();
        render_check_json_to_writer(&stale(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["is_stale"], true);
        assert_eq!(parsed["manifest_node_count"], 12);
        assert_eq!(parsed["modified"][0], "models/marts/orders.sql");
    }
}
