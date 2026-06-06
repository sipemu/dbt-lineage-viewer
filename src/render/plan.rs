use std::io::Write;

use crate::graph::plan::PlanReport;

/// Print the dbt selector string to stdout. Trailing newline only when non-empty so
/// shell pipelines like `dbt run -s "$(dbt-lineage plan --base main)"` don't choke.
pub fn render_plan_selector(report: &PlanReport) {
    render_plan_selector_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_plan_selector_to_writer<W: Write>(report: &PlanReport, w: &mut W) {
    if report.selectors.is_empty() {
        // Empty output (no newline) so callers can detect "nothing to rebuild" easily.
        return;
    }
    writeln!(w, "{}", report.selector_string()).unwrap();
}

pub fn render_plan_json(report: &PlanReport) {
    render_plan_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_plan_json_to_writer<W: Write>(report: &PlanReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> PlanReport {
        PlanReport {
            base_ref: "main".into(),
            head_ref: "HEAD".into(),
            selectors: vec!["+a".into(), "+b".into()],
            affected_count: 2,
        }
    }

    #[test]
    fn test_selector_output() {
        let mut buf = Vec::new();
        render_plan_selector_to_writer(&report(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "+a +b\n");
    }

    #[test]
    fn test_empty_plan_emits_nothing() {
        let r = PlanReport {
            base_ref: "main".into(),
            head_ref: "HEAD".into(),
            selectors: vec![],
            affected_count: 0,
        };
        let mut buf = Vec::new();
        render_plan_selector_to_writer(&r, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_json_round_trips() {
        let mut buf = Vec::new();
        render_plan_json_to_writer(&report(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["selectors"][0], "+a");
        assert_eq!(v["affected_count"], 2);
    }
}
