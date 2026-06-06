use std::io::Write;

use colored::Colorize;

use crate::graph::perf::PerfReport;

pub fn render_perf_text(report: &PerfReport, top: usize) {
    render_perf_text_to_writer(report, top, &mut std::io::stdout().lock());
}

pub fn render_perf_text_to_writer<W: Write>(report: &PerfReport, top: usize, w: &mut W) {
    writeln!(
        w,
        "{} {} measured models, total runtime {}",
        "Performance:".bold(),
        report.measured_models,
        fmt_duration(report.total_runtime_seconds)
    )
    .unwrap();
    writeln!(w).unwrap();

    if !report.slowest.is_empty() {
        writeln!(w, "{}", format!("Top {} slowest models:", top).bold()).unwrap();
        for m in report.slowest.iter().take(top) {
            writeln!(
                w,
                "  {:<28} {:>10} (+ {} downstream)",
                m.label,
                fmt_duration(m.seconds),
                m.downstream_count
            )
            .unwrap();
        }
        writeln!(w).unwrap();
    }

    if !report.critical_paths.is_empty() {
        writeln!(
            w,
            "{}",
            "Longest critical paths (own + slowest upstream chain):".bold()
        )
        .unwrap();
        for m in report.critical_paths.iter().take(top) {
            writeln!(
                w,
                "  {:<28} {:>10} (own {})",
                m.label,
                fmt_duration(m.critical_path_seconds),
                fmt_duration(m.seconds)
            )
            .unwrap();
        }
    }
}

pub fn render_perf_json(report: &PerfReport) {
    render_perf_json_to_writer(report, &mut std::io::stdout().lock());
}

pub fn render_perf_json_to_writer<W: Write>(report: &PerfReport, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, report).unwrap();
    writeln!(w).unwrap();
}

fn fmt_duration(seconds: f64) -> String {
    if seconds < 1.0 {
        format!("{}ms", (seconds * 1000.0).round() as i64)
    } else if seconds < 60.0 {
        format!("{:.1}s", seconds)
    } else {
        let m = (seconds / 60.0).floor() as i64;
        let s = (seconds % 60.0).round() as i64;
        format!("{}m {:02}s", m, s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::perf::{ModelTiming, PerfReport};

    fn fixture() -> PerfReport {
        PerfReport {
            total_runtime_seconds: 65.0,
            measured_models: 2,
            slowest: vec![
                ModelTiming {
                    unique_id: "model.slow".into(),
                    label: "slow".into(),
                    seconds: 45.0,
                    critical_path_seconds: 45.0,
                    downstream_count: 3,
                },
                ModelTiming {
                    unique_id: "model.fast".into(),
                    label: "fast".into(),
                    seconds: 20.0,
                    critical_path_seconds: 20.0,
                    downstream_count: 0,
                },
            ],
            critical_paths: vec![],
        }
    }

    #[test]
    fn test_text_shows_summary_and_slowest() {
        let mut buf = Vec::new();
        render_perf_text_to_writer(&fixture(), 5, &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Performance"));
        assert!(s.contains("slow"));
        assert!(s.contains("45.0s"));
    }

    #[test]
    fn test_fmt_duration_branches() {
        assert_eq!(fmt_duration(0.5), "500ms");
        assert_eq!(fmt_duration(12.3), "12.3s");
        assert_eq!(fmt_duration(95.0), "1m 35s");
    }
}
