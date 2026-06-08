//! Render the macro call graph from `graph::macros::MacroGraph`.

use std::io::Write;

use colored::Colorize;

use crate::graph::macros::MacroGraph;

pub fn render_macros_text(graph: &MacroGraph) {
    render_macros_text_to_writer(graph, &mut std::io::stdout().lock());
}

pub fn render_macros_text_to_writer<W: Write>(graph: &MacroGraph, w: &mut W) {
    writeln!(w, "{} {} defined", "Macros:".bold(), graph.macros.len()).unwrap();
    for m in &graph.macros {
        if m.calls.is_empty() {
            writeln!(w, "  {}", m.name).unwrap();
        } else {
            writeln!(w, "  {} → {}", m.name, m.calls.join(", ")).unwrap();
        }
    }
    let orphans = graph.orphans();
    if !orphans.is_empty() {
        writeln!(w).unwrap();
        writeln!(
            w,
            "{} {}",
            "Orphan macros (defined but not called by other macros):".bold(),
            orphans.len()
        )
        .unwrap();
        for o in orphans {
            writeln!(w, "  {}", o.name).unwrap();
        }
    }
}

pub fn render_macros_json(graph: &MacroGraph) {
    render_macros_json_to_writer(graph, &mut std::io::stdout().lock());
}

pub fn render_macros_json_to_writer<W: Write>(graph: &MacroGraph, w: &mut W) {
    serde_json::to_writer_pretty(&mut *w, graph).unwrap();
    writeln!(w).unwrap();
}

/// Render as a Mermaid flowchart.
pub fn render_macros_mermaid(graph: &MacroGraph) {
    render_macros_mermaid_to_writer(graph, &mut std::io::stdout().lock());
}

pub fn render_macros_mermaid_to_writer<W: Write>(graph: &MacroGraph, w: &mut W) {
    writeln!(w, "flowchart LR").unwrap();
    if graph.macros.is_empty() {
        return;
    }
    for m in &graph.macros {
        writeln!(w, "    {0}[\"{0}\"]", sanitize(&m.name)).unwrap();
    }
    writeln!(w).unwrap();
    for m in &graph.macros {
        for c in &m.calls {
            writeln!(w, "    {} --> {}", sanitize(&m.name), sanitize(c)).unwrap();
        }
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::macros::{MacroEntry, MacroGraph};

    fn fixture() -> MacroGraph {
        MacroGraph {
            macros: vec![
                MacroEntry {
                    name: "a".into(),
                    file: "macros/a.sql".into(),
                    calls: vec!["b".into()],
                },
                MacroEntry {
                    name: "b".into(),
                    file: "macros/b.sql".into(),
                    calls: vec![],
                },
            ],
        }
    }

    #[test]
    fn test_text_lists_calls_and_orphans() {
        let mut buf = Vec::new();
        render_macros_text_to_writer(&fixture(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("a → b"));
        assert!(s.contains("Orphan macros"));
        assert!(s.contains("a"));
    }

    #[test]
    fn test_json_round_trips() {
        let mut buf = Vec::new();
        render_macros_json_to_writer(&fixture(), &mut buf);
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["macros"][0]["name"], "a");
        assert_eq!(v["macros"][0]["calls"][0], "b");
    }

    #[test]
    fn test_mermaid_includes_nodes_and_edges() {
        let mut buf = Vec::new();
        render_macros_mermaid_to_writer(&fixture(), &mut buf);
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("flowchart LR"));
        assert!(s.contains("a[\"a\"]"));
        assert!(s.contains("a --> b"));
    }
}
