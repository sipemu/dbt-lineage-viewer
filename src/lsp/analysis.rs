//! Editor-independent analysis behind the language server: find `ref()` /
//! `source()` calls in a buffer and answer hover, definition, code lens and
//! diagnostics questions against a project's lineage graph.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::Result;
use petgraph::stable_graph::NodeIndex;
use petgraph::Direction;
use regex::Regex;

use crate::graph::impact::compute_impact;
use crate::graph::lint::{LintReport, LintSeverity};
use crate::graph::types::{LineageGraph, NodeType};
use crate::render::locations::Locations;

/// What a `ref()` / `source()` call points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefKind {
    /// `ref('model')`, `ref('package', 'model')`, `ref('model', v=2)`.
    Ref { name: String },
    /// `source('source_name', 'table')`.
    Source { source: String, table: String },
}

/// A `ref()` / `source()` call in a buffer. Columns are UTF-16 code units,
/// as LSP positions are by default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefAt {
    pub kind: RefKind,
    pub line: u32,
    pub start: u32,
    pub end: u32,
}

fn call_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b(ref|source)\s*\(([^)]*)\)").unwrap())
}

fn string_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"^\s*['"]([^'"]+)['"]\s*$"#).unwrap())
}

fn utf16_col(line: &str, byte: usize) -> u32 {
    line[..byte].encode_utf16().count() as u32
}

/// All `ref()` / `source()` calls in `text` (single-line calls).
pub fn find_refs(text: &str) -> Vec<RefAt> {
    let mut out = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        for cap in call_re().captures_iter(line) {
            let whole = cap.get(0).unwrap();
            // Positional string arguments only; keyword args like `v=2` are skipped.
            let strings: Vec<String> = cap[2]
                .split(',')
                .filter_map(|arg| string_re().captures(arg).map(|c| c[1].to_string()))
                .collect();
            let kind = match (&cap[1], strings.as_slice()) {
                ("ref", [name]) => RefKind::Ref { name: name.clone() },
                ("ref", [_package, name]) => RefKind::Ref { name: name.clone() },
                ("source", [source, table]) => RefKind::Source {
                    source: source.clone(),
                    table: table.clone(),
                },
                _ => continue,
            };
            out.push(RefAt {
                kind,
                line: line_no as u32,
                start: utf16_col(line, whole.start()),
                end: utf16_col(line, whole.end()),
            });
        }
    }
    out
}

/// The call under the cursor (`character` in UTF-16 code units).
pub fn ref_at(text: &str, line: u32, character: u32) -> Option<RefAt> {
    find_refs(text)
        .into_iter()
        .find(|r| r.line == line && r.start <= character && character <= r.end)
}

/// One diagnostic for an open file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub line: u32,
    pub start: u32,
    pub end: u32,
    pub severity: LintSeverity,
    pub rule: &'static str,
    pub message: String,
}

/// A project's graph plus derived data, rebuilt on save.
pub struct Analysis {
    pub project_dir: PathBuf,
    pub graph: LineageGraph,
    pub lint: LintReport,
    locations: Locations,
}

impl Analysis {
    /// Build from the dbt project at `project_dir` (SQL-parse mode, with the
    /// on-disk parse cache).
    pub fn load(project_dir: &Path) -> Result<Self> {
        let project = crate::parser::project::DbtProject::load(project_dir)?;
        let paths = project.resolve_paths(project_dir);
        let files = crate::parser::discovery::discover_files(&paths)?;
        let graph = crate::graph::builder::build_graph(project_dir, &files)?;
        Ok(Self::from_graph(project_dir.to_path_buf(), graph))
    }

    pub fn from_graph(project_dir: PathBuf, graph: LineageGraph) -> Self {
        let lint = crate::graph::lint::lint(&graph, &crate::graph::lint::LintConfig::default());
        let locations = Locations::from_graph(&graph, &project_dir, "");
        Self {
            project_dir,
            graph,
            lint,
            locations,
        }
    }

    /// Node a `ref()` / `source()` call resolves to.
    pub fn resolve(&self, kind: &RefKind) -> Option<NodeIndex> {
        let g = &self.graph;
        match kind {
            RefKind::Ref { name } => g
                .node_indices()
                .filter(|&i| {
                    g[i].label == *name
                        && matches!(
                            g[i].node_type,
                            NodeType::Model | NodeType::Seed | NodeType::Snapshot
                        )
                })
                // Prefer a model when a seed/snapshot shares the name.
                .min_by_key(|&i| (g[i].node_type != NodeType::Model) as u8),
            RefKind::Source { source, table } => {
                let label = format!("{source}.{table}");
                g.node_indices()
                    .find(|&i| g[i].node_type == NodeType::Source && g[i].label == label)
            }
        }
    }

    /// Absolute path of a node's file.
    pub fn file_of(&self, idx: NodeIndex) -> Option<PathBuf> {
        let p = self.graph[idx].file_path.as_ref()?;
        Some(if p.is_absolute() {
            p.clone()
        } else {
            self.project_dir.join(p)
        })
    }

    /// Model / seed / snapshot defined by the file at `path`.
    pub fn node_for_file(&self, path: &Path) -> Option<NodeIndex> {
        let target = canonical(path);
        self.graph.node_indices().find(|&i| {
            matches!(
                self.graph[i].node_type,
                NodeType::Model | NodeType::Seed | NodeType::Snapshot
            ) && self.file_of(i).map(|p| canonical(&p)) == Some(target.clone())
        })
    }

    /// Transitive (upstream, downstream) node counts, ignoring tests and
    /// unresolved references.
    pub fn counts(&self, idx: NodeIndex) -> (usize, usize) {
        (
            self.reach(idx, Direction::Incoming),
            self.reach(idx, Direction::Outgoing),
        )
    }

    fn reach(&self, start: NodeIndex, dir: Direction) -> usize {
        let mut seen = HashSet::from([start]);
        let mut queue = VecDeque::from([start]);
        while let Some(n) = queue.pop_front() {
            for next in self.graph.neighbors_directed(n, dir) {
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        seen.iter()
            .filter(|&&i| {
                i != start && !matches!(self.graph[i].node_type, NodeType::Test | NodeType::Phantom)
            })
            .count()
    }

    /// Code lens title for a model file.
    pub fn lens_title(&self, idx: NodeIndex) -> String {
        let (up, down) = self.counts(idx);
        let impact = compute_impact(&self.graph, idx);
        let severity = if impact.impacted_nodes.is_empty() {
            "none"
        } else {
            impact.overall_severity.label()
        };
        format!("{up} upstream · {down} downstream · impact: {severity}")
    }

    /// Markdown hover for a node.
    pub fn hover_markdown(&self, idx: NodeIndex) -> String {
        let node = &self.graph[idx];
        let mut md = format!("**{}** `{}`", node.node_type.label(), node.label);
        if let Some(m) = &node.materialization {
            md.push_str(&format!(" · {m}"));
        }
        md.push_str("\n\n");
        if let Some(desc) = node.description.as_deref().filter(|d| !d.trim().is_empty()) {
            md.push_str(desc.trim());
            md.push_str("\n\n");
        }
        let (up, down) = self.counts(idx);
        md.push_str(&format!("↑ {up} upstream · ↓ {down} downstream"));
        let impact = compute_impact(&self.graph, idx);
        if !impact.impacted_nodes.is_empty() {
            md.push_str(&format!(
                "\n\nImpact if changed: **{}** ({} models, {} tests, {} exposures)",
                impact.overall_severity.label(),
                impact.affected_models,
                impact.affected_tests,
                impact.affected_exposures
            ));
        }
        if let Some(path) = &node.file_path {
            let shown = path.strip_prefix(&self.project_dir).unwrap_or(path);
            let shown = shown.to_string_lossy().replace('\\', "/");
            md.push_str(&format!("\n\n`{}`", shown));
        }
        md
    }

    /// Definition target for a call: the node's file, and for sources the line
    /// of the table entry in its YAML.
    pub fn definition(&self, kind: &RefKind) -> Option<(PathBuf, u32)> {
        let idx = self.resolve(kind)?;
        let path = self.file_of(idx)?;
        let line = match kind {
            RefKind::Source { source, table } => std::fs::read_to_string(&path)
                .ok()
                .and_then(|yaml| source_table_line(&yaml, source, table))
                .unwrap_or(0),
            RefKind::Ref { .. } => 0,
        };
        Some((path, line))
    }

    /// Lint findings located in the file at `path`, positioned on the
    /// offending `ref()` / `source()` call when there is one.
    pub fn findings_for(&self, path: &Path, text: &str) -> Vec<Finding> {
        let rel = canonical(path)
            .strip_prefix(canonical(&self.project_dir))
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if rel.is_empty() {
            return Vec::new();
        }
        let refs = find_refs(text);
        let first_line_len = text.lines().next().map_or(0, |l| l.encode_utf16().count()) as u32;
        self.lint
            .findings
            .iter()
            .filter(|f| self.locations.for_id(&f.unique_id) == rel)
            .map(|f| {
                // Undefined refs/sources: underline the call itself.
                let call = refs.iter().find(|r| match &r.kind {
                    RefKind::Ref { name } => *name == f.label,
                    RefKind::Source { source, table } => format!("{source}.{table}") == f.label,
                });
                let (line, start, end) = match call {
                    Some(r) if self.graph_label_is_phantom(&f.unique_id) => {
                        (r.line, r.start, r.end)
                    }
                    _ => (0, 0, first_line_len),
                };
                Finding {
                    line,
                    start,
                    end,
                    severity: f.severity,
                    rule: f.rule,
                    message: f.message.clone(),
                }
            })
            .collect()
    }

    fn graph_label_is_phantom(&self, unique_id: &str) -> bool {
        self.graph.node_indices().any(|i| {
            self.graph[i].unique_id == unique_id && self.graph[i].node_type == NodeType::Phantom
        })
    }
}

/// Line of `- name: <table>` inside the `<source>` entry of a schema YAML.
fn source_table_line(yaml: &str, source: &str, table: &str) -> Option<u32> {
    let name_of = |l: &str| {
        l.trim_start()
            .trim_start_matches("- ")
            .strip_prefix("name:")
            .map(|v| v.trim().trim_matches(|c| c == '"' || c == '\'').to_string())
    };
    let mut in_source = false;
    let mut source_indent = 0;
    for (i, line) in yaml.lines().enumerate() {
        let indent = line.len() - line.trim_start().len();
        if let Some(name) = name_of(line) {
            if in_source && indent > source_indent && name == table {
                return Some(i as u32);
            }
            if name == source && !in_source {
                in_source = true;
                source_indent = indent;
            } else if in_source && indent <= source_indent {
                in_source = name == source;
            }
        }
    }
    None
}

fn canonical(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

/// Nearest ancestor directory of `file` containing `dbt_project.yml`.
pub fn find_project_root(file: &Path) -> Option<PathBuf> {
    file.ancestors()
        .skip(1)
        .find(|d| d.join("dbt_project.yml").is_file())
        .map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_ref_and_source_calls() {
        let sql =
            "select *\nfrom {{ ref('orders') }} o\njoin {{ source(\"raw\", 'customers') }} c\n";
        let refs = find_refs(sql);
        assert_eq!(refs.len(), 2);
        assert_eq!(
            refs[0],
            RefAt {
                kind: RefKind::Ref {
                    name: "orders".into()
                },
                line: 1,
                start: 8,
                end: 21,
            }
        );
        assert_eq!(
            refs[1].kind,
            RefKind::Source {
                source: "raw".into(),
                table: "customers".into()
            }
        );
    }

    #[test]
    fn ref_variants() {
        let k = |s: &str| find_refs(s).into_iter().map(|r| r.kind).collect::<Vec<_>>();
        assert_eq!(
            k("{{ ref('pkg', 'm') }}"),
            vec![RefKind::Ref { name: "m".into() }]
        );
        assert_eq!(
            k("{{ ref('m', v=2) }}"),
            vec![RefKind::Ref { name: "m".into() }]
        );
        assert_eq!(
            k("{{ ref( 'm' ) }}"),
            vec![RefKind::Ref { name: "m".into() }]
        );
        assert!(k("{{ ref(var('x')) }}").is_empty());
        assert!(k("preference('x')").is_empty());
    }

    #[test]
    fn position_lookup_uses_utf16_columns() {
        // "ä" is 2 bytes in UTF-8 but 1 UTF-16 unit.
        let sql = "-- ä\nselect 'ä', * from {{ ref('orders') }}";
        let r = find_refs(sql).remove(0);
        assert_eq!((r.line, r.start), (1, 22));
        assert!(ref_at(sql, 1, 25).is_some());
        assert!(ref_at(sql, 1, 5).is_none());
        assert!(ref_at(sql, 0, 25).is_none());
    }

    #[test]
    fn source_table_line_in_yaml() {
        let yaml = "version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n      - name: customers\n  - name: other\n    tables:\n      - name: orders\n";
        assert_eq!(source_table_line(yaml, "raw", "customers"), Some(5));
        assert_eq!(source_table_line(yaml, "other", "orders"), Some(8));
        assert_eq!(source_table_line(yaml, "raw", "missing"), None);
    }

    fn fixture() -> Analysis {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/simple_project");
        let project = crate::parser::project::DbtProject::load(&dir).unwrap();
        let files = crate::parser::discovery::discover_files(&project.resolve_paths(&dir)).unwrap();
        // No cache: keep the fixture directory clean.
        let graph = crate::graph::builder::build_graph_no_cache(&dir, &files).unwrap();
        Analysis::from_graph(dir, graph)
    }

    #[test]
    fn hover_and_lens_for_a_model() {
        let a = fixture();
        let idx = a
            .resolve(&RefKind::Ref {
                name: "stg_orders".into(),
            })
            .unwrap();
        let md = a.hover_markdown(idx);
        assert!(md.starts_with("**model** `stg_orders`"), "{md}");
        assert!(md.contains("Staged orders data"), "{md}");
        assert!(md.contains("↑ 1 upstream"), "{md}");
        assert!(md.contains("Impact if changed:"), "{md}");
        assert!(md.contains("models/staging/stg_orders.sql"), "{md}");
        let lens = a.lens_title(idx);
        assert!(lens.starts_with("1 upstream · "), "{lens}");
        assert!(lens.contains("impact: "), "{lens}");
    }

    #[test]
    fn definitions_for_model_and_source() {
        let a = fixture();
        let (path, line) = a
            .definition(&RefKind::Ref {
                name: "stg_orders".into(),
            })
            .unwrap();
        assert!(path.ends_with("models/staging/stg_orders.sql"));
        assert_eq!(line, 0);

        let (path, line) = a
            .definition(&RefKind::Source {
                source: "raw".into(),
                table: "payments".into(),
            })
            .unwrap();
        assert!(path.ends_with("models/staging/schema.yml"));
        let yaml = std::fs::read_to_string(&path).unwrap();
        assert!(yaml
            .lines()
            .nth(line as usize)
            .unwrap()
            .contains("name: payments"));

        assert!(a
            .definition(&RefKind::Ref {
                name: "nope".into()
            })
            .is_none());
        assert!(a
            .node_for_file(&a.project_dir.join("models/marts/orders.sql"))
            .is_some());
    }

    #[test]
    fn undefined_ref_is_underlined() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        std::fs::create_dir_all(p.join("models")).unwrap();
        std::fs::write(p.join("dbt_project.yml"), "name: t\nversion: '1'\n").unwrap();
        let sql = "select *\nfrom {{ ref('ghost') }}\n";
        std::fs::write(p.join("models/m.sql"), sql).unwrap();
        let project = crate::parser::project::DbtProject::load(p).unwrap();
        let files = crate::parser::discovery::discover_files(&project.resolve_paths(p)).unwrap();
        let graph = crate::graph::builder::build_graph_no_cache(p, &files).unwrap();
        let a = Analysis::from_graph(p.to_path_buf(), graph);

        let findings = a.findings_for(&p.join("models/m.sql"), sql);
        let undefined = findings
            .iter()
            .find(|f| f.severity == LintSeverity::Error)
            .unwrap_or_else(|| panic!("{findings:?}"));
        assert_eq!((undefined.line, undefined.start), (1, 8));
        assert_eq!(find_project_root(&p.join("models/m.sql")).unwrap(), p);
    }
}
