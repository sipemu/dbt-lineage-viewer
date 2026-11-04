//! Static lineage site (`dbt-lineage site`): an index page with client-side
//! search plus one page per node (description, config, columns with
//! column-level lineage, SQL, upstream/downstream, focused lineage diagram).
//!
//! Everything is plain HTML/CSS/JS written to a directory — no server, no CDN —
//! so it can be published as-is to GitHub or GitLab Pages.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::path::Path;

use anyhow::{Context, Result};
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::Serialize;

use crate::graph::types::{EdgeData, LineageGraph, NodeData, NodeType};
use crate::parser::column_lineage::{ColumnEdge, ColumnLineage};
use crate::render::layout::sugiyama_layout;

/// Hops up- and downstream drawn in a node page's lineage diagram.
const DIAGRAM_HOPS: usize = 2;
/// Max nodes in a node page's lineage diagram.
const DIAGRAM_MAX_NODES: usize = 40;
/// Lines of a seed CSV shown on its page.
const SEED_PREVIEW_LINES: usize = 20;

const CSS: &str = include_str!("site_assets/site.css");
const JS: &str = include_str!("site_assets/site.js");

/// Counts of what [`write_site`] produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SiteStats {
    pub node_pages: usize,
}

/// Write the site for `graph` into `out_dir`. Node SQL is read from
/// `project_root` joined with each node's relative `file_path`.
pub fn write_site(
    graph: &LineageGraph,
    column_lineage: &ColumnLineage,
    project_root: &Path,
    out_dir: &Path,
    title: &str,
) -> Result<SiteStats> {
    let nodes_dir = out_dir.join("nodes");
    let assets_dir = out_dir.join("assets");
    std::fs::create_dir_all(&nodes_dir)
        .with_context(|| format!("cannot create {}", nodes_dir.display()))?;
    std::fs::create_dir_all(&assets_dir)?;
    std::fs::write(assets_dir.join("site.css"), CSS)?;
    std::fs::write(assets_dir.join("site.js"), JS)?;

    let ctx = SiteContext::new(graph, column_lineage, title);

    std::fs::write(out_dir.join("index.html"), render_index(&ctx))?;

    let pages: Vec<NodeIndex> = graph
        .node_indices()
        .filter(|&i| has_page(&graph[i]))
        .collect();
    for &idx in &pages {
        let html = render_node_page(&ctx, idx, project_root);
        std::fs::write(nodes_dir.join(format!("{}.html", ctx.slug(idx))), html)?;
    }
    Ok(SiteStats {
        node_pages: pages.len(),
    })
}

/// Tests and phantom (unresolved) nodes are listed on their neighbours'
/// pages but get no page of their own.
fn has_page(node: &NodeData) -> bool {
    !matches!(node.node_type, NodeType::Test | NodeType::Phantom)
}

/// File-name-safe, unique slug for a unique_id.
pub fn slugify(unique_id: &str) -> String {
    unique_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

struct SiteContext<'a> {
    graph: &'a LineageGraph,
    title: &'a str,
    slugs: HashMap<NodeIndex, String>,
    by_id: HashMap<&'a str, NodeIndex>,
    col_in: HashMap<&'a str, Vec<&'a ColumnEdge>>,
    col_out: HashMap<&'a str, Vec<&'a ColumnEdge>>,
}

impl<'a> SiteContext<'a> {
    fn new(graph: &'a LineageGraph, lineage: &'a ColumnLineage, title: &'a str) -> Self {
        let slugs = graph
            .node_indices()
            .map(|i| (i, slugify(&graph[i].unique_id)))
            .collect();
        let by_id = graph
            .node_indices()
            .map(|i| (graph[i].unique_id.as_str(), i))
            .collect();
        let mut col_in: HashMap<&str, Vec<&ColumnEdge>> = HashMap::new();
        let mut col_out: HashMap<&str, Vec<&ColumnEdge>> = HashMap::new();
        let mut seen = HashSet::new();
        for e in &lineage.edges {
            let key = (
                &e.source_node,
                &e.source_column,
                &e.target_node,
                &e.target_column,
            );
            if !seen.insert(key) {
                continue;
            }
            col_in.entry(e.target_node.as_str()).or_default().push(e);
            col_out.entry(e.source_node.as_str()).or_default().push(e);
        }
        SiteContext {
            graph,
            title,
            slugs,
            by_id,
            col_in,
            col_out,
        }
    }

    fn slug(&self, idx: NodeIndex) -> &str {
        &self.slugs[&idx]
    }

    /// Link to a node from a page in `nodes/`, or plain text if it has no page.
    fn node_link(&self, idx: NodeIndex) -> String {
        let node = &self.graph[idx];
        if has_page(node) {
            format!(
                r#"<a href="{}.html">{}</a>"#,
                self.slug(idx),
                html_escape(&node.label)
            )
        } else {
            html_escape(&node.label)
        }
    }

    /// `model.column` link for a column-lineage endpoint.
    fn column_link(&self, unique_id: &str, column: &str) -> String {
        match self.by_id.get(unique_id) {
            Some(&idx) if has_page(&self.graph[idx]) => format!(
                r#"<a href="{}.html#col-{}">{}.{}</a>"#,
                self.slug(idx),
                html_escape(column),
                html_escape(&self.graph[idx].label),
                html_escape(column)
            ),
            Some(&idx) => format!(
                "{}.{}",
                html_escape(&self.graph[idx].label),
                html_escape(column)
            ),
            None => format!("{}.{}", html_escape(unique_id), html_escape(column)),
        }
    }
}

#[derive(Serialize)]
struct IndexEntry<'a> {
    /// label
    n: &'a str,
    /// node type
    t: &'a str,
    /// page slug
    u: &'a str,
    /// description (truncated)
    d: String,
    /// tags
    g: &'a [String],
    /// file path
    p: String,
}

fn page_head(title: &str, asset_prefix: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="{asset_prefix}assets/site.css">
</head>
<body>
"#,
        title = html_escape(title),
    )
}

fn render_index(ctx: &SiteContext) -> String {
    let graph = ctx.graph;
    let mut entries: Vec<(NodeIndex, &NodeData)> = graph
        .node_indices()
        .map(|i| (i, &graph[i]))
        .filter(|(_, n)| has_page(n))
        .collect();
    entries.sort_by(|a, b| {
        (a.1.node_type.label(), &a.1.label).cmp(&(b.1.node_type.label(), &b.1.label))
    });

    let index: Vec<IndexEntry> = entries
        .iter()
        .map(|(i, n)| IndexEntry {
            n: &n.label,
            t: n.node_type.label(),
            u: ctx.slug(*i),
            d: n.description
                .as_deref()
                .map(|d| d.chars().take(160).collect())
                .unwrap_or_default(),
            g: &n.tags,
            p: n.file_path
                .as_ref()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default(),
        })
        .collect();
    // `</` must not appear inside a <script> element.
    let json = serde_json::to_string(&index).unwrap().replace("</", "<\\/");

    let mut counts: Vec<(&str, usize)> = Vec::new();
    for (_, n) in &entries {
        match counts.last_mut() {
            Some((t, c)) if *t == n.node_type.label() => *c += 1,
            _ => counts.push((n.node_type.label(), 1)),
        }
    }

    let mut html = page_head(ctx.title, "");
    let _ = write!(
        html,
        r#"<header class="top"><h1>{}</h1><p class="muted">{} nodes · generated by <a href="https://github.com/sipemu/dbt-lineage-viewer">dbt-lineage</a></p></header>
<main>
<div class="search">
<input id="q" type="search" placeholder="Search models, sources, seeds, exposures…" autocomplete="off" autofocus aria-label="Search">
<div id="types" class="chips">"#,
        html_escape(ctx.title),
        entries.len()
    );
    for (t, c) in &counts {
        let _ = write!(
            html,
            r#"<button type="button" class="chip" data-type="{t}" aria-pressed="false">{t} <span class="muted">{c}</span></button>"#
        );
    }
    html.push_str("</div></div>\n<ul id=\"results\" class=\"results\">\n");
    // Server-rendered list: works without JS; JS replaces it with filtered results.
    for (i, n) in &entries {
        let _ = writeln!(
            html,
            r#"<li><a href="nodes/{}.html"><span class="badge t-{}">{}</span> {}</a></li>"#,
            ctx.slug(*i),
            n.node_type.label(),
            n.node_type.label(),
            html_escape(&n.label)
        );
    }
    let _ = write!(
        html,
        r#"</ul>
<p id="more" class="muted" hidden></p>
</main>
<script id="index" type="application/json">{json}</script>
<script src="assets/site.js"></script>
</body>
</html>
"#
    );
    html
}

fn render_node_page(ctx: &SiteContext, idx: NodeIndex, project_root: &Path) -> String {
    let graph = ctx.graph;
    let node = &graph[idx];
    let mut html = page_head(&format!("{} · {}", node.label, ctx.title), "../");
    let _ = write!(
        html,
        r#"<header class="top"><a class="back" href="../index.html">← {}</a>
<h1>{} <span class="badge t-{}">{}</span></h1>
<p class="muted mono">{}</p></header>
<main>
"#,
        html_escape(ctx.title),
        html_escape(&node.label),
        node.node_type.label(),
        node.node_type.label(),
        html_escape(&node.unique_id)
    );

    if let Some(desc) = node.description.as_deref().filter(|d| !d.trim().is_empty()) {
        let _ = writeln!(html, r#"<p class="desc">{}</p>"#, html_escape(desc));
    }

    // Facts
    let mut facts: Vec<(&str, String)> = Vec::new();
    if let Some(m) = &node.materialization {
        facts.push(("Materialization", html_escape(m)));
    }
    if !node.tags.is_empty() {
        let tags: Vec<String> = node
            .tags
            .iter()
            .map(|t| format!(r#"<span class="tag">{}</span>"#, html_escape(t)))
            .collect();
        facts.push(("Tags", tags.join(" ")));
    }
    if let Some(p) = &node.file_path {
        let display = p
            .strip_prefix(project_root)
            .unwrap_or(p)
            .to_string_lossy()
            .replace('\\', "/");
        facts.push((
            "File",
            format!(r#"<span class="mono">{}</span>"#, html_escape(&display)),
        ));
    }
    if !facts.is_empty() {
        html.push_str("<dl class=\"facts\">");
        for (k, v) in facts {
            let _ = write!(html, "<dt>{k}</dt><dd>{v}</dd>");
        }
        html.push_str("</dl>\n");
    }

    // Lineage diagram
    let hood = neighborhood(graph, idx, DIAGRAM_HOPS, DIAGRAM_MAX_NODES);
    if hood.len() > 1 {
        html.push_str("<section><h2>Lineage</h2>\n<div class=\"diagram\">");
        html.push_str(&render_diagram(ctx, idx, &hood));
        html.push_str("</div></section>\n");
    }

    // Upstream / downstream / tests
    let (parents, children, tests) = neighbours(graph, idx);
    html.push_str("<section class=\"cols2\">");
    render_link_list(&mut html, ctx, "Upstream", &parents);
    render_link_list(&mut html, ctx, "Downstream", &children);
    html.push_str("</section>\n");
    if !tests.is_empty() {
        let names: Vec<String> = tests
            .iter()
            .map(|&t| html_escape(&graph[t].label))
            .collect();
        let _ = writeln!(
            html,
            r#"<section><h2>Tests <span class="muted">{}</span></h2><p class="mono small">{}</p></section>"#,
            tests.len(),
            names.join(" · ")
        );
    }

    render_columns(&mut html, ctx, node);

    // Source code
    if let Some(rel) = &node.file_path {
        let path = project_root.join(rel);
        let is_yaml = matches!(
            rel.extension().and_then(|e| e.to_str()),
            Some("yml" | "yaml")
        );
        if !is_yaml {
            if let Ok(text) = std::fs::read_to_string(&path) {
                let (heading, body) = match node.node_type {
                    NodeType::Seed => {
                        let preview: Vec<&str> = text.lines().take(SEED_PREVIEW_LINES).collect();
                        ("Preview", html_escape(&preview.join("\n")))
                    }
                    _ => ("SQL", highlight_sql(&text)),
                };
                let _ = writeln!(
                    html,
                    r#"<section><h2>{heading}</h2><pre class="code"><code>{body}</code></pre></section>"#
                );
            }
        }
    }

    html.push_str("</main>\n</body>\n</html>\n");
    html
}

fn render_link_list(html: &mut String, ctx: &SiteContext, title: &str, items: &[NodeIndex]) {
    let _ = write!(
        html,
        r#"<div><h2>{title} <span class="muted">{}</span></h2>"#,
        items.len()
    );
    if items.is_empty() {
        html.push_str("<p class=\"muted\">None</p></div>");
        return;
    }
    html.push_str("<ul class=\"links\">");
    for &i in items {
        let _ = write!(
            html,
            r#"<li><span class="badge t-{}">{}</span> {}</li>"#,
            ctx.graph[i].node_type.label(),
            ctx.graph[i].node_type.label(),
            ctx.node_link(i)
        );
    }
    html.push_str("</ul></div>");
}

fn render_columns(html: &mut String, ctx: &SiteContext, node: &NodeData) {
    let incoming = ctx.col_in.get(node.unique_id.as_str());
    let outgoing = ctx.col_out.get(node.unique_id.as_str());
    let mut columns: Vec<&str> = node.columns.iter().map(String::as_str).collect();
    for e in incoming.into_iter().flatten() {
        if !columns.contains(&e.target_column.as_str()) {
            columns.push(&e.target_column);
        }
    }
    if columns.is_empty() {
        return;
    }
    let _ = write!(
        html,
        r#"<section><h2>Columns <span class="muted">{}</span></h2>
<div class="table"><table><thead><tr><th>Column</th><th>From</th><th>Used by</th></tr></thead><tbody>"#,
        columns.len()
    );
    for col in columns {
        let from: Vec<String> = incoming
            .into_iter()
            .flatten()
            .filter(|e| e.target_column == col)
            .map(|e| ctx.column_link(&e.source_node, &e.source_column))
            .collect();
        // Singular tests select columns too; listing them here is noise.
        let used_by: Vec<String> = outgoing
            .into_iter()
            .flatten()
            .filter(|e| e.source_column == col)
            .filter(|e| {
                ctx.by_id
                    .get(e.target_node.as_str())
                    .is_none_or(|&i| ctx.graph[i].node_type != NodeType::Test)
            })
            .map(|e| ctx.column_link(&e.target_node, &e.target_column))
            .collect();
        let _ = write!(
            html,
            r#"<tr id="col-{c}"><td class="mono">{c}</td><td>{}</td><td>{}</td></tr>"#,
            from.join(", "),
            used_by.join(", "),
            c = html_escape(col)
        );
    }
    html.push_str("</tbody></table></div></section>\n");
}

/// Direct parents and children with pages, plus attached tests; each sorted by label.
fn neighbours(
    graph: &LineageGraph,
    idx: NodeIndex,
) -> (Vec<NodeIndex>, Vec<NodeIndex>, Vec<NodeIndex>) {
    let sorted = |mut v: Vec<NodeIndex>| {
        v.sort_by(|a, b| graph[*a].label.cmp(&graph[*b].label));
        v.dedup();
        v
    };
    let parents = graph
        .neighbors_directed(idx, Direction::Incoming)
        .filter(|&i| graph[i].node_type != NodeType::Test)
        .collect();
    let (tests, children): (Vec<NodeIndex>, Vec<NodeIndex>) = graph
        .neighbors_directed(idx, Direction::Outgoing)
        .partition(|&i| graph[i].node_type == NodeType::Test);
    (sorted(parents), sorted(children), sorted(tests))
}

/// Nodes within `hops` up- and downstream of `center` (tests excluded),
/// breadth-first and capped at `max`. `center` comes first.
pub fn neighborhood(
    graph: &LineageGraph,
    center: NodeIndex,
    hops: usize,
    max: usize,
) -> Vec<NodeIndex> {
    let mut out = vec![center];
    let mut seen: HashSet<NodeIndex> = HashSet::from([center]);
    for dir in [Direction::Incoming, Direction::Outgoing] {
        let mut queue = VecDeque::from([(center, 0)]);
        while let Some((idx, depth)) = queue.pop_front() {
            if depth == hops {
                continue;
            }
            let mut next: Vec<NodeIndex> = graph
                .neighbors_directed(idx, dir)
                .filter(|&n| graph[n].node_type != NodeType::Test)
                .collect();
            next.sort_by(|a, b| graph[*a].label.cmp(&graph[*b].label));
            for n in next {
                if out.len() >= max {
                    return out;
                }
                if seen.insert(n) {
                    out.push(n);
                    queue.push_back((n, depth + 1));
                }
            }
        }
    }
    out
}

const NODE_W: f64 = 168.0;
const NODE_H: f64 = 34.0;
const LAYER_GAP: f64 = 64.0;
const ROW_GAP: f64 = 14.0;
const PAD: f64 = 12.0;

/// Left-to-right SVG of the subgraph induced by `nodes`, each node linking to
/// its page; `center` is highlighted.
fn render_diagram(ctx: &SiteContext, center: NodeIndex, nodes: &[NodeIndex]) -> String {
    let graph = ctx.graph;
    let mut sub = LineageGraph::new();
    let mut map: HashMap<NodeIndex, NodeIndex> = HashMap::new();
    for &i in nodes {
        map.insert(i, sub.add_node(graph[i].clone()));
    }
    for &i in nodes {
        for e in graph.edges_directed(i, Direction::Outgoing) {
            if let (Some(&a), Some(&b)) = (map.get(&i), map.get(&e.target())) {
                sub.add_edge(
                    a,
                    b,
                    EdgeData {
                        edge_type: e.weight().edge_type,
                    },
                );
            }
        }
    }
    let back: HashMap<NodeIndex, NodeIndex> = map.iter().map(|(k, v)| (*v, *k)).collect();
    let layout = sugiyama_layout(&sub);
    let pos = |i: NodeIndex| -> (f64, f64) {
        let (layer, row) = layout.positions[&i];
        (
            PAD + layer as f64 * (NODE_W + LAYER_GAP),
            PAD + row as f64 * (NODE_H + ROW_GAP),
        )
    };
    let width = PAD * 2.0 + layout.num_layers as f64 * (NODE_W + LAYER_GAP) - LAYER_GAP;
    let height = PAD * 2.0 + layout.max_layer_width as f64 * (NODE_H + ROW_GAP) - ROW_GAP;

    let mut svg = format!(
        // Shrink to fit the frame, but never below 60% of natural size (labels
        // stay readable); wider diagrams scroll horizontally instead.
        r#"<svg class="lineage" viewBox="0 0 {width} {height}" width="{width}" height="{height}" style="min-width: {min_width:.0}px" role="img" aria-label="Lineage of {}">"#,
        html_escape(&graph[center].label),
        min_width = width * 0.6
    );
    for e in sub.edge_indices() {
        let (a, b) = sub.edge_endpoints(e).unwrap();
        if !layout.positions.contains_key(&a) || !layout.positions.contains_key(&b) {
            continue;
        }
        let ((x1, y1), (x2, y2)) = (pos(a), pos(b));
        let (sx, sy, tx, ty) = (x1 + NODE_W, y1 + NODE_H / 2.0, x2, y2 + NODE_H / 2.0);
        let mid = (sx + tx) / 2.0;
        let _ = write!(
            svg,
            r#"<path class="edge" d="M{sx} {sy} C{mid} {sy} {mid} {ty} {tx} {ty}"/>"#
        );
    }
    for i in sub.node_indices() {
        if !layout.positions.contains_key(&i) {
            continue;
        }
        let orig = back[&i];
        let node = &graph[orig];
        let (x, y) = pos(i);
        let label: String = if node.label.chars().count() > 22 {
            node.label.chars().take(21).collect::<String>() + "…"
        } else {
            node.label.clone()
        };
        let class = if orig == center { " current" } else { "" };
        let inner = format!(
            r#"<g class="node t-{t}{class}"><title>{full} ({t})</title><rect x="{x}" y="{y}" width="{NODE_W}" height="{NODE_H}" rx="6"/><text x="{tx}" y="{ty}">{label}</text></g>"#,
            t = node.node_type.label(),
            full = html_escape(&node.label),
            tx = x + 10.0,
            ty = y + NODE_H / 2.0 + 4.5,
            label = html_escape(&label),
        );
        if has_page(node) && orig != center {
            let _ = write!(svg, r#"<a href="{}.html">{inner}</a>"#, ctx.slug(orig));
        } else {
            svg.push_str(&inner);
        }
    }
    svg.push_str("</svg>");
    svg
}

const SQL_KEYWORDS: &[&str] = &[
    "select",
    "from",
    "where",
    "join",
    "left",
    "right",
    "inner",
    "outer",
    "full",
    "cross",
    "on",
    "and",
    "or",
    "not",
    "as",
    "group",
    "by",
    "order",
    "having",
    "limit",
    "union",
    "all",
    "with",
    "case",
    "when",
    "then",
    "else",
    "end",
    "distinct",
    "in",
    "is",
    "null",
    "between",
    "like",
    "over",
    "partition",
    "qualify",
    "insert",
    "into",
    "values",
    "create",
    "table",
    "view",
    "using",
    "cast",
    "true",
    "false",
    "asc",
    "desc",
    "except",
    "intersect",
    "exists",
    "lateral",
];

/// Minimal SQL + Jinja highlighter: Jinja blocks, comments, strings and
/// keywords become `<span>`s; everything is HTML-escaped.
pub fn highlight_sql(src: &str) -> String {
    let mut out = String::with_capacity(src.len() * 2);
    let mut rest = src;
    let span = |out: &mut String, class: &str, text: &str| {
        let _ = write!(out, r#"<span class="{class}">{}</span>"#, html_escape(text));
    };
    while !rest.is_empty() {
        let jinja_end = if rest.starts_with("{{") {
            Some("}}")
        } else if rest.starts_with("{%") {
            Some("%}")
        } else if rest.starts_with("{#") {
            Some("#}")
        } else {
            None
        };
        if let Some(end) = jinja_end {
            let len = rest[2..].find(end).map_or(rest.len(), |p| p + 4);
            span(&mut out, "j", &rest[..len]);
            rest = &rest[len..];
            continue;
        }
        if rest.starts_with("--") {
            let len = rest.find('\n').unwrap_or(rest.len());
            span(&mut out, "c", &rest[..len]);
            rest = &rest[len..];
            continue;
        }
        if rest.starts_with("/*") {
            let len = rest[2..].find("*/").map_or(rest.len(), |p| p + 4);
            span(&mut out, "c", &rest[..len]);
            rest = &rest[len..];
            continue;
        }
        if rest.starts_with('\'') {
            let len = rest[1..].find('\'').map_or(rest.len(), |p| p + 2);
            span(&mut out, "s", &rest[..len]);
            rest = &rest[len..];
            continue;
        }
        let c = rest.chars().next().unwrap();
        if c.is_alphabetic() || c == '_' {
            let len = rest
                .find(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
                .unwrap_or(rest.len());
            let word = &rest[..len];
            if SQL_KEYWORDS.contains(&word.to_ascii_lowercase().as_str()) {
                span(&mut out, "k", word);
            } else {
                out.push_str(&html_escape(word));
            }
            rest = &rest[len..];
            continue;
        }
        out.push_str(&html_escape(&rest[..c.len_utf8()]));
        rest = &rest[c.len_utf8()..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::EdgeType;
    use std::path::PathBuf;

    fn node(id: &str, label: &str, t: NodeType, path: Option<&str>) -> NodeData {
        NodeData {
            unique_id: id.into(),
            label: label.into(),
            node_type: t,
            file_path: path.map(PathBuf::from),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn edge(t: EdgeType) -> EdgeData {
        EdgeData { edge_type: t }
    }

    #[test]
    fn slugify_keeps_safe_chars() {
        assert_eq!(slugify("model.proj.orders"), "model.proj.orders");
        assert_eq!(slugify("source.raw/x y"), "source.raw_x_y");
    }

    #[test]
    fn highlight_escapes_and_marks_tokens() {
        let html = highlight_sql("select a < b -- c<d\nfrom {{ ref('x') }} where s = 'it'");
        assert!(html.contains(r#"<span class="k">select</span>"#));
        assert!(html.contains("a &lt; b"));
        assert!(html.contains(r#"<span class="c">-- c&lt;d</span>"#));
        assert!(html.contains(r#"<span class="j">{{ ref(&#39;x&#39;) }}</span>"#));
        assert!(html.contains(r#"<span class="s">&#39;it&#39;</span>"#));
        // Unterminated constructs don't panic.
        highlight_sql("{{ unterminated");
        highlight_sql("'open");
        highlight_sql("/* open");
        highlight_sql("ümlaut ß");
    }

    #[test]
    fn neighborhood_respects_hops_cap_and_skips_tests() {
        let mut g = LineageGraph::new();
        let a = g.add_node(node("model.a", "a", NodeType::Model, None));
        let b = g.add_node(node("model.b", "b", NodeType::Model, None));
        let c = g.add_node(node("model.c", "c", NodeType::Model, None));
        let d = g.add_node(node("model.d", "d", NodeType::Model, None));
        let t = g.add_node(node("test.t", "t", NodeType::Test, None));
        g.add_edge(a, b, edge(EdgeType::Ref));
        g.add_edge(b, c, edge(EdgeType::Ref));
        g.add_edge(c, d, edge(EdgeType::Ref));
        g.add_edge(b, t, edge(EdgeType::Test));

        let hood = neighborhood(&g, b, 1, 10);
        assert_eq!(hood, vec![b, a, c]);
        let hood = neighborhood(&g, a, 2, 10);
        assert_eq!(hood, vec![a, b, c]);
        assert_eq!(neighborhood(&g, a, 5, 2).len(), 2);
    }

    #[test]
    fn writes_index_and_linked_node_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join("proj");
        std::fs::create_dir_all(project.join("models")).unwrap();
        std::fs::write(project.join("models/a.sql"), "select id from raw").unwrap();
        std::fs::write(
            project.join("models/b.sql"),
            "select id as key from {{ ref('a') }}",
        )
        .unwrap();

        let mut g = LineageGraph::new();
        let mut a = node("model.a", "a", NodeType::Model, Some("models/a.sql"));
        a.description = Some("Alpha </script> model".into());
        a.columns = vec!["id".into()];
        let a = g.add_node(a);
        let b = g.add_node(node("model.b", "b", NodeType::Model, Some("models/b.sql")));
        let t = g.add_node(node("test.t", "not_null_a", NodeType::Test, None));
        g.add_edge(a, b, edge(EdgeType::Ref));
        g.add_edge(a, t, edge(EdgeType::Test));
        let lineage = ColumnLineage {
            edges: vec![ColumnEdge {
                source_node: "model.a".into(),
                source_column: "id".into(),
                target_node: "model.b".into(),
                target_column: "key".into(),
                confidence: crate::parser::column_lineage::ColumnConfidence::Direct,
            }],
        };

        let out = tmp.path().join("site");
        let stats = write_site(&g, &lineage, &project, &out, "Demo").unwrap();
        assert_eq!(stats.node_pages, 2);
        for f in [
            "index.html",
            "assets/site.css",
            "assets/site.js",
            "nodes/model.a.html",
            "nodes/model.b.html",
        ] {
            assert!(out.join(f).exists(), "{f}");
        }
        assert!(!out.join("nodes/test.t.html").exists());

        let index = std::fs::read_to_string(out.join("index.html")).unwrap();
        assert!(index.contains(r#""n":"a""#));
        assert!(
            index.contains("Alpha <\\/script> model"),
            "script-safe JSON"
        );
        assert!(index.contains(r#"href="nodes/model.b.html""#));

        let page_a = std::fs::read_to_string(out.join("nodes/model.a.html")).unwrap();
        assert!(page_a.contains(r#"<a href="model.b.html">b</a>"#));
        assert!(page_a.contains("not_null_a"));
        assert!(page_a.contains("Alpha &lt;/script&gt; model"));
        assert!(page_a.contains(r#"<a href="model.b.html#col-key">b.key</a>"#));
        assert!(page_a.contains(r#"<span class="k">select</span>"#));
        assert!(page_a.contains("<svg class=\"lineage\""));

        let page_b = std::fs::read_to_string(out.join("nodes/model.b.html")).unwrap();
        assert!(page_b.contains(r#"id="col-key""#));
        assert!(page_b.contains(r#"<a href="model.a.html#col-id">a.id</a>"#));
    }
}
