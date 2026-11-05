//! Looker, read offline from a LookML project (no API access needed).
//!
//! Views map to warehouse tables (`sql_table_name`, or the tables a
//! `derived_table: { sql: … ;; }` reads, following `${view.SQL_TABLE_NAME}`
//! references and `extends:`). Explores (in any `.lkml` file) use a base view
//! plus joined views. LookML dashboards (`.dashboard.lookml`, YAML) have
//! elements that query `model` + `explore`; a dashboard's tables are the
//! union of the tables behind the explores its elements use.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;

use super::Dashboard;

// ---------------------------------------------------------------------------
// LookML tokenizer: `key: value`, `key: name { … }`, `key: { … }`,
// `key: [ … ]`, SQL-ish values terminated by `;;`, `#` comments.
// ---------------------------------------------------------------------------

static NEXT_PARAM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[ \t]*[A-Za-z_][\w+.]*[ \t]*:").unwrap());

/// One LookML parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub key: String,
    /// Block name in `view: orders { … }`.
    pub name: Option<String>,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Block(Vec<Node>),
    /// Raw text of a scalar, string, list or SQL value (trimmed, unquoted).
    Text(String),
}

impl Node {
    fn text(&self) -> Option<&str> {
        match &self.value {
            Value::Text(t) => Some(t),
            Value::Block(_) => None,
        }
    }
    fn children(&self) -> &[Node] {
        match &self.value {
            Value::Block(c) => c,
            Value::Text(_) => &[],
        }
    }
    fn child_text(&self, key: &str) -> Option<&str> {
        self.children()
            .iter()
            .find(|n| n.key == key)
            .and_then(Node::text)
    }
}

/// Parse a LookML file into its top-level parameters. Malformed input is
/// parsed as far as possible; nothing here fails.
pub fn parse_lookml(src: &str) -> Vec<Node> {
    let mut p = LkmlParser {
        s: src.as_bytes(),
        src,
        i: 0,
    };
    p.block()
}

struct LkmlParser<'a> {
    s: &'a [u8],
    src: &'a str,
    i: usize,
}

impl LkmlParser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    /// Skip whitespace (newlines included) and `#` comments.
    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.i += 1;
            } else if c == b'#' {
                while self.peek().is_some_and(|c| c != b'\n') {
                    self.i += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Skip spaces and tabs only.
    fn skip_inline_ws(&mut self) {
        while self.peek().is_some_and(|c| c == b' ' || c == b'\t') {
            self.i += 1;
        }
    }

    fn block(&mut self) -> Vec<Node> {
        let mut nodes = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                None => return nodes,
                Some(b'}') => {
                    self.i += 1;
                    return nodes;
                }
                _ => {}
            }
            let start = self.i;
            while self.peek().is_some_and(|c| {
                c.is_ascii_alphanumeric() || matches!(c, b'_' | b'+' | b'.' | b'-')
            }) {
                self.i += 1;
            }
            let key = self.src[start..self.i].to_string();
            self.skip_inline_ws();
            if key.is_empty() || self.peek() != Some(b':') {
                // Not a parameter: skip the rest of the line and resync.
                self.i = self.i.max(start + 1);
                while self.peek().is_some_and(|c| c != b'\n') {
                    self.i += 1;
                }
                continue;
            }
            self.i += 1; // ':'
            nodes.push(self.value(key));
        }
    }

    fn value(&mut self, key: String) -> Node {
        self.skip_inline_ws();
        let sqlish = key.starts_with("sql") || matches!(key.as_str(), "html" | "expression");
        // SQL/HTML values first: they may start with Liquid (`html: {% if … %}`).
        if sqlish {
            let text = self.sql_value(key == "sql_table_name");
            return Node {
                key,
                name: None,
                value: Value::Text(text),
            };
        }
        match self.peek() {
            Some(b'{') => {
                self.i += 1;
                return Node {
                    key,
                    name: None,
                    value: Value::Block(self.block()),
                };
            }
            Some(b'[') => {
                let text = self.balanced(b'[', b']');
                return Node {
                    key,
                    name: None,
                    value: Value::Text(text),
                };
            }
            _ => {}
        }
        // Scalar (possibly a block name, e.g. `view: orders {`).
        let scalar = if self.peek() == Some(b'"') {
            self.quoted()
        } else {
            let start = self.i;
            while self
                .peek()
                .is_some_and(|c| !c.is_ascii_whitespace() && c != b'{' && c != b'}' && c != b'#')
            {
                self.i += 1;
            }
            self.src[start..self.i].to_string()
        };
        self.skip_inline_ws();
        if self.peek() == Some(b'{') {
            self.i += 1;
            return Node {
                key,
                name: Some(scalar),
                value: Value::Block(self.block()),
            };
        }
        // Rest of the line belongs to the value (`label: Sales by region`),
        // unless another parameter follows inline (`from: x  sql_on: … ;;`).
        if NEXT_PARAM.is_match(&self.src[self.i..]) {
            return Node {
                key,
                name: None,
                value: Value::Text(scalar),
            };
        }
        let start = self.i;
        while self
            .peek()
            .is_some_and(|c| c != b'\n' && c != b'#' && c != b'}')
        {
            self.i += 1;
        }
        let rest = self.src[start..self.i].trim();
        let text = if rest.is_empty() {
            scalar
        } else {
            format!("{scalar} {rest}")
        };
        Node {
            key,
            name: None,
            value: Value::Text(text),
        }
    }

    /// SQL-ish value up to `;;`. `sql_table_name` may also end at the line end.
    fn sql_value(&mut self, line_ends: bool) -> String {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c == b';' && self.s.get(self.i + 1) == Some(&b';') {
                let text = self.src[start..self.i].trim().to_string();
                self.i += 2;
                return text;
            }
            if line_ends && c == b'\n' {
                break;
            }
            self.i += 1;
        }
        self.src[start..self.i].trim().to_string()
    }

    fn quoted(&mut self) -> String {
        self.i += 1;
        let start = self.i;
        while let Some(c) = self.peek() {
            if c == b'\\' {
                self.i += 2;
                continue;
            }
            if c == b'"' {
                break;
            }
            self.i += 1;
        }
        let end = self.i.min(self.s.len());
        self.i = (self.i + 1).min(self.s.len());
        self.src[start..end].to_string()
    }

    fn balanced(&mut self, open: u8, close: u8) -> String {
        let start = self.i;
        let mut depth = 0usize;
        while let Some(c) = self.peek() {
            match c {
                b'"' => {
                    self.quoted();
                    continue;
                }
                c if c == open => depth += 1,
                c if c == close => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        self.i += 1;
                        break;
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
        self.src[start..self.i].to_string()
    }
}

// ---------------------------------------------------------------------------
// Project model: views, explores, dashboards.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct View {
    table: Option<String>,
    derived_sql: Option<String>,
    extends: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct Explore {
    views: Vec<String>,
    extends: Vec<String>,
}

/// A parsed LookML project.
#[derive(Debug, Default)]
pub struct Project {
    views: HashMap<String, View>,
    /// `(model, explore)` → explore; model is the `.model.lkml` file stem, or
    /// empty for explores defined in other files (included by models).
    explores: HashMap<(String, String), Explore>,
}

static VIEW_REF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{\s*(\w+)\.SQL_TABLE_NAME\s*\}").unwrap());
static CTE_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b([A-Za-z_]\w*)\s+as\s+(?:not\s+)?(?:materialized\s+)?\(").unwrap()
});
static FIELD_REF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$\{[^}]*\}").unwrap());
static CONSTANT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"@\{\s*(\w+)\s*\}").unwrap());
static LIQUID_TAG: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{%.*?%\}").unwrap());
static LIQUID_OUT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{\{.*?\}\}").unwrap());

fn list_items(text: &str) -> Vec<String> {
    text.trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Strip LookML constants, Liquid and quoting from a table reference.
fn clean_table(raw: &str) -> String {
    let s = CONSTANT.replace_all(raw, "$1");
    let s = LIQUID_TAG.replace_all(&s, "");
    let s = LIQUID_OUT.replace_all(&s, "x");
    s.trim()
        .trim_end_matches(";;")
        .replace(['`', '"'], "")
        .trim()
        .to_lowercase()
}

impl Project {
    /// Add one `.lkml` file. `model` is the file's model name when it is a
    /// `.model.lkml` file.
    pub fn add_file(&mut self, nodes: &[Node], model: &str) {
        for n in nodes {
            let Some(name) = n.name.as_deref() else {
                continue;
            };
            // Refinements (`view: +orders`) extend the original.
            let (name, refine) = match name.strip_prefix('+') {
                Some(base) => (base.to_string(), true),
                None => (name.to_string(), false),
            };
            match n.key.as_str() {
                "view" => {
                    let mut v = View::default();
                    if let Some(t) = n.child_text("sql_table_name") {
                        v.table = Some(t.to_string());
                    }
                    if let Some(d) = n.children().iter().find(|c| c.key == "derived_table") {
                        v.derived_sql = d.child_text("sql").map(String::from);
                    }
                    if let Some(e) = n.child_text("extends") {
                        v.extends = list_items(e);
                    }
                    let entry = self.views.entry(name).or_default();
                    if refine {
                        entry.table = v.table.or(entry.table.take());
                        entry.derived_sql = v.derived_sql.or(entry.derived_sql.take());
                        entry.extends.extend(v.extends);
                    } else {
                        *entry = v;
                    }
                }
                "explore" => {
                    let base = n
                        .child_text("from")
                        .or_else(|| n.child_text("view_name"))
                        .unwrap_or(&name)
                        .to_string();
                    let mut views = if refine { vec![] } else { vec![base] };
                    for j in n.children().iter().filter(|c| c.key == "join") {
                        let jname = j.name.clone().unwrap_or_default();
                        views.push(j.child_text("from").map(String::from).unwrap_or(jname));
                    }
                    let extends = n.child_text("extends").map(list_items).unwrap_or_default();
                    let entry = self.explores.entry((model.to_string(), name)).or_default();
                    entry.views.extend(views);
                    entry.extends.extend(extends);
                }
                _ => {}
            }
        }
    }

    fn explore(&self, model: &str, name: &str) -> Option<&Explore> {
        self.explores
            .get(&(model.to_string(), name.to_string()))
            .or_else(|| self.explores.get(&(String::new(), name.to_string())))
            .or_else(|| {
                // Any model: explores are often referenced across models.
                let mut hits: Vec<_> = self
                    .explores
                    .iter()
                    .filter(|((_, e), _)| e == name)
                    .collect();
                hits.sort_by(|a, b| a.0.cmp(b.0));
                hits.first().map(|(_, e)| *e)
            })
    }

    /// Tables behind an explore (base view, joins, extended explores).
    pub fn explore_tables(&self, model: &str, name: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let mut seen = BTreeSet::new();
        self.collect_explore(model, name, &mut out, &mut seen);
        out
    }

    fn collect_explore(
        &self,
        model: &str,
        name: &str,
        out: &mut BTreeSet<String>,
        seen: &mut BTreeSet<String>,
    ) {
        if !seen.insert(format!("explore:{name}")) {
            return;
        }
        let Some(e) = self.explore(model, name) else {
            return;
        };
        for v in &e.views {
            self.collect_view(v, out, seen);
        }
        for x in &e.extends {
            self.collect_explore(model, x, out, seen);
        }
    }

    /// Tables behind a view.
    pub fn view_tables(&self, name: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        self.collect_view(name, &mut out, &mut BTreeSet::new());
        out
    }

    fn collect_view(&self, name: &str, out: &mut BTreeSet<String>, seen: &mut BTreeSet<String>) {
        if !seen.insert(format!("view:{name}")) {
            return;
        }
        let Some(v) = self.views.get(name) else {
            return;
        };
        if let Some(t) = &v.table {
            if let Some(view) = VIEW_REF.captures(t) {
                self.collect_view(&view[1], out, seen);
            } else {
                let t = clean_table(t);
                if !t.is_empty() {
                    out.insert(t);
                }
            }
            return;
        }
        if let Some(sql) = &v.derived_sql {
            // `${other.SQL_TABLE_NAME}` → that view's tables.
            for cap in VIEW_REF.captures_iter(sql) {
                self.collect_view(&cap[1], out, seen);
            }
            let sql = VIEW_REF.replace_all(sql, "lkml_view_ref");
            let sql = FIELD_REF.replace_all(&sql, "1");
            let sql = CONSTANT.replace_all(&sql, "$1");
            let sql = LIQUID_TAG.replace_all(&sql, " ");
            let sql = LIQUID_OUT.replace_all(&sql, "1");
            let sql = sql.replace('`', "\"");
            // CTE names, in case the SQL only parsed via the FROM/JOIN fallback.
            let ctes: BTreeSet<String> = CTE_NAME
                .captures_iter(&sql)
                .map(|c| c[1].to_lowercase())
                .collect();
            for t in super::metabase::sql_tables(&sql) {
                let t = clean_table(&t);
                if t != "lkml_view_ref" && !t.is_empty() && !ctes.contains(&t) {
                    out.insert(t);
                }
            }
            return;
        }
        for parent in &v.extends {
            self.collect_view(parent, out, seen);
        }
    }
}

// ---------------------------------------------------------------------------
// Loading files and dashboards.
// ---------------------------------------------------------------------------

/// Read a LookML project from files and directories (searched recursively)
/// and return its LookML dashboards. `url` is the exposure URL; `{name}` is
/// replaced by the dashboard id. Without `url`, the dashboard file path is used.
pub fn dashboards_from_paths(paths: &[PathBuf], url: Option<&str>) -> Result<Vec<Dashboard>> {
    let mut files = Vec::new();
    for p in paths {
        if p.is_dir() {
            for entry in walkdir::WalkDir::new(p).sort_by_file_name() {
                let entry = entry?;
                if entry.file_type().is_file() {
                    files.push(entry.into_path());
                }
            }
        } else {
            files.push(p.clone());
        }
    }

    let mut project = Project::default();
    let mut dashboard_files = Vec::new();
    for f in &files {
        let name = f.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(".dashboard.lookml") || name.ends_with(".dashboard.lkml") {
            dashboard_files.push(f.clone());
        } else if name.ends_with(".lkml") {
            let src = std::fs::read_to_string(f)
                .with_context(|| format!("cannot read {}", f.display()))?;
            let model = name.strip_suffix(".model.lkml").unwrap_or("");
            project.add_file(&parse_lookml(&src), model);
        }
    }

    let mut dashboards = Vec::new();
    for f in &dashboard_files {
        let src =
            std::fs::read_to_string(f).with_context(|| format!("cannot read {}", f.display()))?;
        dashboards.extend(parse_dashboards(&project, f, &src, url));
    }
    Ok(dashboards)
}

/// Dashboards in one `.dashboard.lookml` file (a YAML list of dashboards).
pub fn parse_dashboards(
    project: &Project,
    path: &Path,
    src: &str,
    url: Option<&str>,
) -> Vec<Dashboard> {
    // Exported LookML dashboards often carry duplicate keys, which Looker
    // (and PyYAML) accept; fall back to the lenient loader for those.
    let Some(doc) = serde_yaml::from_str::<serde_yaml::Value>(src)
        .ok()
        .or_else(|| crate::parser::yaml_schema::lenient_yaml(src))
    else {
        return Vec::new();
    };
    let items: Vec<&serde_yaml::Value> = match &doc {
        serde_yaml::Value::Sequence(s) => s.iter().collect(),
        v @ serde_yaml::Value::Mapping(_) => vec![v],
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for d in items {
        let get = |k: &str| d.get(k).and_then(|v| v.as_str()).map(String::from);
        let Some(id) = get("dashboard") else {
            continue;
        };
        // Every (model, explore) an element queries, at any depth
        // (`model`/`explore`, `query:`, `merged_queries:`).
        let mut pairs: BTreeMap<(String, String), ()> = BTreeMap::new();
        if let Some(elements) = d.get("elements") {
            collect_queries(elements, &mut pairs);
        }
        let mut tables = BTreeSet::new();
        for (model, explore) in pairs.keys() {
            tables.extend(project.explore_tables(model, explore));
        }
        let url = match url {
            Some(u) => u.replace("{name}", &id),
            None => path.to_string_lossy().replace('\\', "/"),
        };
        out.push(Dashboard {
            name: get("title").unwrap_or_else(|| id.clone()),
            id,
            url,
            description: get("description"),
            owner_name: Some("looker".to_string()),
            owner_email: None,
            tables,
        });
    }
    out
}

fn collect_queries(v: &serde_yaml::Value, out: &mut BTreeMap<(String, String), ()>) {
    match v {
        serde_yaml::Value::Mapping(m) => {
            if let Some(explore) = m.get("explore").and_then(|e| e.as_str()) {
                let model = m.get("model").and_then(|e| e.as_str()).unwrap_or("");
                out.insert((model.to_string(), explore.to_string()), ());
            }
            for (_, child) in m {
                collect_queries(child, out);
            }
        }
        serde_yaml::Value::Sequence(s) => {
            for child in s {
                collect_queries(child, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIEWS: &str = r#"
# Orders, straight from the mart
view: orders {
  sql_table_name: analytics.fct_orders ;;
  dimension: id { primary_key: yes  sql: ${TABLE}.id ;; }
  measure: revenue {
    type: sum
    sql: ${TABLE}.amount ;;  # comment after a value
    html: {% if value > 0 %}<b>{{ value }}</b>{% endif %} ;;
  }
}

view: customers {
  sql_table_name: `@{PROJECT}.@{DATASET}.dim_customers`
  dimension: name { label: "Customer name" }
}

view: customer_ltv {
  derived_table: {
    datagroup_trigger: daily
    sql: SELECT c.id, SUM(o.amount) AS ltv
         FROM ${customers.SQL_TABLE_NAME} c
         JOIN analytics.payments p ON p.order_id = c.id
         {% if region._is_filtered %} WHERE {% condition region %} c.region {% endcondition %} {% endif %}
         GROUP BY 1 ;;
  }
}

view: base_template { extension: required  sql_table_name: stg_events ;; }
view: events { extends: [base_template] }
view: +orders { dimension: extra { sql: 1 ;; } }
"#;

    const MODEL: &str = r#"
connection: "warehouse"
include: "*.view"
explore: orders {
  join: customers { sql_on: ${orders.customer_id} = ${customers.id} ;; relationship: many_to_one }
  join: ltv { from: customer_ltv  sql_on: ${ltv.id} = ${customers.id} ;; }
}
explore: event_stream { from: events }
# explore: commented_out {}
"#;

    const DASHBOARD: &str = r#"
- dashboard: sales_overview
  title: Sales Overview
  description: Revenue and customers
  layout: newspaper
  elements:
  - name: revenue
    model: shop
    explore: orders
    type: single_value
  - name: merged
    merged_queries:
    - model: shop
      explore: event_stream
  - name: note
    type: text
    title_text: Notes
  filters:
  - name: Region
    model: shop
    explore: unrelated
"#;

    fn project() -> Project {
        let mut p = Project::default();
        p.add_file(&parse_lookml(VIEWS), "");
        p.add_file(&parse_lookml(MODEL), "shop");
        p
    }

    #[test]
    fn tokenizer_handles_blocks_sql_and_comments() {
        let nodes = parse_lookml(VIEWS);
        let names: Vec<_> = nodes.iter().filter_map(|n| n.name.as_deref()).collect();
        assert_eq!(
            names,
            vec![
                "orders",
                "customers",
                "customer_ltv",
                "base_template",
                "events",
                "+orders"
            ]
        );
        assert_eq!(
            nodes[0].child_text("sql_table_name"),
            Some("analytics.fct_orders")
        );
        // sql_table_name without `;;` ends at the line end.
        assert_eq!(
            nodes[1].child_text("sql_table_name"),
            Some("`@{PROJECT}.@{DATASET}.dim_customers`")
        );
        let commented: Vec<_> = parse_lookml(MODEL)
            .into_iter()
            .filter_map(|n| n.name)
            .collect();
        assert_eq!(commented, vec!["orders", "event_stream"]);
    }

    #[test]
    fn views_resolve_tables() {
        let p = project();
        let v = |n: &str| p.view_tables(n).into_iter().collect::<Vec<_>>();
        assert_eq!(v("orders"), vec!["analytics.fct_orders"]);
        assert_eq!(v("customers"), vec!["project.dataset.dim_customers"]);
        // Derived table: its own table plus the referenced view's table.
        assert_eq!(
            v("customer_ltv"),
            vec!["analytics.payments", "project.dataset.dim_customers"]
        );
        assert_eq!(v("events"), vec!["stg_events"]);
    }

    #[test]
    fn explores_union_base_and_joins() {
        let p = project();
        let t: Vec<_> = p.explore_tables("shop", "orders").into_iter().collect();
        assert_eq!(
            t,
            vec![
                "analytics.fct_orders",
                "analytics.payments",
                "project.dataset.dim_customers"
            ]
        );
        let t: Vec<_> = p
            .explore_tables("shop", "event_stream")
            .into_iter()
            .collect();
        assert_eq!(t, vec!["stg_events"]);
    }

    #[test]
    fn dashboards_use_element_explores_only() {
        let p = project();
        let ds = parse_dashboards(
            &p,
            Path::new("dash/sales.dashboard.lookml"),
            DASHBOARD,
            Some("https://looker.example.com/dashboards/shop::{name}"),
        );
        assert_eq!(ds.len(), 1);
        let d = &ds[0];
        assert_eq!(d.name, "Sales Overview");
        assert_eq!(
            d.url,
            "https://looker.example.com/dashboards/shop::sales_overview"
        );
        assert_eq!(d.description.as_deref(), Some("Revenue and customers"));
        assert_eq!(d.owner_name.as_deref(), Some("looker"));
        let t: Vec<_> = d.tables.iter().map(String::as_str).collect();
        // `unrelated` is only a dashboard filter, not a tile.
        assert_eq!(
            t,
            vec![
                "analytics.fct_orders",
                "analytics.payments",
                "project.dataset.dim_customers",
                "stg_events"
            ]
        );
    }

    #[test]
    fn walks_a_project_directory() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        std::fs::create_dir_all(p.join("views")).unwrap();
        std::fs::write(p.join("views/all.view.lkml"), VIEWS).unwrap();
        std::fs::write(p.join("shop.model.lkml"), MODEL).unwrap();
        std::fs::write(p.join("sales.dashboard.lookml"), DASHBOARD).unwrap();
        let ds = dashboards_from_paths(&[p.to_path_buf()], None).unwrap();
        assert_eq!(ds.len(), 1);
        assert!(ds[0].url.ends_with("sales.dashboard.lookml"));
        assert_eq!(ds[0].tables.len(), 4);
    }
}
