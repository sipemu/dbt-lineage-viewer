//! AST-backed column lineage (#35). Parses each model's SQL with `sqlparser`
//! and traces every output column to the upstream model / source / seed
//! columns it reads.
//!
//! Resolution is scope based, like a SQL engine's binder:
//! - CTEs and subqueries are scopes of their own; a column read from one is
//!   followed into its definition until it reaches a real relation.
//! - `select *` / `t.*` pass columns through by name, and expand to concrete
//!   columns when the relation's columns are known.
//! - `UNION` combines its branches by position.
//! - A projection depends on every column its expression reads (functions,
//!   `CASE`, window `PARTITION BY` / `ORDER BY`); `WHERE`, `JOIN ON` and
//!   `GROUP BY` are filters, not lineage.
//!
//! Models are analyzed in dependency order, so a model's output columns are
//! known when a downstream model selects `*` from it. Source and seed columns
//! come from YAML declarations and CSV headers.
//!
//! Jinja is neutralised before parsing: `ref()` / `source()` become relation
//! placeholders, `dbt_utils.star()` becomes `*`, other `{{ … }}` expressions
//! become a function of the column names they mention, and `{% if %}` keeps
//! its first branch. A model whose SQL still does not parse yields no edges;
//! the regex analyzer covers it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::LazyLock;

use petgraph::stable_graph::NodeIndex;
use regex::Regex;
use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Visit,
    Visitor,
};
use sqlparser::dialect::{
    BigQueryDialect, Dialect, DuckDbDialect, GenericDialect, PostgreSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;

use crate::graph::types::{LineageGraph, NodeType};

use super::column_lineage::{ColumnConfidence, ColumnEdge, ColumnLineage};
use super::manifest::{normalize_relation, CompiledSql};

/// Resolve column lineage for the entire graph using the SQL AST. Relative
/// node file paths are read relative to `root`. Models whose compiled SQL is
/// in `root/target/manifest.json` (and whose file is unchanged since) are
/// analyzed from that SQL instead.
pub fn resolve_column_lineage_ast_in(
    graph: &LineageGraph,
    root: &std::path::Path,
) -> ColumnLineage {
    resolve_column_lineage_ast_for(graph, root, None)
}

/// Like [`resolve_column_lineage_ast_in`], emitting edges only into the nodes
/// in `only` (`None`: every node). Upstream models are still analyzed so
/// their columns are known.
pub fn resolve_column_lineage_ast_for(
    graph: &LineageGraph,
    root: &std::path::Path,
    only: Option<&HashSet<NodeIndex>>,
) -> ColumnLineage {
    let compiled = compiled_sql_in(root);
    let catalog = super::manifest::load_catalog_columns(&root.join("target").join("catalog.json"));
    ColumnLineage {
        edges: analyze_graph(graph, root, only, compiled.as_ref(), catalog.as_ref()).0,
    }
}

/// Like [`resolve_column_lineage_ast_for`] with explicit compiled SQL (`None`:
/// raw SQL only).
pub fn resolve_column_lineage_ast_with(
    graph: &LineageGraph,
    root: &std::path::Path,
    only: Option<&HashSet<NodeIndex>>,
    compiled: Option<&CompiledSql>,
) -> ColumnLineage {
    ColumnLineage {
        edges: analyze_graph(graph, root, only, compiled, None).0,
    }
}

/// Compiled SQL from `root/target/manifest.json`, when it has any.
pub fn compiled_sql_in(root: &std::path::Path) -> Option<CompiledSql> {
    super::manifest::load_compiled_sql(&root.join("target").join("manifest.json"), root)
}

/// Output columns (lowercase) of every model and snapshot whose columns are
/// fully known from its SQL, falling back to its YAML-declared columns.
pub fn model_output_columns(
    graph: &LineageGraph,
    root: &std::path::Path,
    compiled: Option<&CompiledSql>,
) -> HashMap<String, Vec<String>> {
    let models: HashSet<&str> = graph
        .node_indices()
        .map(|i| &graph[i])
        .filter(|n| matches!(n.node_type, NodeType::Model | NodeType::Snapshot))
        .map(|n| n.unique_id.as_str())
        .collect();
    analyze_graph(graph, root, Some(&HashSet::new()), compiled, None)
        .1
        .into_iter()
        .filter(|(id, _)| models.contains(id.as_str()))
        .collect()
}

/// Edges into the nodes in `only` (all when `None`), and the known columns
/// of every node after analysis.
fn analyze_graph(
    graph: &LineageGraph,
    root: &std::path::Path,
    only: Option<&HashSet<NodeIndex>>,
    compiled: Option<&CompiledSql>,
    catalog: Option<&HashMap<String, Vec<String>>>,
) -> (Vec<ColumnEdge>, HashMap<String, Vec<String>>) {
    let relations = Relations::new(graph);
    // Known columns: the warehouse's (catalog.json) when given, else
    // YAML-declared / CSV-header / SELECT-read ones. Models' columns are
    // replaced by what analysis infers as it reaches them.
    let mut schema: HashMap<String, Vec<String>> = graph
        .node_indices()
        .map(|i| &graph[i])
        .filter_map(|n| {
            let cols = catalog
                .and_then(|c| c.get(&n.unique_id))
                .cloned()
                .unwrap_or_else(|| lower_all(&n.columns));
            (!cols.is_empty()).then(|| (n.unique_id.clone(), cols))
        })
        .collect();

    let order =
        petgraph::algo::toposort(graph, None).unwrap_or_else(|_| graph.node_indices().collect());
    let mut edges = Vec::new();
    for idx in order {
        let node = &graph[idx];
        if !matches!(node.node_type, NodeType::Model | NodeType::Snapshot) {
            continue;
        }
        let Some(sql) = node
            .file_path
            .as_ref()
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .and_then(|p| std::fs::read_to_string(root.join(p)).ok())
        else {
            continue;
        };
        let parents: Vec<String> = graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .map(|p| graph[p].unique_id.clone())
            .collect();
        let declared = schema.get(&node.unique_id).cloned().unwrap_or_default();
        let model = match compiled.and_then(|c| c.sql.get(&node.unique_id).map(|sql| (c, sql))) {
            Some((c, compiled_sql)) => {
                analyze_compiled(compiled_sql, &c.relations, &schema, &declared)
                    .or_else(|| analyze_model(&sql, &relations, &schema, &parents, &declared))
            }
            None => analyze_model(&sql, &relations, &schema, &parents, &declared),
        };
        let Some(model) = model else {
            continue;
        };
        if let Some(cols) = &model.columns {
            schema.insert(node.unique_id.clone(), cols.clone());
        }
        if only.is_none_or(|set| set.contains(&idx)) {
            edges.extend(
                model
                    .edges
                    .into_iter()
                    .map(|e| e.into_edge(&node.unique_id)),
            );
        }
    }
    (edges, schema)
}

/// Analyze one model's SQL. Public for direct testing: `schema` maps node ids
/// to known column names, `labels` maps ref names to node ids.
pub fn analyze_sql(
    sql: &str,
    target_node_id: &str,
    labels: &HashMap<String, String>,
    schema: &HashMap<String, Vec<String>>,
) -> Vec<ColumnEdge> {
    let relations = Relations {
        by_ref: labels.clone(),
        sources: schema
            .keys()
            .filter(|k| k.starts_with("source."))
            .cloned()
            .collect(),
    };
    analyze_model(sql, &relations, schema, &[], &[])
        .map(|m| {
            m.edges
                .into_iter()
                .map(|e| e.into_edge(target_node_id))
                .collect()
        })
        .unwrap_or_default()
}

fn lower_all(cols: &[String]) -> Vec<String> {
    cols.iter().map(|c| c.to_lowercase()).collect()
}

/// How ref() / source() names map to graph node ids.
struct Relations {
    /// ref name → node id (models, seeds, snapshots).
    by_ref: HashMap<String, String>,
    /// Known `source.<source>.<table>` ids.
    sources: HashSet<String>,
}

impl Relations {
    fn new(graph: &LineageGraph) -> Self {
        let mut by_ref = HashMap::new();
        let mut sources = HashSet::new();
        // Models win over seeds and snapshots of the same name, like dbt.
        let mut nodes: Vec<_> = graph.node_indices().map(|i| &graph[i]).collect();
        nodes.sort_by_key(|n| match n.node_type {
            NodeType::Model => 0,
            NodeType::Seed => 1,
            NodeType::Snapshot => 2,
            _ => 3,
        });
        for n in nodes {
            match n.node_type {
                NodeType::Model | NodeType::Seed | NodeType::Snapshot => {
                    by_ref
                        .entry(n.label.clone())
                        .or_insert_with(|| n.unique_id.clone());
                }
                NodeType::Source => {
                    sources.insert(n.unique_id.clone());
                }
                _ => {}
            }
        }
        Relations { by_ref, sources }
    }

    /// Node id of `ref(name[, v=version])`. A versioned model's nodes are
    /// labelled `name.v<version>`; without a version this picks the highest,
    /// dbt's default latest (the graph builder also honours `latest_version`).
    fn resolve_ref(&self, name: &str, version: Option<&str>) -> String {
        if let Some(v) = version {
            if let Some(id) = self.by_ref.get(&format!("{name}.v{v}")) {
                return id.clone();
            }
        }
        if let Some(id) = self.by_ref.get(name) {
            return id.clone();
        }
        let prefix = format!("{name}.v");
        let latest = self
            .by_ref
            .iter()
            .filter_map(|(label, id)| Some((label.strip_prefix(&prefix)?, id)))
            .max_by(
                |(a, _), (b, _)| match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(x), Ok(y)) => x.total_cmp(&y),
                    _ => a.cmp(b),
                },
            );
        latest
            .map(|(_, id)| id.clone())
            .unwrap_or_else(|| format!("model.{name}"))
    }
}

/// An edge into the model being analyzed.
struct ModelEdge {
    source_node: String,
    source_column: String,
    target_column: String,
    confidence: ColumnConfidence,
}

impl ModelEdge {
    fn into_edge(self, target: &str) -> ColumnEdge {
        ColumnEdge {
            source_node: self.source_node,
            source_column: self.source_column,
            target_node: target.to_string(),
            target_column: self.target_column,
            confidence: self.confidence,
        }
    }
}

struct ModelResult {
    /// Output columns, when every `*` could be expanded.
    columns: Option<Vec<String>>,
    edges: Vec<ModelEdge>,
}

// ---------------------------------------------------------------------------
// Jinja neutralisation

static JINJA_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{#.*?#\}").unwrap());
static JINJA_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)\{\{.*?\}\}|\{%.*?%\}").unwrap());
static REF_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"^\{\{-?\s*ref\s*\(\s*['"]([^'"]+)['"]\s*(?:,\s*['"]([^'"]+)['"]\s*)?(?:,\s*(?:v|version)\s*=\s*['"]?([^'")\s]+)['"]?\s*)?\)\s*-?\}\}$"#,
    )
    .unwrap()
});
static SOURCE_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^\{\{-?\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*-?\}\}$"#)
        .unwrap()
});
static STRING_ARG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"['"]([A-Za-z_][A-Za-z0-9_]*)['"]"#).unwrap());
static STATEMENT_KEYWORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\{%-?\s*(\w+)").unwrap());
/// Text ending where a type is expected: `cast(x as ` or `x::`.
static TYPE_POSITION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(\bas\s*|::\s*)$").unwrap());

/// String arguments of dbt's date macros (`date_trunc('day', col)`,
/// `dateadd('month', 1, col)`) that name a date part, not a column.
const DATE_PARTS: &[&str] = &[
    "microsecond",
    "millisecond",
    "second",
    "minute",
    "hour",
    "day",
    "dayofweek",
    "dayofyear",
    "week",
    "isoweek",
    "month",
    "quarter",
    "year",
];

/// Function name standing in for a Jinja expression.
const JINJA_FN: &str = "__jinja__";

/// Prefix of relation placeholders; the index points into the placeholder list.
const REL_PREFIX: &str = "__dbtl_rel_";

/// Turn dbt SQL into plain SQL. Returns the SQL and the node id behind each
/// relation placeholder `__dbtl_rel_<i>__`.
fn neutralize_jinja(sql: &str, relations: &Relations) -> (String, Vec<String>) {
    let sql = JINJA_COMMENT.replace_all(sql, "");
    let mut out = String::with_capacity(sql.len());
    let mut placeholders: Vec<String> = Vec::new();
    // One entry per open `{% if %}` / `{% set %}…{% endset %}`: is its
    // current branch kept?
    let mut frames: Vec<bool> = Vec::new();
    let mut last = 0;
    for m in JINJA_TAG.find_iter(&sql) {
        let keeping = frames.iter().all(|&k| k);
        if keeping {
            out.push_str(&sql[last..m.start()]);
        }
        last = m.end();
        let tag = m.as_str();
        if tag.starts_with("{%") {
            let keyword = STATEMENT_KEYWORD
                .captures(tag)
                .map(|c| c[1].to_string())
                .unwrap_or_default();
            match keyword.as_str() {
                "if" => frames.push(true),
                "elif" | "else" => {
                    if let Some(top) = frames.last_mut() {
                        *top = false;
                    }
                }
                "endif" | "endset" | "endcall" => {
                    frames.pop();
                }
                // `{% set x %}…{% endset %}` captures text; `{% set x = … %}` doesn't.
                "set" if !tag.contains('=') => frames.push(false),
                "call" => frames.push(false),
                _ => {}
            }
            continue;
        }
        if !keeping {
            continue;
        }
        let replacement = if let Some(c) = REF_CALL.captures(tag) {
            let name = c.get(2).unwrap_or_else(|| c.get(1).unwrap()).as_str();
            placeholders.push(relations.resolve_ref(name, c.get(3).map(|v| v.as_str())));
            format!("{REL_PREFIX}{}__", placeholders.len() - 1)
        } else if let Some(c) = SOURCE_CALL.captures(tag) {
            placeholders.push(format!("source.{}.{}", &c[1], &c[2]));
            format!("{REL_PREFIX}{}__", placeholders.len() - 1)
        } else if tag.contains("star(") {
            "*".to_string()
        } else if tag.contains("config(") {
            String::new()
        } else if TYPE_POSITION.is_match(&out) {
            // `cast(x as {{ dbt.type_string() }})`: a type name.
            JINJA_FN.to_string()
        } else {
            // Any other expression: a function of the column names it mentions.
            let args: Vec<&str> = STRING_ARG
                .captures_iter(tag)
                .map(|c| c.get(1).unwrap().as_str())
                .filter(|a| !DATE_PARTS.contains(&a.to_lowercase().as_str()))
                .collect();
            format!("{JINJA_FN}({})", args.join(", "))
        };
        out.push_str(&replacement);
    }
    if frames.iter().all(|&k| k) {
        out.push_str(&sql[last..]);
    }
    let _ = &relations.sources;
    (out, placeholders)
}

// ---------------------------------------------------------------------------
// Scopes

#[derive(Clone)]
enum Rel {
    /// A graph node.
    Base(String),
    /// A table dbt-lineage doesn't know (hard-coded name): columns resolve
    /// but yield no edges.
    Unknown,
    /// A CTE or subquery.
    Derived(Rc<Scope>),
}

#[derive(Clone)]
struct Input {
    rel: Rel,
    col: String,
}

#[derive(Default)]
struct Scope {
    /// Named output columns with what each reads, in order.
    cols: Vec<(String, Vec<Input>, bool)>,
    /// Relations selected with `*`, passing their columns through by name.
    stars: Vec<Rel>,
}

impl Scope {
    fn lookup(&self, col: &str) -> Option<&Vec<Input>> {
        self.cols
            .iter()
            .find(|(c, _, _)| c == col)
            .map(|(_, i, _)| i)
    }
}

struct Binder<'a> {
    schema: &'a HashMap<String, Vec<String>>,
    placeholders: &'a [String],
    /// Compiled SQL: normalized relation name → node id.
    relations: Option<&'a HashMap<String, String>>,
}

/// dbt inlines ephemeral models into compiled SQL as CTEs with this prefix.
const EPHEMERAL_CTE: &str = "__dbt__cte__";

impl Binder<'_> {
    /// Known columns of a relation; `None` when unknown.
    fn columns(&self, rel: &Rel, depth: usize) -> Option<Vec<String>> {
        if depth > 32 {
            return None;
        }
        match rel {
            Rel::Base(id) => self.schema.get(id).cloned(),
            Rel::Unknown => None,
            Rel::Derived(scope) => {
                let mut cols: Vec<String> = scope.cols.iter().map(|(c, _, _)| c.clone()).collect();
                for star in &scope.stars {
                    for c in self.columns(star, depth + 1)? {
                        if !cols.contains(&c) {
                            cols.push(c);
                        }
                    }
                }
                Some(cols)
            }
        }
    }

    /// Could `rel` have column `col`? Unknown relations might.
    fn may_have(&self, rel: &Rel, col: &str) -> bool {
        self.columns(rel, 0)
            .is_none_or(|cols| cols.iter().any(|c| c == col))
    }

    /// Follow a column to the graph-node columns it comes from.
    fn flatten(&self, rel: &Rel, col: &str, out: &mut BTreeSet<(String, String)>, depth: usize) {
        if depth > 32 {
            return;
        }
        match rel {
            Rel::Base(id) => {
                out.insert((id.clone(), col.to_string()));
            }
            Rel::Unknown => {}
            Rel::Derived(scope) => {
                if let Some(inputs) = scope.lookup(col) {
                    for i in inputs {
                        self.flatten(&i.rel, &i.col, out, depth + 1);
                    }
                    return;
                }
                let candidates: Vec<&Rel> = scope
                    .stars
                    .iter()
                    .filter(|s| scope.stars.len() == 1 || self.may_have(s, col))
                    .collect();
                for s in candidates {
                    self.flatten(s, col, out, depth + 1);
                }
            }
        }
    }

    /// Graph nodes behind a relation's unexpanded `*`.
    fn star_bases(&self, rel: &Rel, out: &mut BTreeSet<String>, depth: usize) {
        if depth > 32 {
            return;
        }
        match rel {
            Rel::Base(id) => {
                out.insert(id.clone());
            }
            Rel::Unknown => {}
            Rel::Derived(scope) => {
                for s in &scope.stars {
                    self.star_bases(s, out, depth + 1);
                }
            }
        }
    }

    fn query(&self, q: &Query, ctes: &HashMap<String, Rel>) -> Scope {
        let mut ctes = ctes.clone();
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                let mut scope = self.query(&cte.query, &ctes);
                // `with x (a, b) as (...)` renames the outputs by position.
                for (i, alias) in cte.alias.columns.iter().enumerate() {
                    if let Some(col) = scope.cols.get_mut(i) {
                        col.0 = alias.value.to_lowercase();
                    }
                }
                ctes.insert(
                    cte.alias.name.value.to_lowercase(),
                    Rel::Derived(Rc::new(scope)),
                );
            }
        }
        self.set_expr(&q.body, &ctes)
    }

    fn set_expr(&self, body: &SetExpr, ctes: &HashMap<String, Rel>) -> Scope {
        match body {
            SetExpr::Select(s) => self.select(s, ctes),
            SetExpr::Query(q) => self.query(q, ctes),
            SetExpr::SetOperation { left, right, .. } => {
                let mut l = self.set_expr(left, ctes);
                let r = self.set_expr(right, ctes);
                // Branches line up by position; the first branch names them.
                for (i, (_, inputs, bare)) in l.cols.iter_mut().enumerate() {
                    if let Some((_, r_inputs, r_bare)) = r.cols.get(i) {
                        inputs.extend(r_inputs.iter().cloned());
                        *bare &= *r_bare;
                    }
                }
                l.stars.extend(r.stars);
                l
            }
            _ => Scope::default(),
        }
    }

    fn select(&self, s: &Select, ctes: &HashMap<String, Rel>) -> Scope {
        let mut rels: Vec<(String, Rel)> = Vec::new();
        for twj in &s.from {
            self.table_with_joins(twj, ctes, &mut rels);
        }
        let mut scope = Scope::default();
        for item in &s.projection {
            match item {
                // `select {{ fill_staging_columns(...) }} from base`: a macro
                // that expands to a column list passes the FROM columns through.
                SelectItem::UnnamedExpr(Expr::Function(f)) if is_jinja_fn(&f.name) => {
                    scope.stars.extend(rels.iter().map(|(_, r)| r.clone()));
                }
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(name) = expr_name(expr) {
                        let inputs = self.inputs(expr, &rels);
                        scope.cols.push((name, inputs, true));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let inputs = self.inputs(expr, &rels);
                    let bare = matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
                    scope.cols.push((alias.value.to_lowercase(), inputs, bare));
                }
                SelectItem::Wildcard(_) => scope.stars.extend(rels.iter().map(|(_, r)| r.clone())),
                SelectItem::QualifiedWildcard(name, _) => {
                    let alias = name.0.last().map(|i| i.value.to_lowercase());
                    if let Some((_, rel)) = rels.iter().find(|(a, _)| Some(a) == alias.as_ref()) {
                        scope.stars.push(rel.clone());
                    }
                }
            }
        }
        scope
    }

    fn table_with_joins(
        &self,
        twj: &TableWithJoins,
        ctes: &HashMap<String, Rel>,
        rels: &mut Vec<(String, Rel)>,
    ) {
        self.table_factor(&twj.relation, ctes, rels);
        for join in &twj.joins {
            self.table_factor(&join.relation, ctes, rels);
        }
    }

    fn table_factor(
        &self,
        factor: &TableFactor,
        ctes: &HashMap<String, Rel>,
        rels: &mut Vec<(String, Rel)>,
    ) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let last = name
                    .0
                    .last()
                    .map(|i| i.value.to_lowercase())
                    .unwrap_or_default();
                let rel = if let Some(model) = last
                    .strip_prefix(EPHEMERAL_CTE)
                    .filter(|_| self.relations.is_some())
                {
                    // An inlined ephemeral model: lineage stops at the model.
                    Rel::Base(format!("model.{model}"))
                } else if name.0.len() == 1 && ctes.contains_key(&last) {
                    ctes[&last].clone()
                } else if let Some(id) = self.relations.and_then(|r| {
                    let full: Vec<&str> = name.0.iter().map(|i| i.value.as_str()).collect();
                    r.get(&normalize_relation(&full.join(".")))
                }) {
                    Rel::Base(id.clone())
                } else if let Some(i) = last
                    .strip_prefix(REL_PREFIX)
                    .and_then(|s| s.strip_suffix("__"))
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    self.placeholders
                        .get(i)
                        .map(|id| Rel::Base(id.clone()))
                        .unwrap_or(Rel::Unknown)
                } else {
                    Rel::Unknown
                };
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.to_lowercase())
                    .unwrap_or(last);
                rels.push((alias, rel));
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let scope = self.query(subquery, ctes);
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.to_lowercase())
                    .unwrap_or_default();
                rels.push((alias, Rel::Derived(Rc::new(scope))));
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.table_with_joins(table_with_joins, ctes, rels),
            _ => {}
        }
    }

    /// The columns an expression reads, bound to the FROM relations.
    fn inputs(&self, expr: &Expr, rels: &[(String, Rel)]) -> Vec<Input> {
        let mut refs = ColumnRefs::default();
        let _ = expr.visit(&mut refs);
        let mut out = Vec::new();
        for (qualifier, col) in refs.found {
            let rel = match qualifier {
                Some(q) => rels.iter().find(|(a, _)| *a == q).map(|(_, r)| r.clone()),
                None => self.bind_unqualified(&col, rels),
            };
            if let Some(rel) = rel {
                out.push(Input { rel, col });
            }
        }
        out
    }

    fn bind_unqualified(&self, col: &str, rels: &[(String, Rel)]) -> Option<Rel> {
        if rels.len() == 1 {
            // Bound unless the relation's columns are known and lack it (a
            // macro argument such as the 'day' in date_trunc('day', col)).
            return self.may_have(&rels[0].1, col).then(|| rels[0].1.clone());
        }
        let known: Vec<&Rel> = rels
            .iter()
            .filter(|(_, r)| {
                self.columns(r, 0)
                    .is_some_and(|cols| cols.iter().any(|c| c == col))
            })
            .map(|(_, r)| r)
            .collect();
        if known.len() == 1 {
            return Some(known[0].clone());
        }
        let unknown: Vec<&Rel> = rels
            .iter()
            .filter(|(_, r)| self.columns(r, 0).is_none())
            .map(|(_, r)| r)
            .collect();
        (known.is_empty() && unknown.len() == 1).then(|| unknown[0].clone())
    }
}

fn is_jinja_fn(name: &sqlparser::ast::ObjectName) -> bool {
    name.0.len() == 1 && name.0[0].value == JINJA_FN
}

/// Output name of an un-aliased projection: the column it reads.
fn expr_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(i) => Some(i.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.to_lowercase()),
        Expr::Nested(e) => expr_name(e),
        _ => None,
    }
}

/// Words that parse as identifiers but are not columns.
const NOT_COLUMNS: &[&str] = &[
    "current_date",
    "current_timestamp",
    "current_time",
    "true",
    "false",
    "null",
];

/// Column references in an expression, outside nested subqueries.
#[derive(Default)]
struct ColumnRefs {
    found: Vec<(Option<String>, String)>,
    subquery_depth: usize,
}

impl Visitor for ColumnRefs {
    type Break = ();

    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.subquery_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.subquery_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.subquery_depth > 0 {
            return ControlFlow::Continue(());
        }
        match expr {
            Expr::Identifier(i) => {
                let col = i.value.to_lowercase();
                if !NOT_COLUMNS.contains(&col.as_str()) {
                    self.found.push((None, col));
                }
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let n = parts.len();
                self.found.push((
                    Some(parts[n - 2].value.to_lowercase()),
                    parts[n - 1].value.to_lowercase(),
                ));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn parse(sql: &str) -> Option<Query> {
    let dialects: [&dyn Dialect; 5] = [
        &GenericDialect {},
        &SnowflakeDialect {},
        &BigQueryDialect {},
        &DuckDbDialect {},
        &PostgreSqlDialect {},
    ];
    for dialect in dialects {
        if let Ok(statements) = Parser::parse_sql(dialect, sql) {
            for stmt in statements {
                if let Statement::Query(q) = stmt {
                    return Some(*q);
                }
            }
            return None;
        }
    }
    None
}

static ADJACENT_JINJA: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(__jinja__\([^()]*\))(\s+)(__jinja__\(|[A-Za-z_])").unwrap());
static COMMA_BEFORE_FROM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i),(\s*)\bfrom\b").unwrap());

/// Macros that each expand to part of a column list sit next to each other
/// without commas (`select {{ a() }} {{ b() }} from t`): separate them.
fn separate_jinja_items(sql: &str) -> String {
    let mut out = sql.to_string();
    loop {
        let next = ADJACENT_JINJA.replace_all(&out, "$1,$2$3").to_string();
        if next == out {
            break;
        }
        out = next;
    }
    COMMA_BEFORE_FROM.replace_all(&out, "$1 from").to_string()
}

fn analyze_model(
    sql: &str,
    relations: &Relations,
    schema: &HashMap<String, Vec<String>>,
    parents: &[String],
    declared: &[String],
) -> Option<ModelResult> {
    let (mut cleaned, mut placeholders) = neutralize_jinja(sql, relations);
    // A model that is a single macro call reading one relation
    // (`{{ union_connections(...) }}`) passes that relation through.
    let whole_macro = cleaned.trim().strip_prefix(JINJA_FN).is_some_and(|rest| {
        rest.starts_with('(') && rest.ends_with(')') && rest.matches('(').count() == 1
    });
    if whole_macro && parents.len() == 1 {
        placeholders = vec![parents[0].clone()];
        cleaned = format!("select * from {REL_PREFIX}0__");
    }
    let query = parse(&cleaned).or_else(|| parse(&separate_jinja_items(&cleaned)))?;
    let binder = Binder {
        schema,
        placeholders: &placeholders,
        relations: None,
    };
    Some(model_result(&binder, &query, declared))
}

/// Analyze SQL dbt compiled (no Jinja), resolving relation names through the
/// manifest.
fn analyze_compiled(
    sql: &str,
    relations: &HashMap<String, String>,
    schema: &HashMap<String, Vec<String>>,
    declared: &[String],
) -> Option<ModelResult> {
    let query = parse(sql)?;
    let binder = Binder {
        schema,
        placeholders: &[],
        relations: Some(relations),
    };
    Some(model_result(&binder, &query, declared))
}

fn model_result(binder: &Binder, query: &Query, declared: &[String]) -> ModelResult {
    let scope = binder.query(query, &HashMap::new());

    let mut edges = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    for (name, inputs, bare) in &scope.cols {
        columns.push(name.clone());
        let mut sources = BTreeSet::new();
        for i in inputs {
            binder.flatten(&i.rel, &i.col, &mut sources, 0);
        }
        for (node, col) in sources {
            let confidence = match (bare, &col == name) {
                (true, true) => ColumnConfidence::Direct,
                (true, false) => ColumnConfidence::Aliased,
                _ => ColumnConfidence::Derived,
            };
            edges.push(ModelEdge {
                source_node: node,
                source_column: col,
                target_column: name.clone(),
                confidence,
            });
        }
    }

    // `*`: expand by name when the relation's columns are known, otherwise
    // record a whole-relation edge.
    let mut complete = true;
    let unexpanded = scope
        .stars
        .iter()
        .filter(|s| binder.columns(s, 0).is_none())
        .count();
    for star in &scope.stars {
        match binder.columns(star, 0) {
            Some(cols) => {
                for col in cols {
                    if columns.contains(&col) {
                        continue;
                    }
                    let mut sources = BTreeSet::new();
                    binder.flatten(star, &col, &mut sources, 0);
                    for (node, src_col) in sources {
                        edges.push(ModelEdge {
                            source_node: node,
                            confidence: if src_col == col {
                                ColumnConfidence::Direct
                            } else {
                                ColumnConfidence::Aliased
                            },
                            source_column: src_col,
                            target_column: col.clone(),
                        });
                    }
                    columns.push(col);
                }
            }
            None if unexpanded == 1 && !declared.is_empty() => {
                // The model's declared columns name what the `*` yields.
                for col in declared {
                    if columns.contains(col) {
                        continue;
                    }
                    let mut sources = BTreeSet::new();
                    binder.flatten(star, col, &mut sources, 0);
                    for (node, src_col) in sources {
                        edges.push(ModelEdge {
                            source_node: node,
                            confidence: ColumnConfidence::Direct,
                            source_column: src_col,
                            target_column: col.clone(),
                        });
                    }
                    columns.push(col.clone());
                }
            }
            None => {
                complete = false;
                let mut bases = BTreeSet::new();
                binder.star_bases(star, &mut bases, 0);
                for node in bases {
                    edges.push(ModelEdge {
                        source_node: node,
                        source_column: "*".into(),
                        target_column: "*".into(),
                        confidence: ColumnConfidence::Star,
                    });
                }
            }
        }
    }
    ModelResult {
        columns: complete.then_some(columns),
        edges,
    }
}

static FOR_LOOP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{%-?\s*for\s").unwrap());

/// Whether Jinja writes part of this model's SELECT (a loop, a macro that
/// expands to columns or to the whole query), or the SQL can't be parsed. The
/// model's column lineage may then be incomplete.
pub fn sql_generated_by_jinja(sql: &str) -> bool {
    if FOR_LOOP.is_match(&JINJA_COMMENT.replace_all(sql, "")) {
        return true;
    }
    let relations = Relations {
        by_ref: HashMap::new(),
        sources: HashSet::new(),
    };
    let (cleaned, _) = neutralize_jinja(sql, &relations);
    match parse(&cleaned).or_else(|| parse(&separate_jinja_items(&cleaned))) {
        Some(query) => query_has_jinja_projection(&query),
        None => true,
    }
}

fn query_has_jinja_projection(q: &Query) -> bool {
    q.with.as_ref().is_some_and(|w| {
        w.cte_tables
            .iter()
            .any(|c| query_has_jinja_projection(&c.query))
    }) || set_expr_has_jinja_projection(&q.body)
}

fn set_expr_has_jinja_projection(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(s) => {
            s.projection.iter().any(|item| {
                matches!(item, SelectItem::UnnamedExpr(Expr::Function(f)) if is_jinja_fn(&f.name))
            }) || s.from.iter().any(|twj| {
                std::iter::once(&twj.relation)
                    .chain(twj.joins.iter().map(|j| &j.relation))
                    .any(|f| matches!(f, TableFactor::Derived { subquery, .. } if query_has_jinja_projection(subquery)))
            })
        }
        SetExpr::Query(q) => query_has_jinja_projection(q),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_jinja_projection(left) || set_expr_has_jinja_projection(right)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> HashMap<String, String> {
        ["stg_orders", "order_items", "stg_customers", "a", "b"]
            .iter()
            .map(|l| (l.to_string(), format!("model.{l}")))
            .collect()
    }

    fn edges(sql: &str, schema: &[(&str, &[&str])]) -> Vec<(String, String, String)> {
        let schema: HashMap<String, Vec<String>> = schema
            .iter()
            .map(|(id, cols)| (id.to_string(), cols.iter().map(|c| c.to_string()).collect()))
            .collect();
        let mut out: Vec<_> = analyze_sql(sql, "model.t", &labels(), &schema)
            .into_iter()
            .map(|e| {
                (
                    format!("{}.{}", e.source_node, e.source_column),
                    e.target_column,
                    e.confidence.label().to_string(),
                )
            })
            .collect();
        out.sort();
        out
    }

    fn pairs(sql: &str, schema: &[(&str, &[&str])]) -> Vec<(String, String)> {
        edges(sql, schema)
            .into_iter()
            .map(|(s, t, _)| (s, t))
            .collect()
    }

    fn p(s: &str, t: &str) -> (String, String) {
        (s.to_string(), t.to_string())
    }

    #[test]
    fn direct_and_aliased_columns() {
        let e = edges(
            "select order_id, customer_id as cust from {{ ref('stg_orders') }}",
            &[],
        );
        assert_eq!(
            e,
            vec![
                (
                    "model.stg_orders.customer_id".into(),
                    "cust".into(),
                    "Aliased".into()
                ),
                (
                    "model.stg_orders.order_id".into(),
                    "order_id".into(),
                    "Direct".into()
                ),
            ]
        );
    }

    #[test]
    fn traces_through_ctes_and_aggregates() {
        // jaffle-shop's orders.sql shape.
        let sql = "
            with order_items as (select * from {{ ref('order_items') }}),
            summary as (
                select order_id, sum(supply_cost) as order_cost
                from order_items group by 1
            )
            select summary.order_cost from summary";
        assert_eq!(
            pairs(sql, &[]),
            vec![p("model.order_items.supply_cost", "order_cost")]
        );
    }

    #[test]
    fn join_resolves_qualified_and_known_columns() {
        let sql = "
            select o.order_id, c.name, amount
            from {{ ref('stg_orders') }} o
            join {{ ref('stg_customers') }} c on o.customer_id = c.id";
        let schema: &[(&str, &[&str])] = &[
            ("model.stg_orders", &["order_id", "customer_id", "amount"]),
            ("model.stg_customers", &["id", "name"]),
        ];
        assert_eq!(
            pairs(sql, schema),
            vec![
                p("model.stg_customers.name", "name"),
                p("model.stg_orders.amount", "amount"),
                p("model.stg_orders.order_id", "order_id"),
            ]
        );
    }

    #[test]
    fn star_expands_with_known_columns() {
        let sql = "with x as (select * from {{ ref('a') }}) select * from x";
        assert_eq!(
            pairs(sql, &[("model.a", &["id", "v"])]),
            vec![p("model.a.id", "id"), p("model.a.v", "v")]
        );
        // Unknown columns: a whole-relation edge, as before.
        assert_eq!(pairs(sql, &[]), vec![p("model.a.*", "*")]);
    }

    #[test]
    fn named_column_from_star_cte_passes_through() {
        let sql = "with x as (select * from {{ ref('a') }}) select v + 1 as w from x";
        assert_eq!(pairs(sql, &[]), vec![p("model.a.v", "w")]);
    }

    #[test]
    fn window_and_case_inputs() {
        let sql = "select row_number() over (partition by customer_id order by ordered_at) as n,
                          case when amount > 0 then 'paid' end as status
                   from {{ ref('stg_orders') }}";
        assert_eq!(
            pairs(sql, &[]),
            vec![
                p("model.stg_orders.amount", "status"),
                p("model.stg_orders.customer_id", "n"),
                p("model.stg_orders.ordered_at", "n"),
            ]
        );
    }

    #[test]
    fn union_combines_branches_by_position() {
        let sql = "select id from {{ ref('a') }} union all select other_id from {{ ref('b') }}";
        assert_eq!(
            pairs(sql, &[]),
            vec![p("model.a.id", "id"), p("model.b.other_id", "id")]
        );
    }

    #[test]
    fn subquery_in_from() {
        let sql = "select s.total from (select sum(amount) as total from {{ ref('a') }}) s";
        assert_eq!(pairs(sql, &[]), vec![p("model.a.amount", "total")]);
    }

    #[test]
    fn jinja_macros_and_branches_are_neutralised() {
        let sql = "
            {{ config(materialized='table') }}
            select
                {{ cents_to_dollars('subtotal') }} as subtotal,
                {{ dbt.date_trunc('day', 'ordered_at') }} as order_date
            from {{ source('ecom', 'raw_orders') }}
            {% if is_incremental() %}
            where ordered_at > (select max(ordered_at) from {{ this }})
            {% else %}
            where 1 = 1
            {% endif %}";
        let schema: &[(&str, &[&str])] = &[("source.ecom.raw_orders", &["subtotal", "ordered_at"])];
        let expected = vec![
            p("source.ecom.raw_orders.ordered_at", "order_date"),
            p("source.ecom.raw_orders.subtotal", "subtotal"),
        ];
        // Without known source columns, 'day' is still not taken for a column.
        assert_eq!(pairs(sql, &[]), expected);
        assert_eq!(
            pairs(sql, schema),
            vec![
                p("source.ecom.raw_orders.ordered_at", "order_date"),
                p("source.ecom.raw_orders.subtotal", "subtotal"),
            ]
        );
    }

    #[test]
    fn dbt_utils_star_passes_columns_through() {
        let sql = "select {{ dbt_utils.star(from=ref('a'), except=['x']) }} from {{ ref('a') }}";
        assert_eq!(
            pairs(sql, &[("model.a", &["id"])]),
            vec![p("model.a.id", "id")]
        );
    }

    #[test]
    fn detects_sql_written_by_jinja() {
        assert!(!sql_generated_by_jinja(
            "select {{ cents_to_dollars('x') }} as x from {{ ref('a') }}"
        ));
        assert!(sql_generated_by_jinja(
            "select {% for c in cols %}{{ c }},{% endfor %} id from {{ ref('a') }}"
        ));
        assert!(sql_generated_by_jinja(
            "with b as (select {{ fill_staging_columns(a, b) }} from {{ ref('a') }}) select * from b"
        ));
        assert!(sql_generated_by_jinja("{{ union_connections('x', 'y') }}"));
    }

    #[test]
    fn compiled_sql_resolves_relations_and_ephemeral_models() {
        let relations: HashMap<String, String> = [
            (
                "db.main.stg_orders".to_string(),
                "model.stg_orders".to_string(),
            ),
            (
                "db.raw.customers".to_string(),
                "source.raw.customers".to_string(),
            ),
        ]
        .into();
        let sql = r#"
            with __dbt__cte__int_orders as (select order_id, amount from "db"."main"."stg_orders"),
            joined as (
                select o.order_id, o.amount, c.name
                from __dbt__cte__int_orders o
                join "DB"."RAW"."CUSTOMERS" c on c.id = o.order_id
            )
            select * from joined"#;
        let schema = HashMap::from([(
            "source.raw.customers".to_string(),
            vec!["id".to_string(), "name".to_string()],
        )]);
        let mut got: Vec<(String, String, String)> =
            analyze_compiled(sql, &relations, &schema, &[])
                .unwrap()
                .edges
                .into_iter()
                .map(|e| (e.source_node, e.source_column, e.target_column))
                .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("model.int_orders".into(), "amount".into(), "amount".into()),
                (
                    "model.int_orders".into(),
                    "order_id".into(),
                    "order_id".into()
                ),
                ("source.raw.customers".into(), "name".into(), "name".into()),
            ]
        );
    }

    #[test]
    fn versioned_refs_resolve_to_version_nodes() {
        let labels: HashMap<String, String> =
            [("dim.v1", "model.dim.v1"), ("dim.v2", "model.dim.v2")]
                .iter()
                .map(|(l, id)| (l.to_string(), id.to_string()))
                .collect();
        let schema = HashMap::new();
        let src = |sql: &str| -> Vec<String> {
            analyze_sql(sql, "model.t", &labels, &schema)
                .into_iter()
                .map(|e| e.source_node)
                .collect()
        };
        assert_eq!(src("select id from {{ ref('dim') }}"), ["model.dim.v2"]);
        assert_eq!(
            src("select id from {{ ref('dim', v=1) }}"),
            ["model.dim.v1"]
        );
        assert_eq!(
            src("select id from {{ ref('dim', version='1') }}"),
            ["model.dim.v1"]
        );
    }

    #[test]
    fn unknown_tables_yield_no_edges() {
        assert!(pairs("select id from raw.hardcoded_table", &[]).is_empty());
    }

    #[test]
    fn unparseable_sql_yields_nothing() {
        assert!(pairs("this is not sql", &[]).is_empty());
    }

    #[test]
    fn scalar_subquery_columns_are_not_bound_to_outer_tables() {
        let sql = "select id, (select max(x) from other) as m from {{ ref('a') }}";
        assert_eq!(pairs(sql, &[]), vec![p("model.a.id", "id")]);
    }
}
