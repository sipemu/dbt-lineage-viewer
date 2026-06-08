//! AST-backed column lineage (#35). Uses `sqlparser` to parse model SQL into
//! a proper AST and walks the `SELECT` projection + `FROM`/`JOIN` clauses to
//! emit per-column lineage edges.
//!
//! Returns the same `ColumnLineage` shape as the regex-based analyzer, so the
//! existing MCP tools and renderers consume it interchangeably. Compared to
//! the regex analyzer this handles: qualified column references (`t.col`),
//! aliased projections (`col AS new_name`), function applications
//! (`SUM(col) AS total`), CAST expressions, simple CTEs, and JOINs.
//!
//! Falls back gracefully (returns an empty lineage) when sqlparser can't
//! parse the SQL — the caller can then drop back to the regex analyzer.

use std::collections::HashMap;

use sqlparser::ast::{
    Expr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::graph::types::LineageGraph;

use super::column_lineage::{ColumnConfidence, ColumnEdge, ColumnLineage};

/// Resolve column lineage for the entire graph using the SQL AST. Models without
/// readable SQL are silently skipped (caller can fall back to the regex analyzer).
pub fn resolve_column_lineage_ast(graph: &LineageGraph) -> ColumnLineage {
    let mut edges: Vec<ColumnEdge> = Vec::new();
    let label_to_id: HashMap<String, String> = graph
        .node_indices()
        .map(|idx| {
            let n = &graph[idx];
            (n.label.clone(), n.unique_id.clone())
        })
        .collect();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let Some(path) = &node.file_path else {
            continue;
        };
        let Ok(sql) = std::fs::read_to_string(path) else {
            continue;
        };
        edges.extend(analyze_sql(&sql, &node.unique_id, &label_to_id));
    }

    ColumnLineage { edges }
}

/// Analyze a single SQL string. Public for direct testing.
pub fn analyze_sql(
    sql: &str,
    target_node_id: &str,
    label_to_id: &HashMap<String, String>,
) -> Vec<ColumnEdge> {
    // Strip Jinja first — sqlparser doesn't speak Jinja. We replace `{{ ref('x') }}`
    // with `__jinja_x__` so the parser sees a bare table identifier that resolves
    // back to the right node via label lookup.
    let cleaned = preprocess_jinja(sql, label_to_id);
    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, &cleaned) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut edges: Vec<ColumnEdge> = Vec::new();
    for stmt in statements {
        if let Statement::Query(query) = stmt {
            edges.extend(analyze_query(&query, target_node_id, label_to_id));
        }
    }
    edges
}

/// Replace `{{ ref('name') }}` (and `source('s', 't')`) with synthetic table
/// identifiers that match how we'd reference the corresponding graph node.
fn preprocess_jinja(sql: &str, label_to_id: &HashMap<String, String>) -> String {
    let _ = label_to_id;
    let ref_re = regex::Regex::new(r#"\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#).unwrap();
    let source_re = regex::Regex::new(
        r#"\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}"#,
    )
    .unwrap();
    let mut out = sql.to_string();
    // ${1} (not $1) so the regex crate doesn't read the trailing underscore as
    // part of the capture-group identifier.
    out = ref_re.replace_all(&out, "__ref_${1}__").to_string();
    out = source_re
        .replace_all(&out, "__source_${1}_${2}__")
        .to_string();
    out
}

fn analyze_query(
    query: &Query,
    target_node_id: &str,
    label_to_id: &HashMap<String, String>,
) -> Vec<ColumnEdge> {
    let mut edges: Vec<ColumnEdge> = Vec::new();

    // Resolve CTE definitions (WITH clause).
    let mut cte_aliases: HashMap<String, String> = HashMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            // CTE name → target_node_id; treated as if the CTE materializes within
            // the same model. We don't emit edges from CTE name to outer query;
            // we DO resolve `<cte_name>.col` references to come from this CTE.
            cte_aliases.insert(cte.alias.name.value.clone(), target_node_id.to_string());
        }
    }

    if let SetExpr::Select(select) = query.body.as_ref() {
        edges.extend(analyze_select(
            select,
            target_node_id,
            label_to_id,
            &cte_aliases,
        ));
    }
    edges
}

fn analyze_select(
    select: &Select,
    target_node_id: &str,
    label_to_id: &HashMap<String, String>,
    cte_aliases: &HashMap<String, String>,
) -> Vec<ColumnEdge> {
    let from_aliases = build_from_aliases(&select.from, label_to_id, cte_aliases);

    let mut edges: Vec<ColumnEdge> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(edge) = expr_to_edge(expr, None, target_node_id, &from_aliases) {
                    edges.push(edge);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(edge) = expr_to_edge(
                    expr,
                    Some(alias.value.as_str()),
                    target_node_id,
                    &from_aliases,
                ) {
                    edges.push(edge);
                }
            }
            // Wildcards / qualified wildcards: emit no per-column edge here. The
            // regex analyzer's existing "Star" handling covers the case from its
            // own pass; we leave this hole intentionally.
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    edges
}

/// Map of `alias → source_node_id`. When a table has no alias, its name is used.
fn build_from_aliases(
    from: &[TableWithJoins],
    label_to_id: &HashMap<String, String>,
    cte_aliases: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for twj in from {
        collect_factor(&twj.relation, &mut out, label_to_id, cte_aliases);
        for join in &twj.joins {
            collect_factor(&join.relation, &mut out, label_to_id, cte_aliases);
        }
    }
    out
}

fn collect_factor(
    factor: &TableFactor,
    out: &mut HashMap<String, String>,
    label_to_id: &HashMap<String, String>,
    cte_aliases: &HashMap<String, String>,
) {
    if let TableFactor::Table { name, alias, .. } = factor {
        let table_name = object_name_to_string(name);
        let resolved = resolve_table_to_node(&table_name, label_to_id, cte_aliases)
            .unwrap_or_else(|| format!("model.{}", table_name));
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| table_name.clone());
        out.insert(alias_name, resolved.clone());
        // Also index by the bare table name so unqualified-but-implied references resolve.
        out.entry(table_name).or_insert(resolved);
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|p| p.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

/// Translate `__ref_foo__` and `__source_raw_orders__` (our Jinja placeholders)
/// or a raw table name back to the matching graph node's unique_id.
fn resolve_table_to_node(
    name: &str,
    label_to_id: &HashMap<String, String>,
    cte_aliases: &HashMap<String, String>,
) -> Option<String> {
    if let Some(stripped) = name
        .strip_prefix("__ref_")
        .and_then(|s| s.strip_suffix("__"))
    {
        return label_to_id.get(stripped).cloned();
    }
    if let Some(stripped) = name
        .strip_prefix("__source_")
        .and_then(|s| s.strip_suffix("__"))
    {
        // source_<source>_<table>
        let parts: Vec<&str> = stripped.splitn(2, '_').collect();
        if parts.len() == 2 {
            return Some(format!("source.{}.{}", parts[0], parts[1]));
        }
    }
    if let Some(node_id) = cte_aliases.get(name) {
        return Some(node_id.clone());
    }
    label_to_id.get(name).cloned()
}

/// Convert a projection expression into a ColumnEdge (or None when we can't
/// derive a source column — e.g. a literal). The `alias` is the projection's
/// AS-name, defaulted to the column name itself when omitted.
fn expr_to_edge(
    expr: &Expr,
    alias: Option<&str>,
    target_node_id: &str,
    from_aliases: &HashMap<String, String>,
) -> Option<ColumnEdge> {
    match expr {
        Expr::Identifier(ident) => {
            // Bare column. Source = the single FROM table (when there's one).
            let target_col = alias.unwrap_or(ident.value.as_str()).to_string();
            let source_node = resolve_single_source(from_aliases)?;
            Some(ColumnEdge {
                source_node,
                source_column: ident.value.clone(),
                target_node: target_node_id.to_string(),
                target_column: target_col,
                confidence: if alias.is_some() && alias != Some(ident.value.as_str()) {
                    ColumnConfidence::Aliased
                } else {
                    ColumnConfidence::Direct
                },
            })
        }
        Expr::CompoundIdentifier(parts) => {
            // `t.col` — qualified. Source = the table aliased as `t`.
            if parts.len() < 2 {
                return None;
            }
            let table_alias = parts[parts.len() - 2].value.as_str();
            let col_name = parts[parts.len() - 1].value.clone();
            let source_node = from_aliases.get(table_alias)?.clone();
            let target_col = alias.unwrap_or(&col_name).to_string();
            Some(ColumnEdge {
                source_node,
                source_column: col_name.clone(),
                target_node: target_node_id.to_string(),
                target_column: target_col.clone(),
                confidence: if target_col == col_name {
                    ColumnConfidence::Direct
                } else {
                    ColumnConfidence::Aliased
                },
            })
        }
        Expr::Function(_) | Expr::Case { .. } | Expr::Cast { .. } | Expr::BinaryOp { .. } => {
            // Derived expression — pick the first column reference inside it as
            // the "source." Best-effort; the analyzer's job is to mark this as
            // Derived so downstream consumers know not to trust it as a 1:1.
            let target_col = alias?.to_string();
            let source_ident = first_identifier_in(expr)?;
            let source_node = match &source_ident {
                ColumnRef::Bare(_) => resolve_single_source(from_aliases)?,
                ColumnRef::Qualified(alias, _) => from_aliases.get(alias)?.clone(),
            };
            let source_column = match source_ident {
                ColumnRef::Bare(s) => s,
                ColumnRef::Qualified(_, s) => s,
            };
            Some(ColumnEdge {
                source_node,
                source_column,
                target_node: target_node_id.to_string(),
                target_column: target_col,
                confidence: ColumnConfidence::Derived,
            })
        }
        _ => None,
    }
}

enum ColumnRef {
    Bare(String),
    Qualified(String, String),
}

/// Walk an expression in pre-order, returning the first column reference found.
/// Used as a best-effort source attribution for derived columns.
fn first_identifier_in(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(i) => Some(ColumnRef::Bare(i.value.clone())),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some(ColumnRef::Qualified(
            parts[parts.len() - 2].value.clone(),
            parts[parts.len() - 1].value.clone(),
        )),
        Expr::BinaryOp { left, right, .. } => {
            first_identifier_in(left).or_else(|| first_identifier_in(right))
        }
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } | Expr::Nested(expr) => {
            first_identifier_in(expr)
        }
        Expr::Function(f) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            let arg_list = match &f.args {
                FunctionArguments::List(l) => &l.args,
                _ => return None,
            };
            arg_list.iter().find_map(|a| {
                let arg = match a {
                    FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => arg,
                };
                match arg {
                    FunctionArgExpr::Expr(e) => first_identifier_in(e),
                    _ => None,
                }
            })
        }
        Expr::Case {
            conditions,
            results,
            else_result,
            ..
        } => conditions
            .iter()
            .chain(results.iter())
            .chain(else_result.iter().map(|e| e.as_ref()))
            .find_map(first_identifier_in),
        _ => None,
    }
}

fn resolve_single_source(from_aliases: &HashMap<String, String>) -> Option<String> {
    let unique: std::collections::HashSet<&String> = from_aliases.values().collect();
    if unique.len() == 1 {
        unique.into_iter().next().cloned()
    } else {
        None
    }
}

// Re-export the convenience trait for tests that want to write select-only SQL.
#[allow(dead_code)]
fn _silence_unused(_i: &Ident) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::*;

    fn make_node(unique_id: &str, label: &str, nt: NodeType) -> NodeData {
        NodeData {
            unique_id: unique_id.into(),
            label: label.into(),
            node_type: nt,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn label_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(l, id)| (l.to_string(), id.to_string()))
            .collect()
    }

    #[test]
    fn test_bare_column_direct() {
        let sql = "SELECT order_id FROM stg_orders";
        let map = label_map(&[("stg_orders", "model.stg_orders")]);
        let edges = analyze_sql(sql, "model.orders", &map);
        assert_eq!(edges.len(), 1);
        let e = &edges[0];
        assert_eq!(e.source_node, "model.stg_orders");
        assert_eq!(e.source_column, "order_id");
        assert_eq!(e.target_column, "order_id");
        assert_eq!(e.confidence, ColumnConfidence::Direct);
    }

    #[test]
    fn test_aliased_column() {
        let sql = "SELECT order_id AS id FROM stg_orders";
        let map = label_map(&[("stg_orders", "model.stg_orders")]);
        let edges = analyze_sql(sql, "model.orders", &map);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_column, "order_id");
        assert_eq!(edges[0].target_column, "id");
        assert_eq!(edges[0].confidence, ColumnConfidence::Aliased);
    }

    #[test]
    fn test_qualified_join() {
        let sql = "SELECT o.order_id, c.email FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.customer_id";
        let map = label_map(&[
            ("stg_orders", "model.stg_orders"),
            ("stg_customers", "model.stg_customers"),
        ]);
        let edges = analyze_sql(sql, "model.orders", &map);
        assert_eq!(edges.len(), 2);
        let order_edge = edges
            .iter()
            .find(|e| e.target_column == "order_id")
            .unwrap();
        assert_eq!(order_edge.source_node, "model.stg_orders");
        let email_edge = edges.iter().find(|e| e.target_column == "email").unwrap();
        assert_eq!(email_edge.source_node, "model.stg_customers");
    }

    #[test]
    fn test_aggregation_marks_derived() {
        let sql = "SELECT SUM(amount) AS total FROM stg_payments";
        let map = label_map(&[("stg_payments", "model.stg_payments")]);
        let edges = analyze_sql(sql, "model.orders", &map);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_column, "amount");
        assert_eq!(edges[0].target_column, "total");
        assert_eq!(edges[0].confidence, ColumnConfidence::Derived);
    }

    #[test]
    fn test_jinja_ref_resolved() {
        let sql = "SELECT order_id FROM {{ ref('stg_orders') }}";
        let map = label_map(&[("stg_orders", "model.stg_orders")]);
        let edges = analyze_sql(sql, "model.orders", &map);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_node, "model.stg_orders");
    }

    #[test]
    fn test_jinja_source_resolved() {
        let sql = "SELECT order_id FROM {{ source('raw', 'orders') }}";
        let map = HashMap::new();
        let edges = analyze_sql(sql, "model.stg_orders", &map);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source_node, "source.raw.orders");
    }

    #[test]
    fn test_unparseable_sql_returns_empty() {
        let sql = "this is not valid sql";
        let edges = analyze_sql(sql, "model.x", &HashMap::new());
        assert!(edges.is_empty());
    }

    #[test]
    fn test_resolve_column_lineage_ast_full_graph() {
        // End-to-end: build a graph with file_paths pointing at real temp SQL,
        // call the top-level analyzer, and confirm it emits real edges.
        use std::io::Write;
        let tmp = tempfile::tempdir().unwrap();
        let stg_path = tmp.path().join("stg_orders.sql");
        let mart_path = tmp.path().join("orders.sql");
        writeln!(
            std::fs::File::create(&stg_path).unwrap(),
            "SELECT id FROM raw"
        )
        .unwrap();
        writeln!(
            std::fs::File::create(&mart_path).unwrap(),
            "SELECT id AS order_id FROM stg_orders"
        )
        .unwrap();
        let mut g = LineageGraph::new();
        g.add_node(NodeData {
            file_path: Some(stg_path),
            ..make_node("model.stg_orders", "stg_orders", NodeType::Model)
        });
        g.add_node(NodeData {
            file_path: Some(mart_path),
            ..make_node("model.orders", "orders", NodeType::Model)
        });

        let lineage = resolve_column_lineage_ast(&g);
        // Expect at least the order_id edge from stg_orders to orders.
        assert!(
            lineage
                .edges
                .iter()
                .any(|e| e.source_node == "model.stg_orders"
                    && e.target_node == "model.orders"
                    && e.target_column == "order_id"),
            "got: {:?}",
            lineage.edges
        );
    }
}
