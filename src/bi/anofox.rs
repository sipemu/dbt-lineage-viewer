//! anofox-visualization dashboards (DataZooDE/anofox-visualization).
//!
//! A dashboard is one DuckDB `.sql` file: header comments (`-- @title`,
//! `-- @description`, `-- @owner`) describe it, and every `SELECT` with
//! `::ROLE` casts becomes a panel. The tables those statements read are the
//! dashboard's dependencies; tables and views the file creates itself (sample
//! data, CSV loads) are local and skipped.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;
use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use super::Dashboard;

/// Panel roles that mark a file as a dashboard (not a helper script).
static ROLE_CAST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"::\s*(DASHBOARD_TITLE|BARCHART\w*|LINECHART\w*|AREACHART\w*|SCATTER|BUBBLE|PIE|DONUTCHART|HISTOGRAM|DENSITY|BOXPLOT|VIOLIN|HEATMAP|MAP|REGION|METRIC|MONEY|COMPACT|KPI\w*|TABLE|PAGED|MARKDOWN|TEXT_\w+|GAUGE|RADAR|CANDLESTICK|OHLC|SPARKLINE|DROPDOWN|GROUP)\b",
    )
    .unwrap()
});
static HEADER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^--\s*@(\w+)[ \t]+(.+?)\s*$").unwrap());
static TITLE_CAST: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'((?:[^']|'')*)'\s*::\s*DASHBOARD_TITLE\b").unwrap());
static CREATED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\bcreate\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:table|view|macro)\s+(?:if\s+not\s+exists\s+)?([A-Za-z_"][\w."]*)"#,
    )
    .unwrap()
});
static FROM_JOIN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)\b(?:from|join)\s+([A-Za-z_"][\w."]*)(\s*\()?"#).unwrap());
static QUOTED_IDENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""(?:[^"]|"")*""#).unwrap());
static CTE_NAME: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b([A-Za-z_]\w*)\s+as\s+(?:not\s+)?(?:materialized\s+)?\(").unwrap()
});
static STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"'(?:[^']|'')*'").unwrap());

/// Collect dashboards from files and directories (searched recursively for
/// `.sql`). `url` is the exposure URL; `{name}` in it is replaced by the
/// file name without extension. Without `url`, the file path is used.
pub fn dashboards_from_paths(paths: &[PathBuf], url: Option<&str>) -> Result<Vec<Dashboard>> {
    let mut files = Vec::new();
    for p in paths {
        if p.is_dir() {
            for entry in walkdir::WalkDir::new(p).sort_by_file_name() {
                let entry = entry?;
                if entry.file_type().is_file()
                    && entry.path().extension().is_some_and(|e| e == "sql")
                {
                    files.push(entry.into_path());
                }
            }
        } else {
            files.push(p.clone());
        }
    }
    let mut sources = Vec::new();
    for file in files {
        let sql = std::fs::read_to_string(&file)
            .with_context(|| format!("cannot read {}", file.display()))?;
        sources.push((file, sql));
    }
    // Tables created by any scanned file (an `init.sql` loading CSVs, a
    // `compute.sql` materializing results) belong to the dashboards, not dbt.
    let created: BTreeSet<String> = sources
        .iter()
        .flat_map(|(_, sql)| created_tables(sql))
        .collect();
    let mut dashboards = Vec::new();
    for (file, sql) in &sources {
        if let Some(mut d) = parse_dashboard(file, sql, url) {
            d.tables.retain(|t| !created.contains(t));
            dashboards.push(d);
        }
    }
    Ok(dashboards)
}

/// Parse one file; `None` when it is not a dashboard (no title, no panels).
pub fn parse_dashboard(path: &Path, sql: &str, url: Option<&str>) -> Option<Dashboard> {
    let header = |key: &str| {
        HEADER
            .captures_iter(sql)
            .find(|c| &c[1] == key)
            .map(|c| c[2].to_string())
    };
    let title =
        header("title").or_else(|| TITLE_CAST.captures(sql).map(|c| c[1].replace("''", "'")));
    if title.is_none() && !ROLE_CAST.is_match(&strip_comments(sql)) {
        return None;
    }
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("dashboard")
        .to_string();
    let (owner_name, owner_email) = match header("owner") {
        Some(o) if o.contains('@') && !o.contains(' ') => (None, Some(o)),
        Some(o) => (Some(o), None),
        // dbt requires an owner; name the tool when the file has no `@owner`.
        None => (Some("anofox-visualization".to_string()), None),
    };
    let url = match url {
        Some(u) => u.replace("{name}", &stem),
        None => path.to_string_lossy().replace('\\', "/"),
    };
    Some(Dashboard {
        id: stem.clone(),
        name: title.unwrap_or(stem),
        url,
        description: header("description"),
        owner_name,
        owner_email,
        tables: dashboard_tables(sql),
    })
}

/// Tables read by the dashboard, minus what the file creates itself, CTEs
/// and table functions (`read_csv(…)`, `range(…)`).
pub fn dashboard_tables(sql: &str) -> BTreeSet<String> {
    let code = strip_comments(sql);
    let local = created_tables(sql);

    let mut found = BTreeSet::new();
    // Statement by statement, so one DuckDB construct the parser doesn't
    // know only costs that statement; it falls back to FROM/JOIN scanning.
    for stmt in split_statements(&code) {
        match Parser::parse_sql(&DuckDbDialect {}, stmt) {
            Ok(statements) => {
                // Top-level CTEs from the AST, nested ones (in subqueries,
                // macros) by name pattern.
                let mut ctes = cte_names(&statements);
                ctes.extend(CTE_NAME.captures_iter(stmt).map(|c| normalise(&c[1])));
                let _ = visit_relations(&statements, |rel| {
                    let name = normalise(&rel.to_string());
                    if !ctes.contains(&name) && !is_function_call(stmt, &rel.to_string()) {
                        found.insert(name);
                    }
                    ControlFlow::<()>::Continue(())
                });
            }
            Err(_) => {
                // Blank string literals and quoted identifiers with spaces
                // (column labels like "Street km from previous").
                let text = STRING.replace_all(stmt, "''");
                let text = QUOTED_IDENT.replace_all(&text, |c: &regex::Captures| {
                    if c[0].contains(char::is_whitespace) {
                        "\"\"".to_string()
                    } else {
                        c[0].to_string()
                    }
                });
                let ctes: BTreeSet<String> = CTE_NAME
                    .captures_iter(&text)
                    .map(|c| normalise(&c[1]))
                    .collect();
                for cap in FROM_JOIN.captures_iter(&text) {
                    let name = normalise(&cap[1]);
                    if cap.get(2).is_none() && !ctes.contains(&name) {
                        found.insert(name);
                    }
                }
            }
        }
    }
    found.retain(|t| !local.contains(t) && !t.is_empty());
    found
}

/// Drop `--` and `/* */` comments, keeping string literals intact. One pass,
/// so an apostrophe in a comment (`-- the DC's`) doesn't open a string.
/// Tables, views and macros a script creates (lower-cased).
pub fn created_tables(sql: &str) -> BTreeSet<String> {
    CREATED
        .captures_iter(&strip_comments(sql))
        .map(|c| normalise(&c[1]))
        .collect()
}

fn strip_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let (mut i, mut start) = (0, 0);
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                // Skip to the closing quote; a doubled quote is an escape.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        if bytes.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                out.push_str(&sql[start..i]);
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                start = i;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                out.push_str(&sql[start..i]);
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                start = i;
                out.push(' ');
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[start.min(sql.len())..]);
    out
}

/// Split on `;` outside string literals (input has no comments).
fn split_statements(code: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut start, mut quote) = (0, None::<char>);
    for (i, c) in code.char_indices() {
        match (c, quote) {
            ('\'' | '"', None) => quote = Some(c),
            (c, Some(q)) if c == q => quote = None,
            (';', None) => {
                out.push(&code[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&code[start..]);
    out.into_iter().filter(|s| !s.trim().is_empty()).collect()
}

/// `name(` appears in the statement, i.e. a table function like `read_csv(…)`.
fn is_function_call(stmt: &str, name: &str) -> bool {
    let (stmt, name) = (stmt.to_lowercase(), name.to_lowercase());
    stmt.match_indices(&name).any(|(i, _)| {
        stmt[i + name.len()..].trim_start().starts_with('(')
            && !stmt[..i].ends_with(|c: char| c.is_alphanumeric() || c == '_')
    })
}

fn cte_names(statements: &[Statement]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for s in statements {
        let query = match s {
            Statement::Query(q) => Some(q),
            Statement::CreateTable(ct) => ct.query.as_ref(),
            Statement::CreateView { query, .. } => Some(query),
            _ => None,
        };
        if let Some(with) = query.and_then(|q| q.with.as_ref()) {
            for cte in &with.cte_tables {
                names.insert(cte.alias.name.value.to_lowercase());
            }
        }
    }
    names
}

fn normalise(name: &str) -> String {
    name.replace('"', "").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DASHBOARD: &str = r#"-- @title Where will stock run short?
-- @group Demand & stock
-- @description Which items will run short, and when.
-- @owner planning@example.com
--
-- Reads from analytics.orders -- not a statement

CREATE OR REPLACE TABLE it_alarms AS FROM read_csv('data/alarms.csv');
SELECT 'Stock'::DASHBOARD_TITLE;
SELECT region AS region ::DROPDOWN FROM fct_orders GROUP BY region;
WITH recent AS (SELECT * FROM "main"."fct_orders" WHERE d > now() - INTERVAL 7 DAY)
SELECT SUM(amount)::MONEY, 'Revenue'::LABEL FROM recent
WHERE region = getvariable('region');
SELECT c.name::CATEGORY, COUNT(*)::BARCHART
FROM dim_customers c JOIN fct_orders o USING (customer_id)
GROUP BY 1;
SELECT * ::TABLE FROM it_alarms;
SELECT m::XAXIS, m*2 ::LINECHART FROM range(0, 10) t(m);
FROM stg_payments SELECT amount ::HISTOGRAM;
"#;

    #[test]
    fn parses_header_and_tables() {
        let d = parse_dashboard(Path::new("dash/stock.sql"), DASHBOARD, None).unwrap();
        assert_eq!(d.name, "Where will stock run short?");
        assert_eq!(d.id, "stock");
        assert_eq!(d.url, "dash/stock.sql");
        assert_eq!(
            d.description.as_deref(),
            Some("Which items will run short, and when.")
        );
        assert_eq!(d.owner_email.as_deref(), Some("planning@example.com"));
        let tables: Vec<&str> = d.tables.iter().map(String::as_str).collect();
        // CTE `recent`, local `it_alarms`, functions read_csv/range and the
        // commented-out analytics.orders are excluded.
        assert_eq!(
            tables,
            vec![
                "dim_customers",
                "fct_orders",
                "main.fct_orders",
                "stg_payments"
            ]
        );
    }

    #[test]
    fn apostrophes_in_comments_and_strings() {
        let sql = "-- the DC's view\nSELECT 'far from every DC'::TITLE, n::METRIC FROM deliveries;\n/* it's from a note */\nSELECT 'it''s from a table' AS x FROM regions;";
        let tables: Vec<String> = dashboard_tables(sql).into_iter().collect();
        assert_eq!(tables, vec!["deliveries", "regions"]);
    }

    #[test]
    fn fallback_skips_ctes_and_quoted_labels() {
        // `FROM ev SELECT …` (DuckDB FROM-first) forces the fallback scanner.
        let sql = r##"WITH ev AS (FROM events SELECT *), measured AS (SELECT * FROM stock)
FROM ev LEFT JOIN measured m USING (sku) SELECT sku AS "#", km AS "Street km from previous", 'Driver''s "pick"' AS note;"##;
        let tables: Vec<String> = dashboard_tables(sql).into_iter().collect();
        assert_eq!(tables, vec!["events", "stock"]);
    }

    #[test]
    fn url_template_and_title_cast_fallback() {
        let sql = "SELECT 'Sales ''25'::DASHBOARD_TITLE;\nSELECT 1::METRIC FROM kpis;";
        let d =
            parse_dashboard(Path::new("x/sales.sql"), sql, Some("https://viz/d/{name}")).unwrap();
        assert_eq!(d.name, "Sales '25");
        assert_eq!(d.url, "https://viz/d/sales");
        assert_eq!(d.tables.iter().collect::<Vec<_>>(), vec!["kpis"]);
    }

    #[test]
    fn helper_scripts_are_not_dashboards() {
        let sql = "CREATE TABLE sales AS SELECT 1 AS x;\nINSERT INTO sales SELECT 2;";
        assert!(parse_dashboard(Path::new("data.sql"), sql, None).is_none());
        // Untitled but with panels: a dashboard named after the file.
        let sql = "SELECT SUM(x)::METRIC FROM facts;";
        assert_eq!(
            parse_dashboard(Path::new("kpi.sql"), sql, None)
                .unwrap()
                .name,
            "kpi"
        );
    }

    #[test]
    fn walks_directories() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("a.sql"), "SELECT 1::METRIC FROM t1;").unwrap();
        std::fs::write(
            dir.path().join("sub/b.sql"),
            "-- @title B\nSELECT * FROM t2;",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("helper.sql"),
            "CREATE TABLE t2 AS SELECT 1;",
        )
        .unwrap();
        std::fs::write(dir.path().join("notes.md"), "SELECT 1::METRIC FROM t3;").unwrap();
        let ds = dashboards_from_paths(&[dir.path().to_path_buf()], None).unwrap();
        let names: Vec<&str> = ds.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["a", "B"]);
        // t2 is created by helper.sql, so it is local to the dashboards.
        assert!(ds[1].tables.is_empty(), "{:?}", ds[1].tables);
        assert_eq!(ds[0].tables.iter().collect::<Vec<_>>(), vec!["t1"]);
    }
}
