//! anofox-visualization dashboards (DataZooDE/anofox-visualization).
//!
//! A dashboard is one DuckDB `.sql` file: header comments (`-- @title`,
//! `-- @description`, `-- @owner`) describe it, and every `SELECT` with
//! `::ROLE` casts becomes a panel. The tables those statements read are the
//! dashboard's dependencies, and so are the files they read
//! (`read_parquet(getvariable('anofox_data') || '/orders.parquet')`), which
//! map to the model the file is named after. A table or view a scanned file
//! creates (`CREATE TEMP TABLE t AS FROM read_parquet(…)`) stands for what it
//! reads; one with no dbt input (sample data, CSV loads) maps to nothing.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;
use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use super::{Dashboard, FileRead};

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
/// A statement defining a table or view from a query (`… AS SELECT`, `… AS FROM`).
static CREATED_AS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?is)^\s*create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:table|view)\s+(?:if\s+not\s+exists\s+)?([A-Za-z_"][\w."]*)\s+as\b"#,
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
/// DuckDB's file readers: `read_parquet`, `read_csv_auto`, `read_json_objects`, ….
static FILE_READ: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(read_(?:parquet|csv|json|ndjson)(?:_objects)?(?:_auto)?)\s*\(").unwrap()
});
/// A path argument that is a literal or a `||` concatenation ending in one.
static TRAILING_LITERAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)(?:^|\|\|)\s*'((?:[^']|'')*)'$").unwrap());
static WHITESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

/// Names the model a read file stands for: its base name without extension
/// (`…/orders.parquet` → `orders`), or the `{model}` part of a pattern such
/// as `{model}.parquet`.
#[derive(Debug, Default)]
pub struct FileNamer {
    pattern: Option<Regex>,
}

impl FileNamer {
    /// `pattern` must contain `{model}`; it is matched against the end of the
    /// path, so `exports/{model}.csv` requires that folder.
    pub fn new(pattern: Option<&str>) -> Result<Self> {
        let Some(pattern) = pattern else {
            return Ok(Self::default());
        };
        anyhow::ensure!(
            pattern.contains("{model}"),
            "--file-pattern '{pattern}' has no {{model}} placeholder"
        );
        let body = pattern
            .replace('\\', "/")
            .split("{model}")
            .map(regex::escape)
            .collect::<Vec<_>>()
            .join("([^/]+?)");
        let re = Regex::new(&format!("(?i)(?:^|/){body}$"))?;
        Ok(Self { pattern: Some(re) })
    }

    /// `None` for globs and for paths the pattern doesn't fit.
    pub fn name(&self, path: &str) -> Option<String> {
        let path = path.replace('\\', "/");
        if path.contains(['*', '?', '[', '{']) {
            return None;
        }
        match &self.pattern {
            Some(re) => re.captures(&path).map(|c| c[1].to_string()),
            None => {
                let file = path.rsplit('/').next().unwrap_or(&path);
                let stem = file.split('.').next().unwrap_or(file);
                (!stem.is_empty()).then(|| stem.to_string())
            }
        }
    }
}

/// What a statement or file reads.
#[derive(Debug, Default, Clone)]
struct Reads {
    tables: BTreeSet<String>,
    files: BTreeSet<FileRead>,
}

impl Reads {
    fn extend(&mut self, other: &Reads) {
        self.tables.extend(other.tables.iter().cloned());
        self.files.extend(other.files.iter().cloned());
    }
}

/// One scanned file: everything it reads, and the tables it creates.
#[derive(Debug, Default)]
struct Script {
    reads: Reads,
    /// Tables and views created from a query, with what that query reads.
    defs: HashMap<String, Reads>,
    /// Every table, view or macro the file creates.
    created: BTreeSet<String>,
}

impl Script {
    fn scan(sql: &str, namer: &FileNamer) -> Self {
        let mut script = Script {
            created: created_tables(sql),
            ..Script::default()
        };
        for stmt in split_statements(&strip_comments(sql)) {
            let reads = Reads {
                tables: statement_tables(stmt),
                files: file_reads(stmt, namer),
            };
            if let Some(c) = CREATED_AS.captures(stmt) {
                script
                    .defs
                    .entry(normalise(&c[1]))
                    .or_default()
                    .extend(&reads);
            }
            script.reads.extend(&reads);
        }
        script
    }

    fn merge(&mut self, other: &Script) {
        for (name, reads) in &other.defs {
            self.defs.entry(name.clone()).or_default().extend(reads);
        }
        self.created.extend(other.created.iter().cloned());
    }
}

/// `reads` with every created table replaced by what it reads
/// (transitively). A table's definition comes from the first of `scopes`
/// that has one: the dashboard's own file first, since dashboards often each
/// define a view `wk` of their own. Created tables without a query
/// definition (macros, `CREATE TABLE t (…)`) are dropped.
fn resolve(reads: &Reads, scopes: &[&Script], created: &BTreeSet<String>) -> Reads {
    let mut out = Reads {
        tables: BTreeSet::new(),
        files: reads.files.clone(),
    };
    let mut seen = BTreeSet::new();
    let mut todo: Vec<&String> = reads.tables.iter().collect();
    while let Some(table) = todo.pop() {
        if !seen.insert(table) {
            continue;
        }
        if let Some(def) = scopes.iter().find_map(|s| s.defs.get(table)) {
            out.files.extend(def.files.iter().cloned());
            todo.extend(&def.tables);
        } else if !created.contains(table) && !table.is_empty() {
            out.tables.insert(table.clone());
        }
    }
    out
}

/// Collect dashboards from files and directories (searched recursively for
/// `.sql`). `url` is the exposure URL; `{name}` in it is replaced by the
/// file name without extension. Without `url`, the file path is used.
/// `file_pattern` names read files after models (see [`FileNamer`]).
pub fn dashboards_from_paths(
    paths: &[PathBuf],
    url: Option<&str>,
    file_pattern: Option<&str>,
) -> Result<Vec<Dashboard>> {
    let namer = FileNamer::new(file_pattern)?;
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
        let script = Script::scan(&sql, &namer);
        sources.push((file, sql, script));
    }
    // Tables created by any scanned file (an `init.sql` loading files, a
    // `compute.sql` materializing results) stand for what they read. Helper
    // scripts' definitions come before other dashboards' ones.
    let mut parsed = Vec::new();
    let (mut helpers, mut others) = (Script::default(), Script::default());
    for (file, sql, script) in &sources {
        match dashboard(file, sql, url, Reads::default()) {
            Some(d) => {
                others.merge(script);
                parsed.push((d, script));
            }
            None => helpers.merge(script),
        }
    }
    let created: BTreeSet<String> = helpers.created.union(&others.created).cloned().collect();
    let mut dashboards = Vec::new();
    for (mut d, script) in parsed {
        let reads = resolve(&script.reads, &[script, &helpers, &others], &created);
        (d.tables, d.files) = (reads.tables, reads.files);
        dashboards.push(d);
    }
    Ok(dashboards)
}

/// Parse one file; `None` when it is not a dashboard (no title, no panels).
/// Only the tables this file creates are resolved.
pub fn parse_dashboard(path: &Path, sql: &str, url: Option<&str>) -> Option<Dashboard> {
    let script = Script::scan(sql, &FileNamer::default());
    let reads = resolve(&script.reads, &[&script], &script.created);
    dashboard(path, sql, url, reads)
}

fn dashboard(path: &Path, sql: &str, url: Option<&str>, reads: Reads) -> Option<Dashboard> {
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
        tables: reads.tables,
        files: reads.files,
    })
}

/// Tables read by the SQL, minus what it creates itself, CTEs and table
/// functions (`read_csv(…)`, `range(…)`).
pub fn dashboard_tables(sql: &str) -> BTreeSet<String> {
    let local = created_tables(sql);
    let mut found = BTreeSet::new();
    // Statement by statement, so one DuckDB construct the parser doesn't
    // know only costs that statement.
    for stmt in split_statements(&strip_comments(sql)) {
        found.extend(statement_tables(stmt));
    }
    found.retain(|t| !local.contains(t) && !t.is_empty());
    found
}

/// Tables one statement reads, minus CTEs and table functions. Falls back to
/// FROM/JOIN scanning when the parser can't read the statement.
fn statement_tables(stmt: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
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
    found
}

/// Files one statement reads with DuckDB's `read_*` functions. The path is
/// the first argument when it is a string literal or a `||` concatenation
/// ending in one; anything else is kept with no model name.
fn file_reads(stmt: &str, namer: &FileNamer) -> BTreeSet<FileRead> {
    let strings: Vec<_> = STRING.find_iter(stmt).map(|m| m.range()).collect();
    FILE_READ
        .captures_iter(stmt)
        .filter_map(|c| {
            let call = c.get(0)?;
            if strings.iter().any(|r| r.contains(&call.start())) {
                return None;
            }
            let arg = first_argument(&stmt[call.end()..]).trim();
            let path = TRAILING_LITERAL
                .captures(arg)
                .map(|l| l[1].replace("''", "'"));
            Some(FileRead {
                call: format!("{}({})", &c[1], WHITESPACE.replace_all(arg, " ")),
                name: path.and_then(|p| namer.name(&p)),
            })
        })
        .collect()
}

/// Text up to the first top-level `,` or the closing `)` of a call.
fn first_argument(rest: &str) -> &str {
    let (mut depth, mut quote) = (0usize, None::<char>);
    for (i, c) in rest.char_indices() {
        match (c, quote) {
            (c, Some(q)) => {
                if c == q {
                    quote = None;
                }
            }
            ('\'' | '"', None) => quote = Some(c),
            ('(' | '[' | '{', None) => depth += 1,
            (')' | ']' | '}', None) if depth > 0 => depth -= 1,
            (')' | ']' | '}' | ',', None) if depth == 0 => return &rest[..i],
            _ => {}
        }
    }
    rest
}

/// Tables, views and macros a script creates (lower-cased).
pub fn created_tables(sql: &str) -> BTreeSet<String> {
    CREATED
        .captures_iter(&strip_comments(sql))
        .map(|c| normalise(&c[1]))
        .collect()
}

/// Drop `--` and `/* */` comments, keeping string literals intact. One pass,
/// so an apostrophe in a comment (`-- the DC's`) doesn't open a string.
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
        // `it_alarms` stands for its CSV file.
        let files: Vec<_> = d
            .files
            .iter()
            .map(|f| (&f.call[..], f.name.as_deref()))
            .collect();
        assert_eq!(files, vec![("read_csv('data/alarms.csv')", Some("alarms"))]);
    }

    fn files_of(sql: &str) -> Vec<(String, Option<String>)> {
        parse_dashboard(Path::new("d.sql"), sql, None)
            .unwrap()
            .files
            .into_iter()
            .map(|f| (f.call, f.name))
            .collect()
    }

    #[test]
    fn file_reads_name_models() {
        let sql = r#"-- @title Files
SELECT * ::TABLE FROM read_parquet(getvariable('anofox_data') || '/package_downloads.parquet');
SELECT * ::TABLE FROM READ_CSV_AUTO('data\Orders.csv', header = true);
SELECT * ::TABLE FROM read_json_objects('s3://b/events.json.gz') e JOIN read_ndjson_auto('x/it''s.ndjson') USING (id);
SELECT * ::TABLE FROM read_parquet('runs/*/run.parquet');
SELECT * ::TABLE FROM read_parquet(getvariable('file'));
SELECT * ::TABLE FROM read_parquet(['a.parquet', 'b.parquet']);
SELECT 'read_parquet(''not_a_call.parquet'')' ::TEXT_MARKDOWN;
"#;
        let some = |s: &str| Some(s.to_string());
        assert_eq!(
            files_of(sql),
            vec![
                (
                    r"READ_CSV_AUTO('data\Orders.csv')".to_string(),
                    some("Orders")
                ),
                (
                    "read_json_objects('s3://b/events.json.gz')".to_string(),
                    some("events")
                ),
                (
                    "read_ndjson_auto('x/it''s.ndjson')".to_string(),
                    some("it's")
                ),
                ("read_parquet('runs/*/run.parquet')".to_string(), None),
                ("read_parquet(['a.parquet', 'b.parquet'])".to_string(), None),
                (
                    "read_parquet(getvariable('anofox_data') || '/package_downloads.parquet')"
                        .to_string(),
                    some("package_downloads")
                ),
                ("read_parquet(getvariable('file'))".to_string(), None),
            ]
        );
    }

    #[test]
    fn file_pattern_template() {
        let namer = FileNamer::new(Some("{model}.parquet")).unwrap();
        assert_eq!(namer.name("data/orders.parquet").as_deref(), Some("orders"));
        assert_eq!(namer.name("data/orders.csv"), None);
        let namer = FileNamer::new(Some("exports/{model}.csv")).unwrap();
        assert_eq!(
            namer.name("/srv/exports/fct_sales.csv").as_deref(),
            Some("fct_sales")
        );
        assert_eq!(namer.name("/srv/other/fct_sales.csv"), None);
        assert_eq!(namer.name("exports/sales_*.csv"), None);
        assert!(FileNamer::new(Some("orders.parquet")).is_err());
        // Default: the base name without extension(s).
        let namer = FileNamer::default();
        assert_eq!(namer.name("a/b/c.csv.gz").as_deref(), Some("c"));
        assert_eq!(namer.name("data/"), None);
    }

    #[test]
    fn created_tables_stand_for_what_they_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("init.sql"),
            "CREATE OR REPLACE TEMP TABLE pq_orders AS\nFROM read_parquet(getvariable('anofox_data') || '/orders.parquet');\n\
             CREATE VIEW joined AS SELECT * FROM pq_orders JOIN dim_customers USING (id);\n\
             CREATE TABLE sample AS SELECT * FROM range(3) t(x);\n\
             CREATE TABLE loaded (x INT);\n\
             CREATE TABLE wk AS SELECT * FROM fct_weeks;",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("a.sql"),
            "-- @title A\nSELECT * ::TABLE FROM joined, sample, loaded;\nSELECT * ::TABLE FROM wk;",
        )
        .unwrap();
        // A dashboard's own `wk` wins over the one in init.sql.
        std::fs::write(
            dir.path().join("b.sql"),
            "-- @title B\nCREATE TEMP VIEW wk AS FROM read_csv('sample.csv');\nSELECT * ::TABLE FROM wk;",
        )
        .unwrap();
        let ds = dashboards_from_paths(&[dir.path().to_path_buf()], None, None).unwrap();
        let (a, b) = (&ds[0], &ds[1]);
        assert_eq!(
            a.tables.iter().collect::<Vec<_>>(),
            vec!["dim_customers", "fct_weeks"]
        );
        let names: Vec<_> = a.files.iter().map(|f| f.name.as_deref()).collect();
        assert_eq!(names, vec![Some("orders")]);
        assert!(b.tables.is_empty(), "{:?}", b.tables);
        let names: Vec<_> = b.files.iter().map(|f| f.name.as_deref()).collect();
        assert_eq!(names, vec![Some("sample")]);
        // The pattern applies to every scanned file.
        let ds =
            dashboards_from_paths(&[dir.path().to_path_buf()], None, Some("{model}.csv")).unwrap();
        assert_eq!(ds[0].files.iter().next().unwrap().name, None);
        assert!(dashboards_from_paths(&[dir.path().to_path_buf()], None, Some("x")).is_err());
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
        let ds = dashboards_from_paths(&[dir.path().to_path_buf()], None, None).unwrap();
        let names: Vec<&str> = ds.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["a", "B"]);
        // t2 is created by helper.sql, so it is local to the dashboards.
        assert!(ds[1].tables.is_empty(), "{:?}", ds[1].tables);
        assert_eq!(ds[0].tables.iter().collect::<Vec<_>>(), vec!["t1"]);
    }
}
