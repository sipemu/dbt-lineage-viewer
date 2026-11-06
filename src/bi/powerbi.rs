//! Power BI reports, read offline from PBIP projects and `.pbit` templates.
//!
//! A report reads one semantic model, whose tables are loaded by partitions.
//! A partition's source is a Power Query (M) expression, a native SQL query,
//! a Direct Lake entity or a DAX expression (calculated tables, skipped). In M,
//! warehouse tables appear as navigation steps (`Source{[Schema="dbo",
//! Item="orders"]}[Data]`, or nested `{[Name="ORDERS",Kind="Table"]}` for
//! Snowflake, Databricks and BigQuery), as SQL in `Value.NativeQuery(…)` or
//! `Sql.Database(…, [Query="…"])`, or behind shared expressions (queries
//! referenced by name).
//!
//! Inputs:
//! - PBIP: `*.Report/definition.pbir` points at its semantic model
//!   (`datasetReference.byPath`); the model is TMDL
//!   (`*.SemanticModel/definition/**/*.tmdl`) or TMSL JSON (`model.bim`).
//!   One exposure per report; a semantic model no scanned report uses becomes
//!   an exposure of its own.
//! - `.pbit` templates: a zip whose `DataModelSchema` entry is the model as
//!   UTF-16LE JSON. One exposure per template.
//! - `.pbix` files store the model in a binary format and are skipped with a
//!   note; reports bound to a published dataset (`byConnection`) likewise.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;
use serde_json::Value;

use super::Dashboard;

/// A partition (or refresh-policy) source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// Power Query (M) expression.
    M(String),
    /// Native SQL (legacy `query` partitions).
    Sql(String),
    /// Direct Lake / entity partition: `schema.entity` or `entity`.
    Entity(String),
    /// DAX calculated table or anything without a warehouse source.
    Other,
}

/// The parts of a semantic model that say where data comes from.
#[derive(Debug, Default, Clone)]
pub struct SemanticModel {
    /// Table name → its sources.
    pub tables: BTreeMap<String, Vec<Source>>,
    /// Shared expression (query or parameter) name → M.
    pub expressions: BTreeMap<String, String>,
}

impl SemanticModel {
    /// Warehouse tables read by the model, and the names of tables whose M
    /// source reads no warehouse table (files, web, entered data).
    pub fn source_tables(&self) -> (BTreeSet<String>, Vec<String>) {
        let params: BTreeMap<&str, String> = self
            .expressions
            .iter()
            .filter_map(|(k, v)| {
                let code = strip_m_comments(v);
                M_STRING
                    .find(code.trim_start())
                    .filter(|m| m.start() == 0)
                    .map(|m| (k.as_str(), unescape_m(m.as_str())))
            })
            .collect();
        let mut all = BTreeSet::new();
        let mut without = Vec::new();
        for (name, sources) in &self.tables {
            let mut found = BTreeSet::new();
            let mut has_m = false;
            for s in sources {
                match s {
                    Source::M(m) => {
                        has_m = true;
                        let mut seen = HashSet::new();
                        self.resolve_m(m, &params, &mut seen, &mut found);
                    }
                    Source::Sql(sql) => found.extend(sql_tables(sql)),
                    Source::Entity(e) => {
                        found.insert(e.clone());
                    }
                    Source::Other => {}
                }
            }
            if has_m && found.is_empty() {
                without.push(name.clone());
            }
            all.extend(found);
        }
        (all, without)
    }

    /// Tables of one M expression plus those of the shared expressions it
    /// references by name (`#"Base Query"` or `BaseQuery`).
    fn resolve_m(
        &self,
        m: &str,
        params: &BTreeMap<&str, String>,
        seen: &mut HashSet<String>,
        out: &mut BTreeSet<String>,
    ) {
        let code = strip_m_comments(m);
        out.extend(m_tables(&code, params));
        for (name, expr) in &self.expressions {
            if !params.contains_key(name.as_str())
                && references(&code, name)
                && seen.insert(name.clone())
            {
                self.resolve_m(expr, params, seen, out);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inputs: walking paths, PBIP reports, semantic models, .pbit templates.
// ---------------------------------------------------------------------------

/// Dashboards from files and directories (searched recursively for PBIP
/// reports, semantic models, `.pbit`, `.pbix`, `.pbip`). `url` is the
/// exposure URL; `{name}` in it is replaced by the report name. Returns the
/// dashboards and notes about skipped inputs.
pub fn dashboards_from_paths(
    paths: &[PathBuf],
    url: Option<&str>,
) -> Result<(Vec<Dashboard>, Vec<String>)> {
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

    let mut reports: Vec<PathBuf> = Vec::new(); // definition.pbir files
    let mut templates: Vec<PathBuf> = Vec::new(); // .pbit
    let mut models: BTreeSet<PathBuf> = BTreeSet::new(); // model dirs or .bim files
    let mut notes = Vec::new();
    for f in &files {
        let name = f
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_lowercase();
        let ext = f
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        if name == "definition.pbir" {
            reports.push(f.clone());
        } else if ext == "pbip" {
            reports.extend(pbip_reports(f)?);
        } else if ext == "pbit" {
            templates.push(f.clone());
        } else if ext == "pbix" {
            notes.push(format!(
                "{}: .pbix stores its model in a binary format; save it as a .pbit template or a PBIP project to import it",
                f.display()
            ));
        } else if ext == "bim" {
            models.insert(model_location(f));
        } else if ext == "tmdl" {
            // `<model>/definition/…/x.tmdl` → `<model>`.
            if let Some(dir) = f
                .ancestors()
                .find(|a| a.file_name().is_some_and(|n| n == "definition"))
                .and_then(Path::parent)
            {
                models.insert(dir.to_path_buf());
            }
        }
    }
    reports.sort();
    reports.dedup();

    let link = |name: &str, path: &Path| match url {
        Some(u) => u.replace("{name}", name),
        None => path.to_string_lossy().replace('\\', "/"),
    };
    // The id is the full path here; `shorten_ids` trims it below.
    let make = |name: String, path: &Path, tables: BTreeSet<String>| Dashboard {
        id: canonical(path).to_string_lossy().replace('\\', "/"),
        url: link(&name, path),
        name,
        description: None,
        owner_name: Some("powerbi".into()),
        owner_email: None,
        tables,
    };

    let mut dashboards = Vec::new();
    let mut used_models: HashSet<PathBuf> = HashSet::new();
    let mut cache: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let mut load = |model: &Path, notes: &mut Vec<String>| -> BTreeSet<String> {
        let key = canonical(model);
        if let Some(t) = cache.get(&key) {
            return t.clone();
        }
        let tables = match load_model(model) {
            Ok(m) => model_tables(&m, &model.display().to_string(), notes),
            Err(e) => {
                notes.push(format!(
                    "{}: not a readable semantic model ({e:#})",
                    model.display()
                ));
                BTreeSet::new()
            }
        };
        cache.insert(key, tables.clone());
        tables
    };

    for pbir in &reports {
        let report_dir = pbir.parent().unwrap_or(Path::new("."));
        let name = strip_suffix(report_dir, &[".Report"]);
        match report_model(pbir) {
            Ok(ModelRef::Path(model)) => {
                used_models.insert(canonical(&model));
                let tables = load(&model, &mut notes);
                dashboards.push(make(name, report_dir, tables));
            }
            Ok(ModelRef::Connection) => notes.push(format!(
                "report '{name}' uses a published semantic model (byConnection); its tables are not in the project"
            )),
            Err(e) => notes.push(format!("{}: {e:#}", pbir.display())),
        }
    }
    for model in &models {
        if used_models.contains(&canonical(model)) {
            continue;
        }
        let name = if model.is_dir() {
            strip_suffix(model, &[".SemanticModel", ".Dataset"])
        } else {
            model
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("model")
                .to_string()
        };
        let tables = load(model, &mut notes);
        dashboards.push(make(name, model, tables));
    }
    for t in &templates {
        let name = t
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("report")
            .to_string();
        match read_pbit(t) {
            Ok(Some(json)) => match parse_model_json(&json) {
                Ok(m) => {
                    let tables = model_tables(&m, &t.display().to_string(), &mut notes);
                    dashboards.push(make(name, t, tables));
                }
                Err(e) => notes.push(format!("{}: {e:#}", t.display())),
            },
            Ok(None) => notes.push(format!(
                "{}: no DataModelSchema (live connection to a published semantic model); skipped",
                t.display()
            )),
            Err(e) => notes.push(format!("{}: {e:#}", t.display())),
        }
    }
    shorten_ids(&mut dashboards);
    Ok((dashboards, notes))
}

fn model_tables(m: &SemanticModel, label: &str, notes: &mut Vec<String>) -> BTreeSet<String> {
    let (tables, without) = m.source_tables();
    if !without.is_empty() {
        notes.push(format!(
            "{label}: {} table(s) with no warehouse table found (files, web, entered data, dynamic SQL): {}",
            without.len(),
            without.join(", ")
        ));
    }
    tables
}

/// Ids are paths: keep the shortest tail (at least `<parent>/<file>`) that
/// tells same-named reports apart, e.g. `pbip/Sales.Report` and
/// `templates/Sales.pbit`; the id is appended to colliding exposure names.
fn shorten_ids(dashboards: &mut [Dashboard]) {
    let parts: Vec<Vec<&str>> = dashboards
        .iter()
        .map(|d| d.id.split('/').filter(|p| !p.is_empty()).collect())
        .collect();
    let slugs: Vec<String> = dashboards.iter().map(|d| super::slugify(&d.name)).collect();
    let tail = |j: usize, n: usize| parts[j][parts[j].len().saturating_sub(n)..].join("/");
    let ids: Vec<String> = (0..dashboards.len())
        .map(|i| {
            let mut n = 2;
            while n < parts[i].len()
                && (0..parts.len())
                    .any(|j| j != i && slugs[j] == slugs[i] && tail(j, n) == tail(i, n))
            {
                n += 1;
            }
            tail(i, n)
        })
        .collect();
    for (d, id) in dashboards.iter_mut().zip(ids) {
        d.id = id;
    }
}

fn canonical(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

/// Folder name without a PBIP suffix (`Sales.Report` → `Sales`).
fn strip_suffix(dir: &Path, suffixes: &[&str]) -> String {
    let name = dir
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .or_else(|| dir.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "report".into());
    suffixes
        .iter()
        .find_map(|s| name.strip_suffix(s))
        .map(String::from)
        .unwrap_or(name)
}

/// A `.bim` file inside a semantic model folder stands for the folder (so a
/// report's `byPath` reference matches it); any other `.bim` is its own model.
fn model_location(bim: &Path) -> PathBuf {
    let is_model_bim = bim
        .file_name()
        .is_some_and(|n| n.eq_ignore_ascii_case("model.bim"));
    match bim.parent() {
        Some(dir) if is_model_bim => dir.to_path_buf(),
        _ => bim.to_path_buf(),
    }
}

enum ModelRef {
    Path(PathBuf),
    Connection,
}

/// The semantic model a `definition.pbir` points at.
fn report_model(pbir: &Path) -> Result<ModelRef> {
    let v = read_json(pbir)?;
    let r = &v["datasetReference"];
    if let Some(p) = r["byPath"]["path"].as_str() {
        let dir = pbir.parent().unwrap_or(Path::new("."));
        return Ok(ModelRef::Path(dir.join(p.replace('\\', "/"))));
    }
    if r.get("byConnection").is_some_and(|c| !c.is_null()) {
        return Ok(ModelRef::Connection);
    }
    anyhow::bail!("no datasetReference in definition.pbir")
}

/// `definition.pbir` files of the reports a `.pbip` file lists.
fn pbip_reports(pbip: &Path) -> Result<Vec<PathBuf>> {
    let v = read_json(pbip)?;
    let dir = pbip.parent().unwrap_or(Path::new("."));
    Ok(v["artifacts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| a["report"]["path"].as_str())
        .map(|p| dir.join(p.replace('\\', "/")).join("definition.pbir"))
        .filter(|p| p.is_file())
        .collect())
}

fn read_json(path: &Path) -> Result<Value> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .with_context(|| format!("{} is not valid JSON", path.display()))
}

/// Load a semantic model folder (TMDL `definition/` preferred over
/// `model.bim`) or a standalone `.bim` file.
pub fn load_model(path: &Path) -> Result<SemanticModel> {
    if path.is_file() {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        return parse_model_json(&text);
    }
    let definition = path.join("definition");
    if definition.is_dir() {
        let mut model = SemanticModel::default();
        for entry in walkdir::WalkDir::new(&definition).sort_by_file_name() {
            let entry = entry?;
            if entry.file_type().is_file() && entry.path().extension().is_some_and(|e| e == "tmdl")
            {
                let src = std::fs::read_to_string(entry.path())
                    .with_context(|| format!("cannot read {}", entry.path().display()))?;
                parse_tmdl(&src, &mut model);
            }
        }
        return Ok(model);
    }
    let bim = std::fs::read_dir(path)
        .with_context(|| format!("cannot read {}", path.display()))?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .is_some_and(|n| n.eq_ignore_ascii_case("model.bim"))
        })
        .with_context(|| format!("no definition/ or model.bim in {}", path.display()))?;
    let text =
        std::fs::read_to_string(&bim).with_context(|| format!("cannot read {}", bim.display()))?;
    parse_model_json(&text)
}

/// The `DataModelSchema` JSON of a `.pbit` (UTF-16LE); `None` when the
/// template has no model (a live connection).
fn read_pbit(path: &Path) -> Result<Option<String>> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes))
        .with_context(|| format!("{} is not a valid .pbit", path.display()))?;
    let Some(name) = zip
        .file_names()
        .find(|n| n.eq_ignore_ascii_case("DataModelSchema"))
        .map(String::from)
    else {
        return Ok(None);
    };
    let mut raw = Vec::new();
    zip.by_name(&name)?.read_to_end(&mut raw)?;
    Ok(Some(decode_text(&raw)))
}

/// UTF-16LE (with or without BOM) or UTF-8 text.
fn decode_text(raw: &[u8]) -> String {
    let utf16 = raw.starts_with(&[0xFF, 0xFE]) || (raw.len() > 1 && raw[1] == 0 && raw[0] != 0);
    let text = if utf16 {
        let units: Vec<u16> = raw
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u16::from_le_bytes(*c))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(raw).into_owned()
    };
    text.trim_start_matches('\u{feff}').to_string()
}

// ---------------------------------------------------------------------------
// Model formats: TMSL JSON (model.bim, DataModelSchema) and TMDL.
// ---------------------------------------------------------------------------

/// Parse TMSL JSON (`model.bim`, a `.pbit`'s `DataModelSchema`).
pub fn parse_model_json(json: &str) -> Result<SemanticModel> {
    let v: Value = serde_json::from_str(json.trim_start_matches('\u{feff}'))
        .context("semantic model is not valid JSON")?;
    // model.bim wraps the model in a database object; some exports are a
    // bare model.
    let model = if v.get("model").is_some() {
        &v["model"]
    } else {
        &v
    };
    let mut out = SemanticModel::default();
    for t in model["tables"].as_array().into_iter().flatten() {
        let Some(name) = t["name"].as_str() else {
            continue;
        };
        let mut sources = Vec::new();
        for p in t["partitions"].as_array().into_iter().flatten() {
            let s = &p["source"];
            let kind = s["type"].as_str().unwrap_or("").to_lowercase();
            sources.push(match kind.as_str() {
                "m" | "policyrange" => json_text(&s["expression"]).map_or(Source::Other, Source::M),
                "query" => json_text(&s["query"]).map_or(Source::Other, Source::Sql),
                "entity" => s["entityName"].as_str().map_or(Source::Other, |e| {
                    Source::Entity(match s["schemaName"].as_str() {
                        Some(schema) => format!("{schema}.{e}"),
                        None => e.to_string(),
                    })
                }),
                _ => Source::Other,
            });
        }
        if let Some(m) = json_text(&t["refreshPolicy"]["sourceExpression"]) {
            sources.push(Source::M(m));
        }
        out.tables.insert(name.to_string(), sources);
    }
    for e in model["expressions"].as_array().into_iter().flatten() {
        let kind = e["kind"].as_str().unwrap_or("m");
        if let (Some(name), Some(expr), true) = (
            e["name"].as_str(),
            json_text(&e["expression"]),
            kind.eq_ignore_ascii_case("m"),
        ) {
            out.expressions.insert(name.to_string(), expr);
        }
    }
    Ok(out)
}

/// A TMSL text property: a string or an array of lines.
fn json_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Array(lines) => Some(
            lines
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        _ => None,
    }
}

static TMDL_TABLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*table\s+('(?:[^']|'')*'|.+?)\s*$").unwrap());
static TMDL_PARTITION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*partition\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(\w+)\s*$").unwrap()
});
static TMDL_EXPRESSION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\s*expression\s+('(?:[^']|'')*'|[^=]+?)\s*=\s*(.*?)\s*$").unwrap()
});
static TMDL_SOURCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*source\s*=\s*(.*?)\s*$").unwrap());
static TMDL_PROP: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*(entityName|schemaName)\s*:\s*(.+?)\s*$").unwrap());

/// Parse one TMDL file into `model` (tables with partitions, expressions).
/// Unknown objects are ignored; nothing here fails.
pub fn parse_tmdl(src: &str, model: &mut SemanticModel) {
    let lines: Vec<&str> = src.lines().collect();
    let mut table: Option<String> = None;
    // Current partition: (kind, indent, entity name, schema name).
    let mut partition: Option<(String, usize, Option<String>, Option<String>)> = None;
    let mut i = 0;

    let flush = |partition: &mut Option<(String, usize, Option<String>, Option<String>)>,
                 table: &Option<String>,
                 model: &mut SemanticModel| {
        if let (Some((kind, _, Some(entity), schema)), Some(t)) = (partition.take(), table) {
            if kind == "entity" {
                let e = match schema {
                    Some(s) => format!("{s}.{entity}"),
                    None => entity,
                };
                model
                    .tables
                    .entry(t.clone())
                    .or_default()
                    .push(Source::Entity(e));
            }
        }
    };

    while i < lines.len() {
        let line = lines[i];
        let ind = indent(line);
        if partition
            .as_ref()
            .is_some_and(|p| !line.trim().is_empty() && ind <= p.1)
        {
            flush(&mut partition, &table, model);
        }
        if let Some(c) = TMDL_TABLE.captures(line) {
            flush(&mut partition, &table, model);
            let name = unquote_tmdl(&c[1]);
            model.tables.entry(name.clone()).or_default();
            table = Some(name);
        } else if let Some(c) = TMDL_PARTITION.captures(line) {
            flush(&mut partition, &table, model);
            partition = Some((c[2].to_lowercase(), ind, None, None));
        } else if let Some(c) = TMDL_EXPRESSION.captures(line) {
            flush(&mut partition, &table, model);
            table = None;
            let name = unquote_tmdl(&c[1]);
            let (expr, next) = block_value(&lines, i, &c[2]);
            model.expressions.insert(name, expr);
            i = next;
            continue;
        } else if let Some(p) = &mut partition {
            if let Some(c) = TMDL_SOURCE.captures(line) {
                let kind = p.0.clone();
                let (expr, next) = block_value(&lines, i, &c[1]);
                if let Some(t) = &table {
                    let source = match kind.as_str() {
                        "m" | "policyrange" => Source::M(expr),
                        "query" => Source::Sql(expr),
                        _ => Source::Other,
                    };
                    model.tables.entry(t.clone()).or_default().push(source);
                }
                i = next;
                continue;
            } else if let Some(c) = TMDL_PROP.captures(line) {
                let v = unquote_tmdl(&c[2]);
                if &c[1] == "entityName" {
                    p.2 = Some(v);
                } else {
                    p.3 = Some(v);
                }
            }
        }
        i += 1;
    }
    flush(&mut partition, &table, model);
}

/// Value of a `name = <rest>` property starting at line `at`: the rest of the
/// line, or the following more-indented lines (or a ```` ``` ```` fence).
/// Returns the value and the index of the next unread line.
fn block_value(lines: &[&str], at: usize, rest: &str) -> (String, usize) {
    let base = indent(lines[at]);
    let rest = rest.trim();
    if rest.starts_with("```") {
        let mut body = Vec::new();
        let mut i = at + 1;
        while i < lines.len() && lines[i].trim() != "```" {
            body.push(lines[i]);
            i += 1;
        }
        return (body.join("\n"), i + 1);
    }
    if !rest.is_empty() {
        return (rest.to_string(), at + 1);
    }
    // Multi-line value: as deep as its first line (properties of the object
    // that follow sit one level shallower).
    let mut i = at + 1;
    while i < lines.len() && lines[i].trim().is_empty() {
        i += 1;
    }
    let Some(depth) = lines.get(i).map(|l| indent(l)).filter(|d| *d > base) else {
        return (String::new(), i);
    };
    let mut body = Vec::new();
    while i < lines.len() && (lines[i].trim().is_empty() || indent(lines[i]) >= depth) {
        body.push(lines[i].trim());
        i += 1;
    }
    (body.join("\n").trim().to_string(), i)
}

/// Indentation width; a tab counts as four spaces.
fn indent(line: &str) -> usize {
    line.chars()
        .take_while(|c| c.is_whitespace())
        .map(|c| if c == '\t' { 4 } else { 1 })
        .sum()
}

fn unquote_tmdl(name: &str) -> String {
    let n = name.trim();
    match n.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        Some(inner) => inner.replace("''", "'"),
        None => n.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Power Query (M) expressions.
// ---------------------------------------------------------------------------

static M_STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""(?:[^"]|"")*""#).unwrap());
static NAV_RECORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{\s*\[([^\[\]{}]*)\]\s*\}").unwrap());
static NAV_FIELD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"([A-Za-z_]\w*)\s*=\s*("(?:[^"]|"")*"|#"(?:[^"]|"")*"|[A-Za-z_][\w.]*)"#).unwrap()
});
static NAV_DATA: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*\[Data\]").unwrap());
static TRAILING_IDENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(#"(?:[^"]|"")*"|[A-Za-z_]\w*)\s*$"#).unwrap());
static STEP_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(#"(?:[^"]|"")*"|[A-Za-z_]\w*)\s*=\s*(?:#"(?:[^"]|"")*"|[A-Za-z_][\w.]*)\s*$"#)
        .unwrap()
});
static QUERY_OPTION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\bQuery\s*=\s*("(?:[^"]|"")*")"#).unwrap());
static NATIVE_QUERY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bValue\.NativeQuery\s*\(").unwrap());
static ESCAPE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"#\(([^)]*)\)").unwrap());
static BRACKET_IDENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]\[]+)\]").unwrap());

/// Power Query connectors whose navigation records name files, sheets,
/// pages or feed entities rather than warehouse tables.
const NON_WAREHOUSE: &[&str] = &[
    "Excel.Workbook",
    "OData.Feed",
    "Web.Contents",
    "Web.Page",
    "SharePoint.",
    "Csv.Document",
    "Json.Document",
    "Xml.Tables",
    "Xml.Document",
    "Folder.Files",
    "File.Contents",
    "Pdf.Tables",
];

/// Warehouse tables in one M expression (comments already stripped):
/// navigation steps and native SQL. `params` resolves parameter names used
/// as navigation values.
pub fn m_tables(code: &str, params: &BTreeMap<&str, String>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    // Files, web pages and feeds navigate with the same records
    // (`Kind="Table"` for Excel tables, `{[Name="Orders"]}` for OData
    // entities); they are not warehouse tables.
    if !NON_WAREHOUSE.iter().any(|c| code.contains(c)) {
        // Steps navigated from (`Db{[…]}`): a `{[Name="x"]}` step without
        // `Kind` is a table only if nothing navigates further from it.
        let bases: HashSet<String> = NAV_RECORD
            .find_iter(code)
            .filter_map(|m| base_ident(&code[..m.start()]))
            .collect();
        let native = NATIVE_QUERY.is_match(code);
        for rec in NAV_RECORD.captures_iter(code) {
            let whole = rec.get(0).unwrap();
            let mut fields: BTreeMap<String, String> = BTreeMap::new();
            for f in NAV_FIELD.captures_iter(&rec[1]) {
                let raw = &f[2];
                let value = if raw.starts_with('"') {
                    unescape_m(raw)
                } else {
                    let key = raw
                        .strip_prefix('#')
                        .map(unescape_m)
                        .unwrap_or_else(|| raw.to_string());
                    match params.get(key.as_str()) {
                        Some(v) => v.clone(),
                        None => continue,
                    }
                };
                fields.insert(f[1].to_lowercase(), value);
            }
            let kind = fields.get("kind").map(|k| k.to_lowercase());
            let table = match kind.as_deref() {
                Some("table" | "view") => fields.get("name").or(fields.get("item")).cloned(),
                Some(_) => None,
                None => match (fields.get("item"), fields.get("name")) {
                    (Some(item), _) => Some(match fields.get("schema") {
                        Some(s) => format!("{s}.{item}"),
                        None => item.clone(),
                    }),
                    // A native query targets a database step, not a table.
                    (None, Some(name)) if !native => {
                        let after = &code[whole.end()..];
                        let data = NAV_DATA.find(after).filter(|m| m.start() == 0);
                        let innermost = data.is_some_and(|d| {
                            !after[d.end()..].trim_start().starts_with('{')
                                && step_name(&code[..whole.start()])
                                    .is_none_or(|s| !bases.contains(&s))
                        });
                        innermost.then(|| name.clone())
                    }
                    _ => None,
                },
            };
            if let Some(t) = table.filter(|t| !t.is_empty()) {
                out.insert(t);
            }
        }
    }
    for c in QUERY_OPTION.captures_iter(code) {
        out.extend(sql_tables(&unescape_m(&c[1])));
    }
    for m in NATIVE_QUERY.find_iter(code) {
        if let Some(sql) = native_query_sql(code, m.end()) {
            out.extend(sql_tables(&sql));
        }
    }
    out
}

/// Identifier right before a navigation record (`Source` in `Source{[…]}`).
fn base_ident(prefix: &str) -> Option<String> {
    TRAILING_IDENT.captures(prefix).map(|c| ident_name(&c[1]))
}

/// The step a navigation record is assigned to (`tpch` in
/// `tpch = Source{[Name="tpch"]}[Data]`).
fn step_name(prefix: &str) -> Option<String> {
    STEP_PREFIX.captures(prefix).map(|c| ident_name(&c[1]))
}

/// `#"Quoted name"` → `Quoted name`; plain identifiers as is.
fn ident_name(raw: &str) -> String {
    raw.strip_prefix('#')
        .map(unescape_m)
        .unwrap_or_else(|| raw.to_string())
}

/// The SQL argument of `Value.NativeQuery(target, sql, …)` whose argument
/// list starts at `start`: a string literal, or a step holding one.
fn native_query_sql(code: &str, start: usize) -> Option<String> {
    let bytes = code.as_bytes();
    let (mut i, mut depth) = (start, 0i32);
    // Skip the first argument.
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
            }
            b',' if depth == 0 => break,
            _ => {}
        }
        i += 1;
    }
    let rest = code.get(i + 1..)?.trim_start();
    if rest.starts_with('"') {
        return M_STRING.find(rest).map(|m| unescape_m(m.as_str()));
    }
    // A step name: `sql = "select …"` elsewhere in the let.
    let ident: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        return None;
    }
    let step = Regex::new(&format!(
        r#"\b{}\s*=\s*("(?:[^"]|"")*")"#,
        regex::escape(&ident)
    ))
    .ok()?;
    step.captures(code).map(|c| unescape_m(&c[1]))
}

/// Tables read by a native SQL query; T-SQL `[schema].[table]` brackets are
/// turned into quoted identifiers first.
fn sql_tables(sql: &str) -> BTreeSet<String> {
    let sql = BRACKET_IDENT.replace_all(sql, "\"$1\"");
    super::anofox::dashboard_tables(&sql)
}

/// Decode an M string literal (with its quotes): `""` → `"`, `#(lf)`,
/// `#(cr,lf)`, `#(tab)`, `#(#)` and `#(000A)` escapes.
pub fn unescape_m(lit: &str) -> String {
    let inner = lit
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(lit)
        .replace("\"\"", "\"");
    ESCAPE
        .replace_all(&inner, |c: &regex::Captures| {
            let mut s = String::new();
            for part in c[1].split(',') {
                match part.trim().to_lowercase().as_str() {
                    "lf" => s.push('\n'),
                    "cr" => s.push('\r'),
                    "tab" => s.push('\t'),
                    "#" => s.push('#'),
                    hex if (hex.len() == 4 || hex.len() == 8)
                        && hex.chars().all(|c| c.is_ascii_hexdigit()) =>
                    {
                        match u32::from_str_radix(hex, 16).ok().and_then(char::from_u32) {
                            Some(ch) => s.push(ch),
                            None => return c[0].to_string(),
                        }
                    }
                    _ => return c[0].to_string(),
                }
            }
            s
        })
        .into_owned()
}

/// `code` refers to the query `name`, as `#"name"` or as a bare identifier.
fn references(code: &str, name: &str) -> bool {
    let quoted = format!("#\"{}\"", name.replace('"', "\"\""));
    if code.contains(&quoted) {
        return true;
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') || name.is_empty() {
        return false;
    }
    let ident = |c: char| c.is_alphanumeric() || c == '_' || c == '.';
    // Outside string literals only.
    let code = M_STRING.replace_all(code, "\"\"");
    code.match_indices(name)
        .any(|(i, _)| !code[..i].ends_with(ident) && !code[i + name.len()..].starts_with(ident))
}

/// Drop `//` and `/* */` comments outside string literals.
fn strip_m_comments(m: &str) -> String {
    let bytes = m.as_bytes();
    let mut out = String::with_capacity(m.len());
    let (mut i, mut start) = (0, 0);
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                out.push_str(&m[start..i]);
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                start = i;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                out.push_str(&m[start..i]);
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
    out.push_str(&m[start.min(m.len())..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(m: &str) -> Vec<String> {
        m_tables(&strip_m_comments(m), &BTreeMap::new())
            .into_iter()
            .collect()
    }

    #[test]
    fn navigation_steps() {
        // SQL Server / Postgres: schema + item.
        let m = r#"let
    Source = Sql.Database("srv.database.windows.net", "shop"),
    dbo_orders = Source{[Schema="dbo",Item="orders"]}[Data],
    // dbo_old = Source{[Schema="dbo",Item="old_orders"]}[Data],
    #"Filtered Rows" = Table.SelectRows(dbo_orders, each [status] = "done")
in
    #"Filtered Rows""#;
        assert_eq!(tables(m), vec!["dbo.orders"]);

        // Snowflake: database → schema → table, by Kind.
        let m = r#"let
    Source = Snowflake.Databases("acme.snowflakecomputing.com", "WH"),
    DB = Source{[Name="ANALYTICS",Kind="Database"]}[Data],
    MARTS = DB{[Name="MARTS",Kind="Schema"]}[Data],
    FCT = MARTS{[Name="FCT_ORDERS",Kind="Table"]}[Data],
    V = MARTS{[Name="V_CUSTOMERS",Kind="View"]}[Data]
in FCT"#;
        assert_eq!(tables(m), vec!["FCT_ORDERS", "V_CUSTOMERS"]);

        // Redshift / Databricks catalogs without Kind: the innermost
        // `[Name=…]` step is the table, chained inline or step by step.
        let m = r#"let
    Source = AmazonRedshift.Database("host:5439","dev"),
    tpch = Source{[Name="tpch"]}[Data],
    region = tpch{[Name="region"]}[Data],
    nation = Source{[Name="tpch"]}[Data]{[Name="nation"]}[Data]
in region"#;
        assert_eq!(tables(m), vec!["nation", "region"]);

        // Excel sheets and tables, SharePoint files: not warehouse tables.
        let m = r#"let
    Wb = Excel.Workbook(File.Contents("C:\data\budget.xlsx"), null, true),
    Budget = Wb{[Item="Budget",Kind="Table"]}[Data],
    Sheet = Wb{[Item="Sheet1",Kind="Sheet"]}[Data]
in Budget"#;
        assert!(tables(m).is_empty());
        // OData feed entities navigate like Redshift tables but are not.
        let odata = "let\n    Source = OData.Feed(Service),\n    Orders_table = Source{[Name=\"Orders\",Signature=\"table\"]}[Data]\nin\n    Orders_table";
        assert!(m_tables(odata, &BTreeMap::new()).is_empty());
        let m = r#"SharePoint.Files("https://x.sharepoint.com"){[Name="a.csv"]}[Content]"#;
        assert!(tables(m).is_empty());
    }

    #[test]
    fn navigation_values_from_parameters() {
        let mut params = BTreeMap::new();
        params.insert("SchemaName", "marts".to_string());
        params.insert("Table Name", "fct_orders".to_string());
        let m = r#"Source{[Schema=SchemaName,Item=#"Table Name"]}[Data]"#;
        assert_eq!(
            m_tables(m, &params).into_iter().collect::<Vec<_>>(),
            vec!["marts.fct_orders"]
        );
    }

    #[test]
    fn native_sql() {
        let m = r#"let
    Source = Snowflake.Databases("acme", "WH"){[Name="ANALYTICS"]}[Data],
    Q = Value.NativeQuery(Source, "select o.id, c.name#(lf)from marts.fct_orders o#(cr,lf)join ""marts"".""dim_customers"" c on c.id = o.cid", null, [EnableFolding=true])
in Q"#;
        assert_eq!(tables(m), vec!["marts.dim_customers", "marts.fct_orders"]);

        // SQL held in a step, T-SQL brackets.
        let m = r#"let
    sql = "SELECT * FROM [dbo].[Payments] p WITH (NOLOCK)",
    Source = Value.NativeQuery(Sql.Database("srv", "db"), sql)
in Source"#;
        assert_eq!(tables(m), vec!["dbo.payments"]);

        let m = r#"Sql.Database("srv", "db", [Query="select * from stg_refunds where amount > 0", CommandTimeout=#duration(0, 0, 5, 0)])"#;
        assert_eq!(tables(m), vec!["stg_refunds"]);
    }

    #[test]
    fn unescapes_m_strings() {
        assert_eq!(
            unescape_m(r##""say ""hi""#(lf)#(tab)x#(#)(lf) #(00E9)""##),
            "say \"hi\"\n\tx#(lf) \u{e9}"
        );
        assert_eq!(unescape_m(r#""keep #(unknown)""#), "keep #(unknown)");
    }

    #[test]
    fn references_by_name() {
        assert!(references(
            "let s = #\"Customer Base\" in s",
            "Customer Base"
        ));
        assert!(references("Table.Join(Orders, Base)", "Base"));
        assert!(!references("Table.Join(Orders, BaseX)", "Base"));
        assert!(!references("Date.Year([d])", "Date"));
        assert!(!references("\"Base\"", "Base"));
    }

    const MODEL_BIM: &str = r#"{
  "name": "Sales",
  "compatibilityLevel": 1567,
  "model": {
    "tables": [
      {"name": "Orders", "partitions": [{"name": "Orders", "mode": "import", "source": {"type": "m", "expression": [
        "let",
        "    Source = #\"Warehouse\",",
        "    orders = Source{[Schema=\"marts\",Item=\"fct_orders\"]}[Data]",
        "in",
        "    orders"]}}]},
      {"name": "Customers", "partitions": [{"name": "c", "source": {"type": "m",
        "expression": "let Source = #\"Customer Base\" in Source"}}]},
      {"name": "Refunds", "partitions": [{"name": "r", "source": {"type": "query",
        "query": "SELECT * FROM dbo.stg_refunds", "dataSource": "SqlServer srv db"}}]},
      {"name": "Lake", "partitions": [{"name": "l", "mode": "directLake", "source": {"type": "entity",
        "entityName": "dim_products", "schemaName": "gold", "expressionSource": "DatabaseQuery"}}]},
      {"name": "Calendar", "partitions": [{"name": "cal", "source": {"type": "calculated",
        "expression": "CALENDARAUTO()"}}]},
      {"name": "Targets", "partitions": [{"name": "t", "source": {"type": "m",
        "expression": "let Source = Csv.Document(File.Contents(\"targets.csv\")) in Source"}}]},
      {"name": "Events", "refreshPolicy": {"policyType": "basic",
        "sourceExpression": ["let", "  Source = #\"Warehouse\"{[Schema=\"marts\",Item=\"fct_events\"]}[Data]", "in Source"]},
        "partitions": [{"name": "2024", "source": {"type": "policyRange", "start": "2024-01-01"}}]}
    ],
    "expressions": [
      {"name": "Server", "kind": "m", "expression": "\"srv.example.com\" meta [IsParameterQuery=true, Type=\"Text\"]"},
      {"name": "Warehouse", "kind": "m", "expression": "Sql.Database(Server, \"dw\")"},
      {"name": "Customer Base", "kind": "m", "expression": "let Source = Warehouse{[Schema=\"marts\",Item=\"dim_customers\"]}[Data] in Source"},
      {"name": "Unused", "kind": "m", "expression": "Warehouse{[Schema=\"marts\",Item=\"unused\"]}[Data]"}
    ]
  }
}"#;

    #[test]
    fn model_bim_partitions_and_shared_expressions() {
        let model = parse_model_json(&format!("\u{feff}{MODEL_BIM}")).unwrap();
        assert_eq!(model.tables.len(), 7);
        assert_eq!(model.expressions.len(), 4);
        let (t, without) = model.source_tables();
        assert_eq!(
            t.into_iter().collect::<Vec<_>>(),
            vec![
                "dbo.stg_refunds",
                "gold.dim_products",
                "marts.dim_customers",
                "marts.fct_events",
                "marts.fct_orders"
            ]
        );
        // Calendar is DAX (no note); Targets reads a file.
        assert_eq!(without, vec!["Targets"]);
    }

    const TABLE_TMDL: &str = "table Orders
\tlineageTag: 1b2c

\tcolumn id
\t\tdataType: int64
\t\tsourceColumn: id

\tmeasure 'Total Amount' = SUM(Orders[amount])
\t\tformatString: 0.00

\tpartition Orders-1a2b = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Databricks.Catalogs(Host, \"/sql/1.0/warehouses/x\"),
\t\t\t\t    main = Source{[Name=\"main\",Kind=\"Database\"]}[Data],
\t\t\t\t    marts = main{[Name=\"marts\",Kind=\"Schema\"]}[Data],
\t\t\t\t    fct = marts{[Name=\"fct_orders\",Kind=\"Table\"]}[Data]
\t\t\t\tin
\t\t\t\t    fct

\tannotation PBI_ResultType = Table

table 'Order Items'

\tpartition 'Order Items' = m
\t\tmode: directQuery
\t\tsource = ```
\t\t\tlet
\t\t\t    Source = Value.NativeQuery(#\"Base\", \"select * from marts.fct_order_items\")
\t\t\tin Source
\t\t\t```

table Lake
\tpartition Lake = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: dim_products
\t\t\tschemaName: gold
\t\t\texpressionSource: DatabaseQuery

table Dates
\tpartition Dates = calculated
\t\tmode: import
\t\tsource = CALENDAR(DATE(2020, 1, 1), DATE(2030, 12, 31))
";

    const EXPRESSIONS_TMDL: &str = "expression Host = \"adb-1.azuredatabricks.net\" meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]
\tlineageTag: 9f8e

\tannotation PBI_ResultType = Text

/// Shared connection
expression Base =
\t\tlet
\t\t    Source = Databricks.Catalogs(Host, \"/sql\"){[Name=\"main\",Kind=\"Database\"]}[Data]
\t\tin
\t\t    Source
\tlineageTag: 7a6b

\tannotation PBI_NavigationStepName = Navigation
";

    #[test]
    fn tmdl_tables_partitions_and_expressions() {
        let mut model = SemanticModel::default();
        parse_tmdl(TABLE_TMDL, &mut model);
        parse_tmdl(EXPRESSIONS_TMDL, &mut model);
        let names: Vec<&str> = model.tables.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["Dates", "Lake", "Order Items", "Orders"]);
        assert_eq!(model.tables["Dates"], vec![Source::Other]);
        assert_eq!(
            model.tables["Lake"],
            vec![Source::Entity("gold.dim_products".into())]
        );
        assert_eq!(
            model.expressions["Host"],
            "\"adb-1.azuredatabricks.net\" meta [IsParameterQuery=true, Type=\"Text\", IsParameterQueryRequired=true]"
        );
        // The body stops before the expression's own properties.
        assert!(model.expressions["Base"].ends_with("Source"));
        let (t, without) = model.source_tables();
        assert_eq!(
            t.into_iter().collect::<Vec<_>>(),
            vec!["fct_orders", "gold.dim_products", "marts.fct_order_items"]
        );
        assert!(without.is_empty(), "{without:?}");
    }

    /// A `.pbit`: zip with `DataModelSchema` as UTF-16LE JSON (with BOM).
    fn write_pbit(path: &Path, model_json: Option<&str>) {
        use std::io::Write;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let f = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(f);
        let opts = zip::write::SimpleFileOptions::default();
        zip.start_file("Version", opts).unwrap();
        zip.write_all(&[0x31, 0x00, 0x2e, 0x00, 0x32, 0x00])
            .unwrap();
        if let Some(json) = model_json {
            zip.start_file("DataModelSchema", opts).unwrap();
            let mut bytes = vec![0xFF, 0xFE];
            for u in json.encode_utf16() {
                bytes.extend_from_slice(&u.to_le_bytes());
            }
            zip.write_all(&bytes).unwrap();
        }
        zip.start_file("Report/Layout", opts).unwrap();
        zip.write_all(b"{}").unwrap();
        zip.finish().unwrap();
    }

    #[test]
    fn pbip_projects_and_templates() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let write = |rel: &str, text: &str| {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, text).unwrap();
        };
        // TMDL model used by two reports.
        write(
            "Sales.SemanticModel/definition/tables/Orders.tmdl",
            TABLE_TMDL,
        );
        write(
            "Sales.SemanticModel/definition/expressions.tmdl",
            EXPRESSIONS_TMDL,
        );
        write(
            "Sales.SemanticModel/definition/model.tmdl",
            "model Model\n\tculture: en-US\n",
        );
        let by_path = |m: &str| {
            format!(
                r#"{{"version": "4.0", "datasetReference": {{"byPath": {{"path": "../{m}"}}}}}}"#
            )
        };
        write(
            "Sales.Report/definition.pbir",
            &by_path("Sales.SemanticModel"),
        );
        write(
            "Sales Detail.Report/definition.pbir",
            &by_path("Sales.SemanticModel"),
        );
        // model.bim model, plus a report on a published dataset.
        write("Finance.SemanticModel/model.bim", MODEL_BIM);
        write(
            "Finance.Report/definition.pbir",
            &by_path("Finance.SemanticModel"),
        );
        write(
            "Shared.Report/definition.pbir",
            r#"{"version": "1.0", "datasetReference": {"byConnection": {"connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/ws;Initial Catalog=Shared"}}}"#,
        );
        // Model without a report.
        write("Orphan.SemanticModel/definition/tables/T.tmdl", TABLE_TMDL);
        write_pbit(&root.join("templates/Ops.pbit"), Some(MODEL_BIM));
        write_pbit(&root.join("templates/Live.pbit"), None);
        write("templates/Old.pbix", "binary");

        let (ds, notes) = dashboards_from_paths(
            &[root.to_path_buf()],
            Some("https://app.powerbi.com/r/{name}"),
        )
        .unwrap();
        let names: Vec<&str> = ds.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["Finance", "Sales Detail", "Sales", "Orphan", "Ops"]
        );
        let sales = &ds[2];
        assert_eq!(sales.url, "https://app.powerbi.com/r/Sales");
        assert_eq!(sales.owner_name.as_deref(), Some("powerbi"));
        assert!(sales.id.ends_with("/Sales.Report"), "{}", sales.id);
        assert_eq!(
            sales.tables.iter().collect::<Vec<_>>(),
            vec!["fct_orders", "gold.dim_products", "marts.fct_order_items"]
        );
        assert_eq!(ds[1].tables, sales.tables);
        assert_eq!(ds[0].tables.len(), 5);
        assert_eq!(ds[4].tables, ds[0].tables);
        assert_eq!(ds[4].id, "templates/Ops.pbit");

        let has = |s: &str| notes.iter().any(|n| n.contains(s));
        assert!(has("'Shared' uses a published semantic model"), "{notes:?}");
        assert!(has("Live.pbit: no DataModelSchema"), "{notes:?}");
        assert!(has("Old.pbix: .pbix stores"), "{notes:?}");
        assert!(has("1 table(s) with no warehouse table found"), "{notes:?}");

        // Without --url the report folder is the link; a .pbip file selects
        // its report.
        write(
            "Sales.pbip",
            r#"{"version": "1.0", "artifacts": [{"report": {"path": "Sales.Report"}}]}"#,
        );
        let (ds, _) = dashboards_from_paths(&[root.join("Sales.pbip")], None).unwrap();
        assert_eq!(ds.len(), 1);
        assert!(ds[0].url.ends_with("Sales.Report"), "{}", ds[0].url);
    }

    #[test]
    fn same_name_models_get_distinct_ids() {
        let dir = tempfile::tempdir().unwrap();
        for v in ["V1", "V2", "a/V3", "b/V3"] {
            std::fs::create_dir_all(dir.path().join(v)).unwrap();
            std::fs::write(dir.path().join(v).join("Sales Model.bim"), MODEL_BIM).unwrap();
        }
        let (ds, _) = dashboards_from_paths(&[dir.path().to_path_buf()], None).unwrap();
        let ids: Vec<&str> = ds.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "V1/Sales Model.bim",
                "V2/Sales Model.bim",
                "a/V3/Sales Model.bim",
                "b/V3/Sales Model.bim"
            ]
        );
        assert!(ds.iter().all(|d| d.name == "Sales Model"));
    }
}
