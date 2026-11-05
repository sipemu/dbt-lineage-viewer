//! Apache Superset dashboard exports, read offline.
//!
//! An export (`superset export-dashboards`, or *Download* in the UI) is a ZIP
//! of YAML files: `dashboards/*.yaml` (`dashboard_title`, `slug`, `uuid` and a
//! `position` layout whose `CHART` entries carry the chart `uuid`),
//! `charts/*.yaml` (`slice_name`, `uuid`, `dataset_uuid`), `datasets/<db>/*.yaml`
//! (`table_name`, `schema`, `sql` for virtual datasets, `uuid`) and
//! `databases/*.yaml`. A dashboard's tables are those of the datasets behind
//! its charts: `schema.table_name` for a physical dataset, the tables its SQL
//! reads for a virtual one (Jinja neutralised).
//!
//! Files are recognised by content, not path, so unpacked exports and the
//! example layout in the Superset repo (`dashboard.yaml`, `dataset.yaml`) work
//! too. The legacy single-file JSON export (`dashboards` + `datasources` with
//! `__Dashboard__` / `__Slice__` / `__SqlaTable__` objects) is also read.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;
use serde_yaml::Value;

use super::Dashboard;

static JINJA_EXPR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{\{.*?\}\}").unwrap());
static JINJA_BLOCK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)\{%.*?%\}|\{#.*?#\}").unwrap());

/// A Superset dashboard from an export.
#[derive(Debug, Clone)]
struct SupersetDashboard {
    uuid: String,
    title: String,
    slug: Option<String>,
    description: Option<String>,
    /// Chart uuids from `position` `CHART` entries.
    charts: Vec<String>,
    source: String,
}

#[derive(Debug, Clone)]
struct Chart {
    name: String,
    dataset: Option<String>,
}

/// All assets found in the inputs, keyed by uuid.
#[derive(Debug, Default)]
struct Assets {
    dashboards: Vec<SupersetDashboard>,
    charts: HashMap<String, Chart>,
    /// Dataset uuid → (label, tables).
    datasets: HashMap<String, (String, BTreeSet<String>)>,
}

/// Dashboards from export ZIPs, unpacked export directories (searched
/// recursively for `.yaml`/`.yml`/`.zip`/`.json`) and single files. `url` is
/// the exposure URL; `{name}` is replaced by the dashboard slug, or its uuid
/// when it has none. Without `url`, the export path is used. Returns the
/// dashboards and notes about charts or datasets that could not be resolved.
pub fn dashboards_from_paths(
    paths: &[PathBuf],
    url: Option<&str>,
) -> Result<(Vec<Dashboard>, Vec<String>)> {
    let wanted = |p: &Path| {
        p.extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| ["yaml", "yml", "zip", "json"].contains(&e.to_lowercase().as_str()))
    };
    let mut files = Vec::new();
    for p in paths {
        if p.is_dir() {
            for entry in walkdir::WalkDir::new(p).sort_by_file_name() {
                let entry = entry?;
                if entry.file_type().is_file() && wanted(entry.path()) {
                    files.push(entry.into_path());
                }
            }
        } else {
            files.push(p.clone());
        }
    }

    let mut assets = Assets::default();
    let mut legacy = Vec::new();
    let mut notes = Vec::new();
    for file in &files {
        let display = file.to_string_lossy().replace('\\', "/");
        let bytes =
            std::fs::read(file).with_context(|| format!("cannot read {}", file.display()))?;
        if bytes.starts_with(b"PK") {
            let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes))
                .with_context(|| format!("{} is not a valid ZIP", file.display()))?;
            let mut names: Vec<String> = zip
                .file_names()
                .filter(|n| {
                    let n = n.to_lowercase();
                    n.ends_with(".yaml") || n.ends_with(".yml")
                })
                .map(String::from)
                .collect();
            names.sort();
            for name in names {
                let mut s = String::new();
                zip.by_name(&name)?
                    .read_to_string(&mut s)
                    .with_context(|| format!("cannot read {name} in {}", file.display()))?;
                assets.add_yaml(&s, &display);
            }
        } else {
            let text = String::from_utf8_lossy(&bytes);
            let is_json = file
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("json"));
            if is_json {
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(v) if is_legacy_export(&v) => {
                        legacy.extend(legacy_dashboards(&v, &display, url, &mut notes))
                    }
                    Ok(_) => {}
                    Err(e) => notes.push(format!("{display}: not readable JSON ({e})")),
                }
            } else {
                assets.add_yaml(&text, &display);
            }
        }
    }

    let mut dashboards = assets.resolve(url, &mut notes);
    dashboards.extend(legacy);
    Ok((dashboards, notes))
}

impl Assets {
    /// Classify one YAML document by its keys; anything else (databases,
    /// `metadata.yaml`) is ignored.
    fn add_yaml(&mut self, src: &str, source: &str) {
        let Some(doc) = serde_yaml::from_str::<Value>(src)
            .ok()
            .or_else(|| crate::parser::yaml_schema::lenient_yaml(src))
        else {
            return;
        };
        let get = |k: &str| str_field(&doc, k);
        let Some(uuid) = get("uuid") else {
            return;
        };
        if let Some(title) = get("dashboard_title") {
            if self.dashboards.iter().any(|d| d.uuid == uuid) {
                return;
            }
            let mut charts = Vec::new();
            match doc.get("position") {
                // Exports store the layout as a mapping; the database (and
                // some tools) keep it as a JSON string.
                Some(Value::String(s)) => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                        charts = position_charts_json(&v);
                    }
                }
                Some(Value::Mapping(m)) => {
                    let mut items: Vec<(&Value, &Value)> = m.iter().collect();
                    items.sort_by_key(|(k, _)| k.as_str().unwrap_or("").to_string());
                    for (_, item) in items {
                        if item.get("type").and_then(Value::as_str) == Some("CHART") {
                            if let Some(u) = item
                                .get("meta")
                                .and_then(|m| m.get("uuid"))
                                .and_then(Value::as_str)
                            {
                                charts.push(u.to_string());
                            }
                        }
                    }
                }
                _ => {}
            }
            self.dashboards.push(SupersetDashboard {
                uuid,
                title,
                slug: get("slug"),
                description: get("description"),
                charts,
                source: source.to_string(),
            });
        } else if let Some(name) = get("slice_name") {
            // The same asset in several exports: the first copy wins, as
            // for dashboards.
            self.charts.entry(uuid).or_insert(Chart {
                name,
                dataset: get("dataset_uuid"),
            });
        } else if let Some(table) = get("table_name") {
            let tables = match get("sql") {
                Some(sql) => virtual_tables(&sql),
                None => BTreeSet::from([qualified(
                    get("catalog").as_deref(),
                    get("schema").as_deref(),
                    &table,
                )]),
            };
            self.datasets.entry(uuid).or_insert((table, tables));
        }
    }

    fn resolve(&self, url: Option<&str>, notes: &mut Vec<String>) -> Vec<Dashboard> {
        let mut out = Vec::new();
        for d in &self.dashboards {
            let mut tables = BTreeSet::new();
            for c in &d.charts {
                let Some(chart) = self.charts.get(c) else {
                    notes.push(format!(
                        "dashboard '{}': chart {c} is not in the export",
                        d.title
                    ));
                    continue;
                };
                let Some(ds) = &chart.dataset else {
                    notes.push(format!(
                        "dashboard '{}': chart '{}' has no dataset",
                        d.title, chart.name
                    ));
                    continue;
                };
                match self.datasets.get(ds) {
                    Some((label, t)) => {
                        if t.is_empty() {
                            notes.push(format!(
                                "dashboard '{}': no tables found in the SQL of dataset '{label}'",
                                d.title
                            ));
                        }
                        tables.extend(t.iter().cloned());
                    }
                    None => notes.push(format!(
                        "dashboard '{}': dataset {ds} of chart '{}' is not in the export",
                        d.title, chart.name
                    )),
                }
            }
            let id = d.slug.clone().unwrap_or_else(|| d.uuid.clone());
            out.push(make_dashboard(
                id,
                d.title.clone(),
                d.description.clone(),
                url,
                &d.source,
                tables,
            ));
        }
        out
    }
}

fn make_dashboard(
    id: String,
    name: String,
    description: Option<String>,
    url: Option<&str>,
    source: &str,
    tables: BTreeSet<String>,
) -> Dashboard {
    let url = match url {
        Some(u) => u.replace("{name}", &id),
        None => source.to_string(),
    };
    Dashboard {
        id,
        name,
        url,
        description,
        // Exports carry no owners; dbt requires one.
        owner_name: Some("superset".into()),
        owner_email: None,
        tables,
    }
}

/// Non-empty string field (`null`, `''` → `None`).
fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn qualified(catalog: Option<&str>, schema: Option<&str>, table: &str) -> String {
    [catalog, schema, Some(table)]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

/// Tables read by a virtual dataset's SQL. Jinja expressions become a
/// literal and blocks/comments are dropped, so templated SQL still parses.
pub fn virtual_tables(sql: &str) -> BTreeSet<String> {
    let sql = JINJA_BLOCK.replace_all(sql, " ");
    let sql = JINJA_EXPR.replace_all(&sql, "1");
    super::anofox::dashboard_tables(&sql)
}

/// Chart uuids of `CHART` entries in a JSON position layout, in key order.
fn position_charts_json(v: &serde_json::Value) -> Vec<String> {
    let Some(m) = v.as_object() else {
        return Vec::new();
    };
    let sorted: BTreeMap<&String, &serde_json::Value> = m.iter().collect();
    sorted
        .values()
        .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("CHART"))
        .filter_map(|item| item.pointer("/meta/uuid").and_then(|u| u.as_str()))
        .map(String::from)
        .collect()
}

fn is_legacy_export(v: &serde_json::Value) -> bool {
    v.get("dashboards")
        .and_then(|d| d.as_array())
        .is_some_and(|d| d.iter().any(|x| x.get("__Dashboard__").is_some()))
}

/// Dashboards from the legacy JSON export: each `__Dashboard__` embeds its
/// `__Slice__`s, which name their dataset (`datasource_name`, `schema` in
/// `params`); `datasources` holds the `__SqlaTable__`s with their SQL.
fn legacy_dashboards(
    v: &serde_json::Value,
    source: &str,
    url: Option<&str>,
    notes: &mut Vec<String>,
) -> Vec<Dashboard> {
    let s = |v: &serde_json::Value, k: &str| {
        v.get(k)
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(String::from)
    };
    // (schema, table_name) → tables, plus table_name alone for lookups
    // without a schema.
    let mut by_name: HashMap<(Option<String>, String), BTreeSet<String>> = HashMap::new();
    for ds in v
        .get("datasources")
        .and_then(|d| d.as_array())
        .into_iter()
        .flatten()
    {
        let Some(t) = ds.get("__SqlaTable__") else {
            continue;
        };
        let Some(table) = s(t, "table_name") else {
            continue;
        };
        let schema = s(t, "schema");
        let tables = match s(t, "sql") {
            Some(sql) => virtual_tables(&sql),
            None => BTreeSet::from([qualified(None, schema.as_deref(), &table)]),
        };
        by_name.insert((schema, table), tables);
    }

    let mut out = Vec::new();
    for d in v
        .get("dashboards")
        .and_then(|d| d.as_array())
        .into_iter()
        .flatten()
    {
        let Some(d) = d.get("__Dashboard__") else {
            continue;
        };
        let title = s(d, "dashboard_title").unwrap_or_else(|| "dashboard".into());
        let mut tables = BTreeSet::new();
        for slice in d
            .get("slices")
            .and_then(|x| x.as_array())
            .into_iter()
            .flatten()
        {
            let Some(slice) = slice.get("__Slice__") else {
                continue;
            };
            let params: serde_json::Value = s(slice, "params")
                .and_then(|p| serde_json::from_str(&p).ok())
                .unwrap_or_default();
            let Some(table) = s(slice, "datasource_name").or_else(|| s(&params, "datasource_name"))
            else {
                continue;
            };
            let schema = s(&params, "schema");
            let found = by_name.get(&(schema.clone(), table.clone())).or_else(|| {
                let mut same = by_name.iter().filter(|((_, t), _)| *t == table);
                match (same.next(), same.next()) {
                    (Some((_, v)), None) => Some(v),
                    _ => None,
                }
            });
            match found {
                Some(t) => tables.extend(t.iter().cloned()),
                None => {
                    notes.push(format!(
                        "dashboard '{title}': dataset '{table}' is not in the export; using its name"
                    ));
                    tables.insert(qualified(None, schema.as_deref(), &table));
                }
            }
        }
        let slug = s(d, "slug");
        let id = slug.unwrap_or_else(|| {
            d.get("id")
                .map(|i| i.to_string().trim_matches('"').to_string())
                .filter(|i| i != "null")
                .unwrap_or_else(|| super::slugify(&title))
        });
        out.push(make_dashboard(
            id,
            title,
            s(d, "description"),
            url,
            source,
            tables,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const DASHBOARD: &str = r#"dashboard_title: Sales Overview
description: Weekly sales KPIs
css: ''
slug: sales
uuid: 11111111-1111-1111-1111-111111111111
position:
  CHART-abc:
    children: []
    id: CHART-abc
    meta:
      chartId: 1
      sliceName: Revenue
      uuid: aaaaaaaa-0000-0000-0000-000000000001
    type: CHART
  CHART-def:
    children: []
    id: CHART-def
    meta:
      chartId: 2
      sliceName: Payments
      uuid: aaaaaaaa-0000-0000-0000-000000000002
    type: CHART
  CHART-ghost:
    id: CHART-ghost
    meta:
      uuid: aaaaaaaa-0000-0000-0000-00000000dead
    type: CHART
  DASHBOARD_VERSION_KEY: v2
  HEADER_ID:
    id: HEADER_ID
    meta:
      text: Sales Overview
    type: HEADER
metadata:
  color_scheme: ''
version: 1.0.0
"#;

    const CHART_1: &str = "slice_name: Revenue\nviz_type: big_number\nparams: {}\n\
        uuid: aaaaaaaa-0000-0000-0000-000000000001\n\
        dataset_uuid: dddddddd-0000-0000-0000-000000000001\nversion: 1.0.0\n";
    const CHART_2: &str = "slice_name: Payments\nviz_type: table\n\
        uuid: aaaaaaaa-0000-0000-0000-000000000002\n\
        dataset_uuid: dddddddd-0000-0000-0000-000000000002\nversion: 1.0.0\n";
    const DATASET_PHYSICAL: &str = "table_name: fct_orders\nschema: analytics\nsql: null\n\
        uuid: dddddddd-0000-0000-0000-000000000001\n\
        database_uuid: 99999999-0000-0000-0000-000000000000\nversion: 1.0.0\n";
    const DATASET_VIRTUAL: &str = r#"table_name: payments_last_30d
schema: analytics
sql: |-
  {# recent payments #}
  SELECT p.*, c.region
  FROM analytics.stg_payments p
  JOIN {{ params.customers_table | default('dim_customers') }} c ON c.id = p.customer_id
  {% if filter_values('region') %}
  WHERE c.region IN {{ filter_values('region') | where_in }}
  {% endif %}
uuid: dddddddd-0000-0000-0000-000000000002
database_uuid: 99999999-0000-0000-0000-000000000000
version: 1.0.0
"#;
    const DATABASE: &str = "database_name: warehouse\nsqlalchemy_uri: postgresql://x\n\
        uuid: 99999999-0000-0000-0000-000000000000\nversion: 1.0.0\n";
    const METADATA: &str =
        "version: 1.0.0\ntype: Dashboard\ntimestamp: '2024-05-01T00:00:00+00:00'\n";

    fn write_zip(path: &Path) {
        let f = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(f);
        let root = "dashboard_export_20240501T000000";
        for (name, body) in [
            ("metadata.yaml", METADATA),
            ("dashboards/Sales_Overview_1.yaml", DASHBOARD),
            ("charts/Revenue_1.yaml", CHART_1),
            ("charts/Payments_2.yaml", CHART_2),
            ("datasets/warehouse/fct_orders.yaml", DATASET_PHYSICAL),
            ("datasets/warehouse/payments_last_30d.yaml", DATASET_VIRTUAL),
            ("databases/warehouse.yaml", DATABASE),
        ] {
            zip.start_file(
                format!("{root}/{name}"),
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
            zip.write_all(body.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    #[test]
    fn virtual_dataset_sql_with_jinja() {
        let t: Vec<_> = virtual_tables(
            DATASET_VIRTUAL
                .split_once("sql: |-\n")
                .unwrap()
                .1
                .split("\nuuid:")
                .next()
                .unwrap(),
        )
        .into_iter()
        .collect();
        assert_eq!(t, vec!["analytics.stg_payments"]);
        let t: Vec<_> = virtual_tables("select * from {{ ref_table }} join marts.dim_x using (id)")
            .into_iter()
            .collect();
        assert_eq!(t, vec!["marts.dim_x"]);
    }

    #[test]
    fn reads_export_zip() {
        let dir = tempfile::tempdir().unwrap();
        let zip = dir.path().join("export.zip");
        write_zip(&zip);
        let (ds, notes) =
            dashboards_from_paths(&[zip], Some("https://bi/superset/dashboard/{name}/")).unwrap();
        assert_eq!(ds.len(), 1);
        let d = &ds[0];
        assert_eq!(d.name, "Sales Overview");
        assert_eq!(d.id, "sales");
        assert_eq!(d.url, "https://bi/superset/dashboard/sales/");
        assert_eq!(d.description.as_deref(), Some("Weekly sales KPIs"));
        assert_eq!(d.owner_name.as_deref(), Some("superset"));
        assert_eq!(
            d.tables.iter().collect::<Vec<_>>(),
            vec!["analytics.fct_orders", "analytics.stg_payments"]
        );
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("is not in the export"));
    }

    #[test]
    fn reads_unpacked_directory_and_json_position() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("export");
        for (name, body) in [
            ("charts/Revenue_1.yaml", CHART_1.to_string()),
            ("datasets/wh/fct_orders.yaml", DATASET_PHYSICAL.to_string()),
            (
                "dashboards/KPIs.yaml",
                // No slug: `{name}` falls back to the uuid; position as JSON.
                "dashboard_title: KPIs\nslug: null\nuuid: 22222222-2222-2222-2222-222222222222\n\
                 position: '{\"CHART-x\": {\"type\": \"CHART\", \"meta\": {\"uuid\": \"aaaaaaaa-0000-0000-0000-000000000001\"}}}'\n"
                    .to_string(),
            ),
            ("README.md", "not yaml".to_string()),
        ] {
            let p = root.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, body).unwrap();
        }
        let (ds, notes) = dashboards_from_paths(&[root], Some("{name}")).unwrap();
        assert!(notes.is_empty(), "{notes:?}");
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].url, "22222222-2222-2222-2222-222222222222");
        assert_eq!(
            ds[0].tables.iter().collect::<Vec<_>>(),
            vec!["analytics.fct_orders"]
        );
    }

    #[test]
    fn reads_legacy_json_export() {
        let json = r#"{
          "dashboards": [{"__Dashboard__": {
            "dashboard_title": "Births 2", "slug": null, "id": 7,
            "position_json": "{}",
            "slices": [
              {"__Slice__": {"slice_name": "CA births", "datasource_name": "birth_names",
                "params": "{\"datasource_name\": \"birth_names\", \"schema\": \"public\", \"database_name\": \"examples\"}"}},
              {"__Slice__": {"slice_name": "Virtual", "datasource_name": "recent",
                "params": "{\"schema\": null}"}},
              {"__Slice__": {"slice_name": "Missing", "datasource_name": "ghost",
                "params": "{\"schema\": \"raw\"}"}}
            ]}}],
          "datasources": [
            {"__SqlaTable__": {"table_name": "birth_names", "schema": "public", "sql": null}},
            {"__SqlaTable__": {"table_name": "recent", "schema": null,
              "sql": "select * from marts.fct_births where ds > '{{ from_dttm }}'"}}
          ]
        }"#;
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("dashboards.json");
        std::fs::write(&f, json).unwrap();
        let (ds, notes) = dashboards_from_paths(&[f], Some("https://bi/d/{name}")).unwrap();
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].url, "https://bi/d/7");
        assert_eq!(
            ds[0].tables.iter().collect::<Vec<_>>(),
            vec!["marts.fct_births", "public.birth_names", "raw.ghost"]
        );
        assert_eq!(notes.len(), 1, "{notes:?}");
    }
}
