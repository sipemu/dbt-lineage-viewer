//! Import BI dashboards as dbt exposures (`dbt-lineage exposures <tool>`).
//!
//! A BI connector lists dashboards with the warehouse tables they read; this
//! module maps those tables onto the project's models and sources and renders
//! dbt `exposures:` YAML. Connectors: Metabase (REST API),
//! anofox-visualization (dashboard `.sql` files), Tableau (workbook files),
//! Looker (LookML project files), Superset (dashboard exports) and Power BI
//! (PBIP projects, `.pbit` templates); all but Metabase work offline.

pub mod anofox;
pub mod looker;
pub mod metabase;
pub mod powerbi;
pub mod superset;
pub mod tableau;

use std::collections::{BTreeSet, HashMap};

use serde::Serialize;

use crate::graph::types::{LineageGraph, NodeType};

/// A dashboard as reported by a BI tool, before mapping to dbt nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dashboard {
    pub id: String,
    pub name: String,
    pub url: String,
    pub description: Option<String>,
    pub owner_name: Option<String>,
    pub owner_email: Option<String>,
    /// Physical table names read by the dashboard's questions, as written
    /// (`orders`, `analytics.orders`, `"db"."schema"."orders"`).
    pub tables: BTreeSet<String>,
    /// Files read directly (`read_parquet('…/orders.parquet')`); only
    /// anofox-visualization reports these.
    pub files: BTreeSet<FileRead>,
}

/// A file a dashboard reads, e.g. `read_parquet(getvariable('d') || '/x.parquet')`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileRead {
    /// The call as written, for messages: `read_parquet('data/x.parquet')`.
    pub call: String,
    /// The model, seed, snapshot or source table the file is named after
    /// (`x`); `None` for globs and paths that aren't literal.
    pub name: Option<String>,
}

/// dbt `exposures:` YAML document.
#[derive(Debug, Serialize)]
pub struct ExposuresFile {
    pub version: u8,
    pub exposures: Vec<Exposure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Exposure {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub url: String,
    pub description: String,
    pub owner: Owner,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Owner {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Result of mapping dashboards onto the project graph.
#[derive(Debug, Default)]
pub struct MappedExposures {
    pub exposures: Vec<Exposure>,
    /// `(dashboard name, table)` pairs with no matching model or source.
    pub unmatched: Vec<(String, String)>,
    /// `(dashboard name, file read)` pairs whose file names no model or source.
    pub unmatched_files: Vec<(String, String)>,
    /// Dashboards skipped because none of their tables matched.
    pub skipped: Vec<String>,
}

/// Lookup from a lower-cased table name to the dbt reference that builds it.
struct Relations {
    by_name: HashMap<String, String>,
}

impl Relations {
    fn from_graph(graph: &LineageGraph) -> Self {
        let mut by_name = HashMap::new();
        // Sources first, so a model with the same name wins below: dashboards
        // usually read the transformed model, not the raw table.
        for idx in graph.node_indices() {
            let n = &graph[idx];
            if n.node_type == NodeType::Source {
                if let Some((source, table)) = n.label.split_once('.') {
                    by_name.insert(
                        table.to_lowercase(),
                        format!("source('{source}', '{table}')"),
                    );
                }
            }
        }
        for idx in graph.node_indices() {
            let n = &graph[idx];
            if matches!(
                n.node_type,
                NodeType::Model | NodeType::Seed | NodeType::Snapshot
            ) {
                by_name.insert(n.label.to_lowercase(), format!("ref('{}')", n.label));
            }
        }
        Relations { by_name }
    }

    /// Match on the last identifier part, ignoring quotes and case.
    fn lookup(&self, table: &str) -> Option<&String> {
        let last = table
            .rsplit('.')
            .next()
            .unwrap_or(table)
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_lowercase();
        self.by_name.get(&last)
    }
}

/// Lower-case slug usable as an exposure name.
pub fn slugify(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        format!("dashboard_{out}")
    } else {
        out
    }
}

/// Map dashboards onto the graph. Output is sorted by exposure name;
/// colliding names get the dashboard id appended.
pub fn map_dashboards(graph: &LineageGraph, dashboards: &[Dashboard]) -> MappedExposures {
    let relations = Relations::from_graph(graph);
    let mut result = MappedExposures::default();
    let mut used_names: HashMap<String, usize> = HashMap::new();
    for d in dashboards {
        *used_names.entry(slugify(&d.name)).or_default() += 1;
    }
    // Names already emitted: dbt rejects two exposures with one name, which
    // dashboards sharing both name and id (the same workbook in two folders)
    // would otherwise produce.
    let mut emitted: std::collections::HashSet<String> = std::collections::HashSet::new();

    for d in dashboards {
        let mut deps = BTreeSet::new();
        for table in &d.tables {
            match relations.lookup(table) {
                Some(dep) => {
                    deps.insert(dep.clone());
                }
                None => result.unmatched.push((d.name.clone(), table.clone())),
            }
        }
        for file in &d.files {
            match file.name.as_deref().and_then(|n| relations.lookup(n)) {
                Some(dep) => {
                    deps.insert(dep.clone());
                }
                None => result
                    .unmatched_files
                    .push((d.name.clone(), file.call.clone())),
            }
        }
        if deps.is_empty() {
            result.skipped.push(d.name.clone());
            continue;
        }
        let base = slugify(&d.name);
        let mut name = if used_names.get(&base).copied().unwrap_or(0) > 1 {
            format!("{base}_{}", slugify(&d.id).trim_start_matches("dashboard_"))
        } else {
            base
        };
        if emitted.contains(&name) {
            let stem = name.clone();
            name = (2..)
                .map(|n| format!("{stem}_{n}"))
                .find(|n| !emitted.contains(n))
                .unwrap();
        }
        emitted.insert(name.clone());
        let owner = if d.owner_name.is_none() && d.owner_email.is_none() {
            // dbt requires an owner with a name or email.
            Owner {
                name: Some("Metabase".into()),
                email: None,
            }
        } else {
            Owner {
                name: d.owner_name.clone(),
                email: d.owner_email.clone(),
            }
        };
        result.exposures.push(Exposure {
            name,
            label: d.name.clone(),
            kind: "dashboard".into(),
            url: d.url.clone(),
            description: d
                .description
                .clone()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("Dashboard \"{}\", imported by dbt-lineage.", d.name)),
            owner,
            depends_on: deps.into_iter().collect(),
        });
    }
    result.exposures.sort_by(|a, b| a.name.cmp(&b.name));
    result
}

/// Render dbt exposures YAML.
pub fn to_yaml(exposures: &[Exposure]) -> String {
    let doc = ExposuresFile {
        version: 2,
        exposures: exposures.to_vec(),
    };
    let body = serde_yaml::to_string(&doc).unwrap_or_default();
    format!("# Generated by `dbt-lineage exposures`; re-run to refresh.\n{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::NodeData;

    fn node(id: &str, label: &str, node_type: NodeType) -> NodeData {
        NodeData {
            unique_id: id.into(),
            label: label.into(),
            node_type,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        }
    }

    fn graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        g.add_node(node("model.orders", "orders", NodeType::Model));
        g.add_node(node("model.customers", "customers", NodeType::Model));
        g.add_node(node(
            "source.raw.payments",
            "raw.payments",
            NodeType::Source,
        ));
        g.add_node(node("source.raw.orders", "raw.orders", NodeType::Source));
        g
    }

    fn dash(id: &str, name: &str, tables: &[&str]) -> Dashboard {
        Dashboard {
            id: id.into(),
            name: name.into(),
            url: format!("https://mb/dashboard/{id}"),
            description: None,
            owner_name: None,
            owner_email: Some("ana@example.com".into()),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            files: BTreeSet::new(),
        }
    }

    #[test]
    fn slugify_names() {
        assert_eq!(slugify("Revenue — Weekly (EU)"), "revenue_weekly_eu");
        assert_eq!(slugify("2024 KPIs"), "dashboard_2024_kpis");
        assert_eq!(slugify("!!!"), "dashboard_");
    }

    #[test]
    fn maps_models_sources_and_reports_unmatched() {
        let r = map_dashboards(
            &graph(),
            &[
                dash(
                    "1",
                    "Sales",
                    &["analytics.ORDERS", "\"raw\".\"payments\"", "ghost"],
                ),
                dash("2", "Nothing known", &["other_table"]),
            ],
        );
        assert_eq!(r.exposures.len(), 1);
        let e = &r.exposures[0];
        assert_eq!(e.name, "sales");
        // Model wins over the source with the same table name.
        assert_eq!(
            e.depends_on,
            vec!["ref('orders')", "source('raw', 'payments')"]
        );
        assert_eq!(e.owner.email.as_deref(), Some("ana@example.com"));
        assert_eq!(
            r.unmatched,
            vec![
                ("Sales".to_string(), "ghost".to_string()),
                ("Nothing known".to_string(), "other_table".to_string())
            ]
        );
        assert_eq!(r.skipped, vec!["Nothing known".to_string()]);
    }

    #[test]
    fn maps_file_reads_and_reports_unmatched_files() {
        let file = |call: &str, name: Option<&str>| FileRead {
            call: call.into(),
            name: name.map(String::from),
        };
        let mut d = dash("1", "Files", &[]);
        d.files = [
            file("read_parquet('data/Orders.parquet')", Some("Orders")),
            file("read_csv('payments.csv')", Some("payments")),
            file("read_parquet('x/*.parquet')", None),
            file("read_json('ghost.json')", Some("ghost")),
        ]
        .into_iter()
        .collect();
        let r = map_dashboards(&graph(), &[d]);
        assert_eq!(
            r.exposures[0].depends_on,
            vec!["ref('orders')", "source('raw', 'payments')"]
        );
        assert!(r.unmatched.is_empty());
        assert_eq!(
            r.unmatched_files,
            vec![
                ("Files".to_string(), "read_json('ghost.json')".to_string()),
                (
                    "Files".to_string(),
                    "read_parquet('x/*.parquet')".to_string()
                ),
            ]
        );
        // A dashboard whose only reads are unmatched files is skipped.
        let mut d = dash("2", "Glob only", &[]);
        d.files.insert(file("read_parquet('x/*.parquet')", None));
        let r = map_dashboards(&graph(), &[d]);
        assert_eq!(r.skipped, vec!["Glob only".to_string()]);
    }

    #[test]
    fn colliding_names_get_ids_and_owner_placeholder() {
        let mut a = dash("7", "KPIs", &["orders"]);
        a.owner_email = None;
        let b = dash("9", "KPIs", &["customers"]);
        let r = map_dashboards(&graph(), &[b, a]);
        let names: Vec<_> = r.exposures.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["kpis_7", "kpis_9"]);
        assert_eq!(r.exposures[0].owner.name.as_deref(), Some("Metabase"));
    }

    #[test]
    fn dashboards_sharing_name_and_id_get_unique_names() {
        // The same Tableau workbook in two folders: same name, same id.
        let a = dash("filtering: Dash", "filtering: Dash", &["orders"]);
        let b = dash("filtering: Dash", "filtering: Dash", &["orders"]);
        let r = map_dashboards(&graph(), &[a, b]);
        let mut names: Vec<_> = r.exposures.iter().map(|e| e.name.clone()).collect();
        names.dedup();
        assert_eq!(names.len(), 2, "{names:?}");
    }

    #[test]
    fn yaml_is_valid_dbt_shape() {
        let r = map_dashboards(&graph(), &[dash("1", "Sales", &["orders"])]);
        let yaml = to_yaml(&r.exposures);
        let v: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(v["version"], serde_yaml::Value::from(2));
        let e = &v["exposures"][0];
        assert_eq!(e["name"], serde_yaml::Value::from("sales"));
        assert_eq!(e["type"], serde_yaml::Value::from("dashboard"));
        assert_eq!(e["depends_on"][0], serde_yaml::Value::from("ref('orders')"));
        assert!(e["owner"].get("name").is_none());
    }
}
