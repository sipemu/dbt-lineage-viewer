//! Tableau workbooks (`.twb`, `.twbx`), read offline.
//!
//! A workbook's `<datasources>` hold connections and relations: database
//! tables (`<relation type="table" table="[schema].[name]">`, also nested in
//! joins) and custom SQL (`type="text"`). Each `<dashboard>` shows worksheets
//! (`<zone name=…>`), and each worksheet declares the datasources it uses
//! (`<datasource-dependencies datasource=…>`). A dashboard's tables are the
//! tables of the datasources behind its worksheets.
//!
//! Extracts and file sources (`.hyper`, Excel, CSV, …) are not warehouse
//! tables and are skipped, as are published Server datasources (`sqlproxy`),
//! whose tables the workbook doesn't record; both are reported as notes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result};
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use regex::Regex;

use super::Dashboard;

/// Connection classes that read files or extracts rather than a database.
const FILE_CLASSES: &[&str] = &[
    "hyper",
    "dataengine",
    "excel",
    "excel-direct",
    "textscan",
    "csv",
    "ogrdirect",
    "json",
    "pdf",
    "statistical-file",
    "google-sheets",
    "box",
    "dropbox",
    "onedrive",
];

static TABLEAU_PARAM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<\[Parameters\]\.\[[^\]]*\]>").unwrap());

/// Dashboards from workbook files and directories (searched recursively for
/// `.twb` / `.twbx`). `url` is the exposure URL; `{name}` in it is replaced
/// by the workbook file name without extension. Returns the dashboards and
/// notes about skipped sources.
pub fn dashboards_from_paths(
    paths: &[PathBuf],
    url: Option<&str>,
) -> Result<(Vec<Dashboard>, Vec<String>)> {
    let is_workbook = |p: &Path| {
        p.extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("twb") || e.eq_ignore_ascii_case("twbx"))
    };
    let mut files = Vec::new();
    for p in paths {
        if p.is_dir() {
            for entry in walkdir::WalkDir::new(p).sort_by_file_name() {
                let entry = entry?;
                if entry.file_type().is_file() && is_workbook(entry.path()) {
                    files.push(entry.into_path());
                }
            }
        } else {
            files.push(p.clone());
        }
    }
    let (mut dashboards, mut notes) = (Vec::new(), Vec::new());
    for file in files {
        let xml = read_workbook(&file)?;
        let stem = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workbook")
            .to_string();
        let link = match url {
            Some(u) => u.replace("{name}", &stem),
            None => file.to_string_lossy().replace('\\', "/"),
        };
        match parse_workbook(&xml) {
            Ok(wb) => {
                for note in &wb.notes {
                    notes.push(format!("{}: {}", file.display(), note));
                }
                dashboards.extend(wb.dashboards(&stem, &link));
            }
            Err(e) => notes.push(format!(
                "{}: not a readable workbook ({e:#})",
                file.display()
            )),
        }
    }
    Ok((dashboards, notes))
}

/// Workbook XML from a `.twb`, or the `.twb` inside a `.twbx` package.
fn read_workbook(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    let text = if bytes.starts_with(b"PK") {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(bytes))
            .with_context(|| format!("{} is not a valid .twbx", path.display()))?;
        // The workbook sits at the package root; prefer the shallowest .twb.
        let name = zip
            .file_names()
            .filter(|n| n.to_lowercase().ends_with(".twb"))
            .min_by_key(|n| n.matches('/').count())
            .map(String::from)
            .with_context(|| format!("no .twb inside {}", path.display()))?;
        let mut s = String::new();
        zip.by_name(&name)?.read_to_string(&mut s)?;
        s
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };
    Ok(text)
}

/// What a workbook declares, before building dashboards.
#[derive(Debug, Default)]
pub struct Workbook {
    /// Datasource name → (caption, tables).
    pub datasources: BTreeMap<String, (Option<String>, BTreeSet<String>)>,
    /// Worksheet name → datasource names it uses.
    pub worksheets: BTreeMap<String, BTreeSet<String>>,
    /// Dashboard name → zone names (worksheets and other objects).
    pub dashboards: BTreeMap<String, BTreeSet<String>>,
    pub notes: Vec<String>,
}

impl Workbook {
    /// One dashboard per Tableau dashboard; a workbook without dashboards
    /// becomes one dashboard covering its worksheets (or all datasources).
    pub fn dashboards(&self, stem: &str, url: &str) -> Vec<Dashboard> {
        let tables_of = |sources: &mut dyn Iterator<Item = &String>| -> BTreeSet<String> {
            sources
                .filter_map(|ds| self.datasources.get(ds))
                .flat_map(|(_, t)| t.iter().cloned())
                .collect()
        };
        let make = |id: String, name: String, tables: BTreeSet<String>| Dashboard {
            id,
            name,
            url: url.to_string(),
            description: None,
            owner_name: Some("tableau".into()),
            owner_email: None,
            tables,
        };
        if self.dashboards.is_empty() {
            let tables = if self.worksheets.is_empty() {
                tables_of(&mut self.datasources.keys())
            } else {
                tables_of(&mut self.worksheets.values().flatten())
            };
            return vec![make(stem.to_string(), stem.to_string(), tables)];
        }
        self.dashboards
            .iter()
            .map(|(name, zones)| {
                let tables = tables_of(
                    &mut zones
                        .iter()
                        .filter_map(|z| self.worksheets.get(z))
                        .flatten(),
                );
                make(format!("{stem}/{name}"), format!("{stem}: {name}"), tables)
            })
            .collect()
    }
}

/// One relation seen while parsing a datasource.
struct Relation {
    kind: String,
    table: Option<String>,
    connection: Option<String>,
    sql: String,
    in_extract: bool,
}

/// Parse workbook XML (anything before the root element is ignored).
pub fn parse_workbook(xml: &str) -> Result<Workbook> {
    let start = xml
        .find("<?xml")
        .or_else(|| xml.find("<workbook"))
        .context("no <workbook> element")?;
    let mut reader = Reader::from_str(&xml[start..]);
    let mut wb = Workbook::default();

    let mut stack: Vec<String> = Vec::new();
    // Current top-level datasource being read.
    let mut ds: Option<String> = None;
    let mut ds_caption: Option<String> = None;
    let mut ds_class: Option<String> = None;
    let mut named: HashMap<String, String> = HashMap::new(); // named-connection → class
    let mut pending_named: Option<String> = None;
    let mut relations: Vec<Relation> = Vec::new();
    let mut extract_depth: Option<usize> = None;
    let mut open_text: Option<usize> = None; // index into relations of a type=text relation
    let mut worksheet: Option<String> = None;
    let mut dashboard: Option<String> = None;

    loop {
        let event = reader.read_event().context("invalid workbook XML")?;
        let (elem, empty) = match &event {
            Event::Start(e) => (Some(e.clone()), false),
            Event::Empty(e) => (Some(e.clone()), true),
            _ => (None, false),
        };
        if let Some(e) = elem {
            let tag = String::from_utf8_lossy(e.name().as_ref()).into_owned();
            let path_is = |p: &[&str]| {
                stack.len() == p.len() - 1
                    && stack.iter().zip(p).all(|(a, b)| a == b)
                    && tag == *p.last().unwrap()
            };
            if path_is(&["workbook", "datasources", "datasource"]) {
                ds = attr(&e, "name");
                ds_caption = attr(&e, "caption");
                ds_class = None;
                named.clear();
                relations.clear();
            } else if path_is(&["workbook", "worksheets", "worksheet"]) {
                worksheet = attr(&e, "name");
            } else if path_is(&["workbook", "dashboards", "dashboard"]) {
                dashboard = attr(&e, "name");
                if let Some(d) = &dashboard {
                    wb.dashboards.entry(d.clone()).or_default();
                }
            }
            if ds.is_some() {
                match tag.as_str() {
                    "extract" => extract_depth = extract_depth.or(Some(stack.len())),
                    "named-connection" => pending_named = attr(&e, "name"),
                    "connection" => {
                        let class = attr(&e, "class");
                        if let (Some(n), Some(c)) = (pending_named.take(), class.clone()) {
                            named.insert(n, c);
                        } else if ds_class.is_none() && extract_depth.is_none() {
                            ds_class = class;
                        }
                    }
                    "relation" => {
                        let kind = attr(&e, "type").unwrap_or_default();
                        if kind == "table" || kind == "text" {
                            relations.push(Relation {
                                kind: kind.clone(),
                                table: attr(&e, "table"),
                                connection: attr(&e, "connection"),
                                sql: String::new(),
                                in_extract: extract_depth.is_some(),
                            });
                            if kind == "text" && !empty {
                                open_text = Some(relations.len() - 1);
                            }
                        }
                    }
                    _ => {}
                }
            }
            if let Some(w) = &worksheet {
                if tag == "datasource-dependencies" {
                    if let Some(d) = attr(&e, "datasource") {
                        wb.worksheets.entry(w.clone()).or_default().insert(d);
                    }
                }
                wb.worksheets.entry(w.clone()).or_default();
            }
            if let (Some(d), "zone") = (&dashboard, tag.as_str()) {
                if let Some(z) = attr(&e, "name") {
                    wb.dashboards.entry(d.clone()).or_default().insert(z);
                }
            }
            if !empty {
                stack.push(tag);
            }
            continue;
        }
        match event {
            Event::Text(t) => {
                if let Some(i) = open_text {
                    relations[i].sql.push_str(&t.unescape().unwrap_or_default());
                }
            }
            Event::CData(c) => {
                if let Some(i) = open_text {
                    relations[i]
                        .sql
                        .push_str(&String::from_utf8_lossy(&c.into_inner()));
                }
            }
            Event::End(_) => {
                let tag = stack.pop().unwrap_or_default();
                match tag.as_str() {
                    "relation" => open_text = None,
                    "extract" if extract_depth == Some(stack.len()) => extract_depth = None,
                    "datasource" if stack.len() == 2 && ds.is_some() => {
                        let name = ds.take().unwrap();
                        let tables = resolve_tables(
                            &name,
                            ds_caption.as_deref(),
                            ds_class.as_deref(),
                            &named,
                            &relations,
                            &mut wb.notes,
                        );
                        wb.datasources.insert(name, (ds_caption.take(), tables));
                    }
                    "worksheet" if stack.len() == 2 => worksheet = None,
                    "dashboard" if stack.len() == 2 => dashboard = None,
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    // `Parameters` is Tableau's pseudo-datasource for parameters.
    wb.datasources.remove("Parameters");
    Ok(wb)
}

fn resolve_tables(
    name: &str,
    caption: Option<&str>,
    ds_class: Option<&str>,
    named: &HashMap<String, String>,
    relations: &[Relation],
    notes: &mut Vec<String>,
) -> BTreeSet<String> {
    let label = caption.unwrap_or(name);
    let mut tables = BTreeSet::new();
    let (mut skipped_files, mut published) = (false, false);
    for r in relations.iter().filter(|r| !r.in_extract) {
        let class = r
            .connection
            .as_ref()
            .and_then(|c| named.get(c))
            .map(String::as_str)
            .or_else(|| {
                // Without an explicit connection: the datasource's own class,
                // or the only database connection of a federated source.
                match ds_class {
                    Some("federated") | None => {
                        let db: Vec<&String> = named
                            .values()
                            .filter(|c| !FILE_CLASSES.contains(&c.as_str()))
                            .collect();
                        (db.len() == 1).then(|| db[0].as_str())
                    }
                    other => other,
                }
            });
        if r.table.as_deref() == Some("[Extract].[Extract]") {
            continue;
        }
        if class == Some("sqlproxy") || r.table.as_deref() == Some("[sqlproxy]") {
            published = true;
            continue;
        }
        if class.is_none_or(|c| FILE_CLASSES.contains(&c)) {
            skipped_files = true;
            continue;
        }
        match r.kind.as_str() {
            "table" => {
                if let Some(t) = &r.table {
                    tables.insert(t.replace(['[', ']'], ""));
                }
            }
            _ => {
                let sql = TABLEAU_PARAM.replace_all(&r.sql, "1");
                tables.extend(
                    super::anofox::dashboard_tables(&sql)
                        .into_iter()
                        .map(|t| t.replace(['[', ']'], "")),
                );
            }
        }
    }
    if published {
        notes.push(format!(
            "datasource '{label}' is a published Tableau Server datasource; its tables are not in the workbook"
        ));
    }
    if skipped_files {
        notes.push(format!(
            "datasource '{label}': file or extract sources skipped"
        ));
    }
    tables
}

fn attr(e: &BytesStart, key: &str) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| a.key.as_ref() == key.as_bytes())
        .and_then(|a| a.unescape_value().ok())
        .map(|v| v.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKBOOK: &str = r#"<?xml version='1.0' encoding='utf-8' ?>
<workbook version='18.1'>
  <datasources>
    <datasource name='Parameters' hasconnection='false' inline='true' />
    <datasource caption='Orders + Customers' inline='true' name='federated.abc'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='acme.snowflakecomputing.com' name='snowflake.1'>
            <connection class='snowflake' dbname='ANALYTICS' schema='MARTS' />
          </named-connection>
          <named-connection caption='extract' name='hyper.2'>
            <connection class='hyper' dbname='orders.hyper' />
          </named-connection>
        </named-connections>
        <relation join='inner' type='join'>
          <clause type='join'><expression op='=' /></clause>
          <relation connection='snowflake.1' name='FCT_ORDERS' table='[MARTS].[FCT_ORDERS]' type='table' />
          <relation connection='snowflake.1' name='DIM_CUSTOMERS' table='[MARTS].[DIM_CUSTOMERS]' type='table' />
        </relation>
      </connection>
      <extract enabled='true'>
        <connection class='hyper'>
          <relation name='Extract' table='[Extract].[Extract]' type='table' />
        </connection>
      </extract>
    </datasource>
    <datasource caption='Payments (SQL)' inline='true' name='federated.def'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='pg' name='postgres.3'>
            <connection class='postgres' dbname='prod' />
          </named-connection>
        </named-connections>
        <relation connection='postgres.3' name='Custom SQL Query' type='text'><![CDATA[select * from analytics.stg_payments p
where p.amount > <[Parameters].[Min Amount]>]]></relation>
      </connection>
    </datasource>
    <datasource caption='Targets' inline='true' name='federated.ghi'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='targets.xlsx' name='excel-direct.4'>
            <connection class='excel-direct' filename='targets.xlsx' />
          </named-connection>
        </named-connections>
        <relation connection='excel-direct.4' name='Sheet1' table='[Sheet1$]' type='table' />
      </connection>
    </datasource>
    <datasource caption='Shared' inline='true' name='sqlproxy.jkl'>
      <connection class='sqlproxy' dbname='Shared'>
        <relation name='sqlproxy' table='[sqlproxy]' type='table' />
      </connection>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Revenue'>
      <table><view>
        <datasources><datasource name='federated.abc' /></datasources>
        <datasource-dependencies datasource='federated.abc' />
        <datasource-dependencies datasource='Parameters' />
      </view></table>
    </worksheet>
    <worksheet name='Payments'>
      <table><view><datasource-dependencies datasource='federated.def' /></view></table>
    </worksheet>
    <worksheet name='Targets'>
      <table><view><datasource-dependencies datasource='federated.ghi' /></view></table>
    </worksheet>
  </worksheets>
  <dashboards>
    <dashboard name='Exec Summary'>
      <zones><zone id='1' type='layout-basic'><zone id='2' name='Revenue' /><zone id='3' name='Targets' /></zone></zones>
    </dashboard>
    <dashboard name='Finance'>
      <zones><zone id='4' name='Payments' /><zone id='5' name='Revenue' /></zones>
    </dashboard>
  </dashboards>
</workbook>
"#;

    #[test]
    fn datasources_worksheets_and_dashboards() {
        let wb = parse_workbook(WORKBOOK).unwrap();
        let tables = |ds: &str| wb.datasources[ds].1.iter().cloned().collect::<Vec<_>>();
        assert_eq!(
            tables("federated.abc"),
            vec!["MARTS.DIM_CUSTOMERS", "MARTS.FCT_ORDERS"]
        );
        assert_eq!(tables("federated.def"), vec!["analytics.stg_payments"]);
        assert!(tables("federated.ghi").is_empty());
        assert!(tables("sqlproxy.jkl").is_empty());
        assert!(!wb.datasources.contains_key("Parameters"));
        assert_eq!(wb.notes.len(), 2, "{:?}", wb.notes);
        assert!(wb
            .notes
            .iter()
            .any(|n| n.contains("'Shared' is a published")));
        assert!(wb
            .notes
            .iter()
            .any(|n| n.contains("'Targets': file or extract")));

        let ds = wb.dashboards("sales", "https://tableau/views/sales");
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].name, "sales: Exec Summary");
        assert_eq!(ds[0].owner_name.as_deref(), Some("tableau"));
        assert_eq!(
            ds[0].tables.iter().collect::<Vec<_>>(),
            vec!["MARTS.DIM_CUSTOMERS", "MARTS.FCT_ORDERS"]
        );
        assert_eq!(
            ds[1].tables.iter().collect::<Vec<_>>(),
            vec![
                "MARTS.DIM_CUSTOMERS",
                "MARTS.FCT_ORDERS",
                "analytics.stg_payments"
            ]
        );
    }

    #[test]
    fn workbook_without_dashboards_and_plain_connection() {
        // Single-connection datasource (no federation), text before the XML.
        let xml = "// exported\n<workbook><datasources>\
            <datasource caption='Orders' name='sqlserver.1'>\
            <connection class='sqlserver' dbname='shop'>\
            <relation name='Orders' table='[dbo].[Orders]' type='table' />\
            </connection></datasource></datasources>\
            <worksheets><worksheet name='Sheet 1'><table><view>\
            <datasource-dependencies datasource='sqlserver.1' /></view></table></worksheet></worksheets>\
            </workbook>";
        let wb = parse_workbook(xml).unwrap();
        let ds = wb.dashboards("orders", "orders.twb");
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].name, "orders");
        assert_eq!(ds[0].tables.iter().collect::<Vec<_>>(), vec!["dbo.Orders"]);
    }

    #[test]
    fn reads_twbx_packages_and_directories() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.twb"), WORKBOOK).unwrap();
        let f = std::fs::File::create(dir.path().join("b.twbx")).unwrap();
        let mut zip = zip::ZipWriter::new(f);
        zip.start_file(
            "Data/extract.hyper",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(b"binary").unwrap();
        zip.start_file("b.twb", zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(WORKBOOK.as_bytes()).unwrap();
        zip.finish().unwrap();
        std::fs::write(dir.path().join("notes.txt"), "x").unwrap();

        let (ds, notes) =
            dashboards_from_paths(&[dir.path().to_path_buf()], Some("https://t/{name}")).unwrap();
        let names: Vec<&str> = ds.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "a: Exec Summary",
                "a: Finance",
                "b: Exec Summary",
                "b: Finance"
            ]
        );
        assert_eq!(ds[2].url, "https://t/b");
        assert_eq!(notes.len(), 4);
    }
}
