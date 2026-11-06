//! Metabase connector: dashboards and the tables their questions read, via
//! the REST API with an API key (`x-api-key`).
//!
//! - Native SQL questions: table names parsed with sqlparser (Metabase
//!   template tags `{{x}}` and optional clauses `[[…]]` are neutralised
//!   first), falling back to a `FROM`/`JOIN` scan when the SQL doesn't parse.
//! - GUI (MBQL) questions: `source-table` and join tables resolved through
//!   `/api/table/:id`; questions built on other questions (`card__N`) are
//!   followed through `/api/card/:id`.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use sqlparser::ast::visit_relations;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::Dashboard;
use crate::ci::comment::{Http, Request};

/// Max depth when following questions built on other questions.
const MAX_CARD_DEPTH: usize = 5;

pub struct Client<'a> {
    http: &'a dyn Http,
    base: String,
    api_key: String,
    tables: HashMap<u64, Option<String>>,
}

impl<'a> Client<'a> {
    pub fn new(http: &'a dyn Http, base_url: &str, api_key: &str) -> Self {
        Client {
            http,
            base: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            tables: HashMap::new(),
        }
    }

    fn get(&self, path: &str) -> Result<Value> {
        let req = Request {
            method: "GET",
            url: format!("{}{}", self.base, path),
            headers: vec![("x-api-key".into(), self.api_key.clone())],
            body: None,
        };
        let (status, body) = self.http.send(&req)?;
        if !(200..300).contains(&status) {
            bail!("Metabase GET {path} failed with HTTP {status}: {body}");
        }
        Ok(body)
    }

    /// All non-archived dashboards with the tables their questions read.
    pub fn dashboards(&mut self) -> Result<Vec<Dashboard>> {
        let list = self.get("/api/dashboard/")?;
        let ids: Vec<u64> = list
            .as_array()
            .context("unexpected /api/dashboard/ response")?
            .iter()
            .filter(|d| !d["archived"].as_bool().unwrap_or(false))
            .filter_map(|d| d["id"].as_u64())
            .collect();

        let mut users: HashMap<u64, (Option<String>, Option<String>)> = HashMap::new();
        let mut out = Vec::new();
        for id in ids {
            let d = self.get(&format!("/api/dashboard/{id}"))?;
            // `dashcards` since v0.47, `ordered_cards` before.
            let cards = d["dashcards"]
                .as_array()
                .or_else(|| d["ordered_cards"].as_array())
                .cloned()
                .unwrap_or_default();
            let mut tables = BTreeSet::new();
            for dc in &cards {
                if !dc["card"].is_object() || dc["card"]["dataset_query"].is_null() {
                    continue; // text/heading cards
                }
                self.card_tables(&dc["card"], 0, &mut tables)?;
            }

            // Newer versions embed `creator`; older ones only give `creator_id`.
            let (owner_name, owner_email) = if d["creator"].is_object() {
                (
                    d["creator"]["common_name"].as_str().map(String::from),
                    d["creator"]["email"].as_str().map(String::from),
                )
            } else if let Some(uid) = d["creator_id"].as_u64() {
                users.entry(uid).or_insert_with(|| self.user(uid)).clone()
            } else {
                (None, None)
            };
            out.push(Dashboard {
                id: id.to_string(),
                name: d["name"].as_str().unwrap_or("dashboard").to_string(),
                url: format!("{}/dashboard/{id}", self.base),
                description: d["description"].as_str().map(String::from),
                owner_name,
                owner_email,
                tables,
                files: Default::default(),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    /// Creator's name and email; `/api/user/:id` needs admin rights, so
    /// failures just leave the owner empty.
    fn user(&self, id: u64) -> (Option<String>, Option<String>) {
        match self.get(&format!("/api/user/{id}")) {
            Ok(u) => (
                u["common_name"].as_str().map(String::from),
                u["email"].as_str().map(String::from),
            ),
            Err(_) => (None, None),
        }
    }

    fn card_tables(
        &mut self,
        card: &Value,
        depth: usize,
        out: &mut BTreeSet<String>,
    ) -> Result<()> {
        let q = &card["dataset_query"];
        // pMBQL (Metabase v0.5x+ API responses): `stages`, each either native
        // SQL or an MBQL stage with `source-table` / `source-card` / joins.
        if let Some(stages) = q["stages"].as_array() {
            for stage in stages {
                self.stage_tables(stage, depth, out)?;
            }
            return Ok(());
        }
        match q["type"].as_str() {
            Some("native") => {
                if let Some(sql) = q["native"]["query"].as_str() {
                    out.extend(sql_tables(sql));
                }
            }
            Some("query") => {
                let inner = &q["query"];
                let mut sources = vec![&inner["source-table"]];
                if let Some(joins) = inner["joins"].as_array() {
                    sources.extend(joins.iter().map(|j| &j["source-table"]));
                }
                for src in sources {
                    if let Some(tid) = src.as_u64() {
                        if let Some(t) = self.table(tid)? {
                            out.insert(t);
                        }
                    } else if let Some(card_id) =
                        src.as_str().and_then(|s| s.strip_prefix("card__"))
                    {
                        if depth < MAX_CARD_DEPTH {
                            let nested = self.get(&format!("/api/card/{card_id}"))?;
                            self.card_tables(&nested, depth + 1, out)?;
                        }
                    }
                }
                // Nested source-query (questions with summarize-then-filter).
                if inner["source-query"].is_object() && depth < MAX_CARD_DEPTH {
                    let nested = serde_json::json!({
                        "dataset_query": {"type": "query", "query": inner["source-query"]}
                    });
                    self.card_tables(&nested, depth + 1, out)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn stage_tables(
        &mut self,
        stage: &Value,
        depth: usize,
        out: &mut BTreeSet<String>,
    ) -> Result<()> {
        if let Some(sql) = stage["native"].as_str() {
            out.extend(sql_tables(sql));
            return Ok(());
        }
        if let Some(tid) = stage["source-table"].as_u64() {
            if let Some(t) = self.table(tid)? {
                out.insert(t);
            }
        }
        if let Some(card_id) = stage["source-card"].as_u64() {
            if depth < MAX_CARD_DEPTH {
                let nested = self.get(&format!("/api/card/{card_id}"))?;
                self.card_tables(&nested, depth + 1, out)?;
            }
        }
        for join in stage["joins"].as_array().into_iter().flatten() {
            for js in join["stages"].as_array().into_iter().flatten() {
                self.stage_tables(js, depth, out)?;
            }
        }
        Ok(())
    }

    fn table(&mut self, id: u64) -> Result<Option<String>> {
        if let Some(t) = self.tables.get(&id) {
            return Ok(t.clone());
        }
        let t = self.get(&format!("/api/table/{id}"))?;
        let name = t["name"].as_str().map(|n| match t["schema"].as_str() {
            Some(schema) if !schema.is_empty() => format!("{schema}.{n}"),
            _ => n.to_string(),
        });
        self.tables.insert(id, name.clone());
        Ok(name)
    }
}

/// Table names referenced by a native SQL question.
pub fn sql_tables(sql: &str) -> BTreeSet<String> {
    let cleaned = neutralise_template(sql);
    let mut out = BTreeSet::new();
    if let Ok(statements) = Parser::parse_sql(&GenericDialect {}, &cleaned) {
        let ctes: BTreeSet<String> = cte_names(&statements);
        let _ = visit_relations(&statements, |rel| {
            let name = rel.to_string();
            if !ctes.contains(&name.to_lowercase()) {
                out.insert(name);
            }
            ControlFlow::<()>::Continue(())
        });
        return out;
    }
    // Fallback: identifiers after FROM / JOIN.
    let re = regex::Regex::new(r#"(?i)\b(?:from|join)\s+([A-Za-z_"`\[][\w."`\[\]]*)"#).unwrap();
    for cap in re.captures_iter(&cleaned) {
        out.insert(cap[1].to_string());
    }
    out
}

fn cte_names(statements: &[sqlparser::ast::Statement]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for s in statements {
        if let sqlparser::ast::Statement::Query(q) = s {
            if let Some(with) = &q.with {
                for cte in &with.cte_tables {
                    names.insert(cte.alias.name.value.to_lowercase());
                }
            }
        }
    }
    names
}

/// Metabase SQL templates: `[[ AND x = {{x}} ]]` optional clauses and
/// `{{var}}` / `{{snippet: name}}` tags. Drop optional clauses, replace tags
/// with a literal so the SQL parses.
fn neutralise_template(sql: &str) -> String {
    let optional = regex::Regex::new(r"(?s)\[\[.*?\]\]").unwrap();
    let tags = regex::Regex::new(r"\{\{[^}]*\}\}").unwrap();
    let s = optional.replace_all(sql, " ");
    tags.replace_all(&s, "1").into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;

    /// Serves canned JSON by URL path and records requests.
    struct Fake {
        routes: HashMap<String, (u16, Value)>,
        seen: RefCell<Vec<Request>>,
    }

    impl Http for Fake {
        fn send(&self, req: &Request) -> Result<(u16, Value)> {
            self.seen.borrow_mut().push(req.clone());
            let path = req.url.trim_start_matches("https://mb");
            Ok(self
                .routes
                .get(path)
                .cloned()
                .unwrap_or((404, json!({"message": "not found"}))))
        }
    }

    fn fake(routes: Vec<(&str, Value)>) -> Fake {
        Fake {
            routes: routes
                .into_iter()
                .map(|(p, v)| (p.to_string(), (200, v)))
                .collect(),
            seen: RefCell::new(vec![]),
        }
    }

    #[test]
    fn sql_tables_parses_and_skips_ctes_and_templates() {
        let sql = "WITH recent AS (SELECT * FROM analytics.orders WHERE created_at > {{since}})
                   SELECT c.name, count(*) FROM recent r
                   JOIN \"analytics\".\"customers\" c ON c.id = r.customer_id
                   [[ WHERE c.country = {{country}} ]]
                   GROUP BY 1";
        let t = sql_tables(sql);
        assert_eq!(
            t,
            BTreeSet::from([
                "\"analytics\".\"customers\"".to_string(),
                "analytics.orders".to_string()
            ])
        );
    }

    #[test]
    fn sql_tables_falls_back_to_regex() {
        // Not parseable by the generic dialect.
        let t = sql_tables(
            "SELECT * FROM orders o QUALIFY ROW_NUMBER() OVER () = 1 ~~ JOIN payments p",
        );
        assert!(t.contains("orders"), "{t:?}");
        assert!(t.contains("payments"), "{t:?}");
    }

    #[test]
    fn dashboards_resolve_native_mbql_joins_and_nested_cards() {
        let f = fake(vec![
            (
                "/api/dashboard/",
                json!([{"id": 1}, {"id": 2, "archived": true}, {"id": 3}]),
            ),
            (
                "/api/dashboard/1",
                json!({
                    "id": 1, "name": "Sales", "description": "Weekly sales", "creator_id": 5,
                    "dashcards": [
                        {"card": {"dataset_query": {"type": "native", "native": {"query": "select * from orders"}}}},
                        {"card": {"dataset_query": {"type": "query", "query": {
                            "source-table": 10, "joins": [{"source-table": 11}]}}}},
                        {"card": {"name": "text card"}}
                    ]
                }),
            ),
            (
                "/api/dashboard/3",
                json!({
                    "id": 3, "name": "Nested",
                    "ordered_cards": [
                        {"card": {"dataset_query": {"type": "query", "query": {"source-table": "card__7"}}}}
                    ]
                }),
            ),
            (
                "/api/card/7",
                json!({"dataset_query": {"type": "query", "query": {"source-table": 10}}}),
            ),
            (
                "/api/table/10",
                json!({"name": "customers", "schema": "analytics"}),
            ),
            ("/api/table/11", json!({"name": "payments", "schema": ""})),
            (
                "/api/user/5",
                json!({"common_name": "Ana Lytics", "email": "ana@example.com"}),
            ),
        ]);
        let mut client = Client::new(&f, "https://mb/", "key");
        let ds = client.dashboards().unwrap();
        assert_eq!(ds.len(), 2);
        let nested = &ds[0];
        assert_eq!(nested.name, "Nested");
        assert_eq!(
            nested.tables,
            BTreeSet::from(["analytics.customers".to_string()])
        );
        assert_eq!(nested.owner_email, None);
        let sales = &ds[1];
        assert_eq!(sales.url, "https://mb/dashboard/1");
        assert_eq!(
            sales.tables,
            BTreeSet::from([
                "analytics.customers".to_string(),
                "orders".to_string(),
                "payments".to_string()
            ])
        );
        assert_eq!(sales.owner_name.as_deref(), Some("Ana Lytics"));
        assert_eq!(sales.description.as_deref(), Some("Weekly sales"));

        let seen = f.seen.borrow();
        assert!(seen
            .iter()
            .all(|r| r.headers[0] == ("x-api-key".into(), "key".into())));
        // Table 10 fetched once (cached).
        assert_eq!(
            seen.iter()
                .filter(|r| r.url.ends_with("/api/table/10"))
                .count(),
            1
        );
    }

    /// Shapes as returned by Metabase v0.65 (pMBQL `stages`, embedded creator).
    #[test]
    fn dashboards_resolve_pmbql_stages() {
        let f = fake(vec![
            ("/api/dashboard/", json!([{"id": 2}])),
            (
                "/api/dashboard/2",
                json!({
                    "id": 2, "name": "Sales Overview", "creator_id": 1,
                    "creator": {"common_name": "Ana Lytics", "email": "ana@example.com"},
                    "dashcards": [
                        {"card": {"dataset_query": {"lib/type": "mbql/query", "database": 2, "stages": [
                            {"lib/type": "mbql.stage/native",
                             "native": "select * from marts.orders o [[where o.id > {{min_id}}]]"}]}}},
                        {"card": {"dataset_query": {"lib/type": "mbql/query", "stages": [
                            {"lib/type": "mbql.stage/mbql", "source-card": 41}]}}},
                        {"card": {"dataset_query": {"lib/type": "mbql/query", "stages": [
                            {"lib/type": "mbql.stage/mbql", "source-table": 12,
                             "joins": [{"stages": [{"lib/type": "mbql.stage/mbql", "source-table": 13}]}]}]}}}
                    ]
                }),
            ),
            (
                "/api/card/41",
                json!({"dataset_query": {"stages": [{"source-table": 12}]}}),
            ),
            (
                "/api/table/12",
                json!({"name": "customers", "schema": "marts"}),
            ),
            (
                "/api/table/13",
                json!({"name": "payments", "schema": "raw"}),
            ),
        ]);
        let ds = Client::new(&f, "https://mb", "k").dashboards().unwrap();
        assert_eq!(
            ds[0].tables,
            BTreeSet::from([
                "marts.customers".to_string(),
                "marts.orders".to_string(),
                "raw.payments".to_string()
            ])
        );
        assert_eq!(ds[0].owner_name.as_deref(), Some("Ana Lytics"));
        // Embedded creator: no /api/user call.
        assert!(!f.seen.borrow().iter().any(|r| r.url.contains("/api/user/")));
    }

    #[test]
    fn http_errors_surface() {
        let f = Fake {
            routes: HashMap::from([(
                "/api/dashboard/".to_string(),
                (401, json!("Unauthenticated")),
            )]),
            seen: RefCell::new(vec![]),
        };
        let err = Client::new(&f, "https://mb", "bad")
            .dashboards()
            .unwrap_err()
            .to_string();
        assert!(err.contains("HTTP 401"), "{err}");
    }
}
