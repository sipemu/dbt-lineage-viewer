//! Lint findings as inline annotations for `dbt-lineage ci annotations`.
//!
//! - Bitbucket Code Insights: one report per commit plus up to 1,000
//!   annotations, created in bulk requests of at most 100.
//! - Azure Pipelines: `##vso[task.logissue]` logging commands, shown on the
//!   build summary (and on the PR when the build is a PR validation).
//!
//! SARIF (GitHub) and Code Quality (GitLab) live in `render::lint`.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::comment::{bitbucket_api, Http, Request, BITBUCKET_API};
use crate::graph::lint::{LintReport, LintSeverity};
use crate::render::locations::Locations;

/// Default Code Insights report id. Reports are keyed per commit, so
/// re-running on the same commit replaces the previous report.
pub const REPORT_ID: &str = "dbt-lineage";
/// Bitbucket limits: annotations per report, per bulk request, and lengths.
pub const MAX_ANNOTATIONS: usize = 1000;
pub const BATCH: usize = 100;
const MAX_SUMMARY: usize = 450;
const MAX_DETAILS: usize = 2000;
/// Proxy that runs alongside every Bitbucket Pipelines step and adds auth.
/// Pipes run in their own container and reach it as
/// `http://host.docker.internal:29418` instead (the pipe sets that).
pub const PIPELINES_PROXY: &str = "http://localhost:29418";
const LINK: &str = "https://github.com/sipemu/dbt-lineage-viewer";

/// A report id is a URL path segment; keep it to a safe alphabet.
pub fn validate_report_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 100
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        bail!("invalid Code Insights report id {id:?}: use 1-100 of [A-Za-z0-9._-]");
    }
    Ok(())
}

/// Code Insights report body (`PUT …/reports/{report_id}`).
pub fn bitbucket_report(report: &LintReport, report_id: &str) -> Value {
    let errors = report.count_by_severity(LintSeverity::Error);
    let warnings = report.count_by_severity(LintSeverity::Warning);
    let infos = report.count_by_severity(LintSeverity::Info);
    let total = report.findings.len();
    let mut details = format!(
        "Structural lint of the dbt DAG: {errors} error(s), {warnings} warning(s), {infos} info."
    );
    if total > MAX_ANNOTATIONS {
        details.push_str(&format!(
            " Showing the first {MAX_ANNOTATIONS} of {total} findings as annotations (errors first)."
        ));
    }
    json!({
        "title": "dbt-lineage",
        "details": truncate(&details, MAX_DETAILS),
        "external_id": report_id,
        "report_type": "BUG",
        "reporter": "dbt-lineage",
        "link": LINK,
        "result": if errors > 0 { "FAILED" } else { "PASSED" },
        "data": [
            {"title": "Errors", "type": "NUMBER", "value": errors},
            {"title": "Warnings", "type": "NUMBER", "value": warnings},
            {"title": "Info", "type": "NUMBER", "value": infos},
        ],
    })
}

/// Annotation bodies, errors first, capped at Bitbucket's 1,000 per report.
/// `external_id`s are stable per (rule, node), so re-runs produce the same ids.
pub fn bitbucket_annotations(report: &LintReport, locations: &Locations) -> Vec<Value> {
    let mut findings: Vec<_> = report.findings.iter().collect();
    findings.sort_by_key(|f| match f.severity {
        LintSeverity::Error => 0,
        LintSeverity::Warning => 1,
        LintSeverity::Info => 2,
    });
    let mut seen = HashSet::new();
    findings
        .into_iter()
        .filter_map(|f| {
            let key = format!("{}:{}", f.rule, f.unique_id);
            if !seen.insert(key.clone()) {
                return None;
            }
            let id = blake3::hash(key.as_bytes()).to_hex();
            let (annotation_type, severity) = match f.severity {
                LintSeverity::Error => ("BUG", "HIGH"),
                LintSeverity::Warning => ("CODE_SMELL", "MEDIUM"),
                LintSeverity::Info => ("CODE_SMELL", "LOW"),
            };
            let mut a = json!({
                "external_id": format!("dbt-lineage-{}", &id.as_str()[..16]),
                "annotation_type": annotation_type,
                "severity": severity,
                "result": "FAILED",
                "summary": truncate(&f.message, MAX_SUMMARY),
                "details": truncate(&format!("Rule `{}` on `{}`.", f.rule, f.label), MAX_DETAILS),
                "link": LINK,
            });
            // Without a usable path the annotation shows in the report's
            // overview instead of inline, which Bitbucket allows.
            if let Some(path) = repo_path(locations.for_id(&f.unique_id)) {
                a["path"] = json!(path);
                a["line"] = json!(1);
            }
            Some(a)
        })
        .take(MAX_ANNOTATIONS)
        .collect()
}

/// Normalise to a path relative to the repository root (forward slashes,
/// no leading `./` or `/`); `None` when nothing is left.
fn repo_path(p: &str) -> Option<String> {
    let p = p.replace('\\', "/");
    let mut rest = p.as_str();
    loop {
        let next = rest.trim_start_matches('/');
        let next = next.strip_prefix("./").unwrap_or(next);
        if next == rest {
            break;
        }
        rest = next;
    }
    (!rest.is_empty() && rest != ".").then(|| rest.to_string())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max - 1).collect();
    out.push('…');
    out
}

/// Where to send the report, resolved from the Pipelines environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketTarget {
    /// `https://api.bitbucket.org` with a token, `http://api.bitbucket.org`
    /// through the Pipelines proxy (which only handles plain HTTP), or
    /// `BITBUCKET_API_URL` when set.
    pub api: String,
    pub workspace: String,
    pub repo: String,
    pub commit: String,
    pub report_id: String,
    pub token: Option<String>,
    pub proxy: Option<String>,
}

fn var<'a>(env: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    env.get(key).map(String::as_str).filter(|v| !v.is_empty())
}

impl BitbucketTarget {
    /// Token from `DBT_LINEAGE_TOKEN` / `BITBUCKET_ACCESS_TOKEN`; without one,
    /// the Pipelines proxy (`BITBUCKET_PROXY`, default `localhost:29418`
    /// inside Pipelines) authenticates the request.
    pub fn from_env(env: &HashMap<String, String>, report_id: &str) -> Result<Self> {
        validate_report_id(report_id)?;
        let need = |k: &str| {
            var(env, k)
                .map(String::from)
                .with_context(|| format!("{k} is not set (run inside Bitbucket Pipelines)"))
        };
        let workspace = var(env, "BITBUCKET_WORKSPACE")
            .or_else(|| var(env, "BITBUCKET_REPO_OWNER"))
            .map(String::from)
            .context("BITBUCKET_WORKSPACE is not set (run inside Bitbucket Pipelines)")?;
        let token = var(env, "DBT_LINEAGE_TOKEN")
            .or_else(|| var(env, "BITBUCKET_ACCESS_TOKEN"))
            .map(String::from);
        let proxy = match (var(env, "BITBUCKET_PROXY"), &token) {
            (Some(p), None) => Some(p.to_string()),
            (None, None) if var(env, "BITBUCKET_BUILD_NUMBER").is_some() => {
                Some(PIPELINES_PROXY.to_string())
            }
            _ => None,
        };
        if token.is_none() && proxy.is_none() {
            bail!("no Bitbucket credentials: set BITBUCKET_ACCESS_TOKEN, or run inside Pipelines (proxy)");
        }
        let api = match bitbucket_api(env) {
            Some(api) => api.to_string(),
            None if proxy.is_some() => "http://api.bitbucket.org".to_string(),
            None => BITBUCKET_API.to_string(),
        };
        Ok(BitbucketTarget {
            api,
            workspace,
            repo: need("BITBUCKET_REPO_SLUG")?,
            commit: need("BITBUCKET_COMMIT")?,
            report_id: report_id.to_string(),
            token,
            proxy,
        })
    }

    fn report_url(&self) -> String {
        format!(
            "{}/2.0/repositories/{}/{}/commit/{}/reports/{}",
            self.api, self.workspace, self.repo, self.commit, self.report_id
        )
    }
}

/// Replace the commit's report and create its annotations. Returns the
/// number of annotations sent.
///
/// Idempotent per commit: the report id is fixed, so the DELETE removes the
/// previous run's report together with its annotations before the PUT.
pub fn post_bitbucket(
    http: &dyn Http,
    target: &BitbucketTarget,
    report: &Value,
    annotations: &[Value],
) -> Result<usize> {
    let headers: Vec<(String, String)> = target
        .token
        .iter()
        .map(|t| ("Authorization".to_string(), format!("Bearer {t}")))
        .collect();
    let url = target.report_url();
    let send = |method, url: String, body: Option<Value>| {
        http.send(&Request {
            method,
            url,
            headers: headers.clone(),
            body,
        })
    };
    let (status, body) = send("DELETE", url.clone(), None)?;
    if !(200..300).contains(&status) && status != 404 {
        bail!("deleting the previous Code Insights report failed with HTTP {status}: {body}");
    }
    let (status, body) = send("PUT", url.clone(), Some(report.clone()))?;
    if !(200..300).contains(&status) {
        bail!("creating the Code Insights report failed with HTTP {status}: {body}");
    }
    let annotations = &annotations[..annotations.len().min(MAX_ANNOTATIONS)];
    for chunk in annotations.chunks(BATCH) {
        let (status, body) = send(
            "POST",
            format!("{url}/annotations"),
            Some(Value::Array(chunk.to_vec())),
        )?;
        if !(200..300).contains(&status) {
            bail!("creating Code Insights annotations failed with HTTP {status}: {body}");
        }
    }
    Ok(annotations.len())
}

/// Azure Pipelines logging commands, one per finding.
pub fn azure_logging_commands(report: &LintReport, locations: &Locations) -> String {
    let mut out = String::new();
    for f in &report.findings {
        let kind = match f.severity {
            LintSeverity::Error => "error",
            _ => "warning",
        };
        out.push_str(&format!(
            "##vso[task.logissue type={kind};sourcepath={};linenumber=1;code={}]{}\n",
            azure_property(locations.for_id(&f.unique_id)),
            azure_property(f.rule),
            azure_message(&format!("dbt-lineage: {}", f.message)),
        ));
    }
    out
}

/// Escaping for logging-command messages.
fn azure_message(s: &str) -> String {
    s.replace('%', "%AZP25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

/// Escaping for logging-command properties (also `;` and `]`).
fn azure_property(s: &str) -> String {
    azure_message(s).replace(';', "%3B").replace(']', "%5D")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::lint::LintFinding;
    use std::cell::RefCell;

    fn finding(rule: &'static str, sev: LintSeverity, id: &str) -> LintFinding {
        LintFinding {
            rule,
            severity: sev,
            unique_id: id.into(),
            label: id.rsplit('.').next().unwrap().into(),
            message: format!("{rule} on {id}"),
        }
    }

    fn report(n_errors: usize, n_warnings: usize) -> LintReport {
        let mut findings = Vec::new();
        for i in 0..n_errors {
            findings.push(finding(
                "undefined-source",
                LintSeverity::Error,
                &format!("model.e{i}"),
            ));
        }
        for i in 0..n_warnings {
            findings.push(finding(
                "dead-end-model",
                LintSeverity::Warning,
                &format!("model.w{i}"),
            ));
        }
        LintReport { findings }
    }

    #[test]
    fn report_result_follows_errors() {
        let r = bitbucket_report(&report(0, 2), REPORT_ID);
        assert_eq!(r["result"], "PASSED");
        assert_eq!(r["report_type"], "BUG");
        assert_eq!(r["data"][1]["value"], 2);
        let r = bitbucket_report(&report(1, 0), REPORT_ID);
        assert_eq!(r["result"], "FAILED");
        assert_eq!(
            r["data"][0],
            json!({"title": "Errors", "type": "NUMBER", "value": 1})
        );
    }

    #[test]
    fn annotation_shape_and_cap() {
        let a = bitbucket_annotations(&report(1, 1), &Locations::default());
        assert_eq!(a.len(), 2);
        assert_eq!(a[0]["annotation_type"], "BUG");
        assert_eq!(a[0]["severity"], "HIGH");
        assert_eq!(a[1]["annotation_type"], "CODE_SMELL");
        assert_eq!(a[1]["severity"], "MEDIUM");
        assert_eq!(a[0]["path"], "dbt_project.yml");
        assert_eq!(a[0]["line"], 1);
        assert!(a[0]["external_id"]
            .as_str()
            .unwrap()
            .starts_with("dbt-lineage-"));
        assert_ne!(a[0]["external_id"], a[1]["external_id"]);
        assert_eq!(
            bitbucket_annotations(&report(1200, 0), &Locations::default()).len(),
            1000
        );
    }

    struct Fake {
        responses: RefCell<Vec<(u16, Value)>>,
        seen: RefCell<Vec<Request>>,
    }

    impl Http for Fake {
        fn send(&self, req: &Request) -> Result<(u16, Value)> {
            self.seen.borrow_mut().push(req.clone());
            Ok(self
                .responses
                .borrow_mut()
                .pop()
                .unwrap_or((200, json!({}))))
        }
    }

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const PIPELINE: &[(&str, &str)] = &[
        ("BITBUCKET_BUILD_NUMBER", "7"),
        ("BITBUCKET_WORKSPACE", "ws"),
        ("BITBUCKET_REPO_SLUG", "repo"),
        ("BITBUCKET_COMMIT", "abc123"),
    ];

    #[test]
    fn posts_report_then_annotations_in_batches() {
        let target = BitbucketTarget::from_env(&env(PIPELINE), REPORT_ID).unwrap();
        assert_eq!(target.proxy.as_deref(), Some(PIPELINES_PROXY));
        assert_eq!(target.api, "http://api.bitbucket.org");
        let lint = report(150, 100);
        let annotations = bitbucket_annotations(&lint, &Locations::default());
        let fake = Fake {
            // DELETE answers 404 (no earlier report); the rest default to 200.
            responses: RefCell::new(vec![(404, json!({}))]),
            seen: RefCell::new(vec![]),
        };
        let sent = post_bitbucket(
            &fake,
            &target,
            &bitbucket_report(&lint, REPORT_ID),
            &annotations,
        )
        .unwrap();
        assert_eq!(sent, 250);
        let seen = fake.seen.borrow();
        let base =
            "http://api.bitbucket.org/2.0/repositories/ws/repo/commit/abc123/reports/dbt-lineage";
        assert_eq!((seen[0].method, seen[0].url.as_str()), ("DELETE", base));
        assert_eq!((seen[1].method, seen[1].url.as_str()), ("PUT", base));
        assert_eq!(seen[1].body.as_ref().unwrap()["result"], "FAILED");
        let batches: Vec<usize> = seen[2..]
            .iter()
            .map(|r| {
                assert_eq!(r.method, "POST");
                assert_eq!(r.url, format!("{base}/annotations"));
                r.body.as_ref().unwrap().as_array().unwrap().len()
            })
            .collect();
        assert_eq!(batches, vec![100, 100, 50]);
        // Through the proxy no Authorization header is sent.
        assert!(seen.iter().all(|r| r.headers.is_empty()));
    }

    #[test]
    fn token_uses_https_and_bearer() {
        let mut e = env(PIPELINE);
        e.insert("BITBUCKET_ACCESS_TOKEN".into(), "tok".into());
        let target = BitbucketTarget::from_env(&e, REPORT_ID).unwrap();
        assert_eq!(target.api, "https://api.bitbucket.org");
        assert_eq!(target.proxy, None);
        let fake = Fake {
            responses: RefCell::new(vec![]),
            seen: RefCell::new(vec![]),
        };
        post_bitbucket(&fake, &target, &json!({}), &[]).unwrap();
        let seen = fake.seen.borrow();
        assert_eq!(
            seen[0].headers,
            vec![("Authorization".to_string(), "Bearer tok".to_string())]
        );

        // Outside Pipelines without a token there is nothing to authenticate with.
        let outside = env(&[
            ("BITBUCKET_WORKSPACE", "ws"),
            ("BITBUCKET_REPO_SLUG", "r"),
            ("BITBUCKET_COMMIT", "c"),
        ]);
        assert!(BitbucketTarget::from_env(&outside, REPORT_ID).is_err());
    }

    #[test]
    fn http_errors_surface() {
        let target = BitbucketTarget::from_env(&env(PIPELINE), REPORT_ID).unwrap();
        let fake = Fake {
            // Popped from the end: DELETE 200, PUT 400.
            responses: RefCell::new(vec![(400, json!({"error": "bad"})), (200, json!({}))]),
            seen: RefCell::new(vec![]),
        };
        let err = post_bitbucket(&fake, &target, &json!({}), &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("HTTP 400"), "{err}");
    }

    /// Every field of every body is one the Code Insights API defines
    /// (Bitbucket's OpenAPI `report` / `report_annotation` / `report_data`),
    /// with values from its enums and within its limits.
    #[test]
    fn bodies_match_the_code_insights_schema() {
        let lint = report(3, 2);
        let r = bitbucket_report(&lint, REPORT_ID);
        let allowed = [
            "title",
            "details",
            "external_id",
            "reporter",
            "link",
            "report_type",
            "result",
            "data",
        ];
        for k in r.as_object().unwrap().keys() {
            assert!(allowed.contains(&k.as_str()), "unknown report field {k}");
        }
        for k in ["title", "details", "report_type"] {
            assert!(r[k].is_string(), "report needs {k}");
        }
        assert!(
            ["SECURITY", "COVERAGE", "TEST", "BUG"].contains(&r["report_type"].as_str().unwrap())
        );
        assert!(["PASSED", "FAILED", "PENDING"].contains(&r["result"].as_str().unwrap()));
        assert_eq!(r["external_id"], REPORT_ID);
        let data = r["data"].as_array().unwrap();
        assert!(data.len() <= 10);
        for d in data {
            assert!([
                "BOOLEAN",
                "DATE",
                "DURATION",
                "LINK",
                "NUMBER",
                "PERCENTAGE",
                "TEXT"
            ]
            .contains(&d["type"].as_str().unwrap()));
            assert!(d["value"].is_number());
        }

        let allowed = [
            "external_id",
            "annotation_type",
            "path",
            "line",
            "summary",
            "details",
            "result",
            "severity",
            "link",
        ];
        let anns = bitbucket_annotations(&lint, &Locations::default());
        let mut ids = HashSet::new();
        for a in &anns {
            for k in a.as_object().unwrap().keys() {
                assert!(
                    allowed.contains(&k.as_str()),
                    "unknown annotation field {k}"
                );
            }
            assert!(["VULNERABILITY", "CODE_SMELL", "BUG"]
                .contains(&a["annotation_type"].as_str().unwrap()));
            assert!(
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"].contains(&a["severity"].as_str().unwrap())
            );
            assert!(
                ["PASSED", "FAILED", "SKIPPED", "IGNORED"].contains(&a["result"].as_str().unwrap())
            );
            assert!(a["summary"].as_str().unwrap().chars().count() <= 450);
            assert!(a["details"].as_str().unwrap().chars().count() <= 2000);
            assert!(a["line"].as_u64().unwrap() >= 1);
            assert!(ids.insert(a["external_id"].as_str().unwrap().to_string()));
        }
    }

    #[test]
    fn ids_are_stable_across_runs() {
        let a = bitbucket_annotations(&report(5, 5), &Locations::default());
        let b = bitbucket_annotations(&report(5, 5), &Locations::default());
        let ids = |v: &[Value]| {
            v.iter()
                .map(|x| x["external_id"].clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&a), ids(&b));
        // Duplicate (rule, node) findings collapse into one annotation.
        let mut dup = report(1, 0);
        dup.findings.push(dup.findings[0].clone());
        assert_eq!(bitbucket_annotations(&dup, &Locations::default()).len(), 1);
    }

    #[test]
    fn long_texts_are_truncated() {
        let mut lint = report(1, 0);
        lint.findings[0].message = "x".repeat(1000);
        let a = bitbucket_annotations(&lint, &Locations::default());
        let summary = a[0]["summary"].as_str().unwrap();
        assert_eq!(summary.chars().count(), 450);
        assert!(summary.ends_with('…'));
    }

    #[test]
    fn report_mentions_capped_annotations() {
        let r = bitbucket_report(&report(1001, 0), REPORT_ID);
        assert!(r["details"]
            .as_str()
            .unwrap()
            .contains("first 1000 of 1001"));
        let r = bitbucket_report(&report(10, 0), REPORT_ID);
        assert!(!r["details"].as_str().unwrap().contains("Showing"));
    }

    #[test]
    fn paths_are_relative_to_the_repo_root() {
        assert_eq!(
            repo_path("dbt/models/a.sql").as_deref(),
            Some("dbt/models/a.sql")
        );
        assert_eq!(repo_path("./models/a.sql").as_deref(), Some("models/a.sql"));
        assert_eq!(repo_path("/models/a.sql").as_deref(), Some("models/a.sql"));
        assert_eq!(repo_path(".//./x.yml").as_deref(), Some("x.yml"));
        assert_eq!(
            repo_path("dbt\\models\\a.sql").as_deref(),
            Some("dbt/models/a.sql")
        );
        assert_eq!(repo_path(""), None);
        assert_eq!(repo_path("./"), None);

        // A project in a subdirectory: annotation paths carry the prefix.
        use crate::graph::types::{LineageGraph, NodeData, NodeType};
        let mut g = LineageGraph::new();
        g.add_node(NodeData {
            unique_id: "model.e0".into(),
            label: "e0".into(),
            node_type: NodeType::Model,
            file_path: Some(std::path::PathBuf::from("/repo/dbt/models/e0.sql")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let loc = Locations::from_graph(&g, std::path::Path::new("/repo/dbt"), "dbt/");
        let a = bitbucket_annotations(&report(2, 0), &loc);
        assert_eq!(a[0]["path"], "dbt/models/e0.sql");
        assert_eq!(a[0]["line"], 1);
        // Unknown nodes fall back to the project's dbt_project.yml.
        assert_eq!(a[1]["path"], "dbt/dbt_project.yml");
    }

    #[test]
    fn report_id_is_configurable_and_validated() {
        let t = BitbucketTarget::from_env(&env(PIPELINE), "dbt-lineage-marts").unwrap();
        assert!(t
            .report_url()
            .ends_with("/commit/abc123/reports/dbt-lineage-marts"));
        for bad in ["", "a/b", "x y", "../up"] {
            assert!(
                BitbucketTarget::from_env(&env(PIPELINE), bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn pipe_proxy_and_api_override() {
        // The pipe points BITBUCKET_PROXY at host.docker.internal.
        let mut e = env(PIPELINE);
        e.insert(
            "BITBUCKET_PROXY".into(),
            "http://host.docker.internal:29418".into(),
        );
        let t = BitbucketTarget::from_env(&e, REPORT_ID).unwrap();
        assert_eq!(
            t.proxy.as_deref(),
            Some("http://host.docker.internal:29418")
        );
        assert_eq!(t.api, "http://api.bitbucket.org");
        // A token wins over the proxy: straight to the API with Bearer auth.
        e.insert("BITBUCKET_ACCESS_TOKEN".into(), "tok".into());
        let t = BitbucketTarget::from_env(&e, REPORT_ID).unwrap();
        assert_eq!(t.proxy, None);
        assert_eq!(t.api, "https://api.bitbucket.org");
        e.insert("BITBUCKET_API_URL".into(), "http://mock:8080/".into());
        let t = BitbucketTarget::from_env(&e, REPORT_ID).unwrap();
        assert_eq!(t.api, "http://mock:8080");
    }

    #[test]
    fn rerun_on_the_same_commit_sends_identical_requests() {
        let target = BitbucketTarget::from_env(&env(PIPELINE), REPORT_ID).unwrap();
        let lint = report(120, 3);
        let run = || {
            let fake = Fake {
                responses: RefCell::new(vec![]),
                seen: RefCell::new(vec![]),
            };
            let anns = bitbucket_annotations(&lint, &Locations::default());
            post_bitbucket(&fake, &target, &bitbucket_report(&lint, REPORT_ID), &anns).unwrap();
            fake.seen.into_inner()
        };
        let (a, b) = (run(), run());
        assert_eq!(a, b);
        assert_eq!(a[0].method, "DELETE");
        assert_eq!(a.len(), 2 + 2);
    }

    #[test]
    fn never_sends_more_than_the_report_limit() {
        let target = BitbucketTarget::from_env(&env(PIPELINE), REPORT_ID).unwrap();
        let fake = Fake {
            responses: RefCell::new(vec![]),
            seen: RefCell::new(vec![]),
        };
        let many: Vec<Value> = (0..1234).map(|i| json!({"external_id": i})).collect();
        assert_eq!(
            post_bitbucket(&fake, &target, &json!({}), &many).unwrap(),
            1000
        );
        let seen = fake.seen.borrow();
        assert_eq!(seen.len(), 2 + 10);
        assert!(seen[2..]
            .iter()
            .all(|r| r.body.as_ref().unwrap().as_array().unwrap().len() == 100));
    }

    #[test]
    fn azure_commands_escape_properties_and_messages() {
        let lint = LintReport {
            findings: vec![LintFinding {
                rule: "dead-end-model",
                severity: LintSeverity::Warning,
                unique_id: "model.a".into(),
                label: "a".into(),
                message: "100% unused;\nreally]".into(),
            }],
        };
        let out = azure_logging_commands(&lint, &Locations::default());
        assert_eq!(
            out,
            "##vso[task.logissue type=warning;sourcepath=dbt_project.yml;linenumber=1;code=dead-end-model]dbt-lineage: 100%AZP25 unused;%0Areally]\n"
        );
    }
}
