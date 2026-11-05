//! Sticky PR/MR comments for `dbt-lineage ci comment`.
//!
//! Finds the previous comment carrying [`super::COMMENT_MARKER`]
//! on the current pull/merge request and updates it, or creates one. Each
//! platform is a few REST calls; the HTTP transport is injected so the flows
//! are unit-tested without a network.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::{Platform, COMMENT_MARKER};

/// One HTTP request. Bodies and responses are JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub method: &'static str,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Value>,
}

/// Minimal JSON-over-HTTP transport. Non-2xx responses are returned, not errors.
pub trait Http {
    fn send(&self, req: &Request) -> Result<(u16, Value)>;
}

/// What happened to the comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Created,
    Updated,
}

/// Where to comment, resolved from the CI environment.
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    GitHub {
        api: String,
        repo: String,
        number: u64,
        token: String,
    },
    GitLab {
        api: String,
        project: String,
        iid: u64,
        token: String,
    },
    Bitbucket {
        workspace: String,
        repo: String,
        pr: u64,
        token: String,
    },
    Azure {
        collection: String,
        project: String,
        repo_id: String,
        pr: u64,
        token: String,
    },
}

fn var<'a>(env: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    env.get(key).map(String::as_str).filter(|v| !v.is_empty())
}

fn need<'a>(env: &'a HashMap<String, String>, key: &str, hint: &str) -> Result<&'a str> {
    var(env, key).with_context(|| format!("{key} is not set ({hint})"))
}

fn first<'a>(env: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|k| var(env, k))
}

impl Target {
    /// Resolve the PR/MR and credentials for `platform`. `DBT_LINEAGE_TOKEN`
    /// overrides the platform's default token variable everywhere.
    pub fn from_env(platform: Platform, env: &HashMap<String, String>) -> Result<Target> {
        let token_hint = "set DBT_LINEAGE_TOKEN or the platform token variable";
        match platform {
            Platform::GitHub => {
                let number = github_pr_number(env).context(
                    "not a pull_request event (no PR number in GITHUB_EVENT_PATH or GITHUB_REF)",
                )?;
                Ok(Target::GitHub {
                    api: var(env, "GITHUB_API_URL")
                        .unwrap_or("https://api.github.com")
                        .trim_end_matches('/')
                        .to_string(),
                    repo: need(env, "GITHUB_REPOSITORY", "owner/repo")?.to_string(),
                    number,
                    token: first(env, &["DBT_LINEAGE_TOKEN", "GITHUB_TOKEN"])
                        .context(format!("no GitHub token ({token_hint})"))?
                        .to_string(),
                })
            }
            Platform::GitLab => Ok(Target::GitLab {
                api: need(env, "CI_API_V4_URL", "GitLab CI")?
                    .trim_end_matches('/')
                    .to_string(),
                project: need(env, "CI_PROJECT_ID", "GitLab CI")?.to_string(),
                iid: need(env, "CI_MERGE_REQUEST_IID", "run in a merge request pipeline")?
                    .parse()
                    .context("CI_MERGE_REQUEST_IID is not a number")?,
                token: first(env, &["DBT_LINEAGE_TOKEN", "GITLAB_TOKEN"])
                    .context(format!(
                        "no GitLab token: CI_JOB_TOKEN cannot post notes, use a project access token with `api` scope ({token_hint})"
                    ))?
                    .to_string(),
            }),
            Platform::Bitbucket => Ok(Target::Bitbucket {
                workspace: need(env, "BITBUCKET_WORKSPACE", "Bitbucket Pipelines")?.to_string(),
                repo: need(env, "BITBUCKET_REPO_SLUG", "Bitbucket Pipelines")?.to_string(),
                pr: need(env, "BITBUCKET_PR_ID", "run in a pull-requests pipeline")?
                    .parse()
                    .context("BITBUCKET_PR_ID is not a number")?,
                token: first(env, &["DBT_LINEAGE_TOKEN", "BITBUCKET_ACCESS_TOKEN"])
                    .context(format!(
                        "no Bitbucket token: use a repository access token with pull request write scope ({token_hint})"
                    ))?
                    .to_string(),
            }),
            Platform::AzureDevOps => Ok(Target::Azure {
                collection: need(env, "SYSTEM_COLLECTIONURI", "Azure Pipelines")?
                    .trim_end_matches('/')
                    .to_string(),
                project: need(env, "SYSTEM_TEAMPROJECT", "Azure Pipelines")?.to_string(),
                repo_id: need(env, "BUILD_REPOSITORY_ID", "Azure Pipelines")?.to_string(),
                pr: need(env, "SYSTEM_PULLREQUEST_PULLREQUESTID", "run in a PR build")?
                    .parse()
                    .context("SYSTEM_PULLREQUEST_PULLREQUESTID is not a number")?,
                token: first(env, &["DBT_LINEAGE_TOKEN", "SYSTEM_ACCESSTOKEN"])
                    .context(format!(
                        "no Azure token: map SYSTEM_ACCESSTOKEN: $(System.AccessToken) into the step env ({token_hint})"
                    ))?
                    .to_string(),
            }),
            Platform::Unknown => bail!(
                "no supported CI platform detected (GitHub Actions, GitLab CI, Bitbucket Pipelines, Azure Pipelines)"
            ),
        }
    }
}

fn github_pr_number(env: &HashMap<String, String>) -> Option<u64> {
    if let Some(path) = var(env, "GITHUB_EVENT_PATH") {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(event) = serde_json::from_str::<Value>(&text) {
                let n = event["pull_request"]["number"]
                    .as_u64()
                    .or_else(|| event["number"].as_u64());
                if n.is_some() {
                    return n;
                }
            }
        }
    }
    // refs/pull/<n>/merge
    var(env, "GITHUB_REF")?
        .strip_prefix("refs/pull/")?
        .split('/')
        .next()?
        .parse()
        .ok()
}

/// Make sure the body carries the marker so the next run finds it.
fn with_marker(body: &str) -> String {
    if body.contains(COMMENT_MARKER) {
        body.to_string()
    } else {
        format!("{COMMENT_MARKER}\n{body}")
    }
}

fn check(status: u16, body: &Value, what: &str) -> Result<()> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        bail!("{what} failed with HTTP {status}: {body}")
    }
}

/// Create or update the sticky comment on `target`.
pub fn upsert(http: &dyn Http, target: &Target, body: &str) -> Result<Outcome> {
    let body = with_marker(body);
    match target {
        Target::GitHub {
            api,
            repo,
            number,
            token,
        } => {
            let headers = vec![
                ("Authorization".into(), format!("Bearer {token}")),
                ("Accept".into(), "application/vnd.github+json".into()),
                ("User-Agent".into(), "dbt-lineage".into()),
            ];
            let list = format!("{api}/repos/{repo}/issues/{number}/comments?per_page=100");
            let existing = find_paged(http, &list, &headers, "id", |c| c["body"].as_str())?;
            let (method, url, outcome) = match existing {
                Some(id) => (
                    "PATCH",
                    format!("{api}/repos/{repo}/issues/comments/{id}"),
                    Outcome::Updated,
                ),
                None => (
                    "POST",
                    format!("{api}/repos/{repo}/issues/{number}/comments"),
                    Outcome::Created,
                ),
            };
            let req = Request {
                method,
                url,
                headers,
                body: Some(json!({ "body": body })),
            };
            let (status, resp) = http.send(&req)?;
            check(status, &resp, "GitHub comment")?;
            Ok(outcome)
        }
        Target::GitLab {
            api,
            project,
            iid,
            token,
        } => {
            let headers = vec![("PRIVATE-TOKEN".into(), token.clone())];
            let base = format!(
                "{api}/projects/{}/merge_requests/{iid}/notes",
                urlencode(project)
            );
            let list = format!("{base}?per_page=100&sort=asc");
            let existing = find_paged(http, &list, &headers, "id", |n| n["body"].as_str())?;
            let (method, url, outcome) = match existing {
                Some(id) => ("PUT", format!("{base}/{id}"), Outcome::Updated),
                None => ("POST", base, Outcome::Created),
            };
            let req = Request {
                method,
                url,
                headers,
                body: Some(json!({ "body": body })),
            };
            let (status, resp) = http.send(&req)?;
            check(status, &resp, "GitLab note")?;
            Ok(outcome)
        }
        Target::Bitbucket {
            workspace,
            repo,
            pr,
            token,
        } => {
            let headers = vec![("Authorization".into(), format!("Bearer {token}"))];
            let base = format!(
                "https://api.bitbucket.org/2.0/repositories/{workspace}/{repo}/pullrequests/{pr}/comments"
            );
            // Bitbucket paginates with a `next` URL.
            let mut next = Some(format!("{base}?pagelen=100"));
            let mut existing = None;
            while let (Some(url), None) = (next.take(), existing) {
                let (status, page) = http.send(&Request {
                    method: "GET",
                    url,
                    headers: headers.clone(),
                    body: None,
                })?;
                check(status, &page, "Bitbucket comment list")?;
                existing = page["values"].as_array().and_then(|values| {
                    values.iter().find_map(|c| {
                        c["content"]["raw"]
                            .as_str()
                            .filter(|raw| raw.contains(COMMENT_MARKER))
                            .and(c["id"].as_u64())
                    })
                });
                next = page["next"].as_str().map(String::from);
            }
            let (method, url, outcome) = match existing {
                Some(id) => ("PUT", format!("{base}/{id}"), Outcome::Updated),
                None => ("POST", base, Outcome::Created),
            };
            let req = Request {
                method,
                url,
                headers,
                body: Some(json!({ "content": { "raw": body } })),
            };
            let (status, resp) = http.send(&req)?;
            check(status, &resp, "Bitbucket comment")?;
            Ok(outcome)
        }
        Target::Azure {
            collection,
            project,
            repo_id,
            pr,
            token,
        } => {
            let headers = vec![("Authorization".into(), format!("Bearer {token}"))];
            let base = format!(
                "{collection}/{}/_apis/git/repositories/{repo_id}/pullRequests/{pr}/threads",
                urlencode(project)
            );
            let (status, threads) = http.send(&Request {
                method: "GET",
                url: format!("{base}?api-version=7.1"),
                headers: headers.clone(),
                body: None,
            })?;
            check(status, &threads, "Azure DevOps thread list")?;
            // The sticky comment is the first comment of its thread.
            let existing = threads["value"].as_array().and_then(|ts| {
                ts.iter().find_map(|t| {
                    let c = &t["comments"][0];
                    c["content"]
                        .as_str()
                        .filter(|s| s.contains(COMMENT_MARKER))
                        .and(Some((t["id"].as_u64()?, c["id"].as_u64()?)))
                })
            });
            let (req, outcome) = match existing {
                Some((thread, comment)) => (
                    Request {
                        method: "PATCH",
                        url: format!("{base}/{thread}/comments/{comment}?api-version=7.1"),
                        headers,
                        body: Some(json!({ "content": body })),
                    },
                    Outcome::Updated,
                ),
                None => (
                    Request {
                        method: "POST",
                        url: format!("{base}?api-version=7.1"),
                        headers,
                        body: Some(json!({
                            "comments": [{ "parentCommentId": 0, "content": body, "commentType": 1 }],
                            "status": "closed"
                        })),
                    },
                    Outcome::Created,
                ),
            };
            let (status, resp) = http.send(&req)?;
            check(status, &resp, "Azure DevOps comment")?;
            Ok(outcome)
        }
    }
}

/// Walk `page=1,2,…` of a JSON array endpoint (GitHub/GitLab style) and
/// return the `id_key` of the first item whose text contains the marker.
fn find_paged(
    http: &dyn Http,
    list_url: &str,
    headers: &[(String, String)],
    id_key: &str,
    text: impl Fn(&Value) -> Option<&str>,
) -> Result<Option<u64>> {
    for page in 1..=50 {
        let (status, items) = http.send(&Request {
            method: "GET",
            url: format!("{list_url}&page={page}"),
            headers: headers.to_vec(),
            body: None,
        })?;
        check(status, &items, "comment list")?;
        let Some(items) = items.as_array() else {
            break;
        };
        if let Some(id) = items.iter().find_map(|c| {
            text(c)
                .filter(|t| t.contains(COMMENT_MARKER))
                .and(c[id_key].as_u64())
        }) {
            return Ok(Some(id));
        }
        if items.len() < 100 {
            break;
        }
    }
    Ok(None)
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Real transport over `ureq`.
pub struct UreqHttp;

impl Http for UreqHttp {
    fn send(&self, req: &Request) -> Result<(u16, Value)> {
        send_with(&ureq::agent(), req)
    }
}

/// `ureq` transport through an HTTP forward proxy (e.g. the Bitbucket
/// Pipelines proxy at `http://localhost:29418`, which adds authentication).
pub struct ProxyHttp {
    agent: ureq::Agent,
}

impl ProxyHttp {
    pub fn new(proxy: &str) -> Result<Self> {
        let proxy = ureq::Proxy::new(proxy).with_context(|| format!("invalid proxy {proxy}"))?;
        Ok(ProxyHttp {
            agent: ureq::AgentBuilder::new().proxy(proxy).build(),
        })
    }
}

impl Http for ProxyHttp {
    fn send(&self, req: &Request) -> Result<(u16, Value)> {
        send_with(&self.agent, req)
    }
}

fn send_with(agent: &ureq::Agent, req: &Request) -> Result<(u16, Value)> {
    let mut r = agent.request(req.method, &req.url);
    for (k, v) in &req.headers {
        r = r.set(k, v);
    }
    let result = match &req.body {
        Some(body) => r.send_json(body.clone()),
        None => r.call(),
    };
    let resp = match result {
        Ok(resp) => resp,
        Err(ureq::Error::Status(_, resp)) => resp,
        Err(e) => return Err(anyhow::Error::new(e).context(format!("{} {}", req.method, req.url))),
    };
    let status = resp.status();
    let text = resp.into_string().unwrap_or_default();
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Replays canned responses in order and records every request.
    struct Fake {
        responses: RefCell<Vec<(u16, Value)>>,
        seen: RefCell<Vec<Request>>,
    }

    impl Fake {
        fn new(responses: Vec<(u16, Value)>) -> Self {
            Fake {
                responses: RefCell::new(responses.into_iter().rev().collect()),
                seen: RefCell::new(vec![]),
            }
        }
    }

    impl Http for Fake {
        fn send(&self, req: &Request) -> Result<(u16, Value)> {
            self.seen.borrow_mut().push(req.clone());
            Ok(self
                .responses
                .borrow_mut()
                .pop()
                .expect("unexpected request"))
        }
    }

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn github() -> Target {
        Target::GitHub {
            api: "https://api.github.com".into(),
            repo: "o/r".into(),
            number: 7,
            token: "t".into(),
        }
    }

    #[test]
    fn github_creates_then_updates() {
        let fake = Fake::new(vec![
            (200, json!([{"id": 1, "body": "unrelated"}])),
            (201, json!({"id": 2})),
        ]);
        assert_eq!(
            upsert(&fake, &github(), "report").unwrap(),
            Outcome::Created
        );
        let seen = fake.seen.borrow();
        assert_eq!(seen[1].method, "POST");
        assert_eq!(
            seen[1].url,
            "https://api.github.com/repos/o/r/issues/7/comments"
        );
        let body = seen[1].body.as_ref().unwrap()["body"].as_str().unwrap();
        assert!(body.starts_with(COMMENT_MARKER));

        let fake = Fake::new(vec![
            (
                200,
                json!([{"id": 5, "body": format!("{COMMENT_MARKER}\nold")}]),
            ),
            (200, json!({"id": 5})),
        ]);
        assert_eq!(upsert(&fake, &github(), "new").unwrap(), Outcome::Updated);
        let seen = fake.seen.borrow();
        assert_eq!(seen[1].method, "PATCH");
        assert_eq!(
            seen[1].url,
            "https://api.github.com/repos/o/r/issues/comments/5"
        );
    }

    #[test]
    fn github_pages_through_comments() {
        let full_page: Vec<Value> = (0..100).map(|i| json!({"id": i, "body": "x"})).collect();
        let fake = Fake::new(vec![
            (200, Value::Array(full_page)),
            (200, json!([{"id": 500, "body": COMMENT_MARKER}])),
            (200, json!({})),
        ]);
        assert_eq!(upsert(&fake, &github(), "r").unwrap(), Outcome::Updated);
        let seen = fake.seen.borrow();
        assert!(seen[0].url.ends_with("&page=1"));
        assert!(seen[1].url.ends_with("&page=2"));
        assert!(seen[2].url.ends_with("/issues/comments/500"));
    }

    #[test]
    fn http_errors_are_reported() {
        let fake = Fake::new(vec![(200, json!([])), (403, json!({"message": "nope"}))]);
        let err = upsert(&fake, &github(), "r").unwrap_err().to_string();
        assert!(err.contains("HTTP 403"), "{err}");
    }

    #[test]
    fn gitlab_updates_existing_note() {
        let target = Target::GitLab {
            api: "https://gitlab.com/api/v4".into(),
            project: "group/proj".into(),
            iid: 3,
            token: "t".into(),
        };
        let fake = Fake::new(vec![
            (
                200,
                json!([{"id": 9, "body": format!("{COMMENT_MARKER}\nold")}]),
            ),
            (200, json!({})),
        ]);
        assert_eq!(upsert(&fake, &target, "r").unwrap(), Outcome::Updated);
        let seen = fake.seen.borrow();
        assert_eq!(seen[1].method, "PUT");
        assert_eq!(
            seen[1].url,
            "https://gitlab.com/api/v4/projects/group%2Fproj/merge_requests/3/notes/9"
        );
        assert_eq!(seen[1].headers[0], ("PRIVATE-TOKEN".into(), "t".into()));
    }

    #[test]
    fn bitbucket_follows_next_and_creates() {
        let target = Target::Bitbucket {
            workspace: "ws".into(),
            repo: "r".into(),
            pr: 4,
            token: "t".into(),
        };
        let fake = Fake::new(vec![
            (
                200,
                json!({"values": [{"id": 1, "content": {"raw": "hi"}}], "next": "https://next"}),
            ),
            (200, json!({"values": []})),
            (201, json!({})),
        ]);
        assert_eq!(upsert(&fake, &target, "r").unwrap(), Outcome::Created);
        let seen = fake.seen.borrow();
        assert_eq!(seen[1].url, "https://next");
        assert_eq!(seen[2].method, "POST");
        assert!(seen[2].body.as_ref().unwrap()["content"]["raw"]
            .as_str()
            .unwrap()
            .contains(COMMENT_MARKER));
    }

    #[test]
    fn azure_patches_first_comment_of_marked_thread() {
        let target = Target::Azure {
            collection: "https://dev.azure.com/org/".trim_end_matches('/').into(),
            project: "My Proj".into(),
            repo_id: "rid".into(),
            pr: 12,
            token: "t".into(),
        };
        let fake = Fake::new(vec![
            (
                200,
                json!({"value": [
                    {"id": 1, "comments": [{"id": 1, "content": "other"}]},
                    {"id": 2, "comments": [{"id": 1, "content": format!("{COMMENT_MARKER} x")}]}
                ]}),
            ),
            (200, json!({})),
        ]);
        assert_eq!(upsert(&fake, &target, "r").unwrap(), Outcome::Updated);
        let seen = fake.seen.borrow();
        assert_eq!(
            seen[1].url,
            "https://dev.azure.com/org/My%20Proj/_apis/git/repositories/rid/pullRequests/12/threads/2/comments/1?api-version=7.1"
        );
        assert_eq!(seen[1].method, "PATCH");
    }

    #[test]
    fn targets_from_env() {
        let dir = tempfile::tempdir().unwrap();
        let event = dir.path().join("event.json");
        std::fs::write(&event, r#"{"pull_request": {"number": 42}}"#).unwrap();
        let t = Target::from_env(
            Platform::GitHub,
            &env(&[
                ("GITHUB_EVENT_PATH", event.to_str().unwrap()),
                ("GITHUB_REPOSITORY", "o/r"),
                ("GITHUB_TOKEN", "gh"),
            ]),
        )
        .unwrap();
        assert!(matches!(t, Target::GitHub { number: 42, ref token, .. } if token == "gh"));

        // GITHUB_REF fallback and DBT_LINEAGE_TOKEN override.
        let t = Target::from_env(
            Platform::GitHub,
            &env(&[
                ("GITHUB_REF", "refs/pull/8/merge"),
                ("GITHUB_REPOSITORY", "o/r"),
                ("GITHUB_TOKEN", "gh"),
                ("DBT_LINEAGE_TOKEN", "pat"),
            ]),
        )
        .unwrap();
        assert!(matches!(t, Target::GitHub { number: 8, ref token, .. } if token == "pat"));

        let err = Target::from_env(
            Platform::GitLab,
            &env(&[
                ("CI_API_V4_URL", "https://gitlab.com/api/v4"),
                ("CI_PROJECT_ID", "1"),
                ("CI_MERGE_REQUEST_IID", "2"),
            ]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("CI_JOB_TOKEN cannot post notes"), "{err}");

        assert!(Target::from_env(Platform::Unknown, &env(&[])).is_err());
    }
}
