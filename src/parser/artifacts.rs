use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::graph::types::LineageGraph;

#[derive(Debug, Deserialize)]
pub struct RunResults {
    pub results: Vec<RunResult>,
}

#[derive(Debug, Deserialize)]
pub struct RunResult {
    pub unique_id: String,
    pub status: String,
    pub message: Option<String>,
    pub timing: Option<Vec<TimingEntry>>,
}

#[derive(Debug, Deserialize)]
pub struct TimingEntry {
    #[allow(dead_code)]
    pub name: String,
    pub completed_at: Option<DateTime<Utc>>,
}

impl RunResult {
    /// Get the completion timestamp from the last timing entry
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.timing
            .as_ref()
            .and_then(|entries| entries.iter().rev().find_map(|t| t.completed_at))
    }
}

/// Load `target/run_results.json` from the project directory.
/// Returns `None` if the file doesn't exist.
pub fn load_run_results(project_dir: &Path) -> Result<Option<RunResults>> {
    let path = project_dir.join("target").join("run_results.json");
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path)?;
    let results: RunResults = serde_json::from_str(&content)?;
    Ok(Some(results))
}

/// Run status for a single node
#[derive(Debug, Clone)]
pub enum RunStatus {
    NeverRun,
    Success {
        completed_at: DateTime<Utc>,
    },
    Error {
        completed_at: Option<DateTime<Utc>>,
        message: String,
    },
    Skipped {
        #[allow(dead_code)]
        completed_at: Option<DateTime<Utc>>,
    },
    Outdated {
        run_at: DateTime<Utc>,
        #[allow(dead_code)]
        modified_at: std::time::SystemTime,
    },
}

pub type RunStatusMap = HashMap<String, RunStatus>;

/// Build a map from graph unique_id → RunStatus.
///
/// dbt uses unique_ids like `model.my_project.stg_orders`, while the graph uses
/// `model.stg_orders`. We match by comparing `{type}.{last_segment}`.
pub fn build_run_status_map(
    run_results: &RunResults,
    graph: &LineageGraph,
    project_dir: &Path,
) -> RunStatusMap {
    // Build a lookup from simplified dbt unique_id to RunResult
    let dbt_lookup = build_dbt_lookup(run_results);

    let mut status_map = RunStatusMap::new();

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let simplified = simplify_graph_unique_id(&node.unique_id);

        let status = resolve_run_status(dbt_lookup.get(&simplified).copied(), node, project_dir);
        status_map.insert(node.unique_id.clone(), status);
    }

    status_map
}

/// Merge new run results into an existing status map.
/// Only updates nodes present in the new results; leaves others untouched.
pub fn merge_run_status_map(
    existing: &mut RunStatusMap,
    run_results: &RunResults,
    graph: &LineageGraph,
    project_dir: &Path,
) {
    let dbt_lookup = build_dbt_lookup(run_results);

    for idx in graph.node_indices() {
        let node = &graph[idx];
        let simplified = simplify_graph_unique_id(&node.unique_id);

        if let Some(result) = dbt_lookup.get(&simplified) {
            let status = resolve_run_status(Some(result), node, project_dir);
            existing.insert(node.unique_id.clone(), status);
        }
    }
}

fn build_dbt_lookup(run_results: &RunResults) -> HashMap<String, &RunResult> {
    let mut dbt_lookup: HashMap<String, &RunResult> = HashMap::new();
    for result in &run_results.results {
        if let Some(simplified) = simplify_dbt_unique_id(&result.unique_id) {
            dbt_lookup.insert(simplified, result);
        }
    }
    dbt_lookup
}

fn resolve_run_status(
    result: Option<&RunResult>,
    node: &crate::graph::types::NodeData,
    project_dir: &Path,
) -> RunStatus {
    match result {
        None => RunStatus::NeverRun,
        Some(result) => match result.status.as_str() {
            "success" | "pass" => {
                if let Some(completed) = result.completed_at() {
                    // Check freshness for nodes with file_path
                    if let Some(ref file_path) = node.file_path {
                        let full_path = project_dir.join(file_path);
                        if let Ok(metadata) = fs::metadata(&full_path) {
                            if let Ok(modified) = metadata.modified() {
                                let mod_dt: DateTime<Utc> = modified.into();
                                if mod_dt > completed {
                                    return RunStatus::Outdated {
                                        run_at: completed,
                                        modified_at: modified,
                                    };
                                }
                            }
                        }
                    }
                    RunStatus::Success {
                        completed_at: completed,
                    }
                } else {
                    RunStatus::Success {
                        completed_at: Utc::now(),
                    }
                }
            }
            "error" | "fail" => RunStatus::Error {
                completed_at: result.completed_at(),
                message: result
                    .message
                    .clone()
                    .unwrap_or_else(|| "Unknown error".to_string()),
            },
            "skipped" | "skip" => RunStatus::Skipped {
                completed_at: result.completed_at(),
            },
            _ => RunStatus::NeverRun,
        },
    }
}

/// Simplify a dbt unique_id like `model.my_project.stg_orders` to `model.stg_orders`
fn simplify_dbt_unique_id(unique_id: &str) -> Option<String> {
    let parts: Vec<&str> = unique_id.split('.').collect();
    if parts.len() >= 3 {
        // type.project.name → type.name
        Some(format!("{}.{}", parts[0], parts[parts.len() - 1]))
    } else if parts.len() == 2 {
        Some(unique_id.to_string())
    } else {
        None
    }
}

/// Simplify graph unique_id — already in `type.name` form, but handle source.schema.name
fn simplify_graph_unique_id(unique_id: &str) -> String {
    let parts: Vec<&str> = unique_id.split('.').collect();
    if parts.len() >= 3 {
        format!("{}.{}", parts[0], parts[parts.len() - 1])
    } else {
        unique_id.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simplify_dbt_unique_id() {
        assert_eq!(
            simplify_dbt_unique_id("model.my_project.stg_orders"),
            Some("model.stg_orders".to_string())
        );
        assert_eq!(
            simplify_dbt_unique_id("source.my_project.raw.orders"),
            Some("source.orders".to_string())
        );
        assert_eq!(
            simplify_dbt_unique_id("model.stg_orders"),
            Some("model.stg_orders".to_string())
        );
        assert_eq!(simplify_dbt_unique_id("model"), None);
    }

    #[test]
    fn test_simplify_graph_unique_id() {
        assert_eq!(
            simplify_graph_unique_id("model.stg_orders"),
            "model.stg_orders"
        );
        assert_eq!(
            simplify_graph_unique_id("source.raw.orders"),
            "source.orders"
        );
    }

    #[test]
    fn test_load_nonexistent_run_results() {
        let result = load_run_results(Path::new("/nonexistent/path")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_run_results() {
        let json = r#"{
            "results": [
                {
                    "unique_id": "model.my_project.stg_orders",
                    "status": "success",
                    "message": "OK",
                    "timing": [
                        {
                            "name": "execute",
                            "completed_at": "2025-01-15T10:30:00Z"
                        }
                    ]
                },
                {
                    "unique_id": "model.my_project.orders",
                    "status": "error",
                    "message": "Compilation Error",
                    "timing": []
                }
            ]
        }"#;

        let results: RunResults = serde_json::from_str(json).unwrap();
        assert_eq!(results.results.len(), 2);
        assert_eq!(results.results[0].status, "success");
        assert!(results.results[0].completed_at().is_some());
        assert_eq!(results.results[1].status, "error");
    }
}
