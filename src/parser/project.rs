use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::error::DbtLineageError;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DbtProject {
    pub name: String,

    #[serde(rename = "model-paths", default = "default_model_paths")]
    pub model_paths: Vec<String>,

    #[serde(rename = "seed-paths", default = "default_seed_paths")]
    pub seed_paths: Vec<String>,

    #[serde(rename = "snapshot-paths", default = "default_snapshot_paths")]
    pub snapshot_paths: Vec<String>,

    #[serde(rename = "test-paths", default = "default_test_paths")]
    pub test_paths: Vec<String>,
}

fn default_model_paths() -> Vec<String> {
    vec!["models".to_string()]
}

fn default_seed_paths() -> Vec<String> {
    vec!["seeds".to_string()]
}

fn default_snapshot_paths() -> Vec<String> {
    vec!["snapshots".to_string()]
}

fn default_test_paths() -> Vec<String> {
    vec!["tests".to_string()]
}

impl DbtProject {
    pub fn load(project_dir: &Path) -> Result<Self> {
        let project_file = project_dir.join("dbt_project.yml");
        if !project_file.exists() {
            return Err(DbtLineageError::ProjectNotFound(project_dir.to_path_buf()).into());
        }

        let content =
            std::fs::read_to_string(&project_file).map_err(|e| DbtLineageError::FileReadError {
                path: project_file.clone(),
                source: e,
            })?;

        let project: DbtProject = serde_yaml::from_str(&content)
            .context(format!("Failed to parse {}", project_file.display()))?;

        Ok(project)
    }

    pub fn resolve_paths(&self, project_dir: &Path) -> ResolvedPaths {
        ResolvedPaths {
            model_paths: self
                .model_paths
                .iter()
                .map(|p| project_dir.join(p))
                .collect(),
            seed_paths: self
                .seed_paths
                .iter()
                .map(|p| project_dir.join(p))
                .collect(),
            snapshot_paths: self
                .snapshot_paths
                .iter()
                .map(|p| project_dir.join(p))
                .collect(),
            test_paths: self
                .test_paths
                .iter()
                .map(|p| project_dir.join(p))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct ResolvedPaths {
    pub model_paths: Vec<PathBuf>,
    pub seed_paths: Vec<PathBuf>,
    pub snapshot_paths: Vec<PathBuf>,
    pub test_paths: Vec<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let yaml = "name: my_project\n";
        let project: DbtProject = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(project.name, "my_project");
        assert_eq!(project.model_paths, vec!["models"]);
        assert_eq!(project.seed_paths, vec!["seeds"]);
        assert_eq!(project.snapshot_paths, vec!["snapshots"]);
        assert_eq!(project.test_paths, vec!["tests"]);
    }

    #[test]
    fn test_custom_paths() {
        let yaml = r#"
name: my_project
model-paths: ["models", "extra_models"]
seed-paths: ["data"]
"#;
        let project: DbtProject = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(project.model_paths, vec!["models", "extra_models"]);
        assert_eq!(project.seed_paths, vec!["data"]);
        assert_eq!(project.snapshot_paths, vec!["snapshots"]); // default
    }
}
