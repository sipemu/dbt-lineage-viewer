use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum DbtLineageError {
    #[error("dbt project not found: no dbt_project.yml in {0}")]
    ProjectNotFound(PathBuf),

    #[error("failed to read file {path}: {source}")]
    FileReadError {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse YAML in {path}: {source}")]
    YamlParseError {
        path: PathBuf,
        source: serde_yaml::Error,
    },

    #[error("model not found: {0}")]
    ModelNotFound(String),

    #[error("cycle detected in lineage graph")]
    CycleDetected,

    #[error("duplicate model name '{name}' found in {path1} and {path2}")]
    DuplicateModel {
        name: String,
        path1: PathBuf,
        path2: PathBuf,
    },

    #[error("failed to parse artifact {path}: {source}")]
    ArtifactParseError {
        path: PathBuf,
        source: serde_json::Error,
    },
}
