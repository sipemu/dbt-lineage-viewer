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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_display() {
        let err = DbtLineageError::ProjectNotFound(PathBuf::from("/foo"));
        assert_eq!(
            err.to_string(),
            "dbt project not found: no dbt_project.yml in /foo"
        );

        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = DbtLineageError::FileReadError {
            path: PathBuf::from("/bar.sql"),
            source: io_err,
        };
        assert!(err.to_string().contains("/bar.sql"));

        let err = DbtLineageError::ModelNotFound("orders".into());
        assert_eq!(err.to_string(), "model not found: orders");

        let err = DbtLineageError::CycleDetected;
        assert_eq!(err.to_string(), "cycle detected in lineage graph");

        let err = DbtLineageError::DuplicateModel {
            name: "orders".into(),
            path1: PathBuf::from("a.sql"),
            path2: PathBuf::from("b.sql"),
        };
        assert!(err.to_string().contains("duplicate model name"));
        assert!(err.to_string().contains("orders"));
    }
}
