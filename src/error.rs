use std::path::PathBuf;

use serde::Serialize;
use thiserror::Error;

/// Agent/CI-friendly structured form of an error. Emitted to stderr as one JSON
/// object per line when `--error-format json` is set.
#[derive(Debug, Clone, Serialize)]
pub struct StructuredError {
    /// Always `"error"` for now; reserved for `"warning"` / `"info"` in future.
    pub level: &'static str,
    /// Short noun phrase identifying the problem class (e.g. `"model not found"`).
    pub what: String,
    /// Concrete reason for this occurrence (e.g. `"model 'orders' is not in the graph"`).
    pub why: String,
    /// Actionable suggestion, when one exists.
    pub hint: Option<String>,
}

impl StructuredError {
    pub fn new(what: impl Into<String>, why: impl Into<String>) -> Self {
        Self {
            level: "error",
            what: what.into(),
            why: why.into(),
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

/// Map any `anyhow::Error` to a `StructuredError`. When the error chain contains
/// a `DbtLineageError`, that variant's known shape is used; otherwise the display
/// string is reported as `why` with a generic `what`.
pub fn classify(error: &anyhow::Error) -> StructuredError {
    if let Some(dbt) = error.downcast_ref::<DbtLineageError>() {
        return dbt.to_structured();
    }
    // Walk the chain for a DbtLineageError further down.
    for cause in error.chain() {
        if let Some(dbt) = cause.downcast_ref::<DbtLineageError>() {
            return dbt.to_structured();
        }
    }
    StructuredError::new("error", error.to_string())
}

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

impl DbtLineageError {
    /// Per-variant mapping to the structured form used by `--error-format json`.
    pub fn to_structured(&self) -> StructuredError {
        match self {
            DbtLineageError::ProjectNotFound(path) => StructuredError::new(
                "dbt project not found",
                format!("no dbt_project.yml in {}", path.display()),
            )
            .with_hint("pass --project-dir <path>, or cd into the project directory"),
            DbtLineageError::FileReadError { path, source } => {
                StructuredError::new("file read error", format!("{}: {}", path.display(), source))
            }
            DbtLineageError::YamlParseError { path, source } => StructuredError::new(
                "yaml parse error",
                format!("{}: {}", path.display(), source),
            ),
            DbtLineageError::ModelNotFound(name) => StructuredError::new(
                "model not found",
                format!("model '{}' is not in the project graph", name),
            )
            .with_hint("run 'dbt-lineage list' (or 'dbt-lineage summary') to see available models"),
            DbtLineageError::CycleDetected => StructuredError::new(
                "cycle detected",
                "the lineage graph contains a cycle".to_string(),
            ),
            DbtLineageError::DuplicateModel { name, path1, path2 } => StructuredError::new(
                "duplicate model",
                format!(
                    "model '{}' is defined in both {} and {}",
                    name,
                    path1.display(),
                    path2.display()
                ),
            ),
            DbtLineageError::ArtifactParseError { path, source } => StructuredError::new(
                "artifact parse error",
                format!("{}: {}", path.display(), source),
            )
            .with_hint("check that the file was produced by a current 'dbt compile'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_classify_model_not_found() {
        let err: anyhow::Error = DbtLineageError::ModelNotFound("orders".into()).into();
        let s = classify(&err);
        assert_eq!(s.level, "error");
        assert_eq!(s.what, "model not found");
        assert!(s.why.contains("orders"));
        assert!(s.hint.unwrap().contains("dbt-lineage list"));
    }

    #[test]
    fn test_classify_project_not_found() {
        let err: anyhow::Error = DbtLineageError::ProjectNotFound(PathBuf::from("/tmp/x")).into();
        let s = classify(&err);
        assert_eq!(s.what, "dbt project not found");
        assert!(s.why.contains("/tmp/x"));
        assert!(s.hint.is_some());
    }

    #[test]
    fn test_classify_generic_anyhow() {
        let err = anyhow::anyhow!("something else broke");
        let s = classify(&err);
        // Falls through to the generic mapping.
        assert_eq!(s.what, "error");
        assert_eq!(s.why, "something else broke");
        assert!(s.hint.is_none());
    }

    #[test]
    fn test_classify_walks_chain() {
        // Wrap a DbtLineageError inside an anyhow context layer to ensure the
        // chain walk picks it up via downcast.
        let inner: anyhow::Error = DbtLineageError::CycleDetected.into();
        let outer = inner.context("while building graph");
        let s = classify(&outer);
        assert_eq!(s.what, "cycle detected");
    }

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
