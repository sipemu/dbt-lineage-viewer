//! Helpers shared by the `propose_*` MCP tools. Build apply-ready unified diffs
//! against the actual schema YAML files in a dbt project.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use crate::parser::yaml_schema::{parse_schema_file, ModelDefinition, TestDefinition};

/// What `propose_test` was asked to do.
#[derive(Debug, Clone)]
pub struct ProposeTestRequest<'a> {
    pub project_dir: &'a Path,
    pub model: &'a str,
    pub column: &'a str,
    pub kind: &'a str,
    /// Optional file paths to scan. When empty, the helper walks the project.
    pub yaml_files: Vec<PathBuf>,
}

/// The result of building a test proposal.
#[derive(Debug, Clone)]
pub struct ProposeTestResult {
    /// Schema YAML file the diff applies to. May not exist yet.
    pub file: PathBuf,
    /// Unified diff text against `file`. Empty when `already_present` is true.
    pub diff: String,
    /// True when the requested test was already on the column — nothing to apply.
    pub already_present: bool,
    /// True when we had to create a new schema YAML for this model.
    pub new_file: bool,
}

/// Walk the project to find any schema YAML that mentions the model. Returns the
/// first match. None when nothing references the model yet.
pub fn locate_schema_for_model(project_dir: &Path, model: &str) -> Option<PathBuf> {
    let candidates = discover_yaml_files(project_dir);
    candidates
        .into_iter()
        .find(|p| yaml_mentions_model(p, model))
}

fn discover_yaml_files(project_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    const DBT_ROOTS: &[&str] = &["models", "seeds", "snapshots", "tests", "analyses"];
    for root in DBT_ROOTS {
        let dir = project_dir.join(root);
        if !dir.exists() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
            if ext.eq_ignore_ascii_case("yml") || ext.eq_ignore_ascii_case("yaml") {
                out.push(p.to_path_buf());
            }
        }
    }
    out
}

fn yaml_mentions_model(path: &Path, model: &str) -> bool {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let parsed = match parse_schema_file(&content) {
        Ok(p) => p,
        Err(_) => return false,
    };
    parsed.models.iter().any(|m| m.name == model)
}

/// Validate the test kind early so we fail before touching the filesystem.
pub fn validate_kind(kind: &str) -> Result<()> {
    if matches!(
        kind,
        "not_null" | "unique" | "accepted_values" | "relationships"
    ) {
        Ok(())
    } else {
        Err(anyhow!(
            "unsupported kind '{}' (expected not_null|unique|accepted_values|relationships)",
            kind
        ))
    }
}

/// Build the proposal. Returns idempotently when the test already exists. When
/// no schema YAML is found, suggests creating `models/<model>.yml`.
pub fn build_proposal(req: &ProposeTestRequest<'_>) -> Result<ProposeTestResult> {
    validate_kind(req.kind)?;

    let file = locate_schema_for_model(req.project_dir, req.model);

    let (target_file, original, new_file) = match file {
        Some(path) => {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| anyhow!("read {}: {}", path.display(), e))?;
            (path, content, false)
        }
        None => (
            req.project_dir
                .join("models")
                .join(format!("{}.yml", req.model)),
            String::new(),
            true,
        ),
    };

    let new_content = upsert_test_in_yaml(&original, req.model, req.column, req.kind)?;

    if new_content == original {
        return Ok(ProposeTestResult {
            file: target_file,
            diff: String::new(),
            already_present: true,
            new_file: false,
        });
    }

    let diff = unified_diff(&original, &new_content, &target_file);
    Ok(ProposeTestResult {
        file: target_file,
        diff,
        already_present: false,
        new_file,
    })
}

/// Insert (or no-op) a generic test on a column inside a schema YAML document.
/// Parses, mutates, and re-serializes via serde_yaml. Comment-preservation is
/// out of scope for v1 — diffs are still apply-ready and reviewable.
pub fn upsert_test_in_yaml(
    original: &str,
    model_name: &str,
    column_name: &str,
    test_kind: &str,
) -> Result<String> {
    let mut doc: serde_yaml::Mapping = if original.trim().is_empty() {
        serde_yaml::Mapping::new()
    } else {
        match serde_yaml::from_str::<serde_yaml::Value>(original)? {
            serde_yaml::Value::Mapping(m) => m,
            _ => return Err(anyhow!("schema YAML must be a mapping at the top level")),
        }
    };

    let models = doc
        .entry(serde_yaml::Value::String("models".into()))
        .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()));
    let models_seq = match models {
        serde_yaml::Value::Sequence(s) => s,
        _ => return Err(anyhow!("top-level `models:` must be a sequence")),
    };

    let model_pos = models_seq.iter().position(|m| {
        m.get("name")
            .and_then(|n| n.as_str())
            .map(|n| n == model_name)
            .unwrap_or(false)
    });
    let model_value = match model_pos {
        Some(i) => &mut models_seq[i],
        None => {
            let mut new_model = serde_yaml::Mapping::new();
            new_model.insert(
                serde_yaml::Value::String("name".into()),
                serde_yaml::Value::String(model_name.into()),
            );
            new_model.insert(
                serde_yaml::Value::String("columns".into()),
                serde_yaml::Value::Sequence(Vec::new()),
            );
            models_seq.push(serde_yaml::Value::Mapping(new_model));
            models_seq.last_mut().unwrap()
        }
    };

    let columns = model_value
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("model '{}' entry is not a mapping", model_name))?
        .entry(serde_yaml::Value::String("columns".into()))
        .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()));
    let columns_seq = match columns {
        serde_yaml::Value::Sequence(s) => s,
        _ => return Err(anyhow!("model.columns must be a sequence")),
    };
    let col_pos = columns_seq.iter().position(|c| {
        c.get("name")
            .and_then(|n| n.as_str())
            .map(|n| n == column_name)
            .unwrap_or(false)
    });
    let col_value = match col_pos {
        Some(i) => &mut columns_seq[i],
        None => {
            let mut new_col = serde_yaml::Mapping::new();
            new_col.insert(
                serde_yaml::Value::String("name".into()),
                serde_yaml::Value::String(column_name.into()),
            );
            columns_seq.push(serde_yaml::Value::Mapping(new_col));
            columns_seq.last_mut().unwrap()
        }
    };

    let col_map = col_value
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("column '{}' entry is not a mapping", column_name))?;
    let tests = col_map
        .entry(serde_yaml::Value::String("tests".into()))
        .or_insert_with(|| serde_yaml::Value::Sequence(Vec::new()));
    let tests_seq = match tests {
        serde_yaml::Value::Sequence(s) => s,
        _ => return Err(anyhow!("column.tests must be a sequence")),
    };
    if tests_seq.iter().any(|t| equals_test_kind(t, test_kind)) {
        // Already present — return the original text untouched so the caller can
        // detect no-op via direct equality. Avoids serde_yaml round-trip drift.
        return Ok(original.to_string());
    }
    tests_seq.push(serde_yaml::Value::String(test_kind.into()));

    Ok(serde_yaml::to_string(&serde_yaml::Value::Mapping(doc))?)
}

fn equals_test_kind(v: &serde_yaml::Value, kind: &str) -> bool {
    if let Some(s) = v.as_str() {
        return s == kind;
    }
    if let Some(m) = v.as_mapping() {
        if let Some((k, _)) = m.iter().next() {
            return k.as_str() == Some(kind);
        }
    }
    false
}

/// Produce a minimal unified diff. Only `--- a/file` / `+++ b/file` headers and
/// a single hunk that spans the whole document — agents and humans can review,
/// and `patch` / `git apply` accept this shape.
fn unified_diff(old: &str, new: &str, path: &Path) -> String {
    let label = path.display().to_string();
    let old_lines: Vec<&str> = old.split_inclusive('\n').collect();
    let new_lines: Vec<&str> = new.split_inclusive('\n').collect();

    let mut out = String::new();
    out.push_str(&format!("--- a/{}\n", label));
    out.push_str(&format!("+++ b/{}\n", label));
    out.push_str(&format!(
        "@@ -1,{} +1,{} @@\n",
        old_lines.len(),
        new_lines.len()
    ));
    for line in &old_lines {
        out.push('-');
        out.push_str(line);
        if !line.ends_with('\n') {
            out.push('\n');
        }
    }
    for line in &new_lines {
        out.push('+');
        out.push_str(line);
        if !line.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// Helper for tests in other files: count tests on a column for a specific kind.
#[allow(dead_code)]
pub fn count_tests(model: &ModelDefinition, column: &str, kind: &str) -> usize {
    model
        .columns
        .iter()
        .filter(|c| c.name == column)
        .flat_map(|c| c.tests.iter())
        .filter(|t| match t {
            TestDefinition::Simple(s) => s == kind,
            TestDefinition::Complex(v) => v
                .as_mapping()
                .is_some_and(|m| m.iter().next().and_then(|(k, _)| k.as_str()) == Some(kind)),
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_upsert_adds_test_to_existing_column() {
        let yaml = "models:\n  - name: orders\n    columns:\n      - name: order_id\n";
        let out = upsert_test_in_yaml(yaml, "orders", "order_id", "not_null").unwrap();
        assert!(out.contains("tests:"));
        assert!(out.contains("not_null"));
    }

    #[test]
    fn test_upsert_creates_model_when_missing() {
        let yaml = "version: 2\nmodels: []\n";
        let out = upsert_test_in_yaml(yaml, "orders", "order_id", "unique").unwrap();
        assert!(out.contains("name: orders"));
        assert!(out.contains("name: order_id"));
        assert!(out.contains("unique"));
    }

    #[test]
    fn test_upsert_creates_column_when_missing() {
        let yaml = "models:\n  - name: orders\n    columns: []\n";
        let out = upsert_test_in_yaml(yaml, "orders", "amount", "not_null").unwrap();
        assert!(out.contains("name: amount"));
        assert!(out.contains("not_null"));
    }

    #[test]
    fn test_upsert_idempotent_when_test_already_present() {
        let yaml = "models:\n  - name: orders\n    columns:\n      - name: order_id\n        tests:\n          - not_null\n";
        let out = upsert_test_in_yaml(yaml, "orders", "order_id", "not_null").unwrap();
        // Idempotent: when nothing changes we return the input unchanged so the
        // caller can detect no-op via string equality.
        assert_eq!(out, yaml);
    }

    #[test]
    fn test_build_proposal_no_existing_yaml() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("models")).unwrap();
        let req = ProposeTestRequest {
            project_dir: tmp.path(),
            model: "orders",
            column: "order_id",
            kind: "not_null",
            yaml_files: vec![],
        };
        let res = build_proposal(&req).unwrap();
        assert!(res.new_file);
        assert!(res.diff.contains("+++ b/"));
        assert!(res.diff.contains("not_null"));
        assert_eq!(
            res.file.file_name().unwrap().to_string_lossy(),
            "orders.yml"
        );
    }

    #[test]
    fn test_build_proposal_existing_yaml() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("schema.yml"),
            "models:\n  - name: orders\n    columns:\n      - name: order_id\n",
        )
        .unwrap();
        let req = ProposeTestRequest {
            project_dir: tmp.path(),
            model: "orders",
            column: "order_id",
            kind: "unique",
            yaml_files: vec![],
        };
        let res = build_proposal(&req).unwrap();
        assert!(!res.new_file);
        assert!(!res.already_present);
        assert!(res.diff.contains("unique"));
        assert!(res.file.ends_with("models/schema.yml"));
    }

    #[test]
    fn test_build_proposal_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("schema.yml"),
            "models:\n  - name: orders\n    columns:\n      - name: order_id\n        tests:\n          - not_null\n",
        )
        .unwrap();
        let req = ProposeTestRequest {
            project_dir: tmp.path(),
            model: "orders",
            column: "order_id",
            kind: "not_null",
            yaml_files: vec![],
        };
        let res = build_proposal(&req).unwrap();
        assert!(res.already_present);
        assert!(res.diff.is_empty());
    }

    #[test]
    fn test_validate_kind_rejects_garbage() {
        assert!(validate_kind("bogus").is_err());
        assert!(validate_kind("not_null").is_ok());
    }
}
