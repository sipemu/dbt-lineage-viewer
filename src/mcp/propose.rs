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

    // Prefer the line-level editor (preserves comments + indentation). Fall back
    // to the serde_yaml round-trip when the source YAML doesn't have a layout we
    // can confidently edit (e.g. flow style, no existing model/column entry).
    let new_content = match upsert_test_line_level(&original, req.model, req.column, req.kind) {
        Ok(LineEditOutcome::Updated(s)) => s,
        Ok(LineEditOutcome::AlreadyPresent) => {
            return Ok(ProposeTestResult {
                file: target_file,
                diff: String::new(),
                already_present: true,
                new_file: false,
            });
        }
        Ok(LineEditOutcome::CannotEdit) | Err(_) => {
            upsert_test_in_yaml(&original, req.model, req.column, req.kind)?
        }
    };

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

/// Outcome of attempting a precise line-level edit.
#[derive(Debug)]
enum LineEditOutcome {
    Updated(String),
    AlreadyPresent,
    CannotEdit,
}

/// Locate the column block in the source text and add the test in place.
/// Preserves comments, blank lines, and existing indentation. Falls back to
/// `LineEditOutcome::CannotEdit` whenever the layout doesn't match our
/// recognized patterns — the caller can then use the round-trip path.
fn upsert_test_line_level(
    original: &str,
    model_name: &str,
    column_name: &str,
    test_kind: &str,
) -> Result<LineEditOutcome> {
    if original.trim().is_empty() {
        return Ok(LineEditOutcome::CannotEdit);
    }
    // Validate that the file is parseable YAML and contains the model+column —
    // otherwise we'd be editing blind.
    let doc: serde_yaml::Value = match serde_yaml::from_str(original) {
        Ok(v) => v,
        Err(_) => return Ok(LineEditOutcome::CannotEdit),
    };
    if !has_model_column(&doc, model_name, column_name) {
        return Ok(LineEditOutcome::CannotEdit);
    }

    let lines: Vec<&str> = original.split_inclusive('\n').collect();

    // Find `- name: <model>` line. Use a simple substring match restricted to
    // a `- name: …` line; allow surrounding whitespace.
    let model_start = match find_named_block(&lines, model_name) {
        Some(i) => i,
        None => return Ok(LineEditOutcome::CannotEdit),
    };
    let model_indent = leading_spaces(lines[model_start]);

    // Find the column entry inside this model's block — same shape, deeper
    // indent than the model header.
    let mut column_start: Option<usize> = None;
    for (i, line) in lines.iter().enumerate().skip(model_start + 1) {
        let indent = leading_spaces(line);
        if !line.trim().is_empty() && indent <= model_indent {
            break; // left the model block
        }
        if is_named_entry(line, column_name) {
            column_start = Some(i);
            break;
        }
    }
    let column_start = match column_start {
        Some(i) => i,
        None => return Ok(LineEditOutcome::CannotEdit),
    };
    let column_indent = leading_spaces(lines[column_start]);
    // Child properties of the column sit at column_indent + 2 (standard dbt
    // convention). Refine using the FIRST subsequent property line so nested
    // items (like `- not_null` under `tests:`) don't shift the baseline.
    let mut child_indent = column_indent + 2;
    let mut found_child_indent = false;
    let mut end_of_column: usize = lines.len();
    for (i, line) in lines.iter().enumerate().skip(column_start + 1) {
        let indent = leading_spaces(line);
        if line.trim().is_empty() {
            continue;
        }
        if indent <= column_indent {
            end_of_column = i;
            break;
        }
        if !found_child_indent && indent > column_indent {
            child_indent = indent;
            found_child_indent = true;
        }
    }

    // Locate `tests:` within the column block, if present.
    let mut tests_line: Option<usize> = None;
    for (i, line) in lines
        .iter()
        .enumerate()
        .take(end_of_column)
        .skip(column_start + 1)
    {
        let trimmed = line.trim_start();
        if leading_spaces(line) == child_indent && trimmed.starts_with("tests:") {
            tests_line = Some(i);
            break;
        }
    }

    if let Some(t) = tests_line {
        // Find the end of the tests list (lines indented > child_indent).
        let test_item_indent = child_indent + 2;
        let mut last_test_line = t;
        let mut already = false;
        for (i, line) in lines
            .iter()
            .enumerate()
            .skip(t + 1)
            .take(end_of_column - t - 1)
        {
            if line.trim().is_empty() {
                continue;
            }
            if leading_spaces(line) < test_item_indent {
                break;
            }
            last_test_line = i;
            if is_test_named(line, test_kind) {
                already = true;
            }
        }
        if already {
            return Ok(LineEditOutcome::AlreadyPresent);
        }
        // Insert after last_test_line.
        let mut out = String::with_capacity(original.len() + 32);
        for (i, line) in lines.iter().enumerate() {
            out.push_str(line);
            if i == last_test_line {
                if !out.ends_with('\n') {
                    out.push('\n');
                }
                out.push_str(&format!(
                    "{}- {}\n",
                    " ".repeat(test_item_indent),
                    test_kind
                ));
            }
        }
        return Ok(LineEditOutcome::Updated(out));
    }

    // No tests: block — insert one at the end of the column block.
    let mut out = String::with_capacity(original.len() + 64);
    let last_column_line = (end_of_column - 1).max(column_start);
    for (i, line) in lines.iter().enumerate() {
        out.push_str(line);
        if i == last_column_line {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            out.push_str(&format!("{}tests:\n", " ".repeat(child_indent)));
            out.push_str(&format!(
                "{}- {}\n",
                " ".repeat(child_indent + 2),
                test_kind
            ));
        }
    }
    Ok(LineEditOutcome::Updated(out))
}

fn leading_spaces(line: &str) -> usize {
    line.chars().take_while(|c| *c == ' ').count()
}

/// Match `<indent>- name: <name>` allowing optional quotes around the name.
fn is_named_entry(line: &str, name: &str) -> bool {
    let trimmed = line.trim_start();
    let Some(rest) = trimmed.strip_prefix("- name:") else {
        return false;
    };
    let value = rest.trim();
    let value = value.trim_matches('"').trim_matches('\'').trim();
    value.split_whitespace().next() == Some(name)
}

fn find_named_block(lines: &[&str], name: &str) -> Option<usize> {
    lines.iter().position(|l| is_named_entry(l, name))
}

/// Match `<indent>- <test_kind>` or `<indent>- <test_kind>:` (complex form).
fn is_test_named(line: &str, kind: &str) -> bool {
    let trimmed = line.trim();
    let Some(rest) = trimmed.strip_prefix("- ") else {
        return false;
    };
    // Take chars up to the first non-identifier byte (`:`, whitespace, end-of-line).
    let head: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    head == kind
}

fn has_model_column(doc: &serde_yaml::Value, model: &str, column: &str) -> bool {
    let Some(models) = doc.get("models").and_then(|v| v.as_sequence()) else {
        return false;
    };
    for m in models {
        if m.get("name").and_then(|v| v.as_str()) == Some(model) {
            let Some(cols) = m.get("columns").and_then(|v| v.as_sequence()) else {
                return false;
            };
            return cols
                .iter()
                .any(|c| c.get("name").and_then(|v| v.as_str()) == Some(column));
        }
    }
    false
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
/// Produce a unified diff with three lines of context around the changed
/// region. Assumes a SINGLE contiguous diff hunk — exactly what `propose_test`
/// produces. For multi-region changes the diff is still valid (one wide hunk
/// covering both) but less compact than a real Myers-based diff would be.
fn unified_diff(old: &str, new: &str, path: &Path) -> String {
    const CONTEXT: usize = 3;
    let label = path.display().to_string();
    let old_lines: Vec<&str> = old.split_inclusive('\n').collect();
    let new_lines: Vec<&str> = new.split_inclusive('\n').collect();

    // Longest common prefix.
    let mut prefix = 0;
    while prefix < old_lines.len()
        && prefix < new_lines.len()
        && old_lines[prefix] == new_lines[prefix]
    {
        prefix += 1;
    }
    // Longest common suffix (don't overlap the prefix).
    let mut suffix = 0;
    while suffix < old_lines.len() - prefix
        && suffix < new_lines.len() - prefix
        && old_lines[old_lines.len() - 1 - suffix] == new_lines[new_lines.len() - 1 - suffix]
    {
        suffix += 1;
    }

    // No-op (shouldn't happen — caller guards), bail with empty.
    if prefix == old_lines.len() && new_lines.len() == old_lines.len() {
        return String::new();
    }

    let context_before = prefix.saturating_sub(CONTEXT);
    let old_end = old_lines.len() - suffix;
    let new_end = new_lines.len() - suffix;
    let context_after_old = (old_end + CONTEXT).min(old_lines.len());
    let context_after_new = (new_end + CONTEXT).min(new_lines.len());

    let old_hunk_len = context_after_old - context_before;
    let new_hunk_len = context_after_new - context_before;

    let mut out = String::new();
    out.push_str(&format!("--- a/{}\n", label));
    out.push_str(&format!("+++ b/{}\n", label));
    out.push_str(&format!(
        "@@ -{},{} +{},{} @@\n",
        context_before + 1,
        old_hunk_len,
        context_before + 1,
        new_hunk_len,
    ));
    // Leading context.
    for line in &old_lines[context_before..prefix] {
        out.push(' ');
        push_line(&mut out, line);
    }
    // Removed lines.
    for line in &old_lines[prefix..old_end] {
        out.push('-');
        push_line(&mut out, line);
    }
    // Added lines.
    for line in &new_lines[prefix..new_end] {
        out.push('+');
        push_line(&mut out, line);
    }
    // Trailing context.
    for line in &old_lines[old_end..context_after_old] {
        out.push(' ');
        push_line(&mut out, line);
    }
    out
}

fn push_line(out: &mut String, line: &str) {
    out.push_str(line);
    if !line.ends_with('\n') {
        out.push('\n');
    }
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

    #[test]
    fn test_line_level_preserves_comments() {
        // A schema.yml with comments and explicit blank lines. Line-level edit
        // should leave them untouched.
        let yaml = "\
# top-level comment
version: 2

models:
  - name: orders
    description: Orders fact table
    # column block follows
    columns:
      - name: order_id
        # the primary key
        description: Unique order ID
        tests:
          - unique
";
        let outcome = upsert_test_line_level(yaml, "orders", "order_id", "not_null").unwrap();
        let updated = match outcome {
            LineEditOutcome::Updated(s) => s,
            other => panic!("expected Updated, got {:?}", other),
        };
        // Comments preserved.
        assert!(updated.contains("# top-level comment"));
        assert!(updated.contains("# column block follows"));
        assert!(updated.contains("# the primary key"));
        // Original test preserved.
        assert!(updated.contains("- unique"));
        // New test inserted under the existing tests: block at matching indent.
        assert!(updated.contains("          - not_null"));
    }

    #[test]
    fn test_line_level_creates_tests_block_when_absent() {
        let yaml = "\
models:
  - name: orders
    columns:
      - name: order_id
        description: PK
";
        let outcome = upsert_test_line_level(yaml, "orders", "order_id", "unique").unwrap();
        let updated = match outcome {
            LineEditOutcome::Updated(s) => s,
            other => panic!("expected Updated, got {:?}", other),
        };
        assert!(updated.contains("tests:"));
        assert!(updated.contains("- unique"));
        // Existing description kept.
        assert!(updated.contains("description: PK"));
    }

    #[test]
    fn test_line_level_idempotent_when_present() {
        let yaml = "\
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - not_null
";
        let outcome = upsert_test_line_level(yaml, "orders", "order_id", "not_null").unwrap();
        assert!(matches!(outcome, LineEditOutcome::AlreadyPresent));
    }

    #[test]
    fn test_line_level_returns_cannot_edit_when_model_missing() {
        let yaml = "models:\n  - name: customers\n    columns:\n      - name: id\n";
        let outcome = upsert_test_line_level(yaml, "orders", "id", "not_null").unwrap();
        assert!(matches!(outcome, LineEditOutcome::CannotEdit));
    }

    #[test]
    fn test_build_proposal_uses_line_level_path() {
        // Integration: build_proposal should produce a comment-preserving diff
        // when the YAML is parseable, not the round-trip noise.
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("schema.yml"),
            "\
# annotated schema
version: 2
models:
  - name: orders
    description: Orders
    columns:
      - name: order_id
        description: PK
        tests:
          - unique
",
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
        assert!(!res.already_present);
        // Diff should be SMALL (~3 lines), not a full-file rewrite.
        let plus_lines = res.diff.lines().filter(|l| l.starts_with('+')).count();
        let minus_lines = res.diff.lines().filter(|l| l.starts_with('-')).count();
        assert!(
            plus_lines <= 5,
            "diff has too many additions ({}): {}",
            plus_lines,
            res.diff
        );
        assert!(
            minus_lines <= 3, // headers don't count
            "diff has unexpected deletions ({}): {}",
            minus_lines,
            res.diff
        );
    }
}
