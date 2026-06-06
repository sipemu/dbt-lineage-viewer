use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use super::manifest::Manifest;

/// Normalize a path to its project-relative, forward-slash string form. Used as
/// the comparison key so that filesystem-walked paths (which use backslashes on
/// Windows) match the manifest's forward-slash paths.
fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Result of comparing a `manifest.json` against the current state of the project's
/// SQL/YAML files. Stale if any of `missing`, `modified`, or `untracked` is non-empty.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestCheckReport {
    /// True iff any drift was detected.
    pub is_stale: bool,
    /// Files referenced in the manifest that no longer exist on disk.
    pub missing: Vec<String>,
    /// Files referenced in the manifest whose mtime is newer than the manifest's.
    pub modified: Vec<String>,
    /// SQL files in the project that are NOT referenced by the manifest at all
    /// (e.g. a model added after the last `dbt compile`).
    pub untracked: Vec<String>,
    /// Total node count from the manifest, for context.
    pub manifest_node_count: usize,
}

/// Compare a parsed manifest at `manifest_path` against the SQL/YAML files under
/// `project_dir`. The manifest's own mtime is used as the freshness reference.
pub fn check_manifest(project_dir: &Path, manifest_path: &Path) -> Result<ManifestCheckReport> {
    let content = std::fs::read_to_string(manifest_path).map_err(|e| {
        crate::error::DbtLineageError::FileReadError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;
    let manifest: Manifest = serde_json::from_str(&content).map_err(|e| {
        crate::error::DbtLineageError::ArtifactParseError {
            path: manifest_path.to_path_buf(),
            source: e,
        }
    })?;

    let manifest_mtime = std::fs::metadata(manifest_path)
        .and_then(|m| m.modified())
        .map_err(|e| crate::error::DbtLineageError::FileReadError {
            path: manifest_path.to_path_buf(),
            source: e,
        })?;

    let referenced: HashSet<String> = collect_referenced_paths(&manifest);

    let mut missing = Vec::new();
    let mut modified = Vec::new();
    for rel in &referenced {
        let abs = project_dir.join(rel);
        match std::fs::metadata(&abs) {
            Err(_) => missing.push(rel.clone()),
            Ok(meta) => {
                if let Ok(mtime) = meta.modified() {
                    if mtime > manifest_mtime {
                        modified.push(rel.clone());
                    }
                }
            }
        }
    }

    let untracked = scan_untracked_sql(project_dir, &referenced)?;

    missing.sort();
    modified.sort();
    let manifest_node_count =
        manifest.nodes.len() + manifest.sources.len() + manifest.exposures.len();

    let is_stale = !missing.is_empty() || !modified.is_empty() || !untracked.is_empty();

    Ok(ManifestCheckReport {
        is_stale,
        missing,
        modified,
        untracked,
        manifest_node_count,
    })
}

/// Collect every project-relative path referenced anywhere in the manifest, normalized
/// to forward-slash strings for portable cross-platform comparison.
fn collect_referenced_paths(manifest: &Manifest) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    for n in manifest.nodes.values() {
        if let Some(p) = &n.path {
            out.insert(p.replace('\\', "/"));
        }
    }
    for s in manifest.sources.values() {
        if let Some(p) = &s.path {
            out.insert(p.replace('\\', "/"));
        }
    }
    out
}

/// Walk standard dbt directories under `project_dir` and return any `.sql` file that
/// isn't already in `referenced`. We intentionally only flag SQL (not YAML) as
/// "untracked" — YAML files often live outside the manifest's path list, so we'd get
/// noisy false positives.
fn scan_untracked_sql(project_dir: &Path, referenced: &HashSet<String>) -> Result<Vec<String>> {
    const DBT_ROOTS: &[&str] = &["models", "seeds", "snapshots", "tests", "analyses"];
    let mut untracked = Vec::new();
    for root in DBT_ROOTS {
        let dir = project_dir.join(root);
        if !dir.exists() {
            continue;
        }
        for entry in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            if !ext.eq_ignore_ascii_case("sql") {
                continue;
            }
            let rel = path
                .strip_prefix(project_dir)
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|_| path.to_path_buf());
            let rel_str = normalize_path(&rel);
            if !referenced.contains(&rel_str) {
                untracked.push(rel_str);
            }
        }
    }
    untracked.sort();
    Ok(untracked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    fn write_at(path: &Path, mtime: SystemTime, contents: &str) {
        fs::write(path, contents).unwrap();
        // Windows requires write access to call set_modified; read-only File::open
        // panics with PermissionDenied. OpenOptions::write works on all platforms.
        let f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(mtime).unwrap();
    }

    fn touch_mtime(path: &Path, mtime: SystemTime) {
        let f = fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(mtime).unwrap();
    }

    fn write_manifest(dir: &Path, paths: &[&str]) -> PathBuf {
        let mut nodes_json = String::new();
        let mut sources_json = String::new();
        for (i, p) in paths.iter().enumerate() {
            if p.ends_with(".sql") {
                nodes_json.push_str(&format!(
                    r#""model.x.n{i}": {{ "unique_id": "model.x.n{i}", "name": "n{i}", "resource_type": "model", "path": "{p}" }}{}"#,
                    if i + 1 < paths.len() { "," } else { "" }
                ));
            } else {
                sources_json.push_str(&format!(
                    r#""source.x.s.n{i}": {{ "unique_id": "source.x.s.n{i}", "name": "n{i}", "source_name": "s", "path": "{p}" }}{}"#,
                    if i + 1 < paths.len() { "," } else { "" }
                ));
            }
        }
        let json = format!(
            r#"{{"nodes": {{{}}}, "sources": {{{}}}, "exposures": {{}}}}"#,
            nodes_json, sources_json
        );
        let manifest_path = dir.join("target").join("manifest.json");
        fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        fs::write(&manifest_path, json).unwrap();
        manifest_path
    }

    #[test]
    fn test_fresh_manifest_is_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let past = SystemTime::now() - Duration::from_secs(120);
        write_at(&project.join("models/marts/orders.sql"), past, "select 1");

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Bump the manifest mtime to NOW so the SQL file is older.
        touch_mtime(&manifest, SystemTime::now());

        let report = check_manifest(project, &manifest).unwrap();
        assert!(
            !report.is_stale,
            "fresh project should be clean: {:?}",
            report
        );
        assert!(report.missing.is_empty());
        assert!(report.modified.is_empty());
        assert!(report.untracked.is_empty());
    }

    #[test]
    fn test_missing_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();
        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Do NOT create the referenced SQL file.
        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(report.missing, vec!["models/marts/orders.sql".to_string()]);
    }

    #[test]
    fn test_modified_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        // Manifest mtime in the past.
        touch_mtime(&manifest, SystemTime::now() - Duration::from_secs(300));
        // SQL file fresher than the manifest.
        write_at(
            &project.join("models/marts/orders.sql"),
            SystemTime::now(),
            "select 2",
        );

        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(report.modified, vec!["models/marts/orders.sql".to_string()]);
    }

    #[test]
    fn test_untracked_file_flagged() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path();
        fs::create_dir_all(project.join("models/marts")).unwrap();

        let manifest = write_manifest(project, &["models/marts/orders.sql"]);
        write_at(
            &project.join("models/marts/orders.sql"),
            SystemTime::now() - Duration::from_secs(120),
            "select 1",
        );
        write_at(
            &project.join("models/marts/brand_new.sql"),
            SystemTime::now() - Duration::from_secs(120),
            "select 2",
        );
        // Manifest is the freshest thing on disk → no "modified" entry, but the
        // brand_new.sql isn't referenced → untracked.
        touch_mtime(&manifest, SystemTime::now());

        let report = check_manifest(project, &manifest).unwrap();
        assert!(report.is_stale);
        assert_eq!(
            report.untracked,
            vec!["models/marts/brand_new.sql".to_string()]
        );
        assert!(report.missing.is_empty());
        assert!(report.modified.is_empty());
    }
}
