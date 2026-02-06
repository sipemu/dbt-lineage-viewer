use anyhow::Result;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use super::project::ResolvedPaths;

/// All discovered files in the dbt project, categorized by type
#[derive(Debug, Default)]
pub struct DiscoveredFiles {
    pub model_sql_files: Vec<PathBuf>,
    pub seed_files: Vec<PathBuf>,
    pub snapshot_sql_files: Vec<PathBuf>,
    pub test_sql_files: Vec<PathBuf>,
    pub yaml_files: Vec<PathBuf>,
}

/// Walk all configured paths and collect SQL/YAML files
pub fn discover_files(paths: &ResolvedPaths) -> Result<DiscoveredFiles> {
    let mut discovered = DiscoveredFiles::default();

    // Models
    for dir in &paths.model_paths {
        let (sql, yaml) = walk_directory(dir);
        discovered.model_sql_files.extend(sql);
        discovered.yaml_files.extend(yaml);
    }

    // Seeds
    for dir in &paths.seed_paths {
        let (_, yaml) = walk_directory(dir);
        // Seeds are CSV files typically, but we collect their YAML schema files
        discovered.yaml_files.extend(yaml);
        // Also look for .csv files
        discovered.seed_files.extend(walk_csv_files(dir));
    }

    // Snapshots
    for dir in &paths.snapshot_paths {
        let (sql, yaml) = walk_directory(dir);
        discovered.snapshot_sql_files.extend(sql);
        discovered.yaml_files.extend(yaml);
    }

    // Tests
    for dir in &paths.test_paths {
        let (sql, yaml) = walk_directory(dir);
        discovered.test_sql_files.extend(sql);
        discovered.yaml_files.extend(yaml);
    }

    Ok(discovered)
}

/// Walk a directory and return (sql_files, yaml_files)
fn walk_directory(dir: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut sql_files = Vec::new();
    let mut yaml_files = Vec::new();

    if !dir.exists() {
        return (sql_files, yaml_files);
    }

    for entry in WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        match path.extension().and_then(|e| e.to_str()) {
            Some("sql") => sql_files.push(path.to_path_buf()),
            Some("yml" | "yaml") => yaml_files.push(path.to_path_buf()),
            _ => {}
        }
    }

    (sql_files, yaml_files)
}

/// Walk a directory and return CSV files (for seeds)
fn walk_csv_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }

    WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("csv"))
        .map(|e| e.path().to_path_buf())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_walk_nonexistent_directory() {
        let (sql, yaml) = walk_directory(Path::new("/nonexistent/path"));
        assert!(sql.is_empty());
        assert!(yaml.is_empty());
    }

    #[test]
    fn test_walk_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("model_a.sql"), "SELECT 1").unwrap();
        fs::write(models_dir.join("schema.yml"), "version: 2").unwrap();
        fs::write(models_dir.join("readme.md"), "# Readme").unwrap();

        let (sql, yaml) = walk_directory(&models_dir);
        assert_eq!(sql.len(), 1);
        assert_eq!(yaml.len(), 1);
    }
}
