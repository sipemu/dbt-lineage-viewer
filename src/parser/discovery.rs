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

    #[test]
    fn test_walk_csv_files() {
        let tmp = tempfile::tempdir().unwrap();
        let seeds_dir = tmp.path().join("seeds");
        fs::create_dir_all(&seeds_dir).unwrap();
        fs::write(seeds_dir.join("countries.csv"), "id,name\n1,US").unwrap();
        fs::write(seeds_dir.join("schema.yml"), "version: 2").unwrap();
        fs::write(seeds_dir.join("notes.txt"), "notes").unwrap();

        let csv_files = walk_csv_files(&seeds_dir);
        assert_eq!(csv_files.len(), 1);
        assert!(csv_files[0].ends_with("countries.csv"));
    }

    #[test]
    fn test_walk_csv_files_nonexistent() {
        let csv_files = walk_csv_files(Path::new("/nonexistent/path"));
        assert!(csv_files.is_empty());
    }

    #[test]
    fn test_walk_directory_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        let staging_dir = models_dir.join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        fs::write(staging_dir.join("stg_a.sql"), "SELECT 1").unwrap();
        fs::write(staging_dir.join("stg_b.sql"), "SELECT 2").unwrap();
        fs::write(models_dir.join("schema.yaml"), "version: 2").unwrap();

        let (sql, yaml) = walk_directory(&models_dir);
        assert_eq!(sql.len(), 2);
        assert_eq!(yaml.len(), 1);
    }

    #[test]
    fn test_discover_files_full() {
        let tmp = tempfile::tempdir().unwrap();
        let project_dir = tmp.path();

        // Models
        let models_dir = project_dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("model_a.sql"), "SELECT 1").unwrap();
        fs::write(models_dir.join("schema.yml"), "version: 2").unwrap();

        // Seeds
        let seeds_dir = project_dir.join("seeds");
        fs::create_dir_all(&seeds_dir).unwrap();
        fs::write(seeds_dir.join("seed.csv"), "a,b\n1,2").unwrap();

        // Snapshots
        let snap_dir = project_dir.join("snapshots");
        fs::create_dir_all(&snap_dir).unwrap();
        fs::write(snap_dir.join("snap.sql"), "SELECT 1").unwrap();

        // Tests
        let test_dir = project_dir.join("tests");
        fs::create_dir_all(&test_dir).unwrap();
        fs::write(test_dir.join("test_a.sql"), "SELECT 1").unwrap();

        let paths = ResolvedPaths {
            model_paths: vec![models_dir],
            seed_paths: vec![seeds_dir],
            snapshot_paths: vec![snap_dir],
            test_paths: vec![test_dir],
        };

        let discovered = discover_files(&paths).unwrap();
        assert_eq!(discovered.model_sql_files.len(), 1);
        assert_eq!(discovered.seed_files.len(), 1);
        assert_eq!(discovered.snapshot_sql_files.len(), 1);
        assert_eq!(discovered.test_sql_files.len(), 1);
        assert_eq!(discovered.yaml_files.len(), 1);
    }

    #[test]
    fn test_discover_files_missing_dirs() {
        let paths = ResolvedPaths {
            model_paths: vec![PathBuf::from("/nonexistent/models")],
            seed_paths: vec![PathBuf::from("/nonexistent/seeds")],
            snapshot_paths: vec![PathBuf::from("/nonexistent/snapshots")],
            test_paths: vec![PathBuf::from("/nonexistent/tests")],
        };

        let discovered = discover_files(&paths).unwrap();
        assert!(discovered.model_sql_files.is_empty());
        assert!(discovered.seed_files.is_empty());
        assert!(discovered.snapshot_sql_files.is_empty());
        assert!(discovered.test_sql_files.is_empty());
        assert!(discovered.yaml_files.is_empty());
    }
}
