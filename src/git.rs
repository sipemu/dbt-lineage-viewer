use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

/// Check if a path is inside a git repository
pub fn is_git_repo(path: &Path) -> bool {
    Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(path)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Validate that a git ref (branch, tag, commit) exists
pub fn validate_ref(path: &Path, git_ref: &str) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", git_ref])
        .current_dir(path)
        .output()
        .context("Failed to run git rev-parse")?;

    if !output.status.success() {
        anyhow::bail!("Invalid git ref: {}", git_ref);
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Get the current HEAD ref name or commit
pub fn current_ref(path: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(path)
        .output()
        .context("Failed to get current git ref")?;

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Show a file at a specific git ref
pub fn git_show(path: &Path, git_ref: &str, file_path: &str) -> Result<String> {
    let spec = format!("{}:{}", git_ref, file_path);
    let output = Command::new("git")
        .args(["show", &spec])
        .current_dir(path)
        .output()
        .with_context(|| format!("Failed to run git show {}", spec))?;

    if !output.status.success() {
        anyhow::bail!(
            "git show {} failed: {}",
            spec,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// List files at a git ref matching a pattern
pub fn git_ls_tree(path: &Path, git_ref: &str, pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["ls-tree", "-r", "--name-only", git_ref, "--", pattern])
        .current_dir(path)
        .output()
        .context("Failed to run git ls-tree")?;

    if !output.status.success() {
        return Ok(Vec::new());
    }

    let files = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect();

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn setup_temp_git_repo() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()
            .unwrap();

        Command::new("git")
            .args(["config", "user.email", "test@test.com"])
            .current_dir(&path)
            .output()
            .unwrap();

        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(&path)
            .output()
            .unwrap();

        // Create initial commit
        std::fs::write(path.join("README.md"), "# test\n").unwrap();
        Command::new("git")
            .args(["add", "."])
            .current_dir(&path)
            .output()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&path)
            .output()
            .unwrap();

        (tmp, path)
    }

    #[test]
    fn test_is_git_repo() {
        let (_tmp, path) = setup_temp_git_repo();
        assert!(is_git_repo(&path));
    }

    #[test]
    fn test_is_git_repo_false() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_git_repo(tmp.path()));
    }

    #[test]
    fn test_validate_ref() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = validate_ref(&path, "HEAD");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ref_invalid() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = validate_ref(&path, "nonexistent_branch_xyz");
        assert!(result.is_err());
    }

    #[test]
    fn test_current_ref() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = current_ref(&path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_git_show() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = git_show(&path, "HEAD", "README.md");
        assert!(result.is_ok());
        assert!(result.unwrap().contains("# test"));
    }

    #[test]
    fn test_git_show_not_found() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = git_show(&path, "HEAD", "nonexistent.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_git_ls_tree() {
        let (_tmp, path) = setup_temp_git_repo();
        let result = git_ls_tree(&path, "HEAD", ".");
        assert!(result.is_ok());
        let files = result.unwrap();
        assert!(files.contains(&"README.md".to_string()));
    }
}
