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

/// Resolve a base ref for CI use. PR/MR checkouts usually lack a local branch
/// for the target (and are often shallow), so try, in order: the ref as given,
/// `origin/<ref>`, and finally fetch `<ref>` from `origin`. Returns the ref name
/// that resolved.
pub fn resolve_base_ref(path: &Path, git_ref: &str) -> Result<String> {
    if validate_ref(path, git_ref).is_ok() {
        return Ok(git_ref.to_string());
    }
    let remote_ref = format!("origin/{}", git_ref);
    if validate_ref(path, &remote_ref).is_ok() {
        return Ok(remote_ref);
    }

    let shallow = Command::new("git")
        .args(["rev-parse", "--is-shallow-repository"])
        .current_dir(path)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "true")
        .unwrap_or(false);
    let refspec = format!("+refs/heads/{0}:refs/remotes/origin/{0}", git_ref);
    let mut args = vec!["fetch", "--no-tags", "origin"];
    // In a shallow clone only the tip is needed; `git show <ref>:<file>` works
    // without history. In a full clone, don't make the repository shallow.
    if shallow {
        args.push("--depth=1");
    }
    args.push(&refspec);
    let output = Command::new("git")
        .args(&args)
        .current_dir(path)
        .output()
        .context("Failed to run git fetch")?;
    if output.status.success() && validate_ref(path, &remote_ref).is_ok() {
        return Ok(remote_ref);
    }
    anyhow::bail!(
        "Invalid git ref: {} (also tried origin/{} and fetching it: {})",
        git_ref,
        git_ref,
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

/// Path of `path` inside its repository (`git rev-parse --show-prefix`),
/// e.g. `"dbt/"`, or `""` at the root. `None` outside a git repository.
pub fn show_prefix(path: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-prefix"])
        .current_dir(path)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
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

/// Unpack the tree at `git_ref` into `dest`. Run from a subdirectory, `git
/// archive` includes only that subtree with paths relative to it, so `dest`
/// mirrors `path` as it was at `git_ref`.
pub fn archive_to(path: &Path, git_ref: &str, dest: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["archive", "--format=tar", git_ref])
        .current_dir(path)
        .output()
        .context("Failed to run git archive")?;
    if !output.status.success() {
        anyhow::bail!(
            "git archive {} failed: {}",
            git_ref,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    tar::Archive::new(output.stdout.as_slice())
        .unpack(dest)
        .with_context(|| format!("Failed to unpack tree of {}", git_ref))
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
    fn test_resolve_base_ref_local_remote_and_fetch() {
        let (_up_tmp, upstream) = setup_temp_git_repo();
        let git = |dir: &Path, args: &[&str]| {
            let o = Command::new("git")
                .args(args)
                .current_dir(dir)
                .output()
                .unwrap();
            assert!(o.status.success(), "git {:?}: {:?}", args, o);
        };
        git(&upstream, &["branch", "-M", "main"]);
        git(&upstream, &["branch", "release"]);

        // Clone only `main`, like a single-branch CI checkout.
        let clone_tmp = tempfile::tempdir().unwrap();
        let clone = clone_tmp.path().join("c");
        let up = format!("file://{}", upstream.display());
        let o = Command::new("git")
            .args(["clone", "--single-branch", "--branch", "main", "--depth=1"])
            .arg(&up)
            .arg(&clone)
            .output()
            .unwrap();
        assert!(o.status.success(), "{:?}", o);
        git(&clone, &["checkout", "-b", "feature"]);
        git(&clone, &["branch", "-D", "main"]);

        // Local ref resolves as given.
        assert_eq!(resolve_base_ref(&clone, "HEAD").unwrap(), "HEAD");
        // No local `main`, but origin/main exists.
        assert_eq!(resolve_base_ref(&clone, "main").unwrap(), "origin/main");
        // `release` was never fetched: fetched on demand.
        assert_eq!(
            resolve_base_ref(&clone, "release").unwrap(),
            "origin/release"
        );
        assert!(resolve_base_ref(&clone, "does-not-exist").is_err());
    }

    #[test]
    fn test_show_prefix() {
        let (_tmp, path) = setup_temp_git_repo();
        assert_eq!(show_prefix(&path).as_deref(), Some(""));
        std::fs::create_dir_all(path.join("dbt")).unwrap();
        assert_eq!(show_prefix(&path.join("dbt")).as_deref(), Some("dbt/"));
        let outside = tempfile::tempdir().unwrap();
        assert_eq!(show_prefix(outside.path()), None);
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
