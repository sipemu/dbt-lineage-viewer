//! Bitbucket Pipe (`integrations/bitbucket`): metadata consistency, and an
//! end-to-end run of the pipe image against a mocked Bitbucket API.

use std::path::{Path, PathBuf};
use std::process::Command;

fn pipe_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("integrations/bitbucket")
}

fn read(name: &str) -> String {
    // Windows checkouts may convert line endings to CRLF.
    std::fs::read_to_string(pipe_dir().join(name))
        .unwrap()
        .replace("\r\n", "\n")
}

/// Variable names declared under `variables:` in pipe.yml.
fn pipe_yml_variables(yml: &str) -> Vec<String> {
    yml.lines()
        .filter_map(|l| l.trim().strip_prefix("- name: "))
        .map(|s| s.trim().to_string())
        .collect()
}

#[test]
fn pipe_image_tags_follow_the_crate_version() {
    let version = env!("CARGO_PKG_VERSION");
    let yml = read("pipe.yml");
    assert!(
        yml.contains(&format!(
            "\nimage: ghcr.io/sipemu/dbt-lineage-pipe:{version}\n"
        )),
        "pipe.yml `image` must pin dbt-lineage-pipe:{version} (bump it with the release)"
    );
    assert!(
        read("Dockerfile").contains(&format!(
            "ARG BASE_IMAGE=ghcr.io/sipemu/dbt-lineage:{version}\n"
        )),
        "Dockerfile BASE_IMAGE default must pin dbt-lineage:{version}"
    );
    let readme = read("README.md");
    assert!(
        readme.contains(&format!("dbt-lineage-pipe:{version}")),
        "README YAML definition must use dbt-lineage-pipe:{version}"
    );
    assert!(!yml.contains(":latest") && !read("Dockerfile").contains(":latest"));
}

#[test]
fn pipe_variables_are_declared_defaulted_and_documented() {
    let yml = read("pipe.yml");
    let script = read("pipe.sh");
    let readme = read("README.md");
    let vars = pipe_yml_variables(&yml);
    assert!(vars.len() >= 8, "{vars:?}");
    for v in &vars {
        assert!(
            script.contains(&format!("{v}=${{{v}:=")),
            "pipe.sh must default {v} with ${{{v}:=…}}"
        );
        assert!(
            readme.contains(&format!("| `{v}`")),
            "README Variables table must list {v}"
        );
    }
    for k in [
        "name:",
        "image:",
        "category:",
        "description:",
        "repository:",
    ] {
        assert!(
            yml.lines().any(|l| l.starts_with(k)),
            "pipe.yml needs `{k}`"
        );
    }
    // Atlassian's README structure, in order.
    let mut at = 0;
    for h in [
        "## YAML Definition",
        "## Variables",
        "## Details",
        "## Prerequisites",
        "## Examples",
        "## Support",
    ] {
        let i = readme[at..]
            .find(h)
            .unwrap_or_else(|| panic!("README is missing `{h}` (or it is out of order)"));
        at += i + h.len();
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Builds the release image and the pipe image (minutes on the first run,
/// cached afterwards), so it is opt-in:
/// `cargo test --release --test bitbucket_pipe_test -- --ignored`.
#[test]
#[ignore = "builds Docker images; run with --ignored"]
fn pipe_image_end_to_end_with_mock_bitbucket() {
    if !docker_available() {
        eprintln!("docker is not available: skipping");
        return;
    }
    let status = Command::new("python3")
        .arg(pipe_dir().join("test/test_pipe.py"))
        .status()
        .expect("python3 is needed for the mock Bitbucket API");
    assert!(
        status.success(),
        "integrations/bitbucket/test/test_pipe.py failed"
    );
}
