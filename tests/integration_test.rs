use std::path::PathBuf;

// We need to reference the library modules — use the binary crate via process for CLI tests,
// but for unit-level integration tests, we'll test the core logic inline.
// For artifact tests, we test the JSON parsing directly.

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("simple_project")
}

mod parsing {
    use super::*;

    #[test]
    fn test_load_project() {
        let dir = fixture_dir();
        let content = std::fs::read_to_string(dir.join("dbt_project.yml")).unwrap();
        let project: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        assert_eq!(project["name"].as_str().unwrap(), "simple_project");
    }

    #[test]
    fn test_sql_ref_extraction() {
        let sql = std::fs::read_to_string(
            fixture_dir().join("models/marts/orders.sql"),
        )
        .unwrap();

        // Check that refs are found using regex
        let ref_re = regex::Regex::new(r#"\{\{-?\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*-?\}\}"#).unwrap();
        let refs: Vec<String> = ref_re
            .captures_iter(&sql)
            .map(|c| c[1].to_string())
            .collect();

        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&"stg_orders".to_string()));
        assert!(refs.contains(&"stg_payments".to_string()));
    }

    #[test]
    fn test_sql_source_extraction() {
        let sql = std::fs::read_to_string(
            fixture_dir().join("models/staging/stg_orders.sql"),
        )
        .unwrap();

        let source_re = regex::Regex::new(
            r#"\{\{-?\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*-?\}\}"#,
        )
        .unwrap();

        let sources: Vec<(String, String)> = source_re
            .captures_iter(&sql)
            .map(|c| (c[1].to_string(), c[2].to_string()))
            .collect();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0], ("raw".to_string(), "orders".to_string()));
    }

    #[test]
    fn test_yaml_sources_parsing() {
        let content = std::fs::read_to_string(
            fixture_dir().join("models/staging/schema.yml"),
        )
        .unwrap();

        let schema: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let sources = schema["sources"].as_sequence().unwrap();
        assert_eq!(sources.len(), 1);

        let tables = sources[0]["tables"].as_sequence().unwrap();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn test_yaml_exposures_parsing() {
        let content = std::fs::read_to_string(
            fixture_dir().join("models/marts/schema.yml"),
        )
        .unwrap();

        let schema: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let exposures = schema["exposures"].as_sequence().unwrap();
        assert_eq!(exposures.len(), 1);
        assert_eq!(
            exposures[0]["name"].as_str().unwrap(),
            "weekly_report"
        );
    }
}

mod artifacts {
    use super::*;

    #[test]
    fn test_load_run_results_fixture() {
        let dir = fixture_dir();
        let path = dir.join("target").join("run_results.json");
        let content = std::fs::read_to_string(&path).unwrap();
        let results: serde_json::Value = serde_json::from_str(&content).unwrap();

        let result_list = results["results"].as_array().unwrap();
        assert_eq!(result_list.len(), 5);

        // Check first result
        assert_eq!(
            result_list[0]["unique_id"].as_str().unwrap(),
            "model.simple_project.stg_customers"
        );
        assert_eq!(result_list[0]["status"].as_str().unwrap(), "success");

        // Check error result
        assert_eq!(
            result_list[4]["unique_id"].as_str().unwrap(),
            "model.simple_project.orders"
        );
        assert_eq!(result_list[4]["status"].as_str().unwrap(), "error");
    }

    #[test]
    fn test_run_results_timing_parsing() {
        let json = r#"{
            "results": [{
                "unique_id": "model.proj.test",
                "status": "success",
                "message": "OK",
                "timing": [{
                    "name": "execute",
                    "completed_at": "2025-01-15T10:30:00Z"
                }]
            }]
        }"#;

        let results: serde_json::Value = serde_json::from_str(json).unwrap();
        let timing = results["results"][0]["timing"][0]["completed_at"]
            .as_str()
            .unwrap();
        assert_eq!(timing, "2025-01-15T10:30:00Z");
    }
}

mod cli {
    use std::process::Command;

    fn binary_path() -> std::path::PathBuf {
        // The built binary path
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("target");
        path.push("debug");
        path.push("dbt-lineage");
        path
    }

    #[test]
    fn test_help_flag() {
        let output = Command::new(binary_path())
            .arg("--help")
            .output()
            .expect("Failed to run binary");

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("dbt-lineage"));
        assert!(stdout.contains("--project-dir"));
        assert!(stdout.contains("--interactive"));
    }

    #[test]
    fn test_nonexistent_project() {
        let output = Command::new(binary_path())
            .args(["--project-dir", "/nonexistent/path"])
            .output()
            .expect("Failed to run binary");

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("not found") || stderr.contains("No such file"));
    }

    #[test]
    fn test_run_on_fixture_project() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args(["--project-dir", fixture.to_str().unwrap()])
            .output()
            .expect("Failed to run binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Should succeed (exit 0) and produce output
        assert!(
            output.status.success(),
            "Failed with stderr: {}",
            stderr
        );
        // Should contain some model names in the output
        assert!(
            stdout.contains("stg_orders") || stdout.contains("orders"),
            "Output should contain model names: {}",
            stdout
        );
    }

    #[test]
    fn test_dot_output() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args([
                "--project-dir",
                fixture.to_str().unwrap(),
                "--output",
                "dot",
                "--include-exposures",
            ])
            .output()
            .expect("Failed to run binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success());
        assert!(stdout.contains("digraph"));
        assert!(stdout.contains("rankdir=LR"));
    }

    #[test]
    fn test_focus_model() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args([
                "--project-dir",
                fixture.to_str().unwrap(),
                "stg_orders",
                "--upstream",
                "1",
                "--downstream",
                "1",
            ])
            .output()
            .expect("Failed to run binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "Failed with stderr: {}",
            stderr
        );
        assert!(
            stdout.contains("stg_orders"),
            "Output should contain focused model: {}",
            stdout
        );
    }

    #[test]
    fn test_model_not_found() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args([
                "--project-dir",
                fixture.to_str().unwrap(),
                "nonexistent_model",
            ])
            .output()
            .expect("Failed to run binary");

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("not found") || stderr.contains("nonexistent_model"));
    }

    #[test]
    fn test_include_seeds() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args([
                "--project-dir",
                fixture.to_str().unwrap(),
                "--include-seeds",
                "--output",
                "dot",
            ])
            .output()
            .expect("Failed to run binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success());
        assert!(stdout.contains("countries"));
    }

    #[test]
    fn test_include_tests() {
        let fixture = super::fixture_dir();
        let output = Command::new(binary_path())
            .args([
                "--project-dir",
                fixture.to_str().unwrap(),
                "--include-tests",
                "--output",
                "dot",
            ])
            .output()
            .expect("Failed to run binary");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success());
        assert!(stdout.contains("assert_orders_positive_amount"));
    }
}
