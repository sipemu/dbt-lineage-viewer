use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::parser::discovery::DiscoveredFiles;
use crate::parser::yaml_schema::{parse_schema_file, TestDefinition};

/// Per-model coverage figures.
#[derive(Debug, Clone, Serialize)]
pub struct ModelCoverage {
    pub model: String,
    /// Number of generic tests applied to this model (counting per-column).
    pub generic_tests: usize,
    /// Number of custom (singular/data) tests targeting this model.
    /// Detection here is approximate — tracked only via YAML for v1.
    pub custom_tests: usize,
    /// Total columns documented in YAML for this model.
    pub columns_total: usize,
    /// Columns that have at least one test attached.
    pub columns_tested: usize,
}

impl ModelCoverage {
    pub fn has_any_test(&self) -> bool {
        self.generic_tests > 0 || self.custom_tests > 0
    }
    pub fn column_coverage_pct(&self) -> f64 {
        if self.columns_total == 0 {
            0.0
        } else {
            (self.columns_tested as f64 / self.columns_total as f64) * 100.0
        }
    }
}

/// Project-wide test-coverage report.
#[derive(Debug, Clone, Serialize)]
pub struct CoverageReport {
    pub total_models: usize,
    pub models_with_any_test: usize,
    pub coverage_pct: f64,
    /// Models sorted by label, ascending.
    pub models: Vec<ModelCoverage>,
}

/// Build a coverage report by parsing every discovered schema YAML file.
pub fn compute_coverage(_project_dir: &Path, files: &DiscoveredFiles) -> Result<CoverageReport> {
    let mut by_model: HashMap<String, ModelCoverage> = HashMap::new();

    for yaml_path in &files.yaml_files {
        let content = match std::fs::read_to_string(yaml_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let schema = match parse_schema_file(&content) {
            Ok(s) => s,
            Err(_) => continue,
        };
        for model in schema.models {
            let entry = by_model
                .entry(model.name.clone())
                .or_insert_with(|| ModelCoverage {
                    model: model.name.clone(),
                    generic_tests: 0,
                    custom_tests: 0,
                    columns_total: 0,
                    columns_tested: 0,
                });
            for col in &model.columns {
                entry.columns_total += 1;
                let mut col_has_test = false;
                for t in &col.tests {
                    if is_generic_test(t) {
                        entry.generic_tests += 1;
                    } else {
                        entry.custom_tests += 1;
                    }
                    col_has_test = true;
                }
                if col_has_test {
                    entry.columns_tested += 1;
                }
            }
        }
    }

    let mut models: Vec<ModelCoverage> = by_model.into_values().collect();
    models.sort_by(|a, b| a.model.cmp(&b.model));

    let total_models = models.len();
    let models_with_any_test = models.iter().filter(|m| m.has_any_test()).count();
    let coverage_pct = if total_models == 0 {
        0.0
    } else {
        (models_with_any_test as f64 / total_models as f64) * 100.0
    };

    Ok(CoverageReport {
        total_models,
        models_with_any_test,
        coverage_pct,
        models,
    })
}

/// Generic dbt tests are the canonical four (`not_null`, `unique`, `accepted_values`,
/// `relationships`). Anything else — `dbt_expectations.*`, custom packages, singular
/// `.sql` tests, etc. — counts as a custom test.
fn is_generic_test(t: &TestDefinition) -> bool {
    let name = match t {
        TestDefinition::Simple(s) => s.to_string(),
        TestDefinition::Complex(v) => {
            if let serde_yaml::Value::Mapping(m) = v {
                m.keys()
                    .next()
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        }
    };
    matches!(
        name.as_str(),
        "not_null" | "unique" | "accepted_values" | "relationships"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::discovery::DiscoveredFiles;
    use std::fs;
    use std::path::PathBuf;

    fn discovered(paths: Vec<PathBuf>) -> DiscoveredFiles {
        DiscoveredFiles {
            yaml_files: paths,
            ..Default::default()
        }
    }

    #[test]
    fn test_empty_project() {
        let tmp = tempfile::tempdir().unwrap();
        let report = compute_coverage(tmp.path(), &discovered(vec![])).unwrap();
        assert_eq!(report.total_models, 0);
        assert_eq!(report.coverage_pct, 0.0);
    }

    #[test]
    fn test_single_model_full_coverage() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = tmp.path().join("schema.yml");
        fs::write(
            &yaml,
            "models:\n  - name: orders\n    columns:\n      - name: order_id\n        tests:\n          - not_null\n          - unique\n",
        )
        .unwrap();
        let report = compute_coverage(tmp.path(), &discovered(vec![yaml])).unwrap();
        assert_eq!(report.total_models, 1);
        assert_eq!(report.models_with_any_test, 1);
        assert_eq!(report.coverage_pct, 100.0);
        let m = &report.models[0];
        assert_eq!(m.generic_tests, 2);
        assert_eq!(m.columns_tested, 1);
        assert_eq!(m.columns_total, 1);
    }

    #[test]
    fn test_mixed_coverage() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = tmp.path().join("schema.yml");
        fs::write(
            &yaml,
            "models:\n  - name: orders\n    columns:\n      - name: order_id\n        tests:\n          - not_null\n      - name: amount\n  - name: customers\n",
        )
        .unwrap();
        let report = compute_coverage(tmp.path(), &discovered(vec![yaml])).unwrap();
        assert_eq!(report.total_models, 2);
        assert_eq!(report.models_with_any_test, 1);
        // 50% of models have at least one test.
        assert!((report.coverage_pct - 50.0).abs() < 1e-6);
        let orders = report.models.iter().find(|m| m.model == "orders").unwrap();
        assert_eq!(orders.columns_total, 2);
        assert_eq!(orders.columns_tested, 1);
        assert!((orders.column_coverage_pct() - 50.0).abs() < 1e-6);
    }

    #[test]
    fn test_custom_test_classified_separately() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = tmp.path().join("schema.yml");
        fs::write(
            &yaml,
            "models:\n  - name: orders\n    columns:\n      - name: amount\n        tests:\n          - dbt_expectations.expect_column_values_to_be_positive\n",
        )
        .unwrap();
        let report = compute_coverage(tmp.path(), &discovered(vec![yaml])).unwrap();
        let m = &report.models[0];
        assert_eq!(m.generic_tests, 0);
        assert_eq!(m.custom_tests, 1);
    }

    #[test]
    fn test_complex_test_with_args_still_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = tmp.path().join("schema.yml");
        fs::write(
            &yaml,
            "models:\n  - name: orders\n    columns:\n      - name: status\n        tests:\n          - accepted_values:\n              values: ['placed', 'shipped']\n",
        )
        .unwrap();
        let report = compute_coverage(tmp.path(), &discovered(vec![yaml])).unwrap();
        assert_eq!(report.models[0].generic_tests, 1);
    }
}
