//! Benchmarks for the SQL-parse-mode graph builder. Generates a synthetic
//! 500-model dbt project into a temp dir on first iteration, then measures:
//!
//! - `build_graph_cold`: empty cache, all files parsed from scratch.
//! - `build_graph_warm`: cache prepopulated, every file is a hit.
//! - `compute_summary` and `compute_impact` on the resulting graph.
//!
//! Run with `cargo bench`. Baseline numbers live in `BENCHMARKS.md`.

use std::path::{Path, PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};

use dbt_lineage::graph::{builder, impact, summary};
use dbt_lineage::parser::{discovery, project};

const STAGING_MODELS: usize = 200;
const MART_MODELS: usize = 300;

/// Lay out a realistic-ish dbt project: a `dbt_project.yml`, a `models/`
/// directory with two layers (staging + marts), one schema.yml per layer,
/// and one macros file. Each mart pulls from two staging models so the DAG
/// has real fan-in. Total: 500 models, 500 ref() calls.
fn generate_fixture(root: &Path) {
    let models_dir = root.join("models");
    let staging_dir = models_dir.join("staging");
    let marts_dir = models_dir.join("marts");
    let macros_dir = root.join("macros");
    std::fs::create_dir_all(&staging_dir).unwrap();
    std::fs::create_dir_all(&marts_dir).unwrap();
    std::fs::create_dir_all(&macros_dir).unwrap();

    std::fs::write(
        root.join("dbt_project.yml"),
        "name: bench_project\nmodel-paths: [\"models\"]\nmacro-paths: [\"macros\"]\n",
    )
    .unwrap();

    // A simple ref-wrapper macro so #13 / macro-aware parsing also exercises.
    std::fs::write(
        macros_dir.join("smart_ref.sql"),
        "{% macro smart_ref(name) %}{{ ref(name) }}{% endmacro %}\n",
    )
    .unwrap();

    // Staging models — each one declares 4 columns.
    for i in 0..STAGING_MODELS {
        let path = staging_dir.join(format!("stg_model_{:04}.sql", i));
        let sql = format!(
            "{{{{ config(materialized='view', tags=['staging']) }}}}\n\
             select\n  col_a as id_{i},\n  col_b as name_{i},\n  col_c,\n  col_d\n\
             from raw_{i}\n",
            i = i
        );
        std::fs::write(path, sql).unwrap();
    }

    // Marts — each pulls from two staging models and uses smart_ref.
    for i in 0..MART_MODELS {
        let stg_a = i % STAGING_MODELS;
        let stg_b = (i + 7) % STAGING_MODELS;
        let path = marts_dir.join(format!("mart_model_{:04}.sql", i));
        let sql = format!(
            "{{{{ config(materialized='table', tags=['marts']) }}}}\n\
             with a as (\n  select * from {{{{ ref('stg_model_{a:04}') }}}}\n),\n\
             b as (\n  select * from {{{{ smart_ref('stg_model_{b:04}') }}}}\n)\n\
             select a.id_{a} as mart_id, b.name_{b} as label, a.col_c\n\
             from a join b on a.col_d = b.col_d\n",
            a = stg_a,
            b = stg_b
        );
        std::fs::write(path, sql).unwrap();
    }

    // One schema.yml per layer with model descriptions.
    let mut staging_yaml = String::from("version: 2\nmodels:\n");
    for i in 0..STAGING_MODELS {
        staging_yaml.push_str(&format!(
            "  - name: stg_model_{i:04}\n    description: Staging model {i}\n"
        ));
    }
    std::fs::write(staging_dir.join("schema.yml"), staging_yaml).unwrap();

    let mut marts_yaml = String::from("version: 2\nmodels:\n");
    for i in 0..MART_MODELS {
        marts_yaml.push_str(&format!(
            "  - name: mart_model_{i:04}\n    description: Mart model {i}\n"
        ));
    }
    std::fs::write(marts_dir.join("schema.yml"), marts_yaml).unwrap();
}

/// Set up the fixture once and return its path. Shared across all bench targets.
fn fixture_dir() -> PathBuf {
    let dir = std::env::temp_dir().join("dbt-lineage-bench-fixture");
    if !dir.join("dbt_project.yml").exists() {
        if dir.exists() {
            let _ = std::fs::remove_dir_all(&dir);
        }
        std::fs::create_dir_all(&dir).unwrap();
        generate_fixture(&dir);
    }
    dir
}

fn discover(root: &Path) -> discovery::DiscoveredFiles {
    let project = project::DbtProject::load(root).unwrap();
    let paths = project.resolve_paths(root);
    discovery::discover_files(&paths).unwrap()
}

fn bench_build_graph(c: &mut Criterion) {
    let root = fixture_dir();
    let files = discover(&root);

    let mut group = c.benchmark_group("build_graph");
    group.sample_size(20);

    // Cold: bypass the on-disk cache and parse every file.
    group.bench_function("cold_no_cache", |b| {
        b.iter(|| {
            let _g = builder::build_graph_no_cache(&root, &files).unwrap();
        });
    });

    // Warm: persistent cache prepopulated by the cold path above.
    // First run primes the cache file; subsequent iterations all hit.
    let _prime = builder::build_graph(&root, &files).unwrap();
    group.bench_function("warm_with_cache", |b| {
        b.iter(|| {
            let _g = builder::build_graph(&root, &files).unwrap();
        });
    });

    group.finish();
}

fn bench_summary_and_impact(c: &mut Criterion) {
    let root = fixture_dir();
    let files = discover(&root);
    let graph = builder::build_graph(&root, &files).unwrap();

    let mut group = c.benchmark_group("analysis");
    group.sample_size(50);

    group.bench_function("compute_summary", |b| {
        b.iter(|| {
            let _ = summary::compute_summary(&graph, None);
        });
    });

    // Pick a deep-fan-in staging model (used by ~every 200th mart).
    let target = graph
        .node_indices()
        .find(|&i| graph[i].label == "stg_model_0007")
        .expect("fixture must include stg_model_0007");
    group.bench_function("compute_impact", |b| {
        b.iter(|| {
            let _ = impact::compute_impact(&graph, target);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_build_graph, bench_summary_and_impact);
criterion_main!(benches);
