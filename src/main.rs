use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use dbt_lineage::cli::{self, Cli};
use dbt_lineage::graph;
use dbt_lineage::parser;
use dbt_lineage::render;

#[cfg(not(tarpaulin_include))]
fn main() -> Result<()> {
    let cli = Cli::parse();

    let project_dir = cli.project_dir.canonicalize().unwrap_or(cli.project_dir);

    // Build graph: either from manifest.json or by parsing SQL files
    let dag = if let Some(manifest_arg) = &cli.manifest {
        let manifest_path = resolve_manifest_path(manifest_arg)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)?
    } else {
        // Parse dbt project
        let project = parser::project::DbtProject::load(&project_dir)?;
        let paths = project.resolve_paths(&project_dir);

        // Discover files
        let files = parser::discovery::discover_files(&paths)?;

        // Build graph
        graph::builder::build_graph(&project_dir, &files)?
    };

    // Parse selectors
    let selectors = cli
        .select
        .as_deref()
        .map(graph::filter::parse_selectors)
        .unwrap_or_default();

    // Filter graph
    let filtered = graph::filter::filter_graph(
        &dag,
        cli.model.as_deref(),
        cli.upstream,
        cli.downstream,
        &graph::filter::NodeTypeFilter {
            include_tests: cli.include_tests,
            include_seeds: cli.include_seeds,
            include_snapshots: cli.include_snapshots,
            include_exposures: cli.include_exposures,
        },
        &selectors,
    )?;

    // Render
    #[cfg(feature = "tui")]
    if cli.interactive {
        dbt_lineage::tui::run_tui(filtered, project_dir.clone())?;
        return Ok(());
    }

    #[cfg(not(feature = "tui"))]
    if cli.interactive {
        anyhow::bail!("TUI feature not enabled. Rebuild with --features tui");
    }

    match cli.output {
        cli::OutputFormat::Ascii => render::ascii::render_ascii(&filtered),
        cli::OutputFormat::Dot => render::dot::render_dot(&filtered),
        cli::OutputFormat::Json => render::json::render_json(&filtered),
        cli::OutputFormat::Mermaid => render::mermaid::render_mermaid(&filtered),
    }

    Ok(())
}

/// Resolve the manifest path from the --manifest argument.
/// If the path is a directory, look for `target/manifest.json` inside it.
/// If it's a file, use it directly.
#[cfg(not(tarpaulin_include))]
fn resolve_manifest_path(manifest_arg: &Path) -> Result<PathBuf> {
    if manifest_arg.is_dir() {
        let candidate = manifest_arg.join("target").join("manifest.json");
        if candidate.exists() {
            Ok(candidate)
        } else {
            anyhow::bail!(
                "No manifest.json found at {}. Expected target/manifest.json in the directory.",
                candidate.display()
            );
        }
    } else if manifest_arg.exists() {
        Ok(manifest_arg.to_path_buf())
    } else {
        anyhow::bail!(
            "Manifest path does not exist: {}",
            manifest_arg.display()
        );
    }
}
