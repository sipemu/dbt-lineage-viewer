use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use dbt_lineage::cli::{self, Cli, Command};
use dbt_lineage::graph;
use dbt_lineage::parser;
use dbt_lineage::render;

#[cfg(not(tarpaulin_include))]
fn main() -> Result<()> {
    let cli = Cli::parse();

    // Handle subcommands first
    if let Some(command) = &cli.command {
        return match command {
            Command::Impact {
                model,
                project_dir,
                output,
                manifest,
            } => run_impact_command(model, project_dir, output, manifest.as_ref()),
            Command::Diff {
                base,
                head,
                project_dir,
                output,
            } => run_diff_command(base, head.as_deref(), project_dir, output),
        };
    }

    let project_dir = cli.project_dir.canonicalize().unwrap_or(cli.project_dir);

    let dag = build_dag(&project_dir, cli.manifest.as_ref())?;

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

    render_output(&cli.output, &filtered);

    Ok(())
}

/// Build the lineage DAG from either a manifest file or by parsing SQL files
#[cfg(not(tarpaulin_include))]
fn build_dag(project_dir: &Path, manifest: Option<&PathBuf>) -> Result<graph::types::LineageGraph> {
    if let Some(manifest_arg) = manifest {
        let manifest_path = resolve_manifest_path(manifest_arg)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)
    } else {
        let project = parser::project::DbtProject::load(project_dir)?;
        let paths = project.resolve_paths(project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        graph::builder::build_graph(project_dir, &files)
    }
}

/// Dispatch rendering based on output format
#[cfg(not(tarpaulin_include))]
fn render_output(format: &cli::OutputFormat, graph: &graph::types::LineageGraph) {
    match format {
        cli::OutputFormat::Ascii => render::ascii::render_ascii(graph),
        cli::OutputFormat::Dot => render::dot::render_dot(graph),
        cli::OutputFormat::Json => render::json::render_json(graph),
        cli::OutputFormat::Mermaid => render::mermaid::render_mermaid(graph),
        cli::OutputFormat::Svg => render::svg::render_svg(graph),
        cli::OutputFormat::Html => render::html::render_html(graph),
    }
}

/// Run the `impact` subcommand
#[cfg(not(tarpaulin_include))]
fn run_impact_command(
    model: &str,
    project_dir: &Path,
    output: &cli::ImpactOutputFormat,
    manifest: Option<&PathBuf>,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let dag = if let Some(manifest_arg) = manifest {
        let manifest_path = resolve_manifest_path(manifest_arg)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)?
    } else {
        let project = parser::project::DbtProject::load(&project_dir)?;
        let paths = project.resolve_paths(&project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        graph::builder::build_graph(&project_dir, &files)?
    };

    // Find the source model node
    let source_idx = dag
        .node_indices()
        .find(|&idx| {
            let node = &dag[idx];
            node.label == model || node.unique_id.ends_with(&format!(".{}", model))
        })
        .ok_or_else(|| anyhow::anyhow!("Model '{}' not found in the graph", model))?;

    let report = graph::impact::compute_impact(&dag, source_idx);

    match output {
        cli::ImpactOutputFormat::Text => render::impact::render_impact_text(&report),
        cli::ImpactOutputFormat::Json => render::impact::render_impact_json(&report),
    }

    Ok(())
}

/// Run the `diff` subcommand
#[cfg(not(tarpaulin_include))]
fn run_diff_command(
    base: &str,
    head: Option<&str>,
    project_dir: &Path,
    output: &cli::DiffOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    if !dbt_lineage::git::is_git_repo(&project_dir) {
        anyhow::bail!("Not a git repository: {}", project_dir.display());
    }

    // Validate base ref
    dbt_lineage::git::validate_ref(&project_dir, base)?;

    // Build base graph from git ref
    let base_graph = graph::diff::build_graph_from_ref(&project_dir, base)?;

    // Build head graph (from git ref or working tree)
    let (head_graph, head_label) = if let Some(head_ref) = head {
        dbt_lineage::git::validate_ref(&project_dir, head_ref)?;
        let g = graph::diff::build_graph_from_ref(&project_dir, head_ref)?;
        (g, head_ref.to_string())
    } else {
        // Use current working tree
        let g = build_working_tree_graph(&project_dir)?;
        let label = dbt_lineage::git::current_ref(&project_dir).unwrap_or_else(|_| "HEAD".into());
        (g, label)
    };

    let diff = graph::diff::compute_diff(&base_graph, &head_graph, base, &head_label);

    match output {
        cli::DiffOutputFormat::Text => render::diff::render_diff_text(&diff),
        cli::DiffOutputFormat::Json => render::diff::render_diff_json(&diff),
    }

    Ok(())
}

/// Build a graph from the current working tree
#[cfg(not(tarpaulin_include))]
fn build_working_tree_graph(project_dir: &Path) -> Result<graph::types::LineageGraph> {
    // Try manifest first
    let manifest_path = project_dir.join("target").join("manifest.json");
    if manifest_path.exists() {
        return parser::manifest::build_graph_from_manifest(&manifest_path);
    }

    // Fall back to SQL parsing
    let project = parser::project::DbtProject::load(project_dir)?;
    let paths = project.resolve_paths(project_dir);
    let files = parser::discovery::discover_files(&paths)?;
    graph::builder::build_graph(project_dir, &files)
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
        anyhow::bail!("Manifest path does not exist: {}", manifest_arg.display());
    }
}
