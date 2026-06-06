use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Parser;

use dbt_lineage::cli::{self, Cli, Command, ErrorFormat};
use dbt_lineage::error::classify;
use dbt_lineage::graph;
use dbt_lineage::parser;
use dbt_lineage::render;

#[cfg(not(tarpaulin_include))]
fn main() {
    let cli = Cli::parse();
    let error_format = cli.error_format;

    if let Err(err) = run(cli) {
        report_error(&err, error_format);
        std::process::exit(1);
    }
}

#[cfg(not(tarpaulin_include))]
fn report_error(err: &anyhow::Error, format: ErrorFormat) {
    match format {
        ErrorFormat::Plain => {
            // Mirror anyhow's default rendering: top-level error followed by causes.
            eprintln!("Error: {}", err);
            let mut chain = err.chain();
            chain.next(); // skip the top error itself
            for cause in chain {
                eprintln!("  caused by: {}", cause);
            }
        }
        ErrorFormat::Json => {
            let s = classify(err);
            // One JSON object per line — stable, line-oriented for agent consumers.
            let line = serde_json::to_string(&s).unwrap_or_else(|_| {
                // Fallback if serialization itself fails (shouldn't happen with our schema).
                format!(
                    r#"{{"level":"error","what":"error","why":{}}}"#,
                    serde_json::to_string(&err.to_string()).unwrap_or_else(|_| "\"\"".into())
                )
            });
            eprintln!("{}", line);
        }
    }
}

#[cfg(not(tarpaulin_include))]
fn run(cli: Cli) -> Result<()> {
    // Handle subcommands first
    if let Some(command) = &cli.command {
        return match command {
            Command::Impact {
                model,
                project_dir,
                output,
                manifest,
            } => run_impact_command(model, project_dir, output, manifest.as_ref()),
            Command::Summary {
                project_dir,
                output,
                manifest,
            } => run_summary_command(project_dir, output, manifest.as_ref()),
            Command::CheckManifest {
                project_dir,
                manifest,
                output,
            } => run_check_manifest_command(project_dir, manifest.as_ref(), output),
            Command::Mcp {
                project_dir,
                manifest,
            } => run_mcp_command(project_dir, manifest.as_ref()),
            Command::Diff {
                base,
                head,
                project_dir,
                output,
            } => run_diff_command(base, head.as_deref(), project_dir, output),
        };
    }

    let project_dir = cli
        .project_dir
        .canonicalize()
        .unwrap_or_else(|_| cli.project_dir.clone());

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

    // Apply --collapse before rendering. Auto/Focal produce a new graph plus a
    // sidecar map of intermediate-hop counts.
    let collapsed = match cli.collapse {
        cli::CollapseMode::None => None,
        cli::CollapseMode::Auto => Some(graph::collapse::collapse_graph(
            &filtered,
            graph::collapse::CollapseMode::Auto,
            cli.model.as_deref(),
        )),
        cli::CollapseMode::Focal => Some(graph::collapse::collapse_graph(
            &filtered,
            graph::collapse::CollapseMode::Focal,
            cli.model.as_deref(),
        )),
    };

    let (graph_ref, via_hops_ref): (&graph::types::LineageGraph, Option<&_>) = match &collapsed {
        Some(c) => (&c.graph, Some(&c.via_hops)),
        None => (&filtered, None),
    };

    render_output(&cli, graph_ref, via_hops_ref);

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
fn render_output(
    cli: &Cli,
    graph: &graph::types::LineageGraph,
    via_hops: Option<&std::collections::HashMap<petgraph::stable_graph::EdgeIndex, usize>>,
) {
    match cli.output {
        cli::OutputFormat::Ascii => render::ascii::render_ascii(graph),
        cli::OutputFormat::Dot => render::dot::render_dot(graph),
        cli::OutputFormat::Json => match via_hops {
            Some(m) => render::json::render_json_with_via(graph, m),
            None => render::json::render_json(graph),
        },
        cli::OutputFormat::Mermaid => {
            let options = render::mermaid::MermaidOptions {
                show_columns: cli.show_columns,
                group_by_directory: matches!(cli.group_by, cli::GroupBy::Directory),
                via_hops,
            };
            render::mermaid::render_mermaid_with_options(graph, options);
        }
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

/// Run the `summary` subcommand
#[cfg(not(tarpaulin_include))]
fn run_summary_command(
    project_dir: &Path,
    output: &cli::SummaryOutputFormat,
    manifest: Option<&PathBuf>,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let (dag, project_name) = if let Some(manifest_arg) = manifest {
        let manifest_path = resolve_manifest_path(manifest_arg)?;
        let g = parser::manifest::build_graph_from_manifest(&manifest_path)?;
        // Try to read project name from a sibling dbt_project.yml; ignore if missing.
        let name = parser::project::DbtProject::load(&project_dir)
            .ok()
            .map(|p| p.name);
        (g, name)
    } else {
        let project = parser::project::DbtProject::load(&project_dir)?;
        let name = Some(project.name.clone());
        let paths = project.resolve_paths(&project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        let g = graph::builder::build_graph(&project_dir, &files)?;
        (g, name)
    };

    let report = graph::summary::compute_summary(&dag, project_name);

    match output {
        cli::SummaryOutputFormat::Text => render::summary::render_summary_text(&report),
        cli::SummaryOutputFormat::Json => render::summary::render_summary_json(&report),
    }

    Ok(())
}

/// Run the `mcp` subcommand: load the manifest, then serve JSON-RPC over stdio.
#[cfg(not(tarpaulin_include))]
fn run_mcp_command(project_dir: &Path, manifest: Option<&PathBuf>) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let manifest_path = match manifest {
        Some(p) => resolve_manifest_path(p)?,
        None => project_dir.join("target").join("manifest.json"),
    };
    if !manifest_path.exists() {
        anyhow::bail!(
            "manifest required for MCP server but not found at {}",
            manifest_path.display()
        );
    }
    let graph = parser::manifest::build_graph_from_manifest(&manifest_path)?;

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    dbt_lineage::mcp::server::run(graph, stdin.lock(), &mut stdout)?;
    Ok(())
}

/// Run the `check-manifest` subcommand. Exits the process with code 1 if the
/// manifest is stale, so it works directly as a CI gate.
#[cfg(not(tarpaulin_include))]
fn run_check_manifest_command(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    output: &cli::CheckManifestOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let manifest_path = match manifest {
        Some(p) => resolve_manifest_path(p)?,
        None => project_dir.join("target").join("manifest.json"),
    };
    if !manifest_path.exists() {
        anyhow::bail!("Manifest path does not exist: {}", manifest_path.display());
    }

    let report = parser::manifest_check::check_manifest(&project_dir, &manifest_path)?;

    match output {
        cli::CheckManifestOutputFormat::Text => render::manifest_check::render_check_text(&report),
        cli::CheckManifestOutputFormat::Json => render::manifest_check::render_check_json(&report),
    }

    if report.is_stale {
        std::process::exit(1);
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
