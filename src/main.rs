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
            Command::Plan {
                base,
                head,
                project_dir,
                output,
                leaf_only,
                include_tests,
            } => run_plan_command(
                base,
                head.as_deref(),
                project_dir,
                output,
                *leaf_only,
                *include_tests,
            ),
            Command::Coverage {
                project_dir,
                output,
            } => run_coverage_command(project_dir, output),
            Command::Lint {
                project_dir,
                manifest,
                output,
                disable,
            } => run_lint_command(project_dir, manifest.as_ref(), output, disable),
            Command::Macros {
                project_dir,
                output,
            } => run_macros_command(project_dir, output),
            Command::Site {
                project_dir,
                manifest,
                out,
                title,
            } => run_site_command(project_dir, manifest.as_ref(), out, title.as_deref()),
            Command::Docs {
                project_dir,
                manifest,
                out,
                single,
                model,
            } => run_docs_command(
                project_dir,
                manifest.as_ref(),
                out,
                single.as_ref(),
                model.as_deref(),
            ),
            Command::Perf {
                project_dir,
                manifest,
                top,
                output,
            } => run_perf_command(project_dir, manifest.as_ref(), *top, output),
            Command::Diff {
                base,
                head,
                project_dir,
                output,
            } => run_diff_command(base, head.as_deref(), project_dir, output),
            Command::Exposures { source } => match source {
                cli::ExposureSource::Metabase {
                    url,
                    api_key,
                    project_dir,
                    manifest,
                    write,
                    output,
                } => run_exposures_metabase_command(
                    url,
                    api_key,
                    project_dir,
                    manifest.as_ref(),
                    write.as_ref(),
                    output,
                ),
                cli::ExposureSource::Tableau {
                    workbooks,
                    url,
                    project_dir,
                    manifest,
                    write,
                    output,
                } => run_exposures_tableau_command(
                    workbooks,
                    url.as_deref(),
                    project_dir,
                    manifest.as_ref(),
                    write.as_ref(),
                    output,
                ),
                cli::ExposureSource::Looker {
                    lookml,
                    url,
                    project_dir,
                    manifest,
                    write,
                    output,
                } => run_exposures_looker_command(
                    lookml,
                    url.as_deref(),
                    project_dir,
                    manifest.as_ref(),
                    write.as_ref(),
                    output,
                ),
                cli::ExposureSource::Superset {
                    exports,
                    url,
                    project_dir,
                    manifest,
                    write,
                    output,
                } => run_exposures_superset_command(
                    exports,
                    url.as_deref(),
                    project_dir,
                    manifest.as_ref(),
                    write.as_ref(),
                    output,
                ),
                cli::ExposureSource::Anofox {
                    dashboards,
                    url,
                    project_dir,
                    manifest,
                    write,
                    output,
                } => run_exposures_anofox_command(
                    dashboards,
                    url.as_deref(),
                    project_dir,
                    manifest.as_ref(),
                    write.as_ref(),
                    output,
                ),
            },
            Command::Lsp => run_lsp_command(),
            Command::Ci { action } => match action {
                cli::CiAction::Report {
                    base,
                    head,
                    project_dir,
                    manifest,
                    out,
                    outputs,
                    fail_on,
                    no_lint,
                } => run_ci_report_command(&CiReportArgs {
                    base,
                    head: head.as_deref(),
                    project_dir,
                    manifest: manifest.as_ref(),
                    out: out.as_ref(),
                    outputs: outputs.as_ref(),
                    fail_on,
                    no_lint: *no_lint,
                }),
                cli::CiAction::Comment { file } => run_ci_comment_command(file),
                cli::CiAction::Annotations {
                    format,
                    project_dir,
                    manifest,
                    out,
                    post,
                } => run_ci_annotations_command(
                    *format,
                    project_dir,
                    manifest.as_ref(),
                    out.as_ref(),
                    *post,
                ),
            },
        };
    }

    let project_dir = cli
        .project_dir
        .canonicalize()
        .unwrap_or_else(|_| cli.project_dir.clone());

    let dag = build_dag(&project_dir, cli.manifest.as_ref(), cli.no_cache)?;

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
fn build_dag(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    no_cache: bool,
) -> Result<graph::types::LineageGraph> {
    if let Some(manifest_arg) = manifest {
        let manifest_path = resolve_manifest_path(manifest_arg)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)
    } else {
        let project = parser::project::DbtProject::load(project_dir)?;
        let paths = project.resolve_paths(project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        if no_cache {
            graph::builder::build_graph_no_cache(project_dir, &files)
        } else {
            graph::builder::build_graph(project_dir, &files)
        }
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

/// Run the `plan` subcommand: emit a dbt selector for the rebuild set.
#[cfg(not(tarpaulin_include))]
fn run_plan_command(
    base: &str,
    head: Option<&str>,
    project_dir: &Path,
    output: &cli::PlanOutputFormat,
    leaf_only: bool,
    include_tests: bool,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    if !dbt_lineage::git::is_git_repo(&project_dir) {
        anyhow::bail!("Not a git repository: {}", project_dir.display());
    }
    dbt_lineage::git::validate_ref(&project_dir, base)?;
    if let Some(head_ref) = head {
        dbt_lineage::git::validate_ref(&project_dir, head_ref)?;
    }
    let diff = graph::diff::diff_git(&project_dir, base, head)?;
    let style = if leaf_only {
        graph::plan::PlanStyle::LeafOnly
    } else {
        graph::plan::PlanStyle::Downstream
    };
    let report = graph::plan::build_plan(&diff, style, !include_tests);

    match output {
        cli::PlanOutputFormat::Selector => render::plan::render_plan_selector(&report),
        cli::PlanOutputFormat::Json => render::plan::render_plan_json(&report),
    }
    Ok(())
}

/// Run the `perf` subcommand: read run_results.json + compute critical paths.
#[cfg(not(tarpaulin_include))]
fn run_perf_command(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    top: usize,
    output: &cli::PerfOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let dag = if let Some(m) = manifest {
        let manifest_path = resolve_manifest_path(m)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)?
    } else {
        let project = parser::project::DbtProject::load(&project_dir)?;
        let paths = project.resolve_paths(&project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        graph::builder::build_graph(&project_dir, &files)?
    };

    let run_results = parser::artifacts::load_run_results(&project_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no run_results.json in {}/target — run 'dbt build' first",
            project_dir.display()
        )
    })?;
    let report = graph::perf::compute_perf(&dag, &run_results);

    match output {
        cli::PerfOutputFormat::Text => render::perf::render_perf_text(&report, top),
        cli::PerfOutputFormat::Json => render::perf::render_perf_json(&report),
    }
    Ok(())
}

/// Run the `macros` subcommand: scan `macros/*.sql` and emit the call graph.
#[cfg(not(tarpaulin_include))]
fn run_macros_command(project_dir: &Path, output: &cli::MacrosOutputFormat) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let graph = graph::macros::analyze_macros(&project_dir)?;
    match output {
        cli::MacrosOutputFormat::Text => render::macros::render_macros_text(&graph),
        cli::MacrosOutputFormat::Json => render::macros::render_macros_json(&graph),
        cli::MacrosOutputFormat::Mermaid => render::macros::render_macros_mermaid(&graph),
    }
    Ok(())
}

/// Run the `site` subcommand: write the static lineage site.
#[cfg(not(tarpaulin_include))]
fn run_site_command(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    out: &Path,
    title: Option<&str>,
) -> Result<()> {
    let started = std::time::Instant::now();
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let column_lineage = merge_column_lineage(
        parser::ast_column_lineage::resolve_column_lineage_ast_in(&dag, &project_dir),
        parser::column_lineage::resolve_column_lineage_in(&dag, &project_dir),
    );
    let title = title.map(String::from).unwrap_or_else(|| {
        parser::project::DbtProject::load(&project_dir)
            .map(|p| p.name)
            .unwrap_or_else(|_| "dbt lineage".into())
    });
    let stats = render::site::write_site(&dag, &column_lineage, &project_dir, out, &title)?;
    eprintln!(
        "site: wrote index + {} node pages to {} in {:.2}s",
        stats.node_pages,
        out.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Run the `docs` subcommand: write per-model Markdown.
#[cfg(not(tarpaulin_include))]
fn run_docs_command(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    out: &Path,
    single: Option<&PathBuf>,
    model: Option<&str>,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = if let Some(m) = manifest {
        let manifest_path = resolve_manifest_path(m)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)?
    } else {
        let project = parser::project::DbtProject::load(&project_dir)?;
        let paths = project.resolve_paths(&project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        graph::builder::build_graph(&project_dir, &files)?
    };

    // Single-model mode → stdout.
    if let Some(label) = model {
        let idx = dag
            .node_indices()
            .find(|&i| dag[i].label == label)
            .ok_or_else(|| anyhow::anyhow!("model '{}' not found in project", label))?;
        let mut out = std::io::stdout().lock();
        render::docs::render_model_md(&dag, idx, &mut out)?;
        return Ok(());
    }

    if let Some(path) = single {
        let mut buf: Vec<u8> = Vec::new();
        render::docs::for_each_model_doc(&dag, |_label, md| {
            use std::io::Write;
            buf.write_all(md.as_bytes())?;
            buf.write_all(b"\n---\n\n")?;
            Ok(())
        })?;
        std::fs::write(path, buf)?;
        eprintln!("docs: wrote consolidated file to {}", path.display());
        return Ok(());
    }

    let count = render::docs::write_models_to_dir(&dag, out)?;
    eprintln!("docs: wrote {} model files to {}", count, out.display());
    Ok(())
}

/// Run the `lint` subcommand. Exits 1 if there are any error-severity findings.
#[cfg(not(tarpaulin_include))]
fn run_lint_command(
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    output: &cli::LintOutputFormat,
    disabled: &[String],
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let dag = if let Some(m) = manifest {
        let manifest_path = resolve_manifest_path(m)?;
        parser::manifest::build_graph_from_manifest(&manifest_path)?
    } else {
        let project = parser::project::DbtProject::load(&project_dir)?;
        let paths = project.resolve_paths(&project_dir);
        let files = parser::discovery::discover_files(&paths)?;
        graph::builder::build_graph(&project_dir, &files)?
    };

    let mut config = graph::lint::LintConfig::default();
    for d in disabled {
        match d.as_str() {
            "unused-source" => config.unused_source = false,
            "undefined-source" => config.undefined_source = false,
            "dead-end-model" => config.dead_end_model = false,
            "missing-description" => config.missing_description = false,
            "model-without-source" => config.model_without_source = false,
            "circular-ref" => config.circular_ref = false,
            "private-ref" => {} // governance rule, filtered below
            other => anyhow::bail!("unknown lint rule: {}", other),
        }
    }
    let mut report = graph::lint::lint(&dag, &config);
    if !disabled.iter().any(|d| d == "private-ref") {
        report
            .findings
            .extend(graph::governance::private_ref_findings(
                &dag,
                &graph::governance::load_governance(&project_dir),
            ));
    }

    match output {
        cli::LintOutputFormat::Text => render::lint::render_lint_text(&report),
        cli::LintOutputFormat::Json => render::lint::render_lint_json(&report),
        cli::LintOutputFormat::Sarif => {
            render::lint::render_lint_sarif(&report, &repo_locations(&dag, &project_dir))
        }
        cli::LintOutputFormat::Codeclimate => {
            render::lint::render_lint_codeclimate(&report, &repo_locations(&dag, &project_dir))
        }
    }

    if report.count_by_severity(graph::lint::LintSeverity::Error) > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Run the `coverage` subcommand: walk YAML schema files and aggregate test data.
#[cfg(not(tarpaulin_include))]
fn run_coverage_command(project_dir: &Path, output: &cli::CoverageOutputFormat) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    let project = parser::project::DbtProject::load(&project_dir)?;
    let paths = project.resolve_paths(&project_dir);
    let files = parser::discovery::discover_files(&paths)?;
    let report = graph::coverage::compute_coverage(&project_dir, &files)?;

    match output {
        cli::CoverageOutputFormat::Text => render::coverage::render_coverage_text(&report),
        cli::CoverageOutputFormat::Json => render::coverage::render_coverage_json(&report),
        cli::CoverageOutputFormat::Sarif => {
            let dag = graph::builder::build_graph(&project_dir, &files)?;
            render::coverage::render_coverage_sarif(&report, &repo_locations(&dag, &project_dir))
        }
    }
    Ok(())
}

/// Union two `ColumnLineage` outputs, deduplicating by (source_node,
/// source_column, target_node, target_column). The AST analyzer's edges take
/// precedence on ties (better confidence). Edges unique to either analyzer
/// are kept as-is.
#[cfg(not(tarpaulin_include))]
fn merge_column_lineage(
    primary: parser::column_lineage::ColumnLineage,
    secondary: parser::column_lineage::ColumnLineage,
) -> parser::column_lineage::ColumnLineage {
    use std::collections::HashSet;
    let mut seen: HashSet<(String, String, String, String)> = HashSet::new();
    let mut edges = Vec::with_capacity(primary.edges.len() + secondary.edges.len());
    for e in primary.edges.into_iter().chain(secondary.edges) {
        let key = (
            e.source_node.clone(),
            e.source_column.clone(),
            e.target_node.clone(),
            e.target_column.clone(),
        );
        if seen.insert(key) {
            edges.push(e);
        }
    }
    parser::column_lineage::ColumnLineage { edges }
}

/// Run the `mcp` subcommand: load the graph (manifest preferred, SQL-parse fallback)
/// and serve JSON-RPC over stdio.
#[cfg(not(tarpaulin_include))]
fn run_mcp_command(project_dir: &Path, manifest: Option<&PathBuf>) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());

    // Explicit --manifest is honored as-is and surfaces errors normally.
    // Otherwise: try target/manifest.json, fall back to SQL-parse mode silently.
    // When a manifest is involved, also extract its raw/compiled SQL so
    // read_model_sql can serve content without on-disk files.
    let (graph, manifest_sql) = if let Some(p) = manifest {
        let manifest_path = resolve_manifest_path(p)?;
        let g = parser::manifest::build_graph_from_manifest(&manifest_path)?;
        let sql_map = parser::manifest::build_sql_map_from_manifest(&manifest_path)?;
        (g, sql_map)
    } else {
        let candidate = project_dir.join("target").join("manifest.json");
        if candidate.exists() {
            let g = parser::manifest::build_graph_from_manifest(&candidate)?;
            let sql_map = parser::manifest::build_sql_map_from_manifest(&candidate)?;
            (g, sql_map)
        } else {
            let project = parser::project::DbtProject::load(&project_dir)?;
            let paths = project.resolve_paths(&project_dir);
            let files = parser::discovery::discover_files(&paths)?;
            let g = graph::builder::build_graph(&project_dir, &files)?;
            (g, std::collections::HashMap::new())
        }
    };

    // Union the AST-backed analyzer (#35) and the regex-backed analyzer. The AST
    // path is accurate when it parses, the regex path provides coverage for SQL
    // dialects/shapes sqlparser can't yet handle. Duplicate edges are deduped.
    let column_lineage = merge_column_lineage(
        parser::ast_column_lineage::resolve_column_lineage_ast_in(&graph, &project_dir),
        parser::column_lineage::resolve_column_lineage_in(&graph, &project_dir),
    );

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let ctx = dbt_lineage::mcp::server::McpContext {
        graph,
        project_dir,
        manifest_sql,
        column_lineage,
    };
    dbt_lineage::mcp::server::run(ctx, stdin.lock(), &mut stdout)?;
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

    dbt_lineage::git::validate_ref(&project_dir, base)?;
    if let Some(head_ref) = head {
        dbt_lineage::git::validate_ref(&project_dir, head_ref)?;
    }
    let diff = graph::diff::diff_git(&project_dir, base, head)?;

    match output {
        cli::DiffOutputFormat::Text => render::diff::render_diff_text(&diff),
        cli::DiffOutputFormat::Json => render::diff::render_diff_json(&diff),
    }

    Ok(())
}

struct CiReportArgs<'a> {
    base: &'a str,
    head: Option<&'a str>,
    project_dir: &'a Path,
    manifest: Option<&'a PathBuf>,
    out: Option<&'a PathBuf>,
    outputs: Option<&'a PathBuf>,
    fail_on: &'a [dbt_lineage::ci::FailOn],
    no_lint: bool,
}

/// Run `ci report`: every analysis a PR/MR comment needs, rendered as one
/// Markdown document. A failing section is reported in the document instead
/// of aborting, so the comment still appears.
#[cfg(not(tarpaulin_include))]
fn run_ci_report_command(args: &CiReportArgs) -> Result<()> {
    use dbt_lineage::ci::{self, CiReport, Platform};
    use std::io::Write;

    let project_dir = args
        .project_dir
        .canonicalize()
        .unwrap_or_else(|_| args.project_dir.to_path_buf());
    let env: std::collections::HashMap<String, String> = std::env::vars().collect();
    let platform = Platform::detect(&env);

    let mut report = CiReport::default();
    let mut record = |section: &'static str, err: anyhow::Error| {
        report_section_error(&mut report.errors, section, err)
    };

    // Summary + lint share one graph (manifest when given).
    let project_name = parser::project::DbtProject::load(&project_dir)
        .ok()
        .map(|p| p.name);
    match build_dag(&project_dir, args.manifest, false) {
        Ok(dag) => {
            report.summary = Some(graph::summary::compute_summary(&dag, project_name));
            if !args.no_lint {
                let mut lint = graph::lint::lint(&dag, &graph::lint::LintConfig::default());
                lint.findings
                    .extend(graph::governance::private_ref_findings(
                        &dag,
                        &graph::governance::load_governance(&project_dir),
                    ));
                report.lint = Some(lint);
            }
        }
        Err(e) => record("summary", e),
    }

    // Diff + plan against the PR/MR base.
    let base = match args.base {
        "auto" => platform.target_branch(&env),
        "" => None,
        b => Some(b.to_string()),
    };
    if let Some(base) = base {
        match ci_diff(&project_dir, &base, args.head) {
            Ok((diff, head_graph)) => {
                report.impact_diagram = ci::impact_mermaid(&diff, &head_graph);
                report.plan = Some(graph::plan::build_plan(
                    &diff,
                    graph::plan::PlanStyle::Downstream,
                    true,
                ));
                report.diff = Some(diff);
            }
            Err(e) => record("diff", e),
        }
    }

    // Manifest freshness, only when a manifest is in play.
    let manifest_path = match args.manifest {
        Some(m) => resolve_manifest_path(m).ok(),
        None => Some(project_dir.join("target").join("manifest.json")).filter(|p| p.exists()),
    };
    if let Some(path) = manifest_path {
        match parser::manifest_check::check_manifest(&project_dir, &path) {
            Ok(check) => report.manifest_check = Some(check),
            Err(e) => record("manifest", e),
        }
    }

    let failures = report.failures(args.fail_on);
    let markdown = ci::render_markdown(&report);
    match args.out {
        Some(path) => std::fs::write(path, &markdown)?,
        None => print!("{}", markdown),
    }
    if let Some(path) = args.outputs {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(f, "selector={}", report.selector())?;
        writeln!(f, "failed={}", !failures.is_empty())?;
    }

    if !failures.is_empty() {
        for reason in &failures {
            match platform {
                Platform::GitHub => eprintln!("::error::dbt-lineage: {}", reason),
                Platform::AzureDevOps => {
                    eprintln!("##vso[task.logissue type=error]dbt-lineage: {}", reason)
                }
                _ => eprintln!("dbt-lineage: {}", reason),
            }
        }
        std::process::exit(1);
    }
    Ok(())
}

/// Run `exposures metabase`: fetch dashboards, map their tables onto models
/// and sources, emit dbt exposures YAML.
#[cfg(not(tarpaulin_include))]
fn run_exposures_metabase_command(
    url: &str,
    api_key: &str,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;

    let http = dbt_lineage::ci::comment::UreqHttp;
    let mut client = dbt_lineage::bi::metabase::Client::new(&http, url, api_key);
    let dashboards = client.dashboards()?;
    emit_exposures(&dag, &dashboards, write, output)
}

/// Run `exposures anofox`: read dashboard `.sql` files.
#[cfg(not(tarpaulin_include))]
fn run_exposures_anofox_command(
    dashboards: &[PathBuf],
    url: Option<&str>,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let dashboards = dbt_lineage::bi::anofox::dashboards_from_paths(dashboards, url)?;
    emit_exposures(&dag, &dashboards, write, output)
}

/// Run `exposures tableau`: read workbook files offline.
#[cfg(not(tarpaulin_include))]
fn run_exposures_tableau_command(
    workbooks: &[PathBuf],
    url: Option<&str>,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let (dashboards, notes) = dbt_lineage::bi::tableau::dashboards_from_paths(workbooks, url)?;
    for note in &notes {
        eprintln!("dbt-lineage: {}", note);
    }
    emit_exposures(&dag, &dashboards, write, output)
}

/// Run `exposures looker`: read a LookML project offline.
#[cfg(not(tarpaulin_include))]
fn run_exposures_looker_command(
    lookml: &[PathBuf],
    url: Option<&str>,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let dashboards = dbt_lineage::bi::looker::dashboards_from_paths(lookml, url)?;
    emit_exposures(&dag, &dashboards, write, output)
}

/// Run `exposures superset`: read dashboard exports offline.
#[cfg(not(tarpaulin_include))]
fn run_exposures_superset_command(
    exports: &[PathBuf],
    url: Option<&str>,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let (dashboards, notes) = dbt_lineage::bi::superset::dashboards_from_paths(exports, url)?;
    for note in &notes {
        eprintln!("dbt-lineage: {}", note);
    }
    emit_exposures(&dag, &dashboards, write, output)
}

/// Map dashboards onto the graph, report unmatched tables, emit YAML/JSON.
#[cfg(not(tarpaulin_include))]
fn emit_exposures(
    dag: &graph::types::LineageGraph,
    dashboards: &[dbt_lineage::bi::Dashboard],
    write: Option<&PathBuf>,
    output: &cli::ExposuresOutputFormat,
) -> Result<()> {
    let mapped = dbt_lineage::bi::map_dashboards(dag, dashboards);

    for (dashboard, table) in &mapped.unmatched {
        eprintln!(
            "dbt-lineage: '{}' reads '{}', which matches no model or source",
            dashboard, table
        );
    }
    for dashboard in &mapped.skipped {
        eprintln!(
            "dbt-lineage: skipped '{}' (no table matches the project)",
            dashboard
        );
    }
    eprintln!(
        "dbt-lineage: {} of {} dashboards mapped to exposures",
        mapped.exposures.len(),
        dashboards.len()
    );

    let yaml = dbt_lineage::bi::to_yaml(&mapped.exposures);
    match write {
        Some(path) => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, yaml)?;
            eprintln!("dbt-lineage: wrote {}", path.display());
        }
        None => match output {
            cli::ExposuresOutputFormat::Yaml => print!("{}", yaml),
            cli::ExposuresOutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&mapped.exposures)?)
            }
        },
    }
    Ok(())
}

/// Run the `lsp` subcommand: serve LSP over stdio.
#[cfg(not(tarpaulin_include))]
fn run_lsp_command() -> Result<()> {
    #[cfg(feature = "lsp")]
    return dbt_lineage::lsp::run_stdio();
    #[cfg(not(feature = "lsp"))]
    anyhow::bail!("LSP feature not enabled. Rebuild with --features lsp")
}

/// Run `ci comment`: upsert the sticky PR/MR comment from a Markdown file.
#[cfg(not(tarpaulin_include))]
fn run_ci_comment_command(file: &Path) -> Result<()> {
    use dbt_lineage::ci::comment::{upsert, Outcome, Target, UreqHttp};
    use std::io::Read;

    let body = if file == Path::new("-") {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        s
    } else {
        std::fs::read_to_string(file)
            .map_err(|e| anyhow::anyhow!("cannot read {}: {}", file.display(), e))?
    };
    let env: std::collections::HashMap<String, String> = std::env::vars().collect();
    let platform = dbt_lineage::ci::Platform::detect(&env);
    let target = Target::from_env(platform, &env)?;
    match upsert(&UreqHttp, &target, &body)? {
        Outcome::Created => eprintln!("dbt-lineage: created PR/MR comment"),
        Outcome::Updated => eprintln!("dbt-lineage: updated PR/MR comment"),
    }
    Ok(())
}

/// Run `ci annotations`: lint findings in a CI platform's annotation format.
#[cfg(not(tarpaulin_include))]
fn run_ci_annotations_command(
    format: cli::AnnotationFormat,
    project_dir: &Path,
    manifest: Option<&PathBuf>,
    out: Option<&PathBuf>,
    post: bool,
) -> Result<()> {
    use dbt_lineage::ci::annotations as ann;

    if post && format != cli::AnnotationFormat::Bitbucket {
        anyhow::bail!("--post is only supported with --format bitbucket");
    }
    let project_dir = project_dir
        .canonicalize()
        .unwrap_or_else(|_| project_dir.to_path_buf());
    let dag = build_dag(&project_dir, manifest, false)?;
    let mut lint = graph::lint::lint(&dag, &graph::lint::LintConfig::default());
    lint.findings
        .extend(graph::governance::private_ref_findings(
            &dag,
            &graph::governance::load_governance(&project_dir),
        ));
    let locations = repo_locations(&dag, &project_dir);

    let mut buf: Vec<u8> = Vec::new();
    match format {
        cli::AnnotationFormat::Sarif => {
            render::lint::render_lint_sarif_to_writer(&lint, &locations, &mut buf)
        }
        cli::AnnotationFormat::Codeclimate => {
            render::lint::render_lint_codeclimate_to_writer(&lint, &locations, &mut buf)
        }
        cli::AnnotationFormat::Azure => {
            buf.extend(ann::azure_logging_commands(&lint, &locations).into_bytes())
        }
        cli::AnnotationFormat::Bitbucket => {
            let report = ann::bitbucket_report(&lint);
            let annotations = ann::bitbucket_annotations(&lint, &locations);
            if post {
                let env: std::collections::HashMap<String, String> = std::env::vars().collect();
                let target = ann::BitbucketTarget::from_env(&env)?;
                let sent = match &target.proxy {
                    Some(proxy) => ann::post_bitbucket(
                        &dbt_lineage::ci::comment::ProxyHttp::new(proxy)?,
                        &target,
                        &report,
                        &annotations,
                    )?,
                    None => ann::post_bitbucket(
                        &dbt_lineage::ci::comment::UreqHttp,
                        &target,
                        &report,
                        &annotations,
                    )?,
                };
                eprintln!(
                    "dbt-lineage: Code Insights report '{}' with {} annotation(s) on {}",
                    ann::REPORT_ID,
                    sent,
                    target.commit
                );
            }
            serde_json::to_writer_pretty(
                &mut buf,
                &serde_json::json!({"report": report, "annotations": annotations}),
            )?;
            buf.push(b'\n');
        }
    }
    match out {
        Some(path) => std::fs::write(path, buf)?,
        None if post => {}
        None => {
            use std::io::Write;
            std::io::stdout().write_all(&buf)?;
        }
    }
    Ok(())
}

#[cfg(not(tarpaulin_include))]
fn report_section_error(
    errors: &mut Vec<dbt_lineage::ci::SectionError>,
    section: &'static str,
    err: anyhow::Error,
) {
    eprintln!("dbt-lineage: {} skipped: {:#}", section, err);
    errors.push(dbt_lineage::ci::SectionError {
        section,
        message: format!("{:#}", err),
    });
}

/// Diff for `ci report`, resolving the base the way CI checkouts need.
/// Also returns the head graph for the impact diagram.
#[cfg(not(tarpaulin_include))]
fn ci_diff(
    project_dir: &Path,
    base: &str,
    head: Option<&str>,
) -> Result<(graph::diff::LineageDiff, graph::types::LineageGraph)> {
    if !dbt_lineage::git::is_git_repo(project_dir) {
        anyhow::bail!("Not a git repository: {}", project_dir.display());
    }
    let base = dbt_lineage::git::resolve_base_ref(project_dir, base)?;
    if let Some(head_ref) = head {
        dbt_lineage::git::validate_ref(project_dir, head_ref)?;
    }
    graph::diff::diff_git_with_head(project_dir, &base, head)
}

/// File locations relative to the repository root, for inline CI annotations.
#[cfg(not(tarpaulin_include))]
fn repo_locations(
    dag: &graph::types::LineageGraph,
    project_dir: &Path,
) -> render::locations::Locations {
    let prefix = dbt_lineage::git::show_prefix(project_dir).unwrap_or_default();
    render::locations::Locations::from_graph(dag, project_dir, &prefix)
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
