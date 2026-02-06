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

    // Parse dbt project
    let project = parser::project::DbtProject::load(&project_dir)?;
    let paths = project.resolve_paths(&project_dir);

    // Discover files
    let files = parser::discovery::discover_files(&paths)?;

    // Build graph
    let dag = graph::builder::build_graph(&project_dir, &files)?;

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
    }

    Ok(())
}
