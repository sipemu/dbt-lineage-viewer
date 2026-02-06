use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "dbt-lineage", about = "Visualize dbt model lineage")]
pub struct Cli {
    /// Model name to focus on (shows full lineage if omitted)
    pub model: Option<String>,

    /// Path to dbt project directory
    #[arg(short = 'p', long = "project-dir", default_value = ".")]
    pub project_dir: PathBuf,

    /// Upstream levels to show (default: all)
    #[arg(short = 'u', long)]
    pub upstream: Option<usize>,

    /// Downstream levels to show (default: all)
    #[arg(short = 'd', long)]
    pub downstream: Option<usize>,

    /// Launch interactive TUI mode
    #[arg(short = 'i', long)]
    pub interactive: bool,

    /// Output format: ascii (default), dot
    #[arg(short = 'o', long, default_value = "ascii")]
    pub output: OutputFormat,

    /// Include test nodes
    #[arg(long)]
    pub include_tests: bool,

    /// Include seed nodes
    #[arg(long)]
    pub include_seeds: bool,

    /// Include snapshot nodes
    #[arg(long)]
    pub include_snapshots: bool,

    /// Include exposure nodes
    #[arg(long)]
    pub include_exposures: bool,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Ascii,
    Dot,
}
