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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_default_args() {
        let cli = Cli::try_parse_from(["dbt-lineage"]).unwrap();
        assert!(cli.model.is_none());
        assert!(!cli.interactive);
        assert!(cli.upstream.is_none());
        assert!(cli.downstream.is_none());
        assert!(!cli.include_tests);
        assert!(!cli.include_seeds);
        assert!(!cli.include_snapshots);
        assert!(!cli.include_exposures);
        assert!(matches!(cli.output, OutputFormat::Ascii));
    }

    #[test]
    fn test_all_flags() {
        let cli = Cli::try_parse_from([
            "dbt-lineage",
            "my_model",
            "-p",
            "/path/to/project",
            "-u",
            "2",
            "-d",
            "3",
            "-i",
            "-o",
            "dot",
            "--include-tests",
            "--include-seeds",
            "--include-snapshots",
            "--include-exposures",
        ])
        .unwrap();
        assert_eq!(cli.model.as_deref(), Some("my_model"));
        assert_eq!(cli.project_dir, PathBuf::from("/path/to/project"));
        assert_eq!(cli.upstream, Some(2));
        assert_eq!(cli.downstream, Some(3));
        assert!(cli.interactive);
        assert!(matches!(cli.output, OutputFormat::Dot));
        assert!(cli.include_tests);
        assert!(cli.include_seeds);
        assert!(cli.include_snapshots);
        assert!(cli.include_exposures);
    }

    #[test]
    fn test_output_format_parsing() {
        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "ascii"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Ascii));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "dot"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Dot));

        // Invalid format
        let result = Cli::try_parse_from(["dbt-lineage", "-o", "json"]);
        assert!(result.is_err());
    }
}
