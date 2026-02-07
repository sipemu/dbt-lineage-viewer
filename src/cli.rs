use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "dbt-lineage", about = "Visualize dbt model lineage")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

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

    /// Output format: ascii (default), dot, json, mermaid, svg, html
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

    /// Selector expression: tag:X, path:Y, or model name (comma-separated)
    #[arg(short = 's', long)]
    pub select: Option<String>,

    /// Use manifest.json instead of parsing SQL (path to manifest file or directory containing target/manifest.json)
    #[arg(long)]
    pub manifest: Option<PathBuf>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Ascii,
    Dot,
    Json,
    Mermaid,
    Svg,
    Html,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Compute downstream impact analysis for a model
    Impact {
        /// Model name to analyze impact for
        model: String,

        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output format: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: ImpactOutputFormat,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
    },

    /// Compare lineage between git refs
    Diff {
        /// Base git ref to compare from (e.g., main, HEAD~1)
        #[arg(long)]
        base: String,

        /// Head git ref to compare to (defaults to working tree)
        #[arg(long)]
        head: Option<String>,

        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output format: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: DiffOutputFormat,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ImpactOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum DiffOutputFormat {
    Text,
    Json,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_default_args() {
        let cli = Cli::try_parse_from(["dbt-lineage"]).unwrap();
        assert!(cli.model.is_none());
        assert!(cli.command.is_none());
        assert!(!cli.interactive);
        assert!(cli.upstream.is_none());
        assert!(cli.downstream.is_none());
        assert!(!cli.include_tests);
        assert!(!cli.include_seeds);
        assert!(!cli.include_snapshots);
        assert!(!cli.include_exposures);
        assert!(cli.select.is_none());
        assert!(cli.manifest.is_none());
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
            "--select",
            "tag:nightly,path:models/staging",
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
        assert_eq!(
            cli.select.as_deref(),
            Some("tag:nightly,path:models/staging")
        );
    }

    #[test]
    fn test_select_short_flag() {
        let cli = Cli::try_parse_from(["dbt-lineage", "-s", "orders,tag:nightly"]).unwrap();
        assert_eq!(cli.select.as_deref(), Some("orders,tag:nightly"));
    }

    #[test]
    fn test_select_long_flag() {
        let cli = Cli::try_parse_from(["dbt-lineage", "--select", "path:models/staging"]).unwrap();
        assert_eq!(cli.select.as_deref(), Some("path:models/staging"));
    }

    #[test]
    fn test_manifest_flag() {
        let cli =
            Cli::try_parse_from(["dbt-lineage", "--manifest", "/path/to/manifest.json"]).unwrap();
        assert_eq!(cli.manifest, Some(PathBuf::from("/path/to/manifest.json")));
    }

    #[test]
    fn test_manifest_flag_directory() {
        let cli = Cli::try_parse_from(["dbt-lineage", "--manifest", "/path/to/project"]).unwrap();
        assert_eq!(cli.manifest, Some(PathBuf::from("/path/to/project")));
    }

    #[test]
    fn test_output_format_parsing() {
        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "ascii"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Ascii));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "dot"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Dot));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "json"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Json));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "mermaid"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Mermaid));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "svg"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Svg));

        let cli = Cli::try_parse_from(["dbt-lineage", "-o", "html"]).unwrap();
        assert!(matches!(cli.output, OutputFormat::Html));

        // Invalid format
        let result = Cli::try_parse_from(["dbt-lineage", "-o", "yaml"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_impact_subcommand() {
        let cli =
            Cli::try_parse_from(["dbt-lineage", "impact", "orders", "-p", "/path/to/project"])
                .unwrap();
        match cli.command {
            Some(Command::Impact {
                ref model,
                ref project_dir,
                ..
            }) => {
                assert_eq!(model, "orders");
                assert_eq!(project_dir, &PathBuf::from("/path/to/project"));
            }
            _ => panic!("Expected Impact subcommand"),
        }
    }

    #[test]
    fn test_impact_subcommand_json() {
        let cli = Cli::try_parse_from(["dbt-lineage", "impact", "orders", "-o", "json"]).unwrap();
        match cli.command {
            Some(Command::Impact { ref output, .. }) => {
                assert!(matches!(output, ImpactOutputFormat::Json));
            }
            _ => panic!("Expected Impact subcommand"),
        }
    }

    #[test]
    fn test_diff_subcommand() {
        let cli = Cli::try_parse_from(["dbt-lineage", "diff", "--base", "main"]).unwrap();
        match cli.command {
            Some(Command::Diff {
                ref base, ref head, ..
            }) => {
                assert_eq!(base, "main");
                assert!(head.is_none());
            }
            _ => panic!("Expected Diff subcommand"),
        }
    }

    #[test]
    fn test_diff_subcommand_with_head() {
        let cli =
            Cli::try_parse_from(["dbt-lineage", "diff", "--base", "main", "--head", "feature"])
                .unwrap();
        match cli.command {
            Some(Command::Diff {
                ref base, ref head, ..
            }) => {
                assert_eq!(base, "main");
                assert_eq!(head.as_deref(), Some("feature"));
            }
            _ => panic!("Expected Diff subcommand"),
        }
    }
}
