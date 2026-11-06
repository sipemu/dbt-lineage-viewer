use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "dbt-lineage", version, about = "Visualize dbt model lineage")]
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

    /// Mermaid only: inline column names inside each node's label.
    #[arg(long)]
    pub show_columns: bool,

    /// Mermaid only: group nodes by directory using Mermaid subgraphs.
    #[arg(long, value_enum, default_value = "none")]
    pub group_by: GroupBy,

    /// How errors are written to stderr: human-readable text (default) or one JSON
    /// object per line (`{level, what, why, hint}`) for agent/CI consumers.
    #[arg(long, value_enum, default_value = "plain", global = true)]
    pub error_format: ErrorFormat,

    /// Drop intermediate nodes and replace transitive paths with `(via N)` edges.
    /// Bare `--collapse` is equivalent to `--collapse auto` (keep endpoints + focus);
    /// `--collapse focal` keeps only sources, exposures, and the focus model.
    #[arg(
        long,
        value_enum,
        num_args = 0..=1,
        default_missing_value = "auto",
        default_value = "none"
    )]
    pub collapse: CollapseMode,

    /// Bypass the on-disk parse cache (.dbt-lineage/cache.bin). Useful when
    /// debugging parser changes or you want a clean baseline benchmark.
    #[arg(long, global = true)]
    pub no_cache: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum CollapseMode {
    /// Do not collapse (default).
    None,
    /// Keep endpoints (no predecessors or no successors), plus sources/exposures
    /// and the positional focus model.
    Auto,
    /// Keep only sources, exposures, and the positional focus model. BFS-window
    /// pseudo-endpoints are NOT kept; pair with `-u`/`-d` to bound traversal.
    Focal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ErrorFormat {
    /// Human-readable, multi-line errors (default).
    Plain,
    /// One JSON object per line on stderr.
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum GroupBy {
    /// No grouping (default).
    None,
    /// Group nodes by source-file directory.
    Directory,
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

    /// Print a one-shot project overview: counts, tags, top fan-out, orphans
    Summary {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output format: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: SummaryOutputFormat,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
    },

    /// Run an MCP (Model Context Protocol) stdio server exposing lineage tools
    /// to AI agents. Reads JSON-RPC requests on stdin, writes responses to stdout.
    Mcp {
        /// Path to dbt project directory (used to find target/manifest.json if --manifest is omitted)
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Manifest file or directory containing target/manifest.json
        #[arg(long)]
        manifest: Option<PathBuf>,
    },

    /// Run a Language Server (stdio) for editors: hover and go-to-definition on
    /// ref()/source(), a lineage code lens per model, and lint diagnostics.
    Lsp,
    /// Critical-path and slowest-model performance report from target/run_results.json.
    Perf {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// How many entries to show in the slowest / critical-path lists
        #[arg(long, default_value = "10")]
        top: usize,

        /// Output: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: PerfOutputFormat,
    },

    /// Show the macro call graph (macros calling other macros).
    Macros {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output: text (default), json, or mermaid
        #[arg(short = 'o', long, default_value = "text")]
        output: MacrosOutputFormat,
    },

    /// Generate a static lineage site (search, one page per node with
    /// columns, column lineage, SQL and a lineage diagram) for GitHub/GitLab Pages.
    Site {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// Output directory
        #[arg(long, default_value = "site")]
        out: PathBuf,

        /// Site title (defaults to the dbt project name)
        #[arg(long)]
        title: Option<String>,
    },
    /// Generate Markdown documentation per model (or one consolidated file).
    Docs {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// Output directory (one .md per model). Mutually exclusive with --single.
        #[arg(long, default_value = "docs/models")]
        out: PathBuf,

        /// Write all models into one concatenated Markdown file at this path.
        #[arg(long, conflicts_with = "out")]
        single: Option<PathBuf>,

        /// Render a single model's docs to stdout (overrides --out / --single).
        model: Option<String>,
    },

    /// Structural lint: dead-end models, unused sources, undefined refs, missing docs.
    /// Exit code 1 if any error-severity finding is reported.
    Lint {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// Output: text (default), json, sarif (GitHub code scanning), or
        /// codeclimate (GitLab Code Quality)
        #[arg(short = 'o', long, default_value = "text")]
        output: LintOutputFormat,

        /// Disable a rule (repeatable). Available: unused-source, undefined-source,
        /// dead-end-model, missing-description, model-without-source, circular-ref,
        /// private-ref.
        #[arg(long = "disable")]
        disable: Vec<String>,
    },

    /// Test coverage map: which models have which tests, column coverage gaps.
    Coverage {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output: text (default), json, or sarif (for GitHub PR annotations)
        #[arg(short = 'o', long, default_value = "text")]
        output: CoverageOutputFormat,
    },

    /// Verify a manifest.json is still in sync with the project's SQL files.
    /// Exits with code 1 on staleness, making it directly usable in CI pipelines.
    CheckManifest {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of the default `<project>/target/manifest.json`
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// Output format: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: CheckManifestOutputFormat,
    },

    /// Print the column lineage of every model as JSON (one edge per upstream
    /// column). Used to measure column-lineage accuracy.
    #[command(hide = true)]
    ColumnLineage {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
    },

    /// Check how closely SQL-parse lineage matches dbt's own: compares the
    /// dependencies found by parsing with a manifest.json from `dbt parse`.
    /// Exits with code 1 when parsing missed a dependency dbt knows about.
    Verify {
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Use manifest.json instead of the default `<project>/target/manifest.json`
        #[arg(long)]
        manifest: Option<PathBuf>,

        /// Output format: text (default) or json
        #[arg(short = 'o', long, default_value = "text")]
        output: VerifyOutputFormat,
    },

    /// Emit a dbt selector expression covering models added or modified since
    /// `--base`. Designed to pipe into `dbt run -s "$(dbt-lineage plan --base main)"`.
    Plan {
        /// Base git ref to compare from (e.g., main, HEAD~1)
        #[arg(long)]
        base: String,

        /// Head git ref to compare to (defaults to working tree)
        #[arg(long)]
        head: Option<String>,

        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,

        /// Output: `selector` (default; whitespace-joined for piping) or `json`.
        #[arg(short = 'o', long, default_value = "selector")]
        output: PlanOutputFormat,

        /// Drop the `+` suffix and emit only the changed models themselves (no downstream).
        #[arg(long)]
        leaf_only: bool,

        /// Include test nodes in the plan (dbt rebuilds them with parents by default).
        #[arg(long)]
        include_tests: bool,
    },

    /// CI integration: one Markdown report for PR/MR comments, plus exit codes
    /// for gating. Works on GitHub Actions, GitLab CI, Bitbucket Pipelines and
    /// Azure Pipelines.
    Ci {
        #[command(subcommand)]
        action: CiAction,
    },
    /// Import BI dashboards as dbt exposures (YAML), mapping the tables they
    /// read onto the project's models and sources.
    Exposures {
        #[command(subcommand)]
        source: ExposureSource,
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

#[derive(Subcommand, Debug)]
pub enum ExposureSource {
    /// Read dashboards from Metabase (REST API with an API key).
    Metabase {
        /// Metabase base URL, e.g. `https://metabase.example.com`
        #[arg(long)]
        url: String,
        /// Metabase API key (Admin > Settings > Authentication > API keys)
        #[arg(long, env = "METABASE_API_KEY", hide_env_values = true)]
        api_key: String,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_metabase.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
    /// Read Tableau workbooks offline (`.twb`, `.twbx`); directories are
    /// searched recursively.
    Tableau {
        /// Workbook files or directories
        #[arg(long = "workbooks", required = true, num_args = 1..)]
        workbooks: Vec<PathBuf>,
        /// Exposure URL; `{name}` is replaced by the workbook file name
        /// without extension (default: the file path)
        #[arg(long)]
        url: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_tableau.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
    /// Read Apache Superset dashboard exports offline (ZIPs from
    /// `superset export-dashboards` or the UI, unpacked export directories,
    /// or the legacy JSON export); no Superset API access needed.
    Superset {
        /// Export ZIPs, directories or files
        #[arg(long = "exports", required = true, num_args = 1..)]
        exports: Vec<PathBuf>,
        /// Exposure URL; `{name}` is replaced by the dashboard slug, or its
        /// uuid (legacy JSON: id) when it has none (default: the export path)
        #[arg(long)]
        url: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_superset.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
    /// Read anofox-visualization dashboards (DuckDB `.sql` files with `::ROLE`
    /// panels); directories are searched recursively.
    Anofox {
        /// Dashboard files or directories
        #[arg(long = "dashboards", required = true, num_args = 1..)]
        dashboards: Vec<PathBuf>,
        /// Exposure URL; `{name}` is replaced by the dashboard file name
        /// without extension (default: the file path)
        #[arg(long)]
        url: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_anofox.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
    /// Read LookML dashboards offline from a LookML project (views, explores,
    /// `.dashboard.lookml` files); no Looker API access needed.
    Looker {
        /// LookML project directories or files
        #[arg(long = "lookml", required = true, num_args = 1..)]
        lookml: Vec<PathBuf>,
        /// Exposure URL; `{name}` is replaced by the dashboard id
        /// (default: the dashboard file path)
        #[arg(long)]
        url: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_looker.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
    /// Read Power BI reports offline: PBIP projects (`*.Report`,
    /// `*.SemanticModel` with TMDL or `model.bim`) and `.pbit` templates;
    /// directories are searched recursively.
    Powerbi {
        /// Report files or directories (PBIP folders, `.pbip`, `.pbit`, `.bim`)
        #[arg(long = "reports", required = true, num_args = 1..)]
        reports: Vec<PathBuf>,
        /// Exposure URL; `{name}` is replaced by the report name
        /// (default: the report path)
        #[arg(long)]
        url: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the YAML to this file (e.g. models/exposures_powerbi.yml)
        /// instead of stdout
        #[arg(long)]
        write: Option<PathBuf>,
        /// Output format for stdout: yaml (default) or json
        #[arg(short = 'o', long, default_value = "yaml")]
        output: ExposuresOutputFormat,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ExposuresOutputFormat {
    Yaml,
    Json,
}

#[derive(Subcommand, Debug)]
pub enum CiAction {
    /// Run summary, diff, plan, lint (and manifest check) and render one
    /// Markdown report. Exits 1 when a `--fail-on` condition is met.
    Report {
        /// Base git ref to diff against. `auto` (default) uses the PR/MR target
        /// branch from the CI environment; empty disables diff and plan.
        /// Missing refs are resolved as `origin/<ref>` and fetched if needed.
        #[arg(long, default_value = "auto")]
        base: String,
        /// Head git ref to compare to (defaults to working tree)
        #[arg(long)]
        head: Option<String>,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json for summary and lint, and check it for staleness
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write the Markdown report to this file instead of stdout
        #[arg(long)]
        out: Option<PathBuf>,
        /// Append `key=value` outputs (`selector`, `failed`) to this file, e.g.
        /// `$GITHUB_OUTPUT` or a GitLab dotenv report
        #[arg(long)]
        outputs: Option<PathBuf>,
        /// Fail on these conditions (comma-separated)
        #[arg(long, value_enum, value_delimiter = ',', default_value = "lint-errors")]
        fail_on: Vec<crate::ci::FailOn>,
        /// Skip the lint section
        #[arg(long)]
        no_lint: bool,
        /// Compare SQL-parse lineage with manifest.json (run `dbt parse`
        /// first) and report how much of dbt's lineage was found
        #[arg(long)]
        verify: bool,
    },
    /// Create or update one sticky comment on the current PR/MR. Token:
    /// `DBT_LINEAGE_TOKEN`, else GITHUB_TOKEN / GITLAB_TOKEN (project access
    /// token, `api` scope) / BITBUCKET_ACCESS_TOKEN / SYSTEM_ACCESSTOKEN.
    Comment {
        /// Markdown file to post (e.g. from `ci report --out`); `-` for stdin
        #[arg(long)]
        file: PathBuf,
    },
    /// Lint findings as inline annotations: SARIF (GitHub code scanning),
    /// Code Quality (GitLab), Bitbucket Code Insights, or Azure Pipelines
    /// logging commands.
    Annotations {
        /// Output format
        #[arg(long, value_enum)]
        format: AnnotationFormat,
        /// Path to dbt project directory
        #[arg(short = 'p', long = "project-dir", default_value = ".")]
        project_dir: PathBuf,
        /// Use manifest.json instead of parsing SQL
        #[arg(long)]
        manifest: Option<PathBuf>,
        /// Write to this file instead of stdout
        #[arg(long)]
        out: Option<PathBuf>,
        /// bitbucket only: send the report and annotations to the Code
        /// Insights API (token from DBT_LINEAGE_TOKEN / BITBUCKET_ACCESS_TOKEN,
        /// else the Pipelines proxy, BITBUCKET_PROXY or localhost:29418)
        #[arg(long)]
        post: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum AnnotationFormat {
    Sarif,
    Codeclimate,
    Bitbucket,
    Azure,
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

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum SummaryOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum CheckManifestOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum VerifyOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum PlanOutputFormat {
    Selector,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum CoverageOutputFormat {
    Text,
    Json,
    Sarif,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum LintOutputFormat {
    Text,
    Json,
    Sarif,
    Codeclimate,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum PerfOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum MacrosOutputFormat {
    Text,
    Json,
    Mermaid,
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
