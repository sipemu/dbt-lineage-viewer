//! `dbt-lineage` — a CLI + library for visualizing dbt model lineage.
//!
//! See the [README](https://github.com/sipemu/dbt-lineage-viewer#readme) and
//! [`docs/MCP.md`](https://github.com/sipemu/dbt-lineage-viewer/blob/master/docs/MCP.md)
//! for usage. Each `pub mod` below corresponds to a layer:
//!
//! - [`cli`] — Clap-derived command-line surface.
//! - [`error`] — error types and the `--error-format json` mapping.
//! - [`graph`] — the lineage DAG, plus computed reports (impact, summary,
//!   diff, plan, lint, coverage, perf, collapse).
//! - [`parser`] — file discovery, SQL parsing, manifest parsing, on-disk cache.
//! - [`render`] — output formatters per format (ASCII, DOT, JSON, Mermaid, SVG, HTML, …).
//! - [`mcp`] — stdio Model Context Protocol server.
//! - [`tui`] — interactive terminal UI (feature-gated).

pub mod cli;
pub mod error;
pub mod git;
pub mod graph;
pub mod mcp;
pub mod parser;
pub mod render;
#[cfg(feature = "tui")]
pub mod tui;
