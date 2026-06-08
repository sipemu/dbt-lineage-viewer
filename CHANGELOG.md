# Changelog

Notable changes per release. Full per-release notes live on the
[GitHub Releases page](https://github.com/sipemu/dbt-lineage-viewer/releases).

## [0.7.2] – 2026-06-08

### Added

- `CHANGELOG.md` — per-release notes from 0.1.0 onward, generated from the
  GitHub release-page history.
- `docs/MCP.md` — cookbook for the MCP server: client setup, full tool
  reference, prompt walkthroughs, composition recipes.
- `docs/CACHE.md` — parse-cache contract: file format, version policy,
  when to delete, why it's gitignore-safe.
- Crate-level doc comment on `src/lib.rs` describing the top-level module
  layout (cli, error, graph, parser, render, mcp, tui).

### Changed

- README features section reorganized under sub-headlines (Source modes,
  Analysis subcommands, Column-level lineage, MCP server, Output,
  Interactive TUI, Filtering and node types) so the now-long capability list
  is scannable.
- "Further reading" section in README links the new docs.

## [0.7.1] – 2026-06-08

### Changed

- `propose_test` now produces minimal, comment-preserving unified diffs against
  the actual schema YAML. New line-level editor finds the column block and
  inserts in place at matching indentation; serde_yaml round-trip remains as a
  fallback. (#37)
- `unified_diff` is now context-aware (3 lines of context around the changed
  region) instead of dumping the full file as removals + additions.

## [0.7.0] – 2026-06-08

### Added

- Lint: `model-without-source` (warning, all upstream refs unresolved) and
  `circular-ref` (error, cycle detected). Both toggleable via `--disable`. (#38)
- Coverage SARIF: `column-untested` and `generic-only` join the existing
  `untested-model`. Each rule declares a `helpUri`. (#39)
- `check-manifest` reads dbt's recorded SHA-256 from `manifest.nodes.*.checksum`
  and compares against the file's actual content hash. Authoritative when
  present; mtime is fallback. Each detected `Modification` carries a
  `detection: "content-hash" | "mtime"` tag. (#41)
- **Release binaries** — new `release-binaries.yml` workflow builds prebuilt
  binaries for Linux (x86_64 + aarch64), macOS (Intel + Apple Silicon), and
  Windows (x86_64) on every `v*` tag. Each archive ships the binary, README,
  LICENSE, and BENCHMARKS plus a `.sha256` checksum.

## [0.6.0] – 2026-06-08

### Added

- AST-backed column lineage via [`sqlparser`](https://crates.io/crates/sqlparser).
  New `parser::ast_column_lineage` parses model SQL into a proper AST and emits
  column edges with correct transformation classification (`Direct`, `Aliased`,
  `Derived`). Handles aliased projections, qualified columns across JOINs,
  function applications, CTEs. Jinja preprocessing maps `{{ ref('x') }}` to
  synthetic identifiers that resolve back to graph nodes. (#35)
- `main.rs` unions the AST analyzer's output with the existing regex-based
  analyzer (`merge_column_lineage`); duplicates are deduped.

## [0.5.1] – 2026-06-08

### Added

- Bench harness (`benches/builder_bench.rs`) with criterion targets over a
  synthetic 500-model fixture. Baseline numbers committed to `BENCHMARKS.md`.
  (#36)
- Jinja+SQL lexer (`parser::lexer::strip_non_code`) — hand-rolled state machine
  tokenizes SQL into regions and blanks out comments + string literals so
  regex-based extractors don't trip over `'I prefer ref(x)'` or `-- ref(`.
  Routes through `extract_refs` and `extract_sources`. (#25)

### Changed

- Cold parse is ~15% slower due to lexer overhead (14.4 → 17 ms on the 500-model
  fixture). Correctness win.

## [0.5.0] – 2026-06-08

### Added

- Parallel parsing via [`rayon`](https://crates.io/crates/rayon). Phase 1
  (per-file ref/source/column extraction) runs across cores; phase 2 (graph
  insertion) stays sequential for deterministic ordering. (#33)
- On-disk parse cache at `<project_dir>/.dbt-lineage/cache.bin`. Keyed by blake3
  hash of file content; warm runs serve straight from cache. (#34)
- `--no-cache` global flag bypasses both load and save.

### Dependencies

- Added `rayon`, `blake3`, `bincode` (all pure Rust, no native code).

## [0.4.1] – 2026-06-08

### Changed

- `dbt-lineage mcp` falls back to SQL-parse mode when `target/manifest.json`
  isn't available — agents can work during active dev without `dbt compile`. (#21)
- `column_upstream` and `column_downstream` MCP tools consult a precomputed
  `ColumnLineage` analyzer; each hop carries its transformation kind. Falls back
  to name-matching heuristic for projects where SQL isn't parseable. (#22)
- `read_model_sql` falls back to `raw_code`/`compiled_code` stored in
  `manifest.json` when no on-disk file is readable. Response tagged with
  `source: "disk" | "manifest"`. (#23)
- `propose_test` returns an apply-ready unified diff against the located
  schema YAML (file located + serde_yaml round-trip). Idempotent. (#24)

## [0.4.0] – 2026-06-06

### Added

- Macro-aware `ref()` / `source()` extraction in SQL-parse mode. Detects simple
  Jinja wrapper macros like `{% macro smart_ref(name) %}{{ ref(name) }}{% endmacro %}`
  and treats calls to them as ref/source equivalents. (#13)
- MCP server (`dbt-lineage mcp`) with a substantial capability surface:
  - **Tools**: `summary`, `search_models`, `lineage`, `impact`,
    `get_model_details`, `read_model_sql`, `column_upstream`,
    `column_downstream`, `lineage_bundle`, `propose_test`.
  - **Resources**: `dbt-lineage://summary`, `dbt-lineage://model/<name>/sql`,
    `dbt-lineage://model/<name>/lineage.mermaid`, source/exposure JSON.
  - **Prompts**: `review-impact`, `propose-tests`, `explain-column`,
    `find-bottleneck`, `document-model`, `onboard-project`.
  (#15, #16, #17, #18, #19, #20)

## [0.3.0] – 2026-06-06

### Added

- `summary` subcommand — one-shot project overview (counts, tags, top fan-out,
  orphans). Text or JSON. (#5)
- `check-manifest` subcommand — verifies `manifest.json` freshness against
  current SQL; exits non-zero on staleness for CI gating. (#8)
- `mcp` subcommand (initial version) — stdio MCP server with `summary`,
  `search_models`, `lineage`, `impact` tools. (#3)
- `--collapse` / `--collapse=focal` — drops intermediate nodes and labels
  transitive paths with `(via N)`. Honors positional focus models. (#4)
- Global `--error-format json` — structured `{level, what, why, hint}` on
  stderr for agent/CI consumers. (#6)
- Mermaid: `--show-columns` (inline column names in node labels) and
  `--group-by directory` (Mermaid `subgraph` blocks per directory). (#7)

## [0.2.1] – 2026-06-06

### Fixed

- Panic on SQL containing multi-byte UTF-8 characters in comments or string
  literals. Three byte-indexed slice operations in `parser::columns` now use
  byte-level matching instead. (#1)

## [0.2.0] – 2026-02-07

Initial public-cut release with the core feature set: SQL-parse + manifest
mode, ASCII/DOT/JSON/Mermaid/SVG/HTML output, impact analysis, lineage diff,
column-level lineage, TUI with mouse + path highlighting.

## [0.1.0] – 2026-02-06

Initial release.
