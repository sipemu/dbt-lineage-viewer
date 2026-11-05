# Changelog

Notable changes per release. Full per-release notes live on the
[GitHub Releases page](https://github.com/sipemu/dbt-lineage-viewer/releases).

## [Unreleased]

### Fixed

- **`check-manifest` reported every real dbt project as stale.** It used the
  manifest's `path` (relative to the resource folder, virtual for generic
  tests) instead of `original_file_path`, ignored disabled nodes, and compared
  plain SHA-256 where dbt hashes stripped (seeds, older dbt) or
  whitespace-collapsed (SQL, dbt 1.10+) contents. Verified on tuva with a
  manifest from `dbt parse`: in sync, and a one-line edit is detected.
- MCP `summary` tool and resource reported `project_name: null`; they now read
  it from `dbt_project.yml`.

## [0.13.0] – 2026-11-05

### Added

- `dbt-lineage exposures anofox --dashboards <files|dirs>` — imports
  [anofox-visualization](https://github.com/DataZooDE/anofox-visualization)
  dashboards (DuckDB `.sql` files with `::ROLE` panels) as dbt exposures:
  title, description and owner from `-- @` headers, dependencies from the
  tables read outside CTEs, table functions and tables the scripts create
  themselves; `--url` template with `{name}`. Validated with `dbt parse` on
  the anofox-visualization examples (#54).

## [0.12.1] – 2026-11-04

### Fixed

- `site`: lineage diagrams wider than the page were clipped at the frame's
  edge (scrollable, but with no visible hint). They now shrink to fit, down to
  60% of natural size, and only scroll beyond that.
- `lsp`: hover shows file paths with forward slashes on Windows.

## [0.12.0] – 2026-11-04

### Added

- **Breaking-change detection for governed models** (#51): `diff`, `ci report`
  and the PR comment flag removed or retyped contract columns, dropped
  contracts, narrowed `access`, and removed versions or governed models;
  `ci report --fail-on breaking-changes`. New lint rule `private-ref` for refs
  to `access: private` models from outside their group.

- `dbt-lineage site` — static lineage site for GitHub/GitLab Pages: searchable
  index, one page per node with a linked lineage diagram, upstream/downstream,
  tests, columns with column-level lineage, and highlighted SQL; light/dark and
  phone-friendly, no server or CDN (#52). See `docs/SITE.md`.
- AutomateDV / dbtvault-style models: dependencies passed as `source_model`
  (a `set` variable, a literal argument, or a YAML metadata block) become
  lineage edges. Previously every Data Vault model looked like an orphan.
- `dbt-lineage exposures metabase --url … [--write models/exposures_metabase.yml]`
  — imports Metabase dashboards as dbt exposures: tables read by native SQL
  and GUI questions (legacy MBQL and staged pMBQL, joins, nested questions)
  are mapped onto models and sources; unmatched tables are reported. See
  `docs/BI.md` (#54).

### Changed

- Column lineage is ~30× faster on large projects: the regex analyzer reuses
  compiled per-column patterns and both analyzers run in parallel (8.7 s →
  0.3 s for 1,500 models). Results are unchanged.
- CI workflows moved to Node 24 action versions (checkout v5, artifact
  v6/v7, docker v4/v7, action-gh-release v3, codecov v6).

### Fixed

- Schema YAML robustness, found on real projects (tuva): `<<: *anchor` merge
  keys are applied, `tags: foo` is accepted as well as a list, and files with
  duplicate keys load PyYAML-style (last value wins) instead of being dropped.
  Each of these silently lost descriptions, materializations and tags for
  every model in the file, which showed up as phantom changes in `diff`.
- Column impact analyzed every model in the base tree; it now analyzes only
  models downstream of the changed ones (1,000-model project: 5.6 s → 66 ms
  for a renamed column).
- Parse cache version bumped so cached results from older versions are
  re-parsed.
- `lint --disable` help listed only four of the six rules; removed the unused
  `LintConfig::generic_only` flag (`generic-only` is a coverage finding).
- `dbt-lineage lsp` — stdio Language Server: hover and go-to-definition on
  `ref()` / `source()`, a code lens per model (upstream/downstream counts,
  impact severity), and `lint` findings as diagnostics refreshed on save.
  Projects are found per file via the nearest `dbt_project.yml` (feature
  `lsp`, on by default) (#53).
- VS Code extension in `editors/vscode` (language client plus a
  *Show lineage* webview), and Neovim / Helix setup in `docs/EDITORS.md`.

## [0.11.0] – 2026-11-04

### Added

- **Column-level impact.** `diff`, `ci report` and the PR comment list
  removed or renamed columns that downstream columns are still computed
  from (e.g. renaming `orders.total_amount` breaks `customers.lifetime_value`),
  using column lineage of the base tree. Modified models list added and
  removed column names instead of a count (#49).

### Changed

- GitHub Action posts its comment with `dbt-lineage ci comment` (no third-party
  comment action) and uses `upload-sarif@v4`.
- PyPI packaging as `dbt-lineage-cli` (publishing pending, #55).

### Fixed

- MCP server and TUI column lineage silently fell back to name matching when
  started outside the project directory (e.g. `claude mcp add … -p /abs/path`):
  model SQL was read relative to the current directory.
- Manifest mode used dbt's `path` (relative to the model folder) instead of
  `original_file_path`, so file reads (column lineage, SQL viewer, SARIF
  locations) pointed at non-existent files.

## [0.10.2] – 2026-11-04

### Fixed

- GitHub Action: SARIF upload was rejected ("multiple SARIF runs with the same
  category"); lint and coverage now upload under separate categories, so
  findings show inline on PRs.
- CI report suggests `dbt build -s` (the selection can include seeds and
  snapshots, which `dbt run` skips).

## [0.10.1] – 2026-11-04

### Fixed

- **GitHub Action failed at install** with exit code 23 when `version: latest`
  (the default): `curl | grep -m1` was killed by SIGPIPE under `pipefail`.
  The release lookup is now authenticated with the job token, avoiding the
  anonymous API rate limit on shared runners.

### Added

- `dbt-lineage ci comment --file report.md` — create or update one sticky
  PR/MR comment on GitHub, GitLab, Bitbucket or Azure DevOps (#43).
- GitLab CI/CD component in `integrations/gitlab` (report, sticky MR note,
  Code Quality, dotenv outputs) (#62).
- Bitbucket Pipe in `integrations/bitbucket`; image
  `ghcr.io/sipemu/dbt-lineage-pipe` published on release (#63).
- `dbt-lineage --version`.
- Homebrew: `brew install sipemu/tap/dbt-lineage` (#56).

## [0.10.0] – 2026-11-04

### Added

- `dbt-lineage ci report` — one Markdown report (diff, rebuild selector,
  manifest freshness, lint, project summary) for PR/MR comments, with
  `--fail-on lint-errors,plan-empty,manifest-stale|never` exit codes and
  `--outputs` for `$GITHUB_OUTPUT` / dotenv files. `--base auto` picks the
  target branch from GitHub, GitLab, Bitbucket or Azure DevOps env vars and
  resolves/fetches `origin/<branch>` in PR checkouts (#43).
- Release workflow moves a floating major tag (`v0`) after each release (#46).
- `ci report` includes a Mermaid diagram of the changed nodes and everything
  downstream of them, styled by status; GitHub, GitLab and Azure DevOps render
  it inline in the PR/MR comment (#50).
- `cargo binstall dbt-lineage` installs the pre-built release binary (#56).
- `.pre-commit-hooks.yaml` with `dbt-lineage-lint` and
  `dbt-lineage-check-manifest` hooks (#57).
- Container image `ghcr.io/sipemu/dbt-lineage` (Alpine + git, static musl
  binary, amd64/arm64), published by the release workflow (#58).
- `lint -o codeclimate` — GitLab Code Quality report for inline MR findings
  (part of #62).

### Changed

- GitHub Action is a thin wrapper around `ci report`: `fail-on` now honors
  `plan-empty` and `manifest-stale`, no longer needs `python3`, keeps error
  output out of the PR comment, quotes the manifest path, posts the report to
  the job summary, and defaults `base` to the PR target branch (#44).

### Fixed

- `diff` / `plan` against a git ref now build the base graph with the same
  pipeline as the working tree. Previously the base used a models-only parser,
  so every seed, test and exposure showed as "added" and every description as
  "changed".
- `diff` / `plan` detect SQL, Python and seed-file body changes. Previously a
  changed `SELECT` or `WHERE` was not "modified" unless materialization, tags,
  columns or description changed, so `plan` left it out of the rebuild set.
- **`plan` selected upstream instead of downstream.** It emitted `+model`,
  which in dbt selects the model's ancestors; it now emits `model+` (the model
  and its descendants), as documented. Sources, exposures, metrics and
  semantic models are no longer emitted, since dbt cannot build them.
- **SARIF findings never showed inline on PRs.** `lint -o sarif` emitted
  results without locations (GitHub drops those) and `coverage -o sarif`
  guessed `models/<name>.sql`, wrong for nested folders and projects in a
  subdirectory. Both now point at the node's real file, relative to the
  repository root; undefined refs point at the model that references them.
- `diff` / `plan` only use `target/manifest.json` when both sides have one; a
  locally compiled manifest no longer makes every node look removed + re-added.

## [0.9.1] – 2026-06-08

### Changed

- `action.yml` description trimmed to fit the GitHub Marketplace's
  125-character limit. No functional changes.

## [0.9.0] – 2026-06-08

### Added

- TUI fuzzy search: `/` ranks matches with nucleo-matcher, so a few characters
  in any order find the model (#29).
- TUI minimap: `m` toggles an overview in the top-right corner of the graph
  area; auto-hides on small terminals (#30).
- TUI SQL viewer: `v` shows the selected node's SQL, syntax-highlighted with
  syntect (#31).

New TUI dependencies (`nucleo-matcher`, `syntect`) sit behind the existing
`tui` feature.

## [0.8.0] – 2026-06-08

### Added

- Python models: `dbt.ref(...)` / `dbt.source(...)` in `.py` files become
  edges in SQL-parse mode (#27).
- Semantic Layer: `semantic_models` and `metrics` from the manifest (dbt 1.6+)
  as their own node types in every output (#26).
- `dbt-lineage macros`: macro→macro call graph (text, JSON, Mermaid) with
  orphan macros (#28).
- GitHub Action (`action.yml`): summary, diff, plan, optional lint/coverage
  SARIF and a sticky PR comment (#32).

## [0.7.3] – 2026-06-08

### Fixed

- `publish.yml` passes `--allow-dirty` to `cargo publish` so the CI runner's
  in-flight Cargo.lock touch doesn't abort the upload. The committed Cargo.lock
  stays the source of truth; this just tolerates cargo's internal resolve
  mutation during the publish step. (0.7.2's docs-pass content didn't make it
  to crates.io because of this issue; 0.7.3 is the same content with the
  workflow fix in place.)

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
