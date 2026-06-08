# dbt-lineage

[![CI](https://github.com/sipemu/dbt-lineage-viewer/actions/workflows/ci.yml/badge.svg)](https://github.com/sipemu/dbt-lineage-viewer/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/dbt-lineage)](https://crates.io/crates/dbt-lineage)
[![docs.rs](https://img.shields.io/docsrs/dbt-lineage)](https://docs.rs/dbt-lineage)
[![codecov](https://codecov.io/gh/sipemu/dbt-lineage-viewer/branch/master/graph/badge.svg)](https://codecov.io/gh/sipemu/dbt-lineage-viewer)

![TUI Demo](assets/tui-demo.gif)

A fast Rust CLI tool for visualizing [dbt](https://www.getdbt.com/) model lineage. Parses SQL files directly to extract `ref()` and `source()` dependencies, builds a DAG, and renders it as ASCII art, Graphviz DOT, SVG, interactive HTML, or a terminal UI.

Supports both direct SQL parsing (no dbt compilation or Python runtime needed) and `manifest.json` for full-fidelity graphs.

## Features

### Source modes

- **Direct SQL parsing** — extracts `ref()` and `source()` via a Jinja-aware lexer, no `dbt compile` needed
- **Manifest support** — optionally read `manifest.json` for column metadata, materializations, and full graph fidelity
- **Macro-aware ref()** — SQL-parse mode follows simple `{% macro smart_ref(name) %}{{ ref(name) }}{% endmacro %}` wrappers automatically
- **Parallel parsing + on-disk cache** — [`rayon`](https://crates.io/crates/rayon)-parallelized parsing and a content-hash cache at `.dbt-lineage/cache.bin`; warm runs are near-instant on large monorepos. Bypass with `--no-cache`. See [`docs/CACHE.md`](docs/CACHE.md)

### Analysis subcommands

- **`summary`** — one-shot project overview (counts, tags, top fan-out, orphans)
- **`impact <model>`** — downstream impact with severity scoring (Critical / High / Medium / Low)
- **`diff --base <ref>`** — compares lineage between git refs, showing added / removed / modified nodes and edges
- **`plan --base <ref>`** — emits a dbt selector for the minimal rebuild set; pipes into `dbt run -s "$(…)"`
- **`coverage`** — tests-per-model + per-column map; text, JSON, or SARIF (PR annotations)
- **`lint`** — flags unused sources, undefined refs, dead-end models, missing descriptions, models with only unresolved upstream, circular refs
- **`docs`** — generates per-model Markdown with description, upstream / downstream, columns, embedded Mermaid lineage
- **`perf`** — joins `run_results.json` with the DAG: slowest models, critical paths
- **`check-manifest`** — flags drift between `manifest.json` and current SQL files using dbt's recorded SHA-256 (mtime fallback); exits non-zero for CI gating

### Column-level lineage

- AST-backed analyzer via [`sqlparser`](https://crates.io/crates/sqlparser) (0.6.0+) handles aliases, JOINs, aggregations, and CTEs with proper transformation classification (`Direct`, `Aliased`, `Derived`)
- Regex-based analyzer remains as a fallback for dialect-specific syntax sqlparser can't yet parse

### MCP server (AI-agent integration)

- **`dbt-lineage mcp`** — stdio Model Context Protocol server. See [`docs/MCP.md`](docs/MCP.md) for setup and cookbook.
- **10 tools**: `summary`, `search_models`, `lineage`, `impact`, `get_model_details`, `read_model_sql`, `column_upstream`, `column_downstream`, `lineage_bundle`, `propose_test`
- **Resources**: model SQL, focused lineage diagrams, source/exposure metadata, always-fresh summary
- **Prompts**: `review-impact`, `propose-tests`, `explain-column`, `find-bottleneck`, `document-model`, `onboard-project`

### Output

- **6 formats** — ASCII, Graphviz DOT, JSON, Mermaid, self-contained SVG, interactive HTML (pan / zoom / search)
- **Mermaid niceties** — `--show-columns` inlines column names in node labels; `--group-by directory` emits Mermaid subgraphs
- **Graph collapse** — `--collapse` / `--collapse=focal` drops intermediate nodes and labels transitive paths with `(via N)` edges
- **Structured errors** — `--error-format json` writes `{level, what, why, hint}` to stderr for deterministic agent / CI consumers

### Interactive TUI

- Navigate, search, and explore lineage in a terminal UI ([`ratatui`](https://crates.io/crates/ratatui)) with Unicode box-drawing nodes, orthogonal edge routing, and full mouse support
- **Run dbt from TUI** — execute `dbt run` / `dbt test` on selected models with scope control (`+upstream`, `downstream+`, `+all+`) via keyboard menu or right-click context menu
- **Run status tracking** — color-coded nodes show success (green), error (red), outdated (yellow), or never-run (default)
- **Path highlighting** — trace upstream / downstream paths with impact analysis

### Filtering and node types

- **Selector expressions** — `-s tag:finance,path:marts` filters by tag, path, or model name (comma-separated)
- **Node-type filters** — `--include-tests`, `--include-seeds`, `--include-snapshots`, `--include-exposures`
- **Node-type support** — models, sources, seeds, snapshots, tests, exposures

## Installation

### From crates.io

```sh
cargo install dbt-lineage
```

### Pre-built binaries

Every tagged release ships pre-built binaries for Linux (x86_64 + aarch64), macOS (Intel + Apple Silicon), and Windows (x86_64) as downloadable assets on the [Releases page](https://github.com/sipemu/dbt-lineage-viewer/releases). Download the archive for your platform, extract, and put the binary on your `PATH`.

### As a GitHub Action

```yaml
- uses: sipemu/dbt-lineage-viewer@v0.7.4
  with:
    base: main           # diff against this ref
    project-dir: dbt/
    comment: true        # post a Markdown summary as a PR comment
    sarif: lint,coverage # also surface findings as PR annotations
```

The action installs the pinned `dbt-lineage` binary (from this repo's release artifacts), runs `summary`, `diff`, `plan`, and optionally `lint`/`coverage` SARIF, then posts a single sticky comment on the PR with the rendered report. See [`action.yml`](action.yml) for the full input/output reference.

### From source

```sh
git clone https://github.com/sipemu/dbt-lineage-viewer.git
cd dbt-lineage-viewer
cargo install --path .
```

The binary is installed to `~/.cargo/bin/dbt-lineage`.

## Usage

### Static output

```sh
# Full lineage of current directory's dbt project
dbt-lineage

# Focus on a specific model
dbt-lineage stg_orders

# Point at a different project directory
dbt-lineage -p path/to/dbt/project

# Show 2 levels upstream, 1 downstream
dbt-lineage stg_orders -u 2 -d 1

# Include seeds, tests, snapshots, exposures
dbt-lineage --include-seeds --include-tests --include-snapshots --include-exposures

# Selector expressions
dbt-lineage -s tag:finance,path:marts

# Use manifest.json instead of parsing SQL
dbt-lineage --manifest target/manifest.json

# Output formats
dbt-lineage -o dot > lineage.dot        # Graphviz DOT
dbt-lineage -o json                      # JSON graph
dbt-lineage -o mermaid                   # Mermaid diagram
dbt-lineage -o svg > lineage.svg         # Self-contained SVG
dbt-lineage -o html > lineage.html       # Interactive HTML (pan/zoom/search)
```

### Interactive TUI

```sh
dbt-lineage -i
dbt-lineage -i -p path/to/dbt/project
dbt-lineage -i stg_orders -u 3 -d 3
```

### Impact analysis

Compute downstream impact for a model with severity scoring:

```sh
dbt-lineage impact orders -p path/to/project          # text report
dbt-lineage impact orders -o json                      # JSON for CI
dbt-lineage impact orders --manifest target/manifest.json
```

Severity levels:
- **Critical** — impacts exposures (dashboards, reports)
- **High** — impacts table/incremental materializations or mart models
- **Medium** — impacts staging or intermediate models
- **Low** — impacts tests only

### Lineage diff

Compare lineage between git refs to see what changed:

```sh
dbt-lineage diff --base main                           # compare main to working tree
dbt-lineage diff --base main --head feature-branch     # compare two branches
dbt-lineage diff --base HEAD~1 -o json                 # JSON for CI integration
```

Shows added, removed, and modified nodes and edges with a summary of changes.

### Project summary

One-shot project overview, designed as the first thing an agent (or human) runs against an unfamiliar dbt project:

```sh
dbt-lineage summary -p path/to/project                 # text
dbt-lineage summary --manifest target/manifest.json    # manifest mode
dbt-lineage summary -o json                            # structured for piping into jq
```

Reports counts per node type, tag distribution, top downstream-reach models, and orphan models (no downstream consumers).

### Manifest freshness

CI-friendly drift check between a distributed `manifest.json` and current SQL files:

```sh
dbt-lineage check-manifest -p path/to/project           # text; exit 1 if stale
dbt-lineage check-manifest -o json                      # JSON for tooling
```

Flags three kinds of drift: SQL files referenced by the manifest that are now missing, files modified since the manifest was generated, and SQL files added but not yet compiled into the manifest.

### Graph collapse

Collapse intermediate nodes so big projects render as just their endpoints, with transitive paths labeled `(via N)`:

```sh
dbt-lineage --collapse -o mermaid                       # keep endpoints + focus model
dbt-lineage orders --collapse=focal -u 3 -o mermaid     # focal: only sources/exposures/focus
dbt-lineage --collapse -o json                          # JSON edges carry a via_hops field
```

Positional focus models (e.g. `dbt-lineage orders --collapse`) are always preserved even when they would otherwise be classified as intermediate.

### Mermaid options

```sh
dbt-lineage -o mermaid --show-columns                   # inline column names in node labels
dbt-lineage -o mermaid --group-by directory             # group nodes by directory via subgraph
dbt-lineage -o mermaid --show-columns --collapse        # combine: endpoint nodes with full column lists
```

### Structured errors for agents and CI

```sh
dbt-lineage --error-format json some_unknown_model
# stderr: {"level":"error","what":"model not found","why":"...","hint":"..."}
```

`--error-format json` writes one JSON object per line to stderr. `stdout` is unaffected, so piping into `jq` still works.

### MCP server (experimental)

Expose lineage as stdio MCP tools that Claude Code, Claude Desktop, and other MCP-aware clients can call directly:

```sh
dbt-lineage mcp -p path/to/project
dbt-lineage mcp --manifest path/to/manifest.json
```

Available tools: `summary`, `search_models`, `lineage`, `impact`. The server reads JSON-RPC 2.0 requests on stdin and writes responses on stdout (one line per message). Manifest mode only — run `dbt compile` first.

## CLI Reference

```
Usage: dbt-lineage [OPTIONS] [MODEL] [COMMAND]

Commands:
  summary         Project overview: counts, tags, top fan-out, orphans
  impact          Compute downstream impact analysis for a model
  diff            Compare lineage between git refs
  check-manifest  Verify manifest.json is in sync with current SQL files (exit 1 if stale)
  mcp             Run an MCP stdio server exposing lineage tools to AI agents

Arguments:
  [MODEL]  Model name to focus on (shows full lineage if omitted)

Options:
  -p, --project-dir <PATH>    Path to dbt project directory [default: .]
  -u, --upstream <N>          Upstream levels to show (default: all)
  -d, --downstream <N>        Downstream levels to show (default: all)
  -i, --interactive           Launch interactive TUI mode
  -o, --output <FORMAT>       Output format [default: ascii]
                              [values: ascii, dot, json, mermaid, svg, html]
  -s, --select <SELECTOR>     Selector expression: tag:X, path:Y, or model name (comma-separated)
      --manifest <PATH>       Use manifest.json instead of parsing SQL
      --include-tests         Include test nodes
      --include-seeds         Include seed nodes
      --include-snapshots     Include snapshot nodes
      --include-exposures     Include exposure nodes
      --show-columns          Mermaid only: inline column names inside node labels
      --group-by <MODE>       Mermaid only: group nodes by directory [values: none, directory]
      --collapse [<MODE>]     Drop intermediate nodes, label transitive edges with (via N)
                              [values: none, auto, focal]  [default: none]  [bare: auto]
      --error-format <FORMAT> Error output: plain (default) or json [global]
  -h, --help                  Print help
```

## TUI Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `h` `j` `k` `l` / arrow keys | Navigate between nodes (left/down/up/right) |
| `H` `J` `K` `L` | Pan the viewport |
| `+` / `-` | Zoom in / out (adjusts spacing) |
| `Tab` / `Shift+Tab` | Cycle through nodes sequentially |
| `r` | Reset view (center + zoom) |

### Mouse

| Action | Target | Effect |
|--------|--------|--------|
| Left click | Node on graph | Select node (no viewport jump) |
| Left click | Empty graph area | Begin drag to pan |
| Drag | Graph area | Pan the viewport |
| Scroll up / down | Graph area | Zoom in / out |
| Left click | Node list entry | Select node and center viewport |
| Left click | Group header | Collapse / expand group |
| Right click | Node on graph | Open context menu (run options) |

### Search

| Key | Action |
|-----|--------|
| `/` | Open search (fuzzy, powered by [nucleo](https://crates.io/crates/nucleo-matcher) — type any few characters in any order) |
| `Tab` | Next search result |
| `Esc` / `Enter` | Close search |

### Analysis

| Key | Action |
|-----|--------|
| `p` | Toggle path highlighting (upstream/downstream trace with impact analysis) |
| `C` (Shift+C) | Toggle column-level lineage in detail panel |
| `v` | Toggle in-app SQL viewer (syntax-highlighted SQL of the selected node) |
| `m` | Toggle minimap overlay (top-right corner of the graph area) |

### Node list panel

| Key | Action |
|-----|--------|
| `n` | Toggle node list sidebar |
| `c` | Collapse/expand directory group |

### Running dbt

| Key | Action |
|-----|--------|
| `x` | Open run menu for selected node |
| Right click | Open context menu on a node (same run options) |
| `o` | View last run output |

Run menu / context menu options:

| Key | Command |
|-----|---------|
| `r` | `dbt run` (this model) |
| `u` | `dbt run` +upstream |
| `d` | `dbt run` downstream+ |
| `a` | `dbt run` +all+ |
| `t` | `dbt test` |

### General

| Key | Action |
|-----|--------|
| `q` | Quit |
| `Ctrl+C` | Quit (any mode) |

## Node colors in TUI

**By run status** (when `target/run_results.json` exists):

| Color | Meaning |
|-------|---------|
| Green | Last run succeeded |
| Red | Last run failed |
| Yellow | Outdated (source file modified after last run) |
| DarkGray | Skipped |

**By node type** (when never run):

| Color | Type |
|-------|------|
| Blue | Model |
| Green | Source |
| Yellow | Seed |
| Magenta | Snapshot |
| Cyan | Test |
| Red | Exposure |
| DarkGray | Phantom (unresolved ref) |

## How it works

1. **Parse** `dbt_project.yml` to find model/seed/snapshot paths (or read `manifest.json`)
2. **Walk** those directories, collecting `.sql` and `.yml` files
3. **Extract** `ref('model')` and `source('schema', 'table')` from SQL via regex
4. **Parse** YAML schema files for sources, model descriptions, and exposures
5. **Build** a directed acyclic graph (petgraph) where edges flow from dependency to dependent
6. **Resolve** column-level lineage by tracing SELECT/FROM/JOIN through the graph
7. **Filter** by focus model, depth, selectors, and node type
8. **Layout** using a Sugiyama-style layered algorithm (longest-path layering + barycenter ordering)
9. **Render** as ASCII, DOT, JSON, Mermaid, SVG, HTML, or interactive TUI

## uv / virtualenv support

When running dbt from the TUI, the tool auto-detects whether to use `uv run dbt` or plain `dbt`:

- If `uv.lock` or `pyproject.toml` exists in the dbt project directory, uses `uv run dbt`
- Otherwise, if `dbt` is on PATH, uses it directly
- If `dbt` is not on PATH but `uv` is, falls back to `uv run dbt`

## Building without TUI

The TUI is enabled by default. To build a minimal binary with only static output:

```sh
cargo build --release --no-default-features
```

## Further reading

- [`CHANGELOG.md`](CHANGELOG.md) — per-release notes from 0.1.0 onward.
- [`docs/MCP.md`](docs/MCP.md) — cookbook for the MCP server: setup, tool reference, prompt walkthroughs, composition recipes.
- [`docs/CACHE.md`](docs/CACHE.md) — parse-cache contract, file format, when it helps.
- [`BENCHMARKS.md`](BENCHMARKS.md) — baseline numbers from the bench harness.

## Credits

The agent-oriented additions in 0.3.0 — `summary`, `check-manifest`, `mcp`, `--collapse`, `--error-format json`, and the Mermaid `--show-columns` / `--group-by directory` options — were inspired by [eitsupi/dlin](https://github.com/eitsupi/dlin), a hard fork that focuses on non-interactive (CI/agent) workflows. dbt-lineage retains its TUI focus and brings these ideas back upstream.

## License

MIT
