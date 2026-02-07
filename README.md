# dbt-lineage

[![CI](https://github.com/sipemu/dbt-lineage-viewer/actions/workflows/ci.yml/badge.svg)](https://github.com/sipemu/dbt-lineage-viewer/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/dbt-lineage)](https://crates.io/crates/dbt-lineage)
[![docs.rs](https://img.shields.io/docsrs/dbt-lineage)](https://docs.rs/dbt-lineage)
[![codecov](https://codecov.io/gh/sipemu/dbt-lineage-viewer/branch/master/graph/badge.svg)](https://codecov.io/gh/sipemu/dbt-lineage-viewer)

![TUI Demo](assets/tui-demo.gif)

A fast Rust CLI tool for visualizing [dbt](https://www.getdbt.com/) model lineage. Parses SQL files directly to extract `ref()` and `source()` dependencies, builds a DAG, and renders it as ASCII art, Graphviz DOT, SVG, interactive HTML, or a terminal UI.

Supports both direct SQL parsing (no dbt compilation or Python runtime needed) and `manifest.json` for full-fidelity graphs.

## Features

- **Direct SQL parsing** — extracts `ref()` and `source()` calls via regex, no `dbt compile` needed
- **Manifest support** — optionally read `manifest.json` for column metadata, materializations, and full graph fidelity
- **Interactive TUI** — navigate, search, and explore lineage in a terminal UI (ratatui) with Unicode box-drawing nodes, orthogonal edge routing, and full mouse support
- **Impact analysis** — `dbt-lineage impact <model>` computes downstream impact with severity scoring (Critical/High/Medium/Low)
- **Lineage diff** — `dbt-lineage diff --base <ref>` compares lineage between git refs, showing added/removed/modified nodes and edges
- **Column-level lineage** — trace column provenance through the DAG with confidence levels (Direct, Aliased, Derived, Star)
- **6 output formats** — ASCII, Graphviz DOT, JSON, Mermaid, self-contained SVG, and interactive HTML (pan/zoom/search)
- **Run dbt from TUI** — execute `dbt run` / `dbt test` on selected models with scope control (`+upstream`, `downstream+`, `+all+`) via keyboard menu or right-click context menu
- **Run status tracking** — color-coded nodes show success (green), error (red), outdated (yellow), or never-run (default)
- **Path highlighting** — trace upstream/downstream paths with impact analysis in the TUI
- **Selector expressions** — filter by tag, path, or model name (`-s tag:finance,path:marts`)
- **Node type support** — models, sources, seeds, snapshots, tests, exposures

## Installation

### From crates.io

```sh
cargo install dbt-lineage
```

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

## CLI Reference

```
Usage: dbt-lineage [OPTIONS] [MODEL] [COMMAND]

Commands:
  impact  Compute downstream impact analysis for a model
  diff    Compare lineage between git refs

Arguments:
  [MODEL]  Model name to focus on (shows full lineage if omitted)

Options:
  -p, --project-dir <PATH>    Path to dbt project directory [default: .]
  -u, --upstream <N>           Upstream levels to show (default: all)
  -d, --downstream <N>         Downstream levels to show (default: all)
  -i, --interactive            Launch interactive TUI mode
  -o, --output <FORMAT>        Output format [default: ascii]
                               [values: ascii, dot, json, mermaid, svg, html]
  -s, --select <SELECTOR>      Selector expression: tag:X, path:Y, or model name (comma-separated)
      --manifest <PATH>        Use manifest.json instead of parsing SQL
      --include-tests          Include test nodes
      --include-seeds          Include seed nodes
      --include-snapshots      Include snapshot nodes
      --include-exposures      Include exposure nodes
  -h, --help                   Print help
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
| `/` | Open search |
| `Tab` | Next search result |
| `Esc` / `Enter` | Close search |

### Analysis

| Key | Action |
|-----|--------|
| `p` | Toggle path highlighting (upstream/downstream trace with impact analysis) |
| `C` (Shift+C) | Toggle column-level lineage in detail panel |

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

## License

MIT
