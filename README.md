# dbt-lineage

A fast Rust CLI tool for visualizing [dbt](https://www.getdbt.com/) model lineage. Parses SQL files directly to extract `ref()` and `source()` dependencies, builds a DAG, and renders it as ASCII art, Graphviz DOT, or an interactive terminal UI.

No dbt compilation, manifest, or Python runtime required for graph building — just point it at a dbt project directory.

## Features

- **Direct SQL parsing** — extracts `ref()` and `source()` calls via regex, no `dbt compile` needed
- **Interactive TUI** — navigate, search, and explore lineage in a terminal UI (ratatui)
- **Run dbt from TUI** — execute `dbt run` / `dbt test` on selected models with scope control (`+upstream`, `downstream+`, `+all+`)
- **Run status tracking** — color-coded nodes show success (green), error (red), outdated (yellow), or never-run (default)
- **Collapsible node list** — directory-grouped, expandable/collapsible sidebar panel
- **Multiple output formats** — colored ASCII boxes or Graphviz DOT for static output
- **Filtering** — focus on a single model with configurable upstream/downstream depth
- **Node type support** — models, sources, seeds, snapshots, tests, exposures

## Installation

```sh
cargo install --path .
```

Or build from source:

```sh
git clone https://github.com/your-username/dbt-lineage-viewer.git
cd dbt-lineage-viewer
cargo build --release
```

The binary is at `target/release/dbt-lineage`.

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

# Graphviz DOT output (pipe to `dot -Tpng` etc.)
dbt-lineage -o dot > lineage.dot
```

### Interactive TUI

```sh
dbt-lineage -i
dbt-lineage -i -p path/to/dbt/project
dbt-lineage -i stg_orders -u 3 -d 3
```

## CLI Reference

```
Usage: dbt-lineage [OPTIONS] [MODEL]

Arguments:
  [MODEL]  Model name to focus on (shows full lineage if omitted)

Options:
  -p, --project-dir <PATH>   Path to dbt project directory [default: .]
  -u, --upstream <N>          Upstream levels to show (default: all)
  -d, --downstream <N>        Downstream levels to show (default: all)
  -i, --interactive           Launch interactive TUI mode
  -o, --output <FORMAT>       Output format: ascii, dot [default: ascii]
      --include-tests         Include test nodes
      --include-seeds         Include seed nodes
      --include-snapshots     Include snapshot nodes
      --include-exposures     Include exposure nodes
  -h, --help                  Print help
```

## TUI Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `h` `j` `k` `l` / arrow keys | Navigate between nodes (left/down/up/right) |
| `H` `J` `K` `L` | Pan the camera |
| `+` / `-` | Zoom in / out |
| `Tab` / `Shift+Tab` | Cycle through nodes sequentially |
| `r` | Reset view (center + zoom) |

### Search

| Key | Action |
|-----|--------|
| `/` | Open search |
| `Tab` | Next search result |
| `Esc` / `Enter` | Close search |

### Node list panel

| Key | Action |
|-----|--------|
| `n` | Toggle node list sidebar |
| `c` | Collapse/expand directory group |

### Running dbt

| Key | Action |
|-----|--------|
| `x` | Open run menu for selected node |
| `o` | View last run output |

Run menu options:

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

1. **Parse** `dbt_project.yml` to find model/seed/snapshot paths
2. **Walk** those directories, collecting `.sql` and `.yml` files
3. **Extract** `ref('model')` and `source('schema', 'table')` from SQL via regex
4. **Parse** YAML schema files for sources, model descriptions, and exposures
5. **Build** a directed acyclic graph (petgraph) where edges flow from dependency to dependent
6. **Filter** by focus model and depth, prune by node type
7. **Layout** using a Sugiyama-style layered algorithm (longest-path layering + barycenter ordering)
8. **Render** as ASCII, DOT, or interactive TUI

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
