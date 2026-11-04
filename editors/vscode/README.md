# dbt-lineage for VS Code

dbt lineage where you write SQL, with no warehouse, no Python and no `dbt compile`.

- **Hover** on `ref('…')` / `source('…', '…')`: node type, materialization, description, upstream/downstream counts, and the impact if it changes.
- **Go to definition** on `ref()` → the model's SQL file; on `source()` → the table entry in its YAML.
- **Code lens** on every model: `N upstream · M downstream · impact: high`. Click it to open the lineage graph.
- **Diagnostics** from `dbt-lineage lint`: undefined refs and sources, dead-end models, missing descriptions. Refreshed on save.
- **dbt-lineage: Show lineage**: an interactive lineage graph of the current model in a side panel.

## Requirements

The extension starts the `dbt-lineage` binary, which provides the language server (`dbt-lineage lsp`). Install it with one of:

```sh
brew install sipemu/tap/dbt-lineage
cargo binstall dbt-lineage
cargo install dbt-lineage
```

If it is not on your `PATH`, set **dbtLineage.path**.

## Settings

| Setting | Default | Description |
| --- | --- | --- |
| `dbtLineage.path` | `dbt-lineage` | Path to the binary |
| `dbtLineage.lineageDepth` | `3` | Levels up and down shown by *Show lineage* |

The extension activates in workspaces containing a `dbt_project.yml`. Projects in subfolders are found per file. The graph is read from disk and rebuilt on save, so unsaved edits are not reflected until you save.

Source and issues: [github.com/sipemu/dbt-lineage-viewer](https://github.com/sipemu/dbt-lineage-viewer).
