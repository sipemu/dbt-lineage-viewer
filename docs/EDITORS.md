# Editor integration

`dbt-lineage lsp` is a Language Server (stdio) for dbt projects. It needs no warehouse, Python or `dbt compile`; it parses the SQL and YAML directly.

| Feature | What you get |
| --- | --- |
| Hover on `ref()` / `source()` | Node type, materialization, description, upstream/downstream counts, impact if it changes |
| Go to definition | `ref()` → model / seed / snapshot file; `source()` → the table entry in its schema YAML |
| Code lens (model files) | `N upstream · M downstream · impact: <severity>`; command `dbt-lineage.showLineage` with the model name |
| Diagnostics | `dbt-lineage lint` findings for the open file; undefined refs/sources are underlined at the call. Refreshed on save |

Each file's project is the nearest ancestor directory with a `dbt_project.yml`, so monorepos with several projects work. The graph is built from disk with the parse cache (`.dbt-lineage/cache.bin`) and rebuilt when a file is saved; unsaved edits change hover positions but not the graph.

The VS Code extension bundles the binary. For the other editors install it first: `pip install dbt-lineage-cli`, `brew install sipemu/tap/dbt-lineage`, `cargo binstall dbt-lineage`, or `cargo install dbt-lineage`.

## VS Code

Install **dbt-lineage: lineage & impact for dbt** from the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=sipemu.dbt-lineage-viewer) or [Open VSX](https://open-vsx.org/extension/sipemu/dbt-lineage-viewer) (VSCodium, Cursor, Windsurf):

```sh
code --install-extension sipemu.dbt-lineage-viewer
```

No separate install is needed: the Marketplace and Open VSX serve a platform package with the `dbt-lineage` binary bundled (Linux x64/arm64, macOS Intel/Apple silicon, Windows x64). On other platforms a universal package uses `dbt-lineage` from your `PATH`.

**dbt-lineage: Show impact of my changes** (also the **dbt impact** status-bar item) runs `dbt-lineage ci report` against the base branch and shows the report in a panel: changed nodes and columns, column impact, breaking changes, the Mermaid impact graph, the rebuild selector and lint, the same content as the PR comment.

Settings: `dbtLineage.baseRef` (ref the impact report compares against; `auto` = `origin/HEAD`, else `main`, else `master`), `dbtLineage.impactOnSave` (refresh the open impact panel on save, default off), `dbtLineage.path` (your own binary instead of the bundled one; empty by default), `dbtLineage.lineageDepth` (levels shown by **dbt-lineage: Show lineage**, default 3).

To build the packages yourself: `cd editors/vscode && npm ci && npm run package` (writes `dist/dbt-lineage-viewer-<target>.vsix` for every target plus `universal`; `node scripts/package.mjs linux-x64` builds one). The bundled CLI version is `dbtLineageVersion` in `package.json`; `scripts/fetch-binary.mjs` downloads it from the GitHub release and verifies its `.sha256`.

## Neovim (nvim-lspconfig)

```lua
local configs = require("lspconfig.configs")
if not configs.dbt_lineage then
  configs.dbt_lineage = {
    default_config = {
      cmd = { "dbt-lineage", "lsp" },
      filetypes = { "sql", "yaml" },
      root_dir = require("lspconfig.util").root_pattern("dbt_project.yml"),
      single_file_support = false,
    },
  }
end
require("lspconfig").dbt_lineage.setup({})
```

Neovim 0.11+ without nvim-lspconfig:

```lua
vim.lsp.config("dbt_lineage", {
  cmd = { "dbt-lineage", "lsp" },
  filetypes = { "sql", "yaml" },
  root_markers = { "dbt_project.yml" },
})
vim.lsp.enable("dbt_lineage")
```

Code lenses: `:lua vim.lsp.codelens.refresh()` (or an autocmd on `BufEnter`/`BufWritePost`).

## Helix (`languages.toml`)

```toml
[language-server.dbt-lineage]
command = "dbt-lineage"
args = ["lsp"]

[[language]]
name = "sql"
language-servers = ["dbt-lineage"]
roots = ["dbt_project.yml"]

[[language]]
name = "yaml"
language-servers = ["yaml-language-server", "dbt-lineage"]
```

## Any other editor

Run `dbt-lineage lsp` over stdio for `sql` and `yaml` files, with `dbt_project.yml` as the root marker. The server advertises full text sync, hover, definition and code lens.
