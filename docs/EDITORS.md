# Editor integration

`dbt-lineage lsp` is a Language Server (stdio) for dbt projects. It needs no warehouse, Python or `dbt compile`; it parses the SQL and YAML directly.

| Feature | What you get |
| --- | --- |
| Hover on `ref()` / `source()` | Node type, materialization, description, upstream/downstream counts, impact if it changes |
| Go to definition | `ref()` → model / seed / snapshot file; `source()` → the table entry in its schema YAML |
| Code lens (model files) | `N upstream · M downstream · impact: <severity>`; command `dbt-lineage.showLineage` with the model name |
| Diagnostics | `dbt-lineage lint` findings for the open file; undefined refs/sources are underlined at the call. Refreshed on save |

Each file's project is the nearest ancestor directory with a `dbt_project.yml`, so monorepos with several projects work. The graph is built from disk with the parse cache (`.dbt-lineage/cache.bin`) and rebuilt when a file is saved; unsaved edits change hover positions but not the graph.

Install the binary first: `brew install sipemu/tap/dbt-lineage`, `cargo binstall dbt-lineage`, or `cargo install dbt-lineage`.

## VS Code

Install the **dbt-lineage** extension (publisher `sipemu`), or build it from [`editors/vscode`](../editors/vscode):

```sh
cd editors/vscode
npm install
npx @vscode/vsce package          # produces dbt-lineage-<version>.vsix
code --install-extension dbt-lineage-*.vsix
```

Settings: `dbtLineage.path` (binary, default `dbt-lineage`), `dbtLineage.lineageDepth` (levels shown by **dbt-lineage: Show lineage**, default 3).

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
