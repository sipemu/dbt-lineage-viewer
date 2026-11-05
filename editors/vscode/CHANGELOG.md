# Changelog

## 0.2.0

- **dbt-lineage: Show impact of my changes**: compares the working tree with the base branch (`dbtLineage.baseRef`, default `auto` → `origin/HEAD`, else `main`/`master`) and shows the `ci report` in a panel: changed nodes and columns, column impact, breaking changes, the Mermaid impact graph (rendered offline), the rebuild selector and lint. Status-bar item **dbt impact**; `dbtLineage.impactOnSave` refreshes the panel on save. Clear messages when the project is not in git or the base ref does not exist.
- **Binary bundled**: platform packages for linux-x64, linux-arm64, darwin-x64, darwin-arm64 and win32-x64 ship dbt-lineage 0.15.0, so nothing else needs installing. `dbtLineage.path` now defaults to empty (= bundled binary); a universal package without a binary uses `dbt-lineage` from `PATH`.
- The extension is bundled with esbuild (no `node_modules` in the package).
- Changing `dbtLineage.path` restarts the language server.

## 0.1.0

- Initial release: hover and go-to-definition on `ref()` / `source()`, lineage code lens per model, lint diagnostics, and the *Show lineage* graph panel, all backed by `dbt-lineage lsp`.
