# BI dashboards as exposures

Impact analysis rates a change as **Critical** when it reaches an exposure, but exposures are usually maintained by hand and drift. `dbt-lineage exposures` reads dashboards from a BI tool, maps the tables they query onto the project's models and sources, and writes dbt `exposures:` YAML.

## Metabase

```sh
export METABASE_API_KEY=mb_…   # Admin > Settings > Authentication > API keys
dbt-lineage exposures metabase --url https://metabase.example.com \
  --write models/exposures_metabase.yml
```

Without `--write` the YAML goes to stdout (`-o json` for JSON). Re-run it to refresh; the file is regenerated, not merged.

What is read:

- every non-archived dashboard and its questions;
- **native SQL questions**: table names from `FROM` / `JOIN` (template tags `{{x}}` and optional clauses `[[…]]` are handled; CTE names are ignored);
- **GUI questions**: source table and joined tables, including questions built on other questions;
- both the legacy MBQL and the newer staged (pMBQL) query formats;
- the dashboard creator as `owner` (falls back to `Metabase` when unknown, since dbt requires an owner).

Mapping: a table matches a model, seed or snapshot by name, otherwise a source by table name (case-insensitive, schema ignored). Unmatched tables are listed on stderr; dashboards with no match are skipped.

Example output:

```yaml
version: 2
exposures:
- name: sales_overview
  label: Sales Overview
  type: dashboard
  url: https://metabase.example.com/dashboard/2
  description: Weekly revenue
  owner:
    name: Ana Lytics
    email: ana@example.com
  depends_on:
  - ref('customers')
  - ref('orders')
```

Verified against Metabase v0.65 (Postgres data source) and `dbt parse` with dbt-core 1.12.

### Limitations

- Matching is by table name only: model `alias` configs and schema names are not considered, so two relations with the same name in different schemas are ambiguous (a model wins over a source).
- The API key needs read access to the dashboards' collections; creator names on Metabase versions that don't embed `creator` need an admin key.

## anofox-visualization

[anofox-visualization](https://github.com/DataZooDE/anofox-visualization) dashboards are DuckDB `.sql` files: header comments describe the dashboard and every `SELECT` with `::ROLE` casts is a panel. With dbt-duckdb building the tables those dashboards read, point the importer at the dashboard files:

```sh
dbt-lineage exposures anofox --dashboards dashboards/ \
  --url 'https://viz.example.com/d/{name}' \
  --write models/exposures_anofox.yml
```

- **Which files:** `--dashboards` takes files and directories (searched recursively for `.sql`). A file is a dashboard if it has a `-- @title` header, a `'…'::DASHBOARD_TITLE` cast, or panel role casts (`::BARCHART`, `::METRIC`, `::TABLE`, …); helper scripts such as `init.sql` or `compute.sql` are skipped.
- **Exposure fields:** name from `-- @title` (else `::DASHBOARD_TITLE`, else the file name), description from `-- @description`, owner from `-- @owner` (an email or a name; default `anofox-visualization`). `--url` sets the link; `{name}` is replaced by the file name without extension. Without `--url` the file path is used.
- **Dependencies:** every table read in `FROM` / `JOIN`, minus CTEs, table functions (`read_csv(…)`, `range(…)`) and tables or views created by any scanned file (sample data, CSV loads, materialized results), which belong to the dashboards rather than to dbt. Statements the SQL parser can't read (DuckDB-specific syntax) fall back to scanning `FROM` / `JOIN` outside strings, comments and quoted labels.

On the anofox-visualization repository itself (72 dashboards) every remaining unmatched table is a real table the dashboards expect to exist.

## Other tools

Looker, Tableau, Power BI and Superset are planned. Contributions welcome: a connector only needs to produce `bi::Dashboard` values (name, URL, owner, table names).
