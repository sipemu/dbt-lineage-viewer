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

## Tableau

Tableau workbooks are read offline, from `.twb` files or `.twbx` packages, e.g. a repository of exported workbooks:

```sh
dbt-lineage exposures tableau --workbooks workbooks/ \
  --url 'https://tableau.example.com/#/views/{name}' \
  --write models/exposures_tableau.yml
```

- **One exposure per dashboard**, named `<workbook>: <dashboard>`; a workbook without dashboards becomes one exposure. Owner defaults to `tableau`; `{name}` in `--url` is the workbook file name without extension (default URL: the file path).
- **Dependencies:** the tables of the datasources behind the dashboard's worksheets (`<zone>` → worksheet → `<datasource-dependencies>`): `<relation type="table">` entries, including joins and federated (multi-connection) sources, plus tables read by custom SQL (`type="text"`; `<[Parameters].[…]>` placeholders are neutralised).
- **Skipped, with a note on stderr:** extracts (`.hyper`, `[Extract].[Extract]`), file sources (Excel, CSV, Google Sheets, …), and published Server datasources (`sqlproxy`), whose tables the workbook doesn't record.

Tested on 15 public workbooks (Postgres, Snowflake custom SQL, BigQuery, SQL Server, MySQL, federated, extracts, Excel, published datasources); generated YAML passes `dbt parse`.

## Looker (LookML, offline)

`dbt-lineage exposures looker` reads a LookML project from disk, so no Looker API access is needed. Point it at the project (or several):

```sh
dbt-lineage exposures looker --lookml path/to/lookml-project \
  --url 'https://looker.example.com/dashboards/{name}' \
  --write models/exposures_looker.yml
```

- **Dashboards:** LookML dashboards (`.dashboard.lookml`, YAML). Every element's `model` + `explore` counts, including `query:` and `merged_queries:`; dashboard-level filters don't. Exposure name from `title`, description from `description`, owner `looker`; `--url` replaces `{name}` with the dashboard id (default: the file path). User-defined dashboards stored only in Looker's database are not in the LookML project and are not imported.
- **Explores** (in any `.lkml` file): base view (`from:` / `view_name:` / the explore name) plus every `join:` (`from:` or the join name), and `extends:`.
- **Views:** `sql_table_name` (with or without `;;`, BigQuery backticks, `@{CONSTANT}` placeholders), or the tables a `derived_table: { sql: … ;; }` reads (CTEs excluded; `${other_view.SQL_TABLE_NAME}` resolves to that view's tables; Liquid neutralised), or the table of an `extends:` parent. Refinements (`view: +orders`) merge into the original view.

Tested on four public LookML projects (llooker/snowflake-usage-block, llooker/google_ga360, llooker/blocks_redshift_admin, looker-open-source/block-cortex-sap): all 44 dashboards resolve to tables, and generated YAML passes `dbt parse`.

### Limitations

- `include:` statements are not followed; all `.lkml` files under the given paths are read. Explores are looked up per model first, then by name across the project.
- Looker constants (`@{GCP_PROJECT}`) are not substituted, only stripped to their names; matching uses the last table-name part anyway.

## Superset (dashboard exports, offline)

`dbt-lineage exposures superset` reads Apache Superset dashboard exports, so no Superset API access is needed. Export dashboards with `superset export-dashboards -f dashboards.zip`, the *Download* action in the UI, or `GET /api/v1/dashboard/export/`, and point the command at the ZIPs (or unpacked export directories):

```sh
dbt-lineage exposures superset --exports exports/ \
  --url 'https://superset.example.com/superset/dashboard/{name}/' \
  --write models/exposures_superset.yml
```

- **Files:** `--exports` takes ZIPs, directories (searched recursively for `.yaml`/`.yml`/`.zip`/`.json`) and single files. YAML files are recognised by content, not path: `dashboard_title` → dashboard, `slice_name` → chart, `table_name` → dataset; databases and `metadata.yaml` are ignored. Assets are matched by `uuid` across all inputs (the first copy wins), so several exports can be read at once.
- **Dependencies:** dashboard → the charts in its `position` layout (`CHART` entries' `meta.uuid`; a JSON-string layout works too) → each chart's `dataset_uuid` → the dataset's tables. A physical dataset is `schema.table_name` (with `catalog` when set); a virtual dataset (`sql` set) contributes the tables its SQL reads (CTEs excluded; Jinja `{{ … }}`, `{% … %}` and `{# … #}` neutralised), not its own name.
- **Exposure fields:** name from `dashboard_title`, description from `description`, owner `superset` (exports carry no owners). `--url` replaces `{name}` with the dashboard `slug`, or its `uuid` when it has none (default URL: the export path).
- **Notes on stderr:** charts in the layout but missing from the export, charts without a dataset (e.g. built on a saved query), datasets missing from the export, and virtual datasets whose SQL yields no table.
- **Legacy JSON export** (pre-1.0 `dashboards` + `datasources` with `__Dashboard__` / `__Slice__` / `__SqlaTable__`) is read as well: each slice's `datasource_name` (+ `schema` in its `params`) is looked up in `datasources`; `{name}` is the slug, else the dashboard id.

Tested on the example dashboards shipped in the Superset repository: `superset/examples/configs` at tag 3.1.0 (export layout, 6 dashboards, as a ZIP; includes virtual datasets) and `superset/examples` on master (9 dashboards, per-example layout). All charts and datasets resolve; virtual datasets map to their base tables (e.g. *Slack Dashboard* → `channel_members`, `channels`, `exported_stats`, `messages`, `threads`, `users`, `users_channels`). Generated YAML passes `dbt parse`.

### Limitations

- Superset routes dashboards by slug or numeric id; v1 exports carry no numeric id, so for dashboards without a slug the `{name}` fallback is the uuid, which is not a working Superset URL. Give those dashboards a slug, or use a `--url` without `{name}`.
- Jinja that generates table names (`FROM {{ my_table }}`) can't be resolved; those references are dropped.
- Dashboards from the export are always imported; a chart's own `dashboards` list (not part of v1 exports) is not used.

## Power BI (PBIP, `.pbit`, offline)

`dbt-lineage exposures powerbi` reads Power BI reports from disk: [PBIP projects](https://learn.microsoft.com/power-bi/developer/projects/projects-overview) (as saved by Power BI Desktop or synced by Fabric Git integration) and `.pbit` templates. No Power BI API access is needed.

```sh
dbt-lineage exposures powerbi --reports powerbi/ \
  --url 'https://app.powerbi.com/groups/me/reports/{name}' \
  --write models/exposures_powerbi.yml
```

- **Inputs:** `--reports` takes directories (searched recursively) and files. A `*.Report/definition.pbir` points at its semantic model (`datasetReference.byPath`); the model is TMDL (`*.SemanticModel/definition/**/*.tmdl`) or TMSL JSON (`model.bim`). A `.pbip` file selects the reports it lists; a `.bim` file or semantic model folder that no scanned report uses becomes an exposure of its own. A `.pbit` is a zip whose `DataModelSchema` entry holds the model (UTF-16 JSON).
- **One exposure per report**, named after the report folder (without `.Report`) or the `.pbit` file; owner `powerbi`; `{name}` in `--url` is that name (default URL: the path). Reports with the same name (`Sales.Report` and `Sales.pbit`) get the shortest distinguishing path appended.
- **Dependencies:** the warehouse tables the model's partitions load:
  - Power Query navigation steps: `Source{[Schema="dbo",Item="orders"]}[Data]` (SQL Server, Postgres, Oracle, Synapse, Fabric), `{[Name="ORDERS",Kind="Table"]}` / `Kind="View"` after database and schema steps (Snowflake, Databricks, BigQuery), and the innermost `{[Name="region"]}[Data]` step of a chain without `Kind` (Redshift, Unity Catalog);
  - native SQL in `Value.NativeQuery(…, "select …")` (a literal or a step holding one) and `Sql.Database("srv", "db", [Query="select …"])`, read with the same SQL extraction as the other importers (M escapes `""` and `#(lf)` decoded; T-SQL `[schema].[table]` handled);
  - shared expressions (`expressions` in `model.bim`, `expressions.tmdl`) referenced by name (`#"Base Query"`), recursively; parameters (`"value" meta [IsParameterQuery=true]`) are substituted into navigation steps;
  - Direct Lake `entity` partitions (`schemaName.entityName`), legacy `query` partitions, and incremental-refresh `sourceExpression`s.
- **Skipped, with a note on stderr:** `.pbix` files (the model is stored in a binary format; save as `.pbit` or PBIP), reports bound to a published semantic model (`byConnection`), `.pbit` live-connection templates without a model, and tables whose M reads no warehouse table (Excel, CSV, web, SharePoint, entered data, SQL built at refresh time). DAX calculated tables have no source and are ignored.

Tested on PBIP projects, `model.bim` files and `.pbit` templates from 13 public repositories (databricks-solutions/powerbi-on-databricks-migration-accelerator — Snowflake, Databricks, Redshift, SQL Server, Synapse, native queries, incremental refresh, 14 reports; datalopes1/deliverycenter_dwh; RuiRomano/pbip-demo and fabric-cli-powerbi-cicd-sample; microsoft/fabric-cicd; ProdataSQL/FinancialModelling and DWA; djouallah/aemo_fabric (Direct Lake); Hugoberry/duckdb-pbix-extension; richardswinbank/community; PowerBiDevCamp/ALM-for-PowerBi; alisonpezzott/DataAIVIX2025; GoogleCloudPlatform/BigQuery-Agent-Analytics-SDK); generated YAML passes `dbt parse`.

### Limitations

- Reports are not inspected visual by visual: every table of the report's semantic model counts, including tables no visual uses.
- M is matched by pattern, not evaluated: SQL assembled from variables at refresh time (`"select * from " & Table`) and navigation by computed values are not resolved. Only the first literal of a concatenated `Query=` string is read.
- Thin reports on a published semantic model (`byConnection`) can't be resolved offline; import the semantic model's own PBIP folder instead.

## Other tools

Contributions welcome: a connector only needs to produce `bi::Dashboard` values (name, URL, owner, table names).
