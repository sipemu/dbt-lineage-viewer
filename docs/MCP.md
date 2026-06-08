# MCP integration — cookbook

`dbt-lineage` ships an MCP (Model Context Protocol) server that exposes
project lineage as tools, resources, and prompts. AI agents (Claude Code,
Claude Desktop, etc.) can call them directly instead of shelling out to the
CLI and parsing text output.

This document covers:

1. [Connecting the server](#connecting-the-server)
2. [What's exposed](#whats-exposed)
3. [Prompt walkthroughs](#prompt-walkthroughs)
4. [Composition recipes](#composition-recipes)
5. [Limitations](#limitations)

## Connecting the server

### Claude Code

```sh
claude mcp add dbt-lineage -- dbt-lineage mcp --manifest /abs/path/to/project/target/manifest.json
```

For SQL-parse mode (no `dbt compile` needed):

```sh
claude mcp add dbt-lineage -- dbt-lineage mcp --project-dir /abs/path/to/project
```

The server picks `target/manifest.json` automatically when present, otherwise
falls back to SQL-parse mode.

### Claude Desktop

In `claude_desktop_config.json` (location varies by OS):

```json
{
  "mcpServers": {
    "dbt-lineage": {
      "command": "dbt-lineage",
      "args": [
        "mcp",
        "--manifest",
        "/abs/path/to/project/target/manifest.json"
      ]
    }
  }
}
```

Restart Claude Desktop. The server appears as a connected tool source.

### Custom MCP clients

The server speaks JSON-RPC 2.0 over stdio. One JSON object per line on both
input and output. Initialize handshake:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize"}
```

Response declares all three capabilities (`tools`, `resources`, `prompts`).

## What's exposed

### Tools (10)

| Tool | Purpose | Required args |
|---|---|---|
| `summary` | Project overview (counts, tags, top fan-out, orphans) | — |
| `search_models` | Find nodes by selector (`tag:X`, `path:Y`, model name) and/or `node_type` | — |
| `lineage` | Upstream/downstream graph for a model | `model` |
| `impact` | Downstream impact + per-node severity | `model` |
| `get_model_details` | Full node info: description, materialization, tags, columns, neighbors | `model` |
| `read_model_sql` | Read raw or compiled SQL of a model. Disk first, manifest fallback | `model` |
| `column_upstream` | Trace a column back through the lineage | `model` (`column` optional) |
| `column_downstream` | Trace a column forward | `model`, `column` |
| `lineage_bundle` | One-shot composition: lineage + SQL + descriptions for N hops | `model` |
| `propose_test` | Draft an apply-ready YAML diff adding a generic test | `model`, `column`, `kind` |

### Resources

URI scheme `dbt-lineage://`. Surface as `@`-mention pickers in MCP clients.

- `dbt-lineage://summary` — always-fresh project summary as JSON.
- `dbt-lineage://model/<name>/sql` — raw SQL (disk or manifest fallback).
- `dbt-lineage://model/<name>/lineage.mermaid` — focused Mermaid diagram
  (direct upstream + downstream of the model).
- `dbt-lineage://source/<name>` — source metadata JSON.
- `dbt-lineage://exposure/<name>` — exposure metadata JSON.

### Prompts

Predefined workflow templates. Show up as slash-commands or buttons in
clients that support them.

| Prompt | Args | What it does |
|---|---|---|
| `review-impact` | `model` | Walk through downstream impact, suggest tests for critical paths |
| `propose-tests` | `model` | Inspect SQL + columns; propose generic tests per column |
| `explain-column` | `model`, `column` | Trace upstream + narrate transformations |
| `find-bottleneck` | `model` | Use run_results.json to find the slowest hop on the path |
| `document-model` | `model` | Generate Markdown docs with lineage diagram |
| `onboard-project` | — | Tour the project: summary, top fan-out, untested marts |

## Prompt walkthroughs

### `/onboard-project`

Best first command on an unfamiliar project. Combines `summary` + `search_models`
to identify high-impact areas:

> "Project `jaffle_shop` (12 models, 4 sources, 1 exposure). Top fan-out:
> `stg_orders` reaches 6 downstream models. Untested marts: 2
> (`customer_lifetime_value`, `daily_revenue`). Suggested next reads:
> `orders` (highest impact mart), `weekly_report` (only exposure)."

### `/review-impact orders`

Useful before a refactor. Runs `impact` to enumerate affected nodes and
suggests tests to add for the critical paths:

> "Changing `orders` impacts 1 exposure (Critical: `weekly_report`),
> 1 model (High: `customers`), and 1 test (Low). The exposure is reachable
> in 2 hops. Suggested tests to add: ..."

### `/explain-column customers lifetime_value`

Traces a column upstream with transformation classification:

> "`customers.lifetime_value` is derived from `orders.total_amount` via
> `SUM`. `orders.total_amount` is a direct copy of `stg_payments.amount`,
> which comes from `source.raw.payments.amount` unchanged."

Pairs the AST-backed `column_upstream` tool with `read_model_sql` calls to
explain *why* each hop is classified the way it is.

### `/find-bottleneck weekly_report`

Uses run_results.json (if present) to identify the slowest hop on the
critical path to `weekly_report`. Without run_results, surfaces the longest
upstream chain by depth.

### `/document-model orders`

Calls `lineage_bundle` for a complete view, then drafts a Markdown
documentation page with description, columns, upstream/downstream, and an
embedded Mermaid diagram via the `dbt-lineage://model/orders/lineage.mermaid`
resource.

### `/propose-tests stg_orders`

For each column on `stg_orders`, suggests the most relevant generic test
(`not_null`, `unique`, `accepted_values`, `relationships`) and calls
`propose_test` to produce an apply-ready YAML diff for each. The user
reviews and applies.

## Composition recipes

The MCP server is designed for tool chains, not isolated calls. A few
patterns that work well:

### "Audit then fix" loop

1. Agent calls `summary` to find untested marts.
2. For each untested mart, calls `read_model_sql` to read the SQL.
3. For each column, calls `propose_test` with the inferred test kind.
4. User reviews the resulting diffs and applies them.

### "Explain a broken value" loop

1. User points at a suspect value (e.g. "why is `customers.lifetime_value`
   zero for some rows?").
2. Agent calls `column_upstream(model="customers", column="lifetime_value")`
   to get the chain.
3. For each hop, calls `read_model_sql` and identifies the relevant
   transformation (CASE, aggregation, filter).
4. Returns a narrative of the data flow with the line numbers where the
   zero values likely originate.

### "Before merge" review

1. Agent calls `lineage_bundle(model="<changed>", upstream=2, downstream=3)`
   to get full context in one call.
2. Calls `impact("<changed>")` to enumerate blast radius.
3. Composes a checklist: tests to verify, downstream models to re-run.

## Limitations

- **Column lineage accuracy**: the AST-backed analyzer (0.6.0+) handles
  direct, aliased, qualified, JOIN, aggregation, and CTE patterns. Window
  functions and complex subqueries fall back to the name-matching heuristic.
- **`propose_test` covers four generic tests**: `not_null`, `unique`,
  `accepted_values`, `relationships`. Custom / singular tests must be
  written by hand.
- **Manifest mode is required for column lineage on models without
  on-disk SQL**: the AST analyzer reads file_path content. If SQL isn't
  available either on disk or in the manifest's `raw_code`, the column
  tools return name-match heuristics only.
- **The server is read-only at the protocol level**: `propose_test` returns
  a diff but does NOT apply it. The MCP client handles application after
  user confirmation.

For deeper investigation, the open issues at
<https://github.com/sipemu/dbt-lineage-viewer/issues> track ongoing
improvements (more rule support, semantic-layer awareness, Python models, etc).
