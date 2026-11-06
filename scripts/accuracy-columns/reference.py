"""Reference column lineage for a dbt project, from compiled SQL.

Reads a manifest.json written by `dbt compile` and traces every output column
of every model to the upstream table columns it reads, using sqlglot. Models
are processed in dependency order so each model's output columns become the
schema for the models that select from it. Seed columns come from the CSV
header, source columns from their YAML declarations.

Output (stdout, JSON): {"edges": [[src_node, src_col, tgt_node, tgt_col], ...],
"measured": [model ids traced], "skipped": {model id: reason}}. Node ids use
dbt-lineage's short form: model.x, seed.x, snapshot.x, source.src.table.

Usage: uv run --with sqlglot --with duckdb reference.py <manifest.json>
       [--dialect duckdb] [--duckdb <database file>]

With --duckdb, table columns come from that database's information_schema
(after `dbt seed`), so `select *` from sources and seeds expands correctly.
"""

import csv
import json
import sys
from graphlib import TopologicalSorter
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema


def short_id(node):
    rt = node["resource_type"]
    if rt == "source":
        return f"source.{node['source_name']}.{node['name']}"
    return f"{rt}.{node['name']}"


def rel_key(table: exp.Table):
    return (table.catalog.lower(), table.db.lower(), table.name.lower())


def main():
    manifest_path = Path(sys.argv[1])
    dialect = sys.argv[sys.argv.index("--dialect") + 1] if "--dialect" in sys.argv else "duckdb"
    m = json.loads(manifest_path.read_text())
    nodes = {**m["nodes"], **m["sources"]}

    by_rel = {}
    schema = {}
    catalog = {}
    if "--duckdb" in sys.argv:
        import duckdb

        con = duckdb.connect(sys.argv[sys.argv.index("--duckdb") + 1], read_only=True)
        for db, sch, table, col in con.execute(
            "select table_catalog, table_schema, table_name, column_name "
            "from information_schema.columns order by ordinal_position"
        ).fetchall():
            catalog.setdefault((db.lower(), sch.lower(), table.lower()), []).append(col.lower())

    def register(node, columns):
        rel = node.get("relation_name")
        if rel:
            key = rel_key(sqlglot.parse_one(rel, into=exp.Table, read=dialect))
        elif node.get("config", {}).get("materialized") == "ephemeral":
            # Inlined into downstream SQL as a `__dbt__cte__<name>` CTE.
            key = ("", "", f"__dbt__cte__{node['name']}".lower())
        else:
            return
        by_rel[key] = short_id(node)
        columns = columns or catalog.get(key)
        if columns:
            schema.setdefault(key[0], {}).setdefault(key[1], {})[key[2]] = {
                c.lower(): "text" for c in columns
            }

    for uid, node in nodes.items():
        rt = node["resource_type"]
        if rt == "source":
            register(node, list(node.get("columns", {})))
        elif rt == "seed":
            path = Path(node["root_path"]) / node["original_file_path"]
            cols = []
            if path.exists():
                with path.open(newline="") as f:
                    cols = next(csv.reader(f), [])
            register(node, cols)
        elif rt in ("model", "snapshot"):
            register(node, [])

    models = {u: n for u, n in m["nodes"].items() if n["resource_type"] == "model"}
    order = TopologicalSorter(
        {u: [d for d in n["depends_on"]["nodes"] if d in models] for u, n in models.items()}
    ).static_order()

    edges, measured, skipped = set(), [], {}
    for uid in order:
        node = models[uid]
        sql = node.get("compiled_code")
        tgt = short_id(node)
        if not sql:
            skipped[tgt] = "not compiled"
            continue
        try:
            mapping = MappingSchema(schema, dialect=dialect)
            tree = sqlglot.parse_one(sql, read=dialect)
            # Ephemeral upstream models are inlined as CTEs; drop them so their
            # references resolve to the model (a table named like the CTE).
            with_ = tree.args.get("with_") or tree.args.get("with")
            if with_:
                for cte in list(with_.expressions):
                    if cte.alias.lower().startswith("__dbt__cte__"):
                        cte.pop()
                if not with_.expressions:
                    with_.pop()
            q = qualify(tree, schema=mapping, dialect=dialect, validate_qualify_columns=False)
            outputs = [s for s in q.named_selects if s and s != "*"]
            if not outputs or any(isinstance(s, exp.Star) for s in q.selects):
                skipped[tgt] = "unexpanded *"
                register(node, outputs)
                continue
            for col in outputs:
                root = lineage(col, q, schema=mapping, dialect=dialect)
                for n in root.walk():
                    if n.downstream or not isinstance(n.expression, exp.Table):
                        continue
                    src = by_rel.get(rel_key(n.expression))
                    src_col = n.name.split(".")[-1].strip("\"`").lower()
                    if src and src_col != "*":
                        edges.add((src, src_col, tgt, col.strip("\"`").lower()))
            measured.append(tgt)
            register(node, outputs)
        except Exception as e:  # noqa: BLE001 - reference is best-effort per model
            skipped[tgt] = f"{type(e).__name__}: {str(e)[:120]}"
            register(node, [])

    known = sorted(
        by_rel[k] for k in by_rel if schema.get(k[0], {}).get(k[1], {}).get(k[2])
    )
    json.dump(
        {"edges": sorted(edges), "measured": measured, "skipped": skipped, "known": known},
        sys.stdout,
    )


if __name__ == "__main__":
    main()
