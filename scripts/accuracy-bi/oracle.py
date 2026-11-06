"""Ground truth for the BI importer accuracy check, computed independently.

Each oracle reads a BI artifact with its own parser (PyYAML, ElementTree,
the `lkml` LookML parser, sqlglot for SQL) and returns
{dashboard label: set of table names}, table names normalised to the last
identifier part, lower-cased (what dbt-lineage matches on). None of this
shares code with dbt-lineage's importers; the rules follow each tool's
semantics as documented in docs/BI.md.
"""

import json
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import logging

import sqlglot
from sqlglot import exp

logging.getLogger("sqlglot").setLevel(logging.ERROR)

DIALECTS = [None, "snowflake", "bigquery", "tsql", "postgres", "duckdb", "mysql", "redshift"]


def norm(name):
    # BigQuery quotes the whole path in one pair of backticks.
    name = name.replace("`", "")
    last = re.split(r"\.(?=(?:[^\"`\[\]]|\"[^\"]*\"|`[^`]*`|\[[^\]]*\])*$)", name.strip())[-1]
    return last.strip("\"`[] ").lower()


def sql_tables(sql):
    """Tables a SQL text reads (CTEs excluded), or None if no dialect parses it."""
    for dialect in DIALECTS:
        try:
            trees = [t for t in sqlglot.parse(sql, read=dialect) if t is not None]
        except Exception:  # noqa: BLE001 - try the next dialect
            continue
        ctes = {c.alias_or_name.lower() for t in trees for c in t.find_all(exp.CTE)}
        out = set()
        for t in trees:
            for table in t.find_all(exp.Table):
                name = table.name.lower()
                if name and name not in ctes and not isinstance(table.this, exp.Anonymous):
                    out.add(name)
        return out
    return None


def strip_jinja(sql):
    sql = re.sub(r"\{#.*?#\}", " ", sql, flags=re.S)
    sql = re.sub(r"\{%.*?%\}", " ", sql, flags=re.S)
    return re.sub(r"\{\{.*?\}\}", "1", sql, flags=re.S)


# ---------------------------------------------------------------- Superset

def superset(root):
    import yaml

    dashboards, charts, datasets = [], {}, {}
    for f in sorted(Path(root).rglob("*.yaml")):
        doc = yaml.safe_load(f.read_text())
        if not isinstance(doc, dict):
            continue
        if "dashboard_title" in doc:
            dashboards.append(doc)
        elif "slice_name" in doc:
            charts[doc["uuid"]] = doc
        elif "table_name" in doc:
            datasets[doc["uuid"]] = doc
    truth = {}
    for d in dashboards:
        position = d.get("position") or {}
        if isinstance(position, str):
            position = json.loads(position)
        tables = set()
        for item in position.values():
            if not isinstance(item, dict) or item.get("type") != "CHART":
                continue
            chart = charts.get((item.get("meta") or {}).get("uuid"))
            ds = datasets.get(chart.get("dataset_uuid")) if chart else None
            if not ds:
                continue
            if ds.get("sql"):
                tables |= sql_tables(strip_jinja(ds["sql"])) or set()
            else:
                tables.add(norm(ds["table_name"]))
        truth[d["dashboard_title"]] = tables
    return truth


# ----------------------------------------------------------------- Tableau

FILE_CLASSES = {"excel-direct", "excel", "textscan", "csv", "google-sheets", "hyper",
                "dataengine", "sqlproxy", "msaccess", "box", "dropbox", "onedrive", "webdata-direct"}


def _relations(rel, classes, out):
    """Collect tables of a <relation> tree (joins, unions, custom SQL)."""
    kind = rel.get("type")
    conn = rel.get("connection")
    if conn is not None and classes.get(conn, "") in FILE_CLASSES:
        return
    # An extract caches the live query; its [Extract].[Extract] relation is
    # not a warehouse table (the live connection's relations are).
    if rel.get("table") == "[Extract].[Extract]":
        return
    if kind == "table" and rel.get("table") and rel.get("name") != "sqlproxy":
        out.add(norm(rel.get("table")))
    elif kind == "text" and rel.text:
        sql = re.sub(r"<\[Parameters\]\.\[[^\]]*\]>", "1", rel.text)
        # Tableau escapes < and > in custom SQL by doubling them.
        sql = sql.replace("<<", "<").replace(">>", ">")
        out |= sql_tables(sql) or set()
    for child in rel.findall("relation"):
        _relations(child, classes, out)


def _datasource_tables(ds):
    conn = ds.find("connection")
    if conn is None:
        return set()
    cls = conn.get("class")
    if cls in FILE_CLASSES:
        return set()
    # Federated: named connections carry the class per relation.
    classes = {nc.get("name"): (nc.find("connection").get("class") if nc.find("connection") is not None else "")
               for nc in conn.iter("named-connection")}
    if cls == "federated" and classes and all(c in FILE_CLASSES for c in classes.values()):
        return set()
    out = set()
    for rel in conn.findall("relation"):
        _relations(rel, classes, out)
    # Newer workbooks keep relations in an object graph.
    for rel in ds.iter("relation"):
        if rel.get("type") in ("table", "text") and _under_extract(ds, rel) is False:
            _relations(rel, classes, out)
    return out


def _under_extract(ds, target):
    for ext in ds.iter("extract"):
        for rel in ext.iter("relation"):
            if rel is target:
                return True
    return False


def _read_twb(path):
    if path.suffix == ".twbx":
        with zipfile.ZipFile(path) as z:
            name = next(n for n in z.namelist() if n.endswith(".twb"))
            return z.read(name)
    return path.read_bytes()


def tableau(root):
    truth = {}
    for path in sorted(list(Path(root).rglob("*.twb")) + list(Path(root).rglob("*.twbx"))):
        wb = ET.fromstring(_read_twb(path))
        datasources = {}
        for ds in wb.findall("./datasources/datasource"):
            datasources[ds.get("name")] = _datasource_tables(ds)
        worksheets = {}
        for ws in wb.findall("./worksheets/worksheet"):
            used = {d.get("datasource") for d in ws.iter("datasource-dependencies")}
            used |= {d.get("name") for d in ws.findall("./table/view/datasources/datasource")}
            worksheets[ws.get("name")] = {u for u in used if u}
        dashboards = {}
        for db in wb.findall("./dashboards/dashboard"):
            dashboards[db.get("name")] = {z.get("name") for z in db.iter("zone") if z.get("name")}

        def tables_of(sheets):
            out = set()
            for s in sheets:
                for d in worksheets.get(s, ()):
                    out |= datasources.get(d, set())
            return out

        stem = path.stem
        if not dashboards:
            tables = tables_of(worksheets) if worksheets else set().union(*datasources.values()) if datasources else set()
            truth[stem] = tables
        for name, zones in dashboards.items():
            truth[f"{stem}: {name}"] = tables_of(z for z in zones if z in worksheets)
    return truth


# ------------------------------------------------------------------ Looker

def looker(root):
    import lkml
    import yaml

    views, explores = {}, {}
    for f in sorted(Path(root).rglob("*.lkml")):
        if f.name.endswith(".dashboard.lookml"):
            continue
        try:
            doc = lkml.load(f.read_text())
        except Exception:  # noqa: BLE001 - unparseable file
            continue
        model = f.name.removesuffix(".model.lkml") if f.name.endswith(".model.lkml") else None
        for v in doc.get("views", []):
            name = v["name"].lstrip("+")
            views.setdefault(name, {}).update({k: val for k, val in v.items() if k != "name"})
        for e in doc.get("explores", []):
            explores.setdefault(e["name"].lstrip("+"), []).append((model, e))

    def view_tables(name, seen=()):
        if name in seen:
            return set()
        v = views.get(name)
        if v is None:
            return set()
        if "sql_table_name" in v:
            t = re.sub(r"@\{(\w+)\}", r"\1", v["sql_table_name"]).replace(";;", "")
            # Liquid can choose between tables: every alternative counts.
            parts = re.split(r"\{%.*?%\}", t, flags=re.S)
            return {norm(p) for p in parts if p.strip()}
        dt = v.get("derived_table") or {}
        if "sql" in dt:
            sql = dt["sql"]
            refs = set()
            for other in re.findall(r"\$\{(\w+)\.SQL_TABLE_NAME\}", sql):
                refs |= view_tables(other, seen + (name,))
            sql = re.sub(r"\$\{(\w+)\.SQL_TABLE_NAME\}", r"\1", sql)
            sql = re.sub(r"\$\{[^}]*\}", "1", sql)
            sql = re.sub(r"\{%.*?%\}", " ", sql, flags=re.S)
            sql = re.sub(r"\{\{.*?\}\}", "1", sql, flags=re.S)
            sql = re.sub(r"@\{(\w+)\}", r"\1", sql)
            found = sql_tables(sql) or set()
            found -= set(re.findall(r"\$\{(\w+)\.SQL_TABLE_NAME\}", dt["sql"]))
            return found | refs
        out = set()
        for parent in v.get("extends__all", [[]])[0] if v.get("extends__all") else v.get("extends", []):
            out |= view_tables(parent, seen + (name,))
        return out

    def explore_views(model, name, seen=()):
        if name in seen:
            return set()
        candidates = explores.get(name, [])
        chosen = [e for m, e in candidates if m == model] or [e for _, e in candidates]
        out = set()
        if not chosen:
            return {name}
        for e in chosen:
            out.add(e.get("from") or e.get("view_name") or name)
            for j in e.get("joins", []):
                out.add(j.get("from") or j["name"])
            for parent in (e.get("extends__all") or [[]])[0] or e.get("extends", []):
                out |= explore_views(model, parent, seen + (name,))
        return out

    truth = {}
    for f in sorted(Path(root).rglob("*.dashboard.lookml")):
        docs = yaml.safe_load(f.read_text()) or []
        for d in docs if isinstance(docs, list) else [docs]:
            if not isinstance(d, dict) or "dashboard" not in d:
                continue
            pairs = set()
            for el in d.get("elements", []) or []:
                for q in [el] + ([el["query"]] if isinstance(el.get("query"), dict) else []) + (el.get("merged_queries") or []):
                    if q.get("model") and q.get("explore"):
                        pairs.add((q["model"], q["explore"]))
            tables = set()
            for model, explore in pairs:
                for v in explore_views(model, explore):
                    tables |= view_tables(v)
            truth[d.get("title") or d["dashboard"]] = tables
    return truth


# ------------------------------------------------------------------ anofox

# Panel and element roles of anofox-visualization (casts like `::BARCHART`);
# SQL types (`::INTEGER`, `::VARCHAR`) are not roles.
PANEL_ROLES = {"BARCHART", "LINECHART", "AREACHART", "PIECHART", "TABLE", "METRIC", "SCATTER",
               "HISTOGRAM", "BOXPLOT", "HEATMAP", "MARKDOWN", "STORY", "MAP", "DROPDOWN", "BUTTONS",
               "TAB", "DELTA", "GAUGE"}
ROLE_CAST = re.compile(r"::\s*([A-Z_]+)\b")
SQL_TYPES = {"INTEGER", "INT", "BIGINT", "DOUBLE", "FLOAT", "VARCHAR", "TEXT", "DATE", "TIMESTAMP",
             "BOOLEAN", "DECIMAL", "NUMERIC", "REAL", "SMALLINT", "TINYINT", "HUGEINT", "INTERVAL",
             "JSON", "UUID", "BLOB", "TIME"}
TABLE_FUNCTIONS = {"read_csv", "read_csv_auto", "read_parquet", "read_json", "read_json_auto",
                   "range", "generate_series", "unnest"}


def _strip_comments(text):
    return re.sub(r"--[^\n]*", " ", text)


def _statements(text):
    return [s for s in re.split(r";\s*(?:\n|$)", _strip_comments(text)) if s.strip()]


def anofox(root):
    files = sorted(Path(root).rglob("*.sql"))
    created = set()
    for f in files:
        for m in re.finditer(r"create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:table|view)\s+"
                             r"(?:if\s+not\s+exists\s+)?([\w.\"]+)", _strip_comments(f.read_text(errors="replace")), re.I):
            created.add(norm(m.group(1)))
    truth = {}
    for f in files:
        text = f.read_text(errors="replace")
        title = re.search(r"^--\s*@title\s+(.+)$", text, re.M)
        roles = set(ROLE_CAST.findall(text))
        if not (title or "DASHBOARD_TITLE" in roles or roles & PANEL_ROLES):
            continue
        if title:
            name = title.group(1).strip()
        else:
            m = re.search(r"'([^']*)'\s*::\s*DASHBOARD_TITLE", text)
            name = m.group(1) if m else f.stem
        tables = set()
        for stmt in _statements(text):
            # Role casts are anofox syntax: make them plain casts for sqlglot.
            sql = ROLE_CAST.sub(lambda m: m.group(0) if m.group(1) in SQL_TYPES else "::VARCHAR", stmt)
            found = sql_tables(sql)
            if found is None:
                plain = re.sub(r"'(?:[^']|'')*'", "''", sql)
                ctes = {c.lower() for c in re.findall(r"\b(\w+)\s+as\s*\(", plain, re.I)}
                found = {norm(t) for t in re.findall(r"\b(?:from|join)\s+([A-Za-z_][\w.\"]*)(?!\s*\()", plain, re.I)}
                found -= ctes
            tables |= found
        # sqlglot can misread DuckDB's read_* table functions as tables.
        truth[name] = {t for t in tables
                       if t and t not in created and t not in TABLE_FUNCTIONS and not t.startswith("read_")}
    return truth


def powerbi(slug):
    gt = json.loads((Path(__file__).parent / "ground_truth" / "powerbi.json").read_text())
    return {k: set(v) for k, v in gt[slug].items()}
