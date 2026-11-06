#!/usr/bin/env python3
"""Metabase part of the BI importer accuracy check (needs Docker).

Starts nothing itself: run Metabase with its sample database first, e.g.

    docker run -d --name dbtl-metabase-bench -p 3999:3000 metabase/metabase:v0.56.4

Then this script sets the instance up through the API (admin user, API key),
adds two dashboards on top of Metabase's example dashboard (native SQL with a
template tag, an optional clause and a CTE; a GUI question with a join; a
question built on another question), computes the ground truth from the API's
card definitions independently of dbt-lineage (sqlglot for native SQL), runs
`dbt-lineage exposures metabase` and scores it like run.py.

    uv run --no-project --with sqlglot python scripts/accuracy-bi/metabase.py [--url http://localhost:3999]
"""

import argparse
import json
import re
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import oracle  # noqa: E402
import run as bench  # noqa: E402

EMAIL, PASSWORD = "bench@example.com", "Bench-pass-2026!"


class Api:
    def __init__(self, url):
        self.url, self.headers = url.rstrip("/"), {}

    def call(self, method, path, body=None):
        req = urllib.request.Request(
            self.url + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json", **self.headers},
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            text = r.read().decode()
            return json.loads(text) if text else None


def wait_healthy(api, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if api.call("GET", "/api/health").get("status") == "ok":
                return
        except Exception:  # noqa: BLE001 - not up yet
            pass
        time.sleep(5)
    raise SystemExit("Metabase did not become healthy")


def setup(api):
    props = api.call("GET", "/api/session/properties")
    if props.get("setup-token"):
        try:
            api.call("POST", "/api/setup", {
                "token": props["setup-token"],
                "user": {"email": EMAIL, "password": PASSWORD, "first_name": "Bench", "last_name": "Mark",
                         "site_name": "bench"},
                "prefs": {"site_name": "bench", "site_locale": "en", "allow_tracking": False},
            })
        except urllib.error.HTTPError:
            pass  # already set up
    session = api.call("POST", "/api/session", {"username": EMAIL, "password": PASSWORD})
    api.headers = {"X-Metabase-Session": session["id"]}
    groups = api.call("GET", "/api/permissions/group")
    admin = next(g["id"] for g in groups if g["name"] == "Administrators")
    key = api.call("POST", "/api/api-key", {"name": f"bench-{int(time.time())}", "group_id": admin})
    return key["unmasked_key"]


def sample_db(api):
    dbs = api.call("GET", "/api/database")
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    db = next(d for d in dbs if d.get("is_sample"))
    tables = {t["name"]: t["id"] for t in api.call("GET", f"/api/database/{db['id']}/metadata")["tables"]}
    return db["id"], tables


def add_dashboards(api, db, tables):
    """Two dashboards covering native SQL features and GUI questions."""
    def card(name, query):
        return api.call("POST", "/api/card", {"name": name, "display": "table", "visualization_settings": {},
                                              "dataset_query": query})["id"]

    native = card("Revenue by category (SQL)", {
        "type": "native", "database": db,
        "native": {
            "query": "with o as (select * from ORDERS where CREATED_AT > {{since}})\n"
                     "select p.CATEGORY, sum(o.TOTAL) from o join PRODUCTS p on p.ID = o.PRODUCT_ID\n"
                     "[[where p.CATEGORY = {{category}}]] group by 1",
            "template-tags": {
                "since": {"id": "a1", "name": "since", "display-name": "Since", "type": "date"},
                "category": {"id": "a2", "name": "category", "display-name": "Category", "type": "text"},
            },
        },
    })
    joined = card("Reviews with products (GUI)", {
        "type": "query", "database": db,
        "query": {"source-table": tables["REVIEWS"],
                  "joins": [{"source-table": tables["PRODUCTS"], "alias": "P", "fields": "all",
                             "condition": ["=", ["field", "PRODUCT_ID", {"base-type": "type/Integer"}],
                                           ["field", "ID", {"base-type": "type/Integer", "join-alias": "P"}]]}]},
    })
    base = card("People (GUI)", {"type": "query", "database": db, "query": {"source-table": tables["PEOPLE"]}})
    nested = card("People by source (nested)", {
        "type": "query", "database": db,
        "query": {"source-table": f"card__{base}", "aggregation": [["count"]],
                  "breakout": [["field", "SOURCE", {"base-type": "type/Text"}]]},
    })
    for title, cards in [("Bench: native SQL", [native]), ("Bench: GUI questions", [joined, nested])]:
        d = api.call("POST", "/api/dashboard", {"name": title})
        api.call("PUT", f"/api/dashboard/{d['id']}", {
            "dashcards": [{"id": -(i + 1), "card_id": c, "row": i * 4, "col": 0, "size_x": 12, "size_y": 4}
                          for i, c in enumerate(cards)]})


def card_tables(api, query, table_names, seen=()):
    """Tables of one card's dataset_query (legacy MBQL, pMBQL stages, native)."""
    if query.get("type") == "native" or "native" in query:
        sql = query["native"]["query"]
        sql = re.sub(r"\[\[(.*?)\]\]", r"\1", sql, flags=re.S)
        sql = re.sub(r"\{\{[^}]*\}\}", "1", sql)
        return oracle.sql_tables(sql) or set()
    stages = query.get("stages") or [query.get("query", {})]
    out = set()
    for stage in stages[:1]:
        if stage.get("native"):
            out |= oracle.sql_tables(re.sub(r"\{\{[^}]*\}\}", "1", stage["native"])) or set()
        sources = [stage.get("source-table"), stage.get("source-card")]
        sources += [j.get("source-table") for j in stage.get("joins", [])]
        for j in stage.get("joins", []):
            if j.get("stages"):
                sources.append(j["stages"][0].get("source-table"))
        for src in sources:
            if isinstance(src, int) and src in table_names:
                out.add(table_names[src].lower())
            elif isinstance(src, str) and src.startswith("card__"):
                cid = int(src.removeprefix("card__"))
                if cid not in seen:
                    c = api.call("GET", f"/api/card/{cid}")
                    out |= card_tables(api, c["dataset_query"], table_names, seen + (cid,))
            elif isinstance(stage.get("source-card"), int) and src == stage.get("source-card"):
                c = api.call("GET", f"/api/card/{src}")
                out |= card_tables(api, c["dataset_query"], table_names, seen + (src,))
        if isinstance(stage.get("source-query"), dict):
            out |= card_tables(api, {"query": stage["source-query"]}, table_names, seen)
    return out


def truth(api):
    table_names = {}
    for db in (api.call("GET", "/api/database").get("data") or []):
        for t in api.call("GET", f"/api/database/{db['id']}/metadata")["tables"]:
            table_names[t["id"]] = t["name"]
    out = {}
    for d in api.call("GET", "/api/dashboard"):
        if d.get("archived"):
            continue
        full = api.call("GET", f"/api/dashboard/{d['id']}")
        tables = set()
        for dc in full.get("dashcards", []):
            if dc.get("card") and dc["card"].get("dataset_query"):
                tables |= card_tables(api, dc["card"]["dataset_query"], table_names)
        out[d["name"]] = tables
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:3999")
    ap.add_argument("--binary", default=str(bench.ROOT / "target" / "release" / "dbt-lineage"))
    args = ap.parse_args()
    api = Api(args.url)
    wait_healthy(api)
    key = setup(api)
    db, tables = sample_db(api)
    if not any(d["name"].startswith("Bench:") for d in api.call("GET", "/api/dashboard")):
        add_dashboards(api, db, tables)
    expected = truth(api)
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp) / "bench"
        models = bench.fixture({t for ts in expected.values() for t in ts}, project)
        import subprocess
        cmd = [args.binary, "exposures", "metabase", "--url", args.url, "--api-key", key, "-p", str(project), "-o", "json"]
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if proc.returncode != 0:
            raise SystemExit(proc.stderr)
        r = bench.score(expected, proc, models)
        subprocess.run(cmd[:-2] + ["--write", str(project / "models" / "exposures.yml")], capture_output=True, check=True)
        r["dbt_parse"], log = bench.dbt_parse(project)
    print(json.dumps({k: v for k, v in r.items()}, indent=2))
    return 0 if r["recall"] == 100.0 and r["precision"] == 100.0 and r["dbt_parse"] else 1


if __name__ == "__main__":
    sys.exit(main())
