#!/usr/bin/env python3
"""Metabase part of the BI importer accuracy check (needs Docker).

Either run Metabase with its sample database first, e.g.

    docker run -d --name dbtl-metabase-bench -p 3999:3000 metabase/metabase:v0.56.4

or pass --docker to have this script start a fresh container (removed again
at the end; this is what CI does). The script then sets the instance up through the API (admin user, API key),
adds two dashboards on top of Metabase's example dashboard (native SQL with a
template tag, an optional clause and a CTE; a GUI question with a join; a
question built on another question), computes the ground truth from the API's
card definitions independently of dbt-lineage (sqlglot for native SQL), runs
`dbt-lineage exposures metabase` and scores it like run.py against the
"Metabase v0.56 (Docker)" entry of thresholds.json. Prints the table row
(also appended to $GITHUB_STEP_SUMMARY when set); exits 1 below the
thresholds or on any error, 2 when the binary or Docker is missing or
Metabase does not come up.

    uv run --no-project --with sqlglot --with pyyaml --with lkml \\
        python scripts/accuracy-bi/metabase.py [--docker | --url http://localhost:3999]
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import oracle  # noqa: E402
import run as bench  # noqa: E402

EMAIL, PASSWORD = "bench@example.com", "Bench-pass-2026!"
IMAGE = "metabase/metabase:v0.56.4"
LABEL = "Metabase v0.56 (Docker)"
CONTAINER = "dbtl-metabase-bench-ci"


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
    print(f"error: Metabase at {api.url} did not become healthy within {timeout}s", file=sys.stderr)
    raise SystemExit(2)


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


def start_container(port):
    if not shutil.which("docker"):
        print("error: --docker needs the docker CLI", file=sys.stderr)
        raise SystemExit(2)
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    print(f"+ docker run {IMAGE} on port {port}", file=sys.stderr, flush=True)
    subprocess.run(["docker", "run", "-d", "--name", CONTAINER, "-p", f"{port}:3000", IMAGE],
                   check=True, stdout=subprocess.DEVNULL)


def measure(api, binary):
    start = time.time()
    wait_healthy(api)
    print(f"Metabase healthy after {time.time() - start:.0f}s", file=sys.stderr, flush=True)
    key = setup(api)
    db, tables = sample_db(api)
    if not any(d["name"].startswith("Bench:") for d in api.call("GET", "/api/dashboard")):
        add_dashboards(api, db, tables)
    expected = truth(api)
    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp) / "bench"
        models = bench.fixture({t for ts in expected.values() for t in ts}, project)
        cmd = [binary, "exposures", "metabase", "--url", api.url, "--api-key", key, "-p", str(project), "-o", "json"]
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if proc.returncode != 0:
            raise RuntimeError(f"dbt-lineage exposures metabase failed:\n{proc.stderr}")
        r = bench.score(expected, proc, models)
        subprocess.run(cmd[:-2] + ["--write", str(project / "models" / "exposures.yml")],
                       capture_output=True, check=True)
        r["dbt_parse"], log = bench.dbt_parse(project)
        if not r["dbt_parse"]:
            print(f"{LABEL}: dbt parse failed\n{log}", file=sys.stderr)
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:3999")
    ap.add_argument("--docker", action="store_true",
                    help=f"start {IMAGE} on the --url port first and remove it afterwards")
    ap.add_argument("--binary", default=str(bench.ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--json", help="write the detailed result here")
    args = ap.parse_args()
    if not Path(args.binary).is_file():
        print(f"error: {args.binary} not found; run `cargo build --release` first", file=sys.stderr)
        return 2
    api = Api(args.url)
    if args.docker:
        start_container(urllib.parse.urlparse(args.url).port or 3999)
    try:
        r = measure(api, args.binary)
    except Exception as e:  # noqa: BLE001 - report it in the summary, then fail
        r = {"error": str(e) or type(e).__name__}
        print(f"{LABEL}: {e}", file=sys.stderr)
    finally:
        if args.docker:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    thresholds = json.loads((HERE / "thresholds.json").read_text())
    failures = bench.check(LABEL, r, thresholds)
    if "error" in r:
        failures.append(f"{LABEL}: {r['error'].splitlines()[0]}")
    bench.report("BI exposure importers: Metabase", {LABEL: r}, failures)
    if args.json:
        Path(args.json).write_text(json.dumps(r, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
