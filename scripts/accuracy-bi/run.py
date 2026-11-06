#!/usr/bin/env python3
"""Measure the BI exposure importers against public dashboards.

For every artifact set in SETS: compute the ground truth with oracle.py (an
independent parser per tool, or hand-derived JSON for Power BI), build a
fixture dbt project that declares every ground-truth table as a source (and
every third identifier-safe one also as a model, to check that a model wins
over a source), run `dbt-lineage exposures <tool>` on the artifacts, and
score:

- dashboards: found / expected, extra;
- table references: (dashboard, table) pairs found vs. expected, recall and
  precision (a table the importer reads but the project lacks shows up as
  "unmatched" on stderr and still counts);
- mapping: share of matched references pointing at the expected dbt node;
- dbt parse: whether the generated exposures YAML parses with dbt Core.

Prints a Markdown table (also appended to $GITHUB_STEP_SUMMARY when set);
exits 1 when any set falls below thresholds.json or fails to run, 2 when
the binary or the downloaded artifacts are missing.

    python3 scripts/accuracy-bi/fetch.py
    uv run --no-project --with sqlglot --with pyyaml --with lkml \\
        python scripts/accuracy-bi/run.py [--binary target/release/dbt-lineage] [--no-dbt]
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
WORK = ROOT / ".accuracy" / "bi"
sys.path.insert(0, str(HERE))
import oracle  # noqa: E402

# (label, tool, importer flag, input paths relative to WORK, oracle)
SETS = [
    ("Superset examples (3.1.0)", "superset", "--exports",
     ["superset/superset/examples/configs"], lambda: oracle.superset(WORK / "superset/superset/examples/configs")),
    ("Tableau: document-api samples", "tableau", "--workbooks",
     ["tableau-document-api"], lambda: oracle.tableau(WORK / "tableau-document-api")),
    ("Tableau: public workbooks", "tableau", "--workbooks",
     ["tableau-recfluence", "tableau-kylin", "tableau-impala", "tableau-palette", "tableau-tdvt",
      "tableau-bayareametro"],
     lambda: {k: v for d in ["tableau-recfluence", "tableau-kylin", "tableau-impala", "tableau-palette",
                             "tableau-tdvt", "tableau-bayareametro"]
              for k, v in oracle.tableau(WORK / d).items()}),
    ("Looker: snowflake-usage-block", "looker", "--lookml",
     ["looker-snowflake-usage"], lambda: oracle.looker(WORK / "looker-snowflake-usage")),
    ("Looker: google_ga360", "looker", "--lookml",
     ["looker-ga360"], lambda: oracle.looker(WORK / "looker-ga360")),
    ("Looker: blocks_redshift_admin", "looker", "--lookml",
     ["looker-redshift-admin"], lambda: oracle.looker(WORK / "looker-redshift-admin")),
    ("Power BI: PBIP (Databricks accelerator)", "powerbi", "--reports",
     ["powerbi-databricks-accelerator/samples-pbip"],
     lambda: oracle.powerbi("powerbi-databricks-accelerator/samples-pbip")),
    ("Power BI: .pbit (Databricks accelerator)", "powerbi", "--reports",
     ["powerbi-databricks-accelerator/samples-pbit"],
     lambda: oracle.powerbi("powerbi-databricks-accelerator/samples-pbit")),
    ("Power BI: CSV/OData reports (pbip-demo)", "powerbi", "--reports",
     ["powerbi-pbip-demo/src"], lambda: oracle.powerbi("powerbi-pbip-demo/src")),
    ("anofox-visualization examples", "anofox", "--dashboards",
     ["anofox-visualization/dashboards", "anofox-visualization/examples"],
     lambda: {**oracle.anofox(WORK / "anofox-visualization")}),
]

UNMATCHED = re.compile(r"^dbt-lineage: '(.*)' reads '(.*)', which matches no model or source$")
SKIPPED = re.compile(r"^dbt-lineage: skipped '(.*)' \(no table matches the project\)$")
DEP = re.compile(r"^(ref|source)\('([^']*)'(?:, '([^']*)')?\)$")
SAFE = re.compile(r"^[a-z_][a-z0-9_]*$")


def fixture(tables, dest):
    """A dbt project with every table as a source; every third safe one is a model too."""
    tables = sorted(tables)
    models = {t for i, t in enumerate(t for t in tables if SAFE.match(t)) if i % 3 == 0}
    (dest / "models").mkdir(parents=True)
    (dest / "dbt_project.yml").write_text(
        "name: bench\nversion: '1.0'\nconfig-version: 2\nprofile: bench\nmodel-paths: [models]\n"
    )
    source_tables = [{"name": t} for t in tables] or [{"name": "placeholder"}]
    (dest / "models" / "sources.yml").write_text(
        json.dumps({"version": 2, "sources": [{"name": "bench", "tables": source_tables}]})
    )
    for t in models:
        (dest / "models" / f"{t}.sql").write_text("select 1 as id\n")
    return models


def run_importer(binary, tool, flag, inputs, project, write=None):
    cmd = [binary, "exposures", tool, flag, *[str(WORK / i) for i in inputs], "-p", str(project), "-o", "json"]
    if write:
        cmd += ["--write", str(write)]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{proc.stderr}")
    return proc


def score(truth, proc, models):
    exposures = json.loads(proc.stdout) if proc.stdout.strip() else []
    found_pairs, mapped_ok, mapped = set(), 0, 0
    dashboards = set()
    for e in exposures:
        dashboards.add(e["label"])
        for dep in e["depends_on"]:
            m = DEP.match(dep)
            if not m:
                continue
            table = (m.group(2) if m.group(1) == "ref" else m.group(3)).lower()
            found_pairs.add((e["label"], table))
            mapped += 1
            expected_kind = "ref" if table in models else "source"
            mapped_ok += m.group(1) == expected_kind
    for line in proc.stderr.splitlines():
        if m := UNMATCHED.match(line):
            found_pairs.add((m.group(1), oracle.norm(m.group(2))))
            dashboards.add(m.group(1))
        elif m := SKIPPED.match(line):
            dashboards.add(m.group(1))
    truth_pairs = {(d, t) for d, ts in truth.items() for t in ts}
    hit = truth_pairs & found_pairs
    return {
        "dashboards": len(truth),
        "dashboards_found": len(set(truth) & dashboards),
        "dashboards_extra": sorted(dashboards - set(truth)),
        "dashboards_missed": sorted(set(truth) - dashboards),
        "refs": len(truth_pairs),
        "refs_found": len(hit),
        "refs_extra": sorted(found_pairs - truth_pairs),
        "refs_missed": sorted(truth_pairs - found_pairs),
        # None where there is nothing to score (no expected / found references).
        "recall": round(100 * len(hit) / len(truth_pairs), 1) if truth_pairs else None,
        "precision": round(100 * len(hit) / len(found_pairs), 1) if found_pairs else None,
        "mapping": round(100 * mapped_ok / mapped, 1) if mapped else None,
    }


def dbt_parse(project):
    profiles = project / "profiles"
    profiles.mkdir(exist_ok=True)
    (profiles / "profiles.yml").write_text(
        f"bench:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {project}/bench.duckdb\n"
    )
    proc = subprocess.run(
        ["uvx", "--quiet", "--from", "dbt-core==1.12.6", "--with", "dbt-duckdb", "dbt", "parse",
         "--profiles-dir", str(profiles), "--project-dir", str(project), "--no-version-check"],
        text=True, capture_output=True,
    )
    return proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]


def check(label, r, thresholds):
    """Failure messages for one set's result against thresholds.json."""
    limit = thresholds.get(label, {})
    failures = [f"{label}: {key} {r[key]} < {limit[key]}"
                for key in ("recall", "precision", "mapping")
                if key in limit and r.get(key) is not None and r[key] < limit[key]]
    if r.get("dbt_parse") is False:
        failures.append(f"{label}: generated YAML fails dbt parse")
    return failures


def table(results):
    """The Markdown table docs/BI.md shows (one row per set)."""
    pct = lambda v: "–" if v is None else f"{v:.0f}%"  # noqa: E731
    lines = ["| Artifacts | Dashboards found | Table references found | Precision | Mapped to the right node | dbt parse |",
             "| --- | ---: | ---: | ---: | ---: | :---: |"]
    for label, r in results.items():
        if "error" in r:
            lines.append(f"| {label} | error: {r['error'].splitlines()[0][:120]} | | | | ✗ |")
            continue
        parse = {True: "✓", False: "✗"}.get(r.get("dbt_parse"), "–")
        lines.append(f"| {label} | {r['dashboards_found']} / {r['dashboards']} | {r['refs_found']} / {r['refs']} "
                     f"({pct(r['recall'])}) | {pct(r['precision'])} | {pct(r['mapping'])} | {parse} |")
    return "\n".join(lines)


def report(title, results, failures, note=None):
    """Print the table and failures; append them to $GITHUB_STEP_SUMMARY in CI."""
    md = table(results)
    print(md)
    for f in failures:
        print("failure:", f, file=sys.stderr)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(f"## {title}\n\n{md}\n\n")
            if note:
                f.write(note + "\n\n")
            if failures:
                f.write("**Below thresholds.json:**\n\n" + "".join(f"- {x}\n" for x in failures) + "\n")
            else:
                f.write("All sets meet thresholds.json.\n\n")


def main():
    ap = argparse.ArgumentParser(description="Measure BI exposure importers.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--no-dbt", action="store_true", help="skip the dbt parse check")
    ap.add_argument("--json", help="write the detailed results here")
    args = ap.parse_args()

    if not Path(args.binary).is_file():
        print(f"error: {args.binary} not found; run `cargo build --release` first", file=sys.stderr)
        return 2
    missing = sorted({i.split("/")[0] for _, _, _, inputs, _ in SETS for i in inputs
                      if not (WORK / i.split("/")[0] / ".commit").exists()})
    if missing:
        print(f"error: artifacts missing in {WORK} ({', '.join(missing)}); "
              "run `python3 scripts/accuracy-bi/fetch.py` first", file=sys.stderr)
        return 2

    thresholds = json.loads((HERE / "thresholds.json").read_text())
    results, failures = {}, []
    for label, tool, flag, inputs, compute in SETS:
        try:
            truth = compute()
            with tempfile.TemporaryDirectory() as tmp:
                project = Path(tmp) / "bench"
                models = fixture({t for ts in truth.values() for t in ts}, project)
                r = score(truth, run_importer(args.binary, tool, flag, inputs, project), models)
                if not args.no_dbt:
                    run_importer(args.binary, tool, flag, inputs, project, project / "models" / "exposures.yml")
                    r["dbt_parse"], log = dbt_parse(project)
                    if not r["dbt_parse"]:
                        print(f"{label}: dbt parse failed\n{log}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001 - one broken set must not hide the others
            r = {"error": str(e) or type(e).__name__}
            failures.append(f"{label}: {r['error'].splitlines()[0]}")
            print(f"{label}: {e}", file=sys.stderr)
        results[label] = r
        failures += check(label, r, thresholds)

    report("BI exposure importers vs. public dashboards", results, failures)
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
