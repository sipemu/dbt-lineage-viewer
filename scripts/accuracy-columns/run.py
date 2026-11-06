#!/usr/bin/env python3
"""Measure column-lineage accuracy on public dbt projects.

For each project in ../accuracy/projects.json with a `columns` entry: check
it out at the pinned commit, `dbt seed` and `dbt run` it on DuckDB (so macros
that query the warehouse compile against real tables), and build the
reference with reference.py (sqlglot over the compiled SQL). Then compare
`dbt-lineage column-lineage` against it twice:

- raw: parsing the project's SQL, as dbt-lineage does without a manifest;
- compiled: with the run's manifest.json in target/, as after `dbt compile`.

Prints a Markdown table and exits 1 when any recall or precision drops more
than 0.5 points below baseline.json (`--update-baseline` rewrites it).

    python3 scripts/accuracy-columns/run.py [--binary path/to/dbt-lineage]

Needs git and uv (`uvx`, `uv run`) on PATH.
"""

import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(HERE))
from compare import compare  # noqa: E402

# Checkout and profile helpers from the model-accuracy script (also run.py).
_spec = importlib.util.spec_from_file_location("model_accuracy", HERE.parent / "accuracy" / "run.py")
_model_accuracy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_model_accuracy)
checkout, profile_name, run, write_profiles = (
    _model_accuracy.checkout,
    _model_accuracy.profile_name,
    _model_accuracy.run,
    _model_accuracy.write_profiles,
)

TOLERANCE = 0.5


def column_lineage(binary, project_dir):
    out = subprocess.run(
        [binary, "column-lineage", "-p", str(project_dir), "--no-cache"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True,
    ).stdout
    return json.loads(out)["edges"]


def main():
    ap = argparse.ArgumentParser(description="Measure column-lineage accuracy.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--work-dir", default=str(ROOT / "target" / "accuracy-columns"))
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    config = json.loads((HERE.parent / "accuracy" / "projects.json").read_text())
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    dbt = ["uvx", "--quiet", "--from", f"dbt-core=={config['dbt_core']}",
           "--with", "dbt-duckdb", "dbt"]
    baseline_path = HERE / "baseline.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else {}

    results, regressions = {}, []
    for p in config["projects"]:
        if "columns" not in p:
            continue
        slug = re.sub(r"[^\w-]+", "-", p["name"]).strip("-").lower()
        repo_dir = work / slug
        checkout(p["repo"], p["commit"], repo_dir)
        parse_dir = repo_dir / p["parse_dir"]
        lineage_dir = repo_dir / p["lineage_dir"]

        profiles = work / "profiles" / slug
        profiles.mkdir(parents=True, exist_ok=True)
        db = work / f"{slug}.duckdb"
        db.unlink(missing_ok=True)
        write_profiles(profiles / "profiles.yml", profile_name(parse_dir), db)
        flags = ["--profiles-dir", str(profiles), "--no-version-check",
                 "--target-path", "target-columns"]
        if p["columns"].get("vars"):
            flags += ["--vars", p["columns"]["vars"]]
        run(dbt + ["deps"] + flags[:3], cwd=parse_dir)
        run(dbt + ["seed"] + flags, cwd=parse_dir)
        run(dbt + ["run"] + flags, cwd=parse_dir)
        manifest = parse_dir / "target-columns" / "manifest.json"

        packages = lineage_dir / "dbt_packages"
        if parse_dir != lineage_dir and not packages.exists():
            packages.symlink_to(parse_dir / "dbt_packages", target_is_directory=True)

        ref = json.loads(subprocess.run(
            ["uv", "run", "-q", "--no-project", "--with", "sqlglot", "--with", "duckdb", "python",
             str(HERE / "reference.py"), str(manifest), "--duckdb", str(db)],
            text=True, stdout=subprocess.PIPE, check=True,
        ).stdout)

        # raw: no compiled manifest where dbt-lineage looks for one.
        target_manifest = lineage_dir / "target" / "manifest.json"
        target_manifest.unlink(missing_ok=True)
        raw = compare(ref, column_lineage(args.binary, lineage_dir))
        target_manifest.parent.mkdir(exist_ok=True)
        shutil.copy(manifest, target_manifest)
        compiled = compare(ref, column_lineage(args.binary, lineage_dir))
        target_manifest.unlink()

        results[p["name"]] = {"raw": raw, "compiled": compiled}
        for mode, r in results[p["name"]].items():
            base = baseline.get(p["name"], {}).get(mode, {})
            for metric in ("recall", "precision"):
                if metric in base and r[metric] < base[metric] - TOLERANCE:
                    regressions.append(f"{p['name']} {mode} {metric}: {r[metric]} < {base[metric]}")

    lines = [
        "| Project | From SQL alone | Precision | With compiled SQL | Precision |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, r in results.items():
        raw, comp = r["raw"], r["compiled"]
        lines.append(
            f"| {name} | {raw['matched']:,} / {raw['ref_edges']:,} | {raw['precision']:.0f}% "
            f"| {comp['matched']:,} / {comp['ref_edges']:,} | {comp['precision']:.0f}% |"
        )
    md = "\n".join(lines)
    print(md)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("## Column-lineage accuracy vs sqlglot\n\n" + md + "\n")

    if args.update_baseline:
        baseline_path.write_text(json.dumps(
            {n: {m: {k: r[m][k] for k in ("recall", "precision")} for m in r}
             for n, r in results.items()}, indent=2) + "\n")
        return 0
    for line in regressions:
        print("regression:", line, file=sys.stderr)
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
