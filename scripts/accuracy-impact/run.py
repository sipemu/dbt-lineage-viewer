#!/usr/bin/env python3
"""Measure the PR comment's column impact end to end.

For each project in ../accuracy/projects.json with a `columns` entry:

1. Check it out at the pinned commit, `dbt seed` + `dbt run` + `dbt docs
   generate` on DuckDB, and build the base column lineage with
   ../accuracy-columns/reference.py (sqlglot over the compiled SQL).
2. Pick columns that other models read, one per model, and remove each from
   its model's SQL in turn (the last `col,` / `expr as col,` line of the file;
   an edit that doesn't compile or doesn't remove the column is skipped).
3. Ground truth: every downstream column whose reference lineage
   (transitively) reads the removed column.
4. dbt-lineage: the "Column impact" row for that column in
   `ci report --base HEAD`, twice:
   - raw: no dbt artifacts;
   - compiled: the edited project compiled into target/manifest.json, and
     `--base-manifest` pointing at the base build's manifest.

Prints recall/precision of the reported downstream columns per project and
mode, plus removals dbt-lineage didn't detect at all and columns it reported
as removed although they weren't. Exits 1 when a number drops more than 0.5
points below baseline.json (`--update-baseline` rewrites it).

    python3 scripts/accuracy-impact/run.py [--binary path] [--edits 8]

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
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
TOLERANCE = 0.5

_spec = importlib.util.spec_from_file_location("model_accuracy", HERE.parent / "accuracy" / "run.py")
_model_accuracy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_model_accuracy)
checkout, profile_name, run, write_profiles = (
    _model_accuracy.checkout,
    _model_accuracy.profile_name,
    _model_accuracy.run,
    _model_accuracy.write_profiles,
)

ROW = re.compile(r"^\| `([^`]+)` \| (.*) \|$")
SELECT_ITEM = re.compile(
    r"^(\s*)(?:(?P<expr>.+?)\s+as\s+)?\"?(?P<col>[A-Za-z_][A-Za-z0-9_]*)\"?\s*(?P<comma>,?)\s*$",
    re.I,
)


def reference(manifest, db):
    out = subprocess.run(
        ["uv", "run", "-q", "--no-project", "--with", "sqlglot", "--with", "duckdb", "python",
         str(HERE.parent / "accuracy-columns" / "reference.py"), str(manifest), "--duckdb", str(db)],
        text=True, stdout=subprocess.PIPE, check=True,
    ).stdout
    return json.loads(out)


def output_columns(db, sql):
    """Columns the compiled SQL returns, asked of DuckDB (`limit 0`); None when
    it doesn't run."""
    code = ("import duckdb, json, sys; con = duckdb.connect(sys.argv[1], read_only=True); "
            "print(json.dumps([c[0].lower() for c in con.execute("
            "'select * from (' + sys.stdin.read() + ') limit 0').description]))")
    res = subprocess.run(["uv", "run", "-q", "--no-project", "--with", "duckdb", "python", "-c",
                          code, str(db)], input=sql, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return json.loads(res.stdout) if res.returncode == 0 else None


def label(node_id):
    """dbt-lineage's display label: `model.x` → `x`, `source.s.t` → `s.t`."""
    return node_id.split(".", 1)[1]


def downstream(edges, start):
    """Columns reading `start` (node, column) directly or transitively."""
    children = defaultdict(set)
    for src, scol, tgt, tcol in edges:
        children[(src, scol)].add((tgt, tcol))
    seen, stack = set(), [start]
    while stack:
        for child in children[stack.pop()]:
            if child not in seen:
                seen.add(child)
                stack.append(child)
    seen.discard(start)
    return seen


def remove_column(path, col):
    """Remove the last select-list line producing `col`; returns the old text
    or None when no such line exists."""
    text = path.read_text()
    lines = text.splitlines(keepends=True)
    for i in range(len(lines) - 1, -1, -1):
        m = SELECT_ITEM.match(lines[i].rstrip("\n"))
        if not m or m.group("col").lower() != col or "{" in lines[i]:
            continue
        if m.group("expr") is None and lines[i].strip().rstrip(",").lower() != col:
            continue
        new = lines[:i] + lines[i + 1:]
        if not m.group("comma"):
            # It was the last item: drop the trailing comma of the one before.
            for j in range(i - 1, -1, -1):
                if new[j].strip():
                    if not new[j].rstrip().endswith(","):
                        return None  # the only select item
                    new[j] = new[j].rstrip()[:-1] + "\n"
                    break
        path.write_text("".join(new))
        return text
    return None


def column_impact(binary, lineage_dir, extra_args=()):
    """`ci report` Column impact rows: removed `label.col` → set of `label.col`."""
    res = subprocess.run(
        [binary, "ci", "report", "-p", str(lineage_dir), "--base", "HEAD", "--no-lint",
         "--fail-on", "never", *extra_args],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True,
    ).stdout
    rows, in_section = {}, False
    for line in res.splitlines():
        if line.startswith("### "):
            in_section = line.startswith("### Column impact")
            continue
        m = ROW.match(line) if in_section else None
        if m:
            rows[m.group(1)] = set(re.findall(r"`([^`]+)`", m.group(2)))
    return rows


def main():
    ap = argparse.ArgumentParser(description="Measure column impact end to end.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--work-dir", default=str(ROOT / ".accuracy" / "impact"))
    ap.add_argument("--edits", type=int, default=8, help="column removals per project")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--json", help="also write per-edit results to this file")
    ap.add_argument("--project", action="append", help="only these projects (by name)")
    args = ap.parse_args()

    config = json.loads((HERE.parent / "accuracy" / "projects.json").read_text())
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    dbt = ["uvx", "--quiet", "--from", f"dbt-core=={config['dbt_core']}",
           "--with", "dbt-duckdb", "dbt"]
    baseline_path = HERE / "baseline.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else {}

    results, details, regressions = {}, [], []
    for p in config["projects"]:
        if "columns" not in p or (args.project and p["name"] not in args.project):
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
        flags = ["--profiles-dir", str(profiles), "--no-version-check"]
        if p["columns"].get("vars"):
            flags += ["--vars", p["columns"]["vars"]]
        base_flags = flags + ["--target-path", "target-base"]
        run(dbt + ["deps"] + flags[:3], cwd=parse_dir)
        run(dbt + ["seed"] + base_flags, cwd=parse_dir)
        run(dbt + ["run"] + base_flags, cwd=parse_dir)
        base_manifest = parse_dir / "target-base" / "manifest.json"
        packages = lineage_dir / "dbt_packages"
        if parse_dir != lineage_dir and not packages.exists():
            packages.symlink_to(parse_dir / "dbt_packages", target_is_directory=True)

        # Measure this binary, not results another build cached.
        shutil.rmtree(lineage_dir / ".dbt-lineage", ignore_errors=True)
        ref = reference(base_manifest, db)
        edges = [tuple(e) for e in ref["edges"]]
        manifest = json.loads(base_manifest.read_text())
        files = {
            f"model.{n['name']}": lineage_dir / n["original_file_path"]
            for n in manifest["nodes"].values()
            if n["resource_type"] == "model" and n.get("original_file_path", "").endswith(".sql")
            and (lineage_dir / n["original_file_path"]).is_file()
        }
        # Candidate removals: project-model columns other models read.
        readers = defaultdict(set)
        for src, scol, tgt, tcol in edges:
            if src in files and tgt != src:
                readers[(src, scol)].add(tgt)
        candidates = sorted(readers, key=lambda k: (-len(downstream(edges, k)), k))

        totals = {m: {"tp": 0, "fp": 0, "fn": 0, "undetected": 0, "spurious": 0}
                  for m in ("raw", "compiled")}
        done_models, measured = set(), 0
        for node, col in candidates:
            if measured >= args.edits or node in done_models:
                continue
            path = files[node]
            original = remove_column(path, col)
            if original is None:
                continue
            try:
                # The edit must compile and drop the column.
                head_target = parse_dir / "target-head"
                shutil.rmtree(head_target, ignore_errors=True)
                compiled = subprocess.run(
                    dbt + ["compile"] + flags + ["--target-path", "target-head"],
                    cwd=parse_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                ).returncode == 0
                head_manifest = head_target / "manifest.json"
                if not compiled or not head_manifest.exists():
                    continue
                head_nodes = json.loads(head_manifest.read_text())["nodes"]
                head_sql = next((n.get("compiled_code") for n in head_nodes.values()
                                 if f"model.{n['name']}" == node), None)
                cols = output_columns(db, head_sql) if head_sql else None
                if cols is None or col in cols:
                    continue  # the edit broke the model, or didn't drop the column
                truth = {f"{label(t)}.{c}" for t, c in downstream(edges, (node, col))}
                key = f"{label(node)}.{col}"
                record = {"project": p["name"], "removed": key, "truth": sorted(truth)}
                for mode in ("raw", "compiled"):
                    target = lineage_dir / "target"
                    for f in ("manifest.json", "catalog.json"):
                        (target / f).unlink(missing_ok=True)
                    extra = []
                    if mode == "compiled":
                        target.mkdir(exist_ok=True)
                        shutil.copy(head_manifest, target / "manifest.json")
                        extra = ["--base-manifest", str(base_manifest)]
                    rows = column_impact(args.binary, lineage_dir, extra)
                    (target / "manifest.json").unlink(missing_ok=True)
                    ours = rows.get(key, set())
                    t = totals[mode]
                    t["tp"] += len(ours & truth)
                    t["fp"] += len(ours - truth)
                    t["fn"] += len(truth - ours)
                    t["undetected"] += key not in rows
                    # Other rows are fine when they're columns that disappear
                    # with it (passed through by `*`): those are in `truth`.
                    t["spurious"] += len(set(rows) - {key} - truth)
                    record[mode] = {"reported": sorted(ours), "missed": sorted(truth - ours),
                                    "extra": sorted(ours - truth),
                                    "other_rows": sorted(set(rows) - {key})}
                details.append(record)
                measured += 1
                done_models.add(node)
            finally:
                path.write_text(original)
        results[p["name"]] = {"edits": measured}
        for mode, t in totals.items():
            results[p["name"]][mode] = {
                "recall": round(100 * t["tp"] / max(t["tp"] + t["fn"], 1), 1),
                "precision": round(100 * t["tp"] / max(t["tp"] + t["fp"], 1), 1),
                "undetected": t["undetected"], "spurious": t["spurious"],
                "truth": t["tp"] + t["fn"],
            }
            base = baseline.get(p["name"], {}).get(mode, {})
            for metric in ("recall", "precision"):
                got = results[p["name"]][mode][metric]
                if metric in base and got < base[metric] - TOLERANCE:
                    regressions.append(f"{p['name']} {mode} {metric}: {got} < {base[metric]}")

    lines = [
        "| Project | Removals | Downstream columns | From SQL alone: recall / precision "
        "| With compiled SQL: recall / precision |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, r in results.items():
        raw, comp = r["raw"], r["compiled"]
        lines.append(f"| {name} | {r['edits']} | {raw['truth']:,} "
                     f"| {raw['recall']}% / {raw['precision']}% "
                     f"| {comp['recall']}% / {comp['precision']}% |")
    md = "\n".join(lines)
    print(md)
    for name, r in results.items():
        for mode in ("raw", "compiled"):
            m = r[mode]
            if m["undetected"] or m["spurious"]:
                print(f"{name} {mode}: {m['undetected']} removals not reported, "
                      f"{m['spurious']} other columns reported as removed", file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps({"results": results, "details": details}, indent=2))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("## Column impact vs sqlglot\n\n" + md + "\n")
    if args.update_baseline:
        baseline_path.write_text(json.dumps(
            {n: {m: {k: r[m][k] for k in ("recall", "precision")} for m in ("raw", "compiled")}
             for n, r in results.items()}, indent=2) + "\n")
        return 0
    for line in regressions:
        print("regression:", line, file=sys.stderr)
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
