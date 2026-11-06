#!/usr/bin/env python3
"""Measure the rebuild selector (`dbt-lineage plan`) against dbt's own.

For each project in ../accuracy/projects.json, walk back through its real
history from the pinned commit and take the last N non-merge commits that
touch the project's files. For each commit C with parent P:

- dbt's answer: `dbt parse` at P and at C (dbt Core + DuckDB adapter, no
  warehouse), then on C `dbt ls -s state:modified+ --state <P's target>`.
- dbt-lineage's answer: `dbt-lineage plan --base P --head C`, its selector
  expanded to models by `dbt ls -s "<selector>"` on C, so both sides use
  dbt's own `+` semantics and only the choice of changed nodes differs.

Both are restricted to models (`--resource-type model`). Prints recall and
precision of the selected model sets per project, lists every mismatching
commit with the files it changed, and exits 1 when a number drops more than
0.5 points below baseline.json (`--update-baseline` rewrites it).

    python3 scripts/accuracy-plan/run.py [--binary path] [--commits 12]

Needs git and uv (`uvx`) on PATH. Slow (two `dbt parse` per commit), so CI
runs it weekly.
"""

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
TOLERANCE = 0.5

# Profile helpers from the model-accuracy script.
_spec = importlib.util.spec_from_file_location("model_accuracy", HERE.parent / "accuracy" / "run.py")
_model_accuracy = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_model_accuracy)
profile_name, run, write_profiles = (
    _model_accuracy.profile_name,
    _model_accuracy.run,
    _model_accuracy.write_profiles,
)


def git(args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, text=True, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout


def clone(repo, commit, dest):
    """Clone with history (blobs fetched on demand) and check out `commit`."""
    if not (dest / ".git").exists():
        run(["git", "clone", "-q", "--filter=blob:none", "--no-checkout", repo, str(dest)])
    try:
        git(["cat-file", "-e", f"{commit}^{{commit}}"], dest)
    except subprocess.CalledProcessError:
        run(["git", "fetch", "-q", "origin"], cwd=dest)
    run(["git", "checkout", "-q", "--force", "--detach", commit], cwd=dest)


def commits(repo_dir, pinned, paths, n):
    """Last `n` first-parent, non-merge commits up to `pinned` touching `paths`."""
    out = git(["log", "--first-parent", "--no-merges", "--format=%H", f"-n{n}",
               pinned, "--", *paths], repo_dir)
    return out.split()


class Parser:
    """`dbt parse` per commit, cached by SHA; returns the checkout dir."""

    def __init__(self, repo_dir, work, project, dbt):
        self.repo_dir, self.work, self.project, self.dbt = repo_dir, work, project, dbt
        slug = work.name
        self.profiles = work / "profiles"
        self.profiles.mkdir(parents=True, exist_ok=True)
        self.db = work / f"{slug}.duckdb"

    def checkout(self, sha):
        tree = self.work / "trees" / sha[:12]
        if not tree.exists():
            # Sparse: only the dbt project (monorepos carry much more).
            run(["git", "worktree", "add", "-q", "--force", "--no-checkout", "--detach",
                 str(tree), sha], cwd=self.repo_dir)
            dirs = sorted({self.project["parse_dir"], self.project["lineage_dir"]} - {"."})
            if dirs and "." not in {self.project["parse_dir"], self.project["lineage_dir"]}:
                run(["git", "sparse-checkout", "set", "--cone", *dirs], cwd=tree)
            run(["git", "checkout", "-q", "--force", sha], cwd=tree)
        return tree

    def parse(self, sha):
        tree = self.checkout(sha)
        parse_dir = tree / self.project["parse_dir"]
        manifest = parse_dir / "target" / "manifest.json"
        if manifest.exists():
            return tree
        write_profiles(self.profiles / "profiles.yml", profile_name(parse_dir), self.db)
        flags = ["--profiles-dir", str(self.profiles), "--no-version-check"]
        try:
            run(self.dbt + ["deps"] + flags, cwd=parse_dir)
            run(self.dbt + ["parse"] + flags, cwd=parse_dir)
        except subprocess.CalledProcessError:
            return None
        return tree

    def ls(self, tree, selector, state=None):
        parse_dir = tree / self.project["parse_dir"]
        cmd = self.dbt + ["-q", "ls", "-s", selector, "--resource-type", "model",
                          "--output", "name", "--profiles-dir", str(self.profiles),
                          "--no-version-check"]
        if state:
            cmd += ["--state", str(state)]
        res = subprocess.run(cmd, cwd=parse_dir, text=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode != 0:
            raise RuntimeError(res.stderr.strip()[-500:] or res.stdout.strip()[-500:])
        return {line.strip() for line in res.stdout.splitlines()
                if line.strip() and not line.startswith(("\x1b", "Warning"))}


def plan_selector(binary, lineage_dir, base, head):
    res = subprocess.run(
        [binary, "plan", "--base", base, "--head", head, "-p", str(lineage_dir), "-o", "json"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip()[-500:])
    return json.loads(res.stdout)["selectors"]


def main():
    ap = argparse.ArgumentParser(description="Measure the rebuild selector against dbt.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--work-dir", default=str(ROOT / ".accuracy" / "plan"))
    ap.add_argument("--commits", type=int, default=12, help="commits per project")
    ap.add_argument("--update-baseline", action="store_true")
    ap.add_argument("--json", help="also write per-commit results to this file")
    ap.add_argument("--project", action="append", help="only these projects (by name)")
    args = ap.parse_args()

    config = json.loads((HERE.parent / "accuracy" / "projects.json").read_text())
    work = Path(args.work_dir).resolve()
    dbt = ["uvx", "--quiet", "--from", f"dbt-core=={config['dbt_core']}",
           "--with", "dbt-duckdb", "dbt"]
    baseline_path = HERE / "baseline.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else {}

    results, details, regressions = {}, [], []
    for p in config["projects"]:
        if args.project and p["name"] not in args.project:
            continue
        slug = re.sub(r"[^\w-]+", "-", p["name"]).strip("-").lower()
        pwork = work / slug
        repo_dir = pwork / "repo"
        pwork.mkdir(parents=True, exist_ok=True)
        clone(p["repo"], p["commit"], repo_dir)
        parser = Parser(repo_dir, pwork, p, dbt)
        # Commits touching the dbt project (models, macros, YAML, config).
        paths = sorted({p["lineage_dir"], p["parse_dir"]})
        tp = fp = fn = exact = measured = 0
        for sha in commits(repo_dir, p["commit"], paths, args.commits):
            parent = git(["rev-parse", f"{sha}^"], repo_dir).strip()
            base_tree, head_tree = parser.parse(parent), parser.parse(sha)
            if base_tree is None or head_tree is None:
                details.append({"project": p["name"], "commit": sha, "skipped": "dbt parse failed"})
                continue
            state = base_tree / p["parse_dir"] / "target"
            try:
                truth = parser.ls(head_tree, "state:modified+", state=state)
                selector = plan_selector(args.binary, repo_dir / p["lineage_dir"], parent, sha)
                ours = parser.ls(head_tree, " ".join(selector)) if selector else set()
            except RuntimeError as e:
                details.append({"project": p["name"], "commit": sha, "skipped": str(e)})
                continue
            measured += 1
            tp += len(truth & ours)
            fp += len(ours - truth)
            fn += len(truth - ours)
            exact += truth == ours
            if truth != ours:
                changed = git(["diff", "--name-only", parent, sha, "--", *paths], repo_dir).split()
                details.append({
                    "project": p["name"], "commit": sha,
                    "missed": sorted(truth - ours), "extra": sorted(ours - truth),
                    "selector": selector, "changed_files": changed,
                })
        recall = round(100 * tp / max(tp + fn, 1), 1)
        precision = round(100 * tp / max(tp + fp, 1), 1)
        results[p["name"]] = {"commits": measured, "exact": exact, "selected": tp + fn,
                              "recall": recall, "precision": precision}
        base = baseline.get(p["name"], {})
        for metric in ("recall", "precision"):
            if metric in base and results[p["name"]][metric] < base[metric] - TOLERANCE:
                regressions.append(f"{p['name']} {metric}: {results[p['name']][metric]} < {base[metric]}")

    lines = [
        "| Project | Commits | Same selection as dbt | Models dbt selects | Recall | Precision |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, r in results.items():
        lines.append(f"| {name} | {r['commits']} | {r['exact']} | {r['selected']:,} "
                     f"| {r['recall']}% | {r['precision']}% |")
    md = "\n".join(lines)
    print(md)
    for d in details:
        if "skipped" in d:
            print(f"\n{d['project']} {d['commit'][:7]}: skipped ({d['skipped'][:200]})", file=sys.stderr)
            continue
        print(f"\n{d['project']} {d['commit'][:7]}: missed {d['missed'][:10]} extra {d['extra'][:10]}"
              f"\n  selector: {' '.join(d['selector'])[:300]}\n  changed: {d['changed_files'][:12]}",
              file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps({"results": results, "details": details}, indent=2))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("## Rebuild selector vs `dbt ls -s state:modified+`\n\n" + md + "\n")
    if args.update_baseline:
        baseline_path.write_text(json.dumps(
            {n: {k: r[k] for k in ("recall", "precision")} for n, r in results.items()},
            indent=2) + "\n")
        return 0
    for line in regressions:
        print("regression:", line, file=sys.stderr)
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
