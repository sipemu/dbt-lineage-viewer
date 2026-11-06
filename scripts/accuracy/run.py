#!/usr/bin/env python3
"""Measure SQL-parse lineage accuracy on public dbt projects.

For each project in projects.json: check it out at the pinned commit, run
`dbt deps` + `dbt parse` (dbt Core with the DuckDB adapter, so no warehouse is
needed), then `dbt-lineage verify` against the resulting manifest.json.

Prints a Markdown table and exits 1 if dbt-lineage missed any dependency or
model that dbt knows about. Extra dependencies (in Jinja branches switched off
by vars) are reported but don't fail the run.

    python3 scripts/accuracy/run.py                      # uses target/release/dbt-lineage
    python3 scripts/accuracy/run.py --binary path/to/dbt-lineage
    python3 scripts/accuracy/run.py --readme             # also rewrite the README table

Needs git and uv (`uvx`) on PATH.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
START, END = "<!-- accuracy:start -->", "<!-- accuracy:end -->"


def run(cmd, cwd=None):
    """Run a setup command; its output goes to stderr so stdout is only the table."""
    print("+", " ".join(str(c) for c in cmd), file=sys.stderr, flush=True)
    return subprocess.run(cmd, cwd=cwd, check=True, text=True, stdout=sys.stderr)


def checkout(repo, commit, dest):
    """Shallow checkout of one commit; reuses an existing checkout, and starts
    over when it is broken."""
    if dest.exists() and subprocess.run(
        ["git", "fsck", "--connectivity-only", "-q"], cwd=dest,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode != 0:
        shutil.rmtree(dest)
    if not dest.exists():
        dest.mkdir(parents=True)
        run(["git", "init", "-q"], cwd=dest)
        run(["git", "remote", "add", "origin", repo], cwd=dest)
    head = subprocess.run(
        ["git", "rev-parse", "-q", "--verify", "HEAD"],
        cwd=dest, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    ).stdout.strip()
    if head != commit:
        # Fetching one commit by SHA occasionally fails on GitHub ("not our
        # ref"); retry, then fall back to fetching the branches.
        for attempt in range(3):
            try:
                run(["git", "fetch", "-q", "--depth", "1", "origin", commit], cwd=dest)
                run(["git", "checkout", "-q", "--force", "FETCH_HEAD"], cwd=dest)
                return
            except subprocess.CalledProcessError:
                time.sleep(5 * (attempt + 1))
        run(["git", "fetch", "-q", "origin"], cwd=dest)
        run(["git", "checkout", "-q", "--force", commit], cwd=dest)


def profile_name(project_dir):
    text = (project_dir / "dbt_project.yml").read_text()
    m = re.search(r"^profile:\s*['\"]?([\w-]+)", text, re.M)
    return m.group(1) if m else "default"


def write_profiles(path, name, db):
    path.write_text(
        f"{name}:\n  target: dev\n  outputs:\n    dev:\n"
        f"      type: duckdb\n      path: {db}\n"
    )


def table(rows):
    out = [
        "| Project | Models compared | Dependencies found | Extra, not in manifest |",
        "| --- | ---: | ---: | ---: |",
    ]
    for p, r in rows:
        out.append(
            f"| [{p['name']}]({p['repo']}) `{p['commit'][:7]}` | {r['nodes_compared']:,} "
            f"| {r['matched_edges']:,} / {r['manifest_edges']:,} | {len(r['extra']):,} |"
        )
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="Measure SQL-parse lineage accuracy.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    # Not under target/: build caches (Swatinem/rust-cache) save and prune
    # target/, which would restore broken checkouts.
    ap.add_argument("--work-dir", default=str(ROOT / ".accuracy" / "models"))
    ap.add_argument("--readme", action="store_true", help="rewrite the README accuracy table")
    args = ap.parse_args()

    config = json.loads((HERE / "projects.json").read_text())
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    dbt = ["uvx", "--quiet", "--from", f"dbt-core=={config['dbt_core']}",
           "--with", "dbt-duckdb", "dbt"]

    rows, failed = [], []
    for p in config["projects"]:
        slug = re.sub(r"[^\w-]+", "-", p["name"]).strip("-").lower()
        repo_dir = work / slug
        checkout(p["repo"], p["commit"], repo_dir)
        parse_dir = repo_dir / p["parse_dir"]
        lineage_dir = repo_dir / p["lineage_dir"]

        profiles = work / "profiles" / slug
        profiles.mkdir(parents=True, exist_ok=True)
        write_profiles(profiles / "profiles.yml", profile_name(parse_dir), work / f"{slug}.duckdb")
        flags = ["--profiles-dir", str(profiles), "--no-version-check"]
        run(dbt + ["deps"] + flags, cwd=parse_dir)
        run(dbt + ["parse"] + flags, cwd=parse_dir)

        # A package measured through its integration_tests project: link the
        # installed packages next to its own dbt_project.yml, where `dbt deps`
        # puts them in a regular project.
        packages = lineage_dir / "dbt_packages"
        if parse_dir != lineage_dir and not packages.exists():
            packages.symlink_to(parse_dir / "dbt_packages", target_is_directory=True)

        # The parse cache is keyed on the release version, not the build:
        # bypass it so a changed parser is measured, not its predecessor.
        # verify exits 1 on a miss; the JSON is what counts here.
        verify = subprocess.run(
            [args.binary, "verify", "-p", str(lineage_dir), "--no-cache",
             "--manifest", str(parse_dir / "target" / "manifest.json"), "-o", "json"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        report = json.loads(verify.stdout)
        rows.append((p, report))
        if report["missing"] or report["only_in_manifest"]:
            failed.append((p["name"], report))

    md = table(rows)
    print(md)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("## Lineage accuracy vs `dbt parse`\n\n" + md + "\n")

    if args.readme:
        readme = ROOT / "README.md"
        text = readme.read_text()
        start, end = text.index(START) + len(START), text.index(END)
        readme.write_text(text[:start] + "\n" + md + "\n" + text[end:])

    for name, r in failed:
        print(
            f"\n{name}: missed {len(r['missing'])} dependencies and "
            f"{len(r['only_in_manifest'])} models",
            file=sys.stderr,
        )
        for m in r["missing"][:20]:
            print(f"  {m['child']} <- {m['parent']}  {m.get('file') or ''}", file=sys.stderr)
        for m in r["only_in_manifest"][:20]:
            print(f"  model not found: {m}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
