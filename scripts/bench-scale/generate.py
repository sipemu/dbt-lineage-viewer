#!/usr/bin/env python3
"""Generate a large dbt project from real public projects.

Replicates the projects checked out by scripts/accuracy/run.py (cal-itp
data-infra, tuva, Fivetran dbt_hubspot and dbt_jira, jaffle-shop) into one
dbt project until it has at least --models models. Each replica gets its own
names: models, seeds, snapshots and sources are prefixed (`r3__orders`), and
every quoted string, YAML `name:` and snapshot block naming one of them is
rewritten, so refs, source() calls and macro arguments stay connected within
the replica. The SQL and Jinja are the projects' own, not synthetic.

Macros and installed packages are copied once. Macro bodies keep their
original names, so their fixed refs resolve to replica 0, which keeps the
original names. Seeds are cut to their header line (dbt-lineage reads only
the header).

    python3 scripts/bench-scale/generate.py --models 5000 --out .accuracy/bench/5k

The output is a git repository with one commit, ready for `ci report --base`.
"""

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# (checkout slug, dbt project dir inside it, packages dir inside it)
SOURCES = [
    ("cal-itp-data-infra", "warehouse", None),
    ("tuva", ".", "integration_tests/dbt_packages"),
    ("fivetran-dbt_hubspot", ".", "integration_tests/dbt_packages"),
    ("fivetran-dbt_jira", ".", "integration_tests/dbt_packages"),
    ("jaffle-shop", ".", "dbt_packages"),
]

YAML_NAME = re.compile(r"^(\s*-?\s*name:\s*)(['\"]?)([A-Za-z_][\w]*)\2(\s*(?:#.*)?)$", re.M)
QUOTED = re.compile(r"(['\"])([A-Za-z_][\w]*)\1")
SNAPSHOT = re.compile(r"(\{%-?\s*snapshot\s+)([A-Za-z_]\w*)")
SOURCE_BLOCK = re.compile(r"^sources:\s*$", re.M)


def paths(project, key, default):
    text = (project / "dbt_project.yml").read_text()
    m = re.search(rf"^{key}:\s*\[([^\]]*)\]", text, re.M)
    if not m:
        return [default]
    return [p.strip().strip("'\"") for p in m.group(1).split(",") if p.strip()]


def files(dirs, suffixes):
    for d in dirs:
        if d.is_dir():
            yield from (p for p in sorted(d.rglob("*")) if p.is_file() and p.suffix in suffixes)


def source_names(yml_files):
    """Names of sources (not tables) declared in YAML: `- name:` lines at the
    first list level under `sources:`."""
    names = set()
    for f in yml_files:
        text = f.read_text(errors="replace")
        for m in SOURCE_BLOCK.finditer(text):
            block = text[m.end():]
            indent = None
            for line in block.splitlines():
                if line and not line[0].isspace() and not line.startswith("-"):
                    break  # next top-level key
                nm = re.match(r"^(\s*)-\s*name:\s*['\"]?([A-Za-z_][\w]*)", line)
                if nm:
                    if indent is None:
                        indent = len(nm.group(1))
                    if len(nm.group(1)) == indent:
                        names.add(nm.group(2))
    return names


def collect(project):
    model_dirs = [project / p for p in paths(project, "model-paths", "models")]
    seed_dirs = [project / p for p in paths(project, "seed-paths", "seeds")]
    snap_dirs = [project / p for p in paths(project, "snapshot-paths", "snapshots")]
    models = [p for p in files(model_dirs, {".sql", ".py"})]
    seeds = [p for p in files(seed_dirs, {".csv"})]
    snapshots = [p for p in files(snap_dirs, {".sql"})]
    ymls = list(files(model_dirs + seed_dirs + snap_dirs, {".yml", ".yaml"}))
    names = {p.stem for p in models + seeds}
    for s in snapshots:
        names.update(m[1] for m in SNAPSHOT.findall(s.read_text(errors="replace")))
    return {
        "models": models, "seeds": seeds, "snapshots": snapshots, "ymls": ymls,
        "names": names, "sources": source_names(ymls),
    }


def rewrite(text, rename):
    if not rename:
        return text
    text = QUOTED.sub(lambda m: m.group(1) + rename.get(m.group(2), m.group(2)) + m.group(1), text)
    text = YAML_NAME.sub(
        lambda m: m.group(1) + m.group(2) + rename.get(m.group(3), m.group(3)) + m.group(2) + m.group(4),
        text,
    )
    return SNAPSHOT.sub(lambda m: m.group(1) + rename.get(m.group(2), m.group(2)), text)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--models", type=int, required=True, help="minimum number of models")
    ap.add_argument("--out", required=True)
    ap.add_argument("--src", default=str(ROOT / ".accuracy" / "models"),
                    help="checkouts from scripts/accuracy/run.py")
    args = ap.parse_args()

    src, out = Path(args.src), Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    (out / "models").mkdir(parents=True)
    projects = []
    for slug, sub, packages in SOURCES:
        project = src / slug / sub
        if not (project / "dbt_project.yml").exists():
            raise SystemExit(f"missing {project}: run scripts/accuracy/run.py first")
        projects.append((slug, project, packages, collect(project)))

    # Macros and packages: once.
    for slug, project, packages, _ in projects:
        for d in paths(project, "macro-paths", "macros"):
            if (project / d).is_dir():
                shutil.copytree(project / d, out / "macros" / slug / d, dirs_exist_ok=True)
        if packages and (src / slug / packages).is_dir():
            for pkg in sorted((src / slug / packages).iterdir()):
                dest = out / "dbt_packages" / pkg.name
                # Skip the project itself (a local package linking back).
                if pkg.is_symlink() or dest.exists() or pkg.name in ("the_tuva_project", "hubspot", "jira"):
                    continue
                shutil.copytree(pkg, dest, symlinks=False, ignore=shutil.ignore_patterns(".git"))

    count, replica = 0, 0
    while count < args.models:
        for slug, project, _, c in projects:
            prefix = f"r{replica}__" if replica else ""
            rename = {n: prefix + n for n in c["names"] | c["sources"]} if prefix else {}
            snapshots = set(c["snapshots"])
            for f in c["models"] + c["ymls"] + c["snapshots"]:
                rel = f.relative_to(project)
                name = rename.get(f.stem, f.stem) + f.suffix if f in c["models"] else f.name
                tree = "snapshots" if f in snapshots else "models"
                dest = out / tree / f"r{replica}" / slug / rel.parent / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(rewrite(f.read_text(errors="replace"), rename))
            for f in c["seeds"]:
                rel = f.relative_to(project)
                dest = out / "seeds" / f"r{replica}" / slug / rel.parent / (rename.get(f.stem, f.stem) + ".csv")
                dest.parent.mkdir(parents=True, exist_ok=True)
                with f.open(errors="replace") as fh:
                    dest.write_text(fh.readline())
            count += len(c["models"])
            if count >= args.models:
                break
        replica += 1

    (out / "dbt_project.yml").write_text(
        "name: bench\nversion: '1.0'\nconfig-version: 2\nprofile: bench\n"
        "model-paths: [models]\nseed-paths: [seeds]\nsnapshot-paths: [snapshots]\n"
        "macro-paths: [macros]\n"
    )
    (out / ".gitignore").write_text("target/\n.dbt-lineage/\n")
    meta = {"models": count, "replicas": replica, "sources": [s for s, *_ in SOURCES]}
    (out / "bench.json").write_text(json.dumps(meta) + "\n")
    git = ["git", "-c", "user.name=bench", "-c", "user.email=bench@example.invalid"]
    subprocess.run(["git", "init", "-q"], cwd=out, check=True)
    subprocess.run(git + ["add", "-A"], cwd=out, check=True)
    subprocess.run(git + ["commit", "-qm", "bench"], cwd=out, check=True)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()
