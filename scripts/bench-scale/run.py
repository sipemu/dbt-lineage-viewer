#!/usr/bin/env python3
"""Time dbt-lineage on large generated dbt projects.

For each size, generates the project with generate.py (once; reused while it
exists) and measures wall time and peak RSS (from the child's rusage) of:

- graph build, cold (`summary --no-cache`) and warm (`summary`, cache primed)
- `lint`
- `column-lineage`
- `ci report --base HEAD` with one model file changed
- `site`
- LSP: initialize, open one model, until its diagnostics arrive

Prints a Markdown table. Two gates:

- --max-scaling F: exits 1 when an operation's time grows more than F times
  faster than the project (time at the largest size / time at the smallest >
  F x model ratio). Hardware-independent, so CI uses it: it catches
  super-linear regressions on any runner.
- --baseline FILE: exits 1 when any wall time exceeds --max-ratio (default 2)
  times the baseline's. Only meaningful on the machine that wrote the
  baseline (--write-baseline FILE).

    python3 scripts/bench-scale/run.py --sizes 2000,5000,10000,20000
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def measure(cmd, cwd=None, stdin=None):
    """(wall seconds, peak RSS in MiB) of one command; output discarded."""
    start = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=cwd, stdin=stdin, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    _, status, usage = os.wait4(proc.pid, 0)
    wall = time.perf_counter() - start
    proc.returncode = os.waitstatus_to_exitcode(status)
    return wall, usage.ru_maxrss / 1024, proc.returncode


def lsp_startup(binary, project, model_file):
    """Initialize, open one model, wait for its publishDiagnostics."""
    def frame(msg):
        body = json.dumps(msg).encode()
        return b"Content-Length: %d\r\n\r\n" % len(body) + body

    def read_msg(out):
        length = 0
        while True:
            line = out.readline()
            if not line:
                return None
            if line in (b"\r\n", b"\n"):
                break
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":")[1])
        return json.loads(out.read(length))

    uri = Path(model_file).resolve().as_uri()
    start = time.perf_counter()
    proc = subprocess.Popen([binary, "lsp"], cwd=project, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    proc.stdin.write(frame({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "processId": None, "rootUri": Path(project).resolve().as_uri(), "capabilities": {}}}))
    proc.stdin.write(frame({"jsonrpc": "2.0", "method": "initialized", "params": {}}))
    proc.stdin.write(frame({"jsonrpc": "2.0", "method": "textDocument/didOpen", "params": {
        "textDocument": {"uri": uri, "languageId": "sql", "version": 1,
                         "text": Path(model_file).read_text()}}}))
    proc.stdin.flush()
    while True:
        msg = read_msg(proc.stdout)
        if msg is None or msg.get("method") == "textDocument/publishDiagnostics":
            break
    wall = time.perf_counter() - start
    proc.stdin.write(frame({"jsonrpc": "2.0", "id": 2, "method": "shutdown"}))
    proc.stdin.write(frame({"jsonrpc": "2.0", "method": "exit"}))
    proc.stdin.close()
    _, _, usage = os.wait4(proc.pid, 0)
    return wall, usage.ru_maxrss / 1024, 0 if msg else 1


def bench(binary, project):
    p = str(project)
    rows = {}
    rows["build_cold"] = measure([binary, "summary", "-p", p, "--no-cache"])
    measure([binary, "summary", "-p", p])  # prime the cache
    rows["build_warm"] = measure([binary, "summary", "-p", p])
    rows["lint"] = measure([binary, "lint", "-p", p])
    rows["column_lineage"] = measure([binary, "column-lineage", "-p", p])

    model = next(f for f in sorted((project / "models").rglob("*.sql")))
    original = model.read_text()
    model.write_text(original + "\n-- bench change\n")
    try:
        rows["ci_report"] = measure([binary, "ci", "report", "-p", p, "--base", "HEAD",
                                     "--no-lint", "--fail-on", "never", "--out", os.devnull])
    finally:
        model.write_text(original)

    site = Path(tempfile.mkdtemp(prefix="bench-site-"))
    try:
        rows["site"] = measure([binary, "site", "-p", p, "--out", str(site)])
    finally:
        shutil.rmtree(site, ignore_errors=True)
    # Builds without the `lsp` feature (CI uses --no-default-features) exit
    # at once; there is no server to time then.
    try:
        rows["lsp_startup"] = lsp_startup(binary, project, model)
    except (BrokenPipeError, OSError):
        pass  # binary built without the `lsp` feature
    return rows


LABELS = {
    "build_cold": "graph build, cold",
    "build_warm": "graph build, cached",
    "lint": "lint",
    "column_lineage": "column lineage",
    "ci_report": "ci report (1 file changed)",
    "site": "static site",
    "lsp_startup": "LSP: open file → diagnostics",
}


def main():
    ap = argparse.ArgumentParser(description="Time dbt-lineage on large generated projects.")
    ap.add_argument("--binary", default=str(ROOT / "target" / "release" / "dbt-lineage"))
    ap.add_argument("--sizes", default="2000,5000,10000,20000")
    ap.add_argument("--work-dir", default=str(ROOT / ".accuracy" / "bench"))
    ap.add_argument("--src", default=str(ROOT / ".accuracy" / "models"))
    ap.add_argument("--baseline")
    ap.add_argument("--write-baseline")
    ap.add_argument("--max-ratio", type=float, default=2.0)
    ap.add_argument("--max-scaling", type=float)
    args = ap.parse_args()

    work = Path(args.work_dir)
    results = {}
    for size in [int(s) for s in args.sizes.split(",")]:
        project = work / f"{size // 1000}k"
        if not (project / "bench.json").exists():
            subprocess.run([sys.executable, str(HERE / "generate.py"), "--models", str(size),
                            "--src", args.src, "--out", str(project)], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        models = json.loads((project / "bench.json").read_text())["models"]
        print(f"benchmarking {models} models", file=sys.stderr, flush=True)
        results[str(models)] = bench(args.binary, project)

    sizes = list(results)
    lines = ["| Operation | " + " | ".join(f"{int(s):,} models" for s in sizes) + " |",
             "| --- |" + " ---: |" * len(sizes)]
    for key, label in LABELS.items():
        cells = []
        if any(key not in results[s] for s in sizes):
            continue
        for s in sizes:
            wall, rss, rc = results[s][key]
            cells.append(f"{wall:.2f} s · {rss:,.0f} MiB" + ("" if rc == 0 else " (exit %d)" % rc))
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    md = "\n".join(lines)
    print(md)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write("## dbt-lineage at scale\n\n" + md + "\n")

    if args.write_baseline:
        Path(args.write_baseline).write_text(json.dumps(
            {s: {k: round(v[0], 3) for k, v in r.items()} for s, r in results.items()},
            indent=2) + "\n")
    failed = False
    if args.max_scaling and len(sizes) > 1:
        small, large = sizes[0], sizes[-1]
        model_ratio = int(large) / int(small)
        for key in LABELS:
            # lint exits 1 on error findings; a missing command (lsp in a
            # --no-default-features build) exits 2 within milliseconds.
            if any(key not in results[s] for s in (small, large)):
                continue
            if any(results[s][key][2] not in (0, 1) for s in (small, large)) or (
                key == "lsp_startup" and results[small][key][2] != 0
            ):
                continue
            ratio = results[large][key][0] / max(results[small][key][0], 0.01)
            if ratio > args.max_scaling * model_ratio:
                print(f"super-linear: {key} grew {ratio:.1f}x for {model_ratio:.1f}x models",
                      file=sys.stderr)
                failed = True
    if args.baseline:
        base = json.loads(Path(args.baseline).read_text())
        slow = [f"{k} at {s} models: {results[s][k][0]:.2f}s vs {base[s][k]:.2f}s"
                for s in results if s in base for k in results[s]
                if k in base[s] and results[s][k][0] > args.max_ratio * max(base[s][k], 0.05)]
        for line in slow:
            print("regression:", line, file=sys.stderr)
        failed |= bool(slow)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
