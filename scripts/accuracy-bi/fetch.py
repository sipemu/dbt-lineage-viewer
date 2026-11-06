#!/usr/bin/env python3
"""Download the public BI artifacts the accuracy check reads.

Each source in sources.json is a GitHub repository pinned to a commit; only
the listed paths are extracted, into .accuracy/bi/<slug>/. Re-running skips
sources already present at the pinned commit (CI caches .accuracy/bi/
keyed on sources.json). Each download is retried; exits 1 when a source
still cannot be fetched.

    python3 scripts/accuracy-bi/fetch.py
"""

import io
import json
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
WORK = ROOT / ".accuracy" / "bi"


def download(url, attempts=4):
    print(f"+ {url}", file=sys.stderr, flush=True)
    for attempt in range(1, attempts + 1):
        try:
            return urllib.request.urlopen(url, timeout=300).read()
        except OSError as e:  # URLError, HTTPError, timeouts
            if attempt == attempts:
                raise
            print(f"  attempt {attempt} failed ({e}); retrying", file=sys.stderr, flush=True)
            time.sleep(5 * attempt)


def fetch(src):
    dest = WORK / src["slug"]
    stamp = dest / ".commit"
    if stamp.exists() and stamp.read_text().strip() == src["commit"]:
        return dest
    if "files" in src:
        # Single files from a large repository: raw downloads.
        for path in src["files"]:
            url = "https://raw.githubusercontent.com/{}/{}/{}".format(
                src["repo"], src["commit"], urllib.request.quote(path)
            )
            out = dest / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(download(url))
        stamp.write_text(src["commit"])
        return dest
    url = f"https://codeload.github.com/{src['repo']}/tar.gz/{src['commit']}"
    data = download(url)
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        for member in tar.getmembers():
            # Strip the "<repo>-<sha>/" prefix GitHub adds.
            rel = member.name.split("/", 1)[1] if "/" in member.name else ""
            wanted = any(
                p == "" or rel == p or rel.startswith(p.rstrip("/") + "/") for p in src["paths"]
            )
            if not rel or not wanted:
                continue
            if member.isfile():
                out = dest / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(tar.extractfile(member).read())
    stamp.write_text(src["commit"])
    return dest


def main():
    sources = json.loads((HERE / "sources.json").read_text())
    failed = []
    for src in sources:
        try:
            fetch(src)
        except OSError as e:
            print(f"error: {src['slug']} ({src['repo']}@{src['commit'][:12]}): {e}", file=sys.stderr)
            failed.append(src["slug"])
    if failed:
        print(f"error: could not fetch {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
