#!/usr/bin/env python3
"""Download the public BI artifacts the accuracy check reads.

Each source in sources.json is a GitHub repository pinned to a commit; only
the listed paths are extracted, into .accuracy/bi/<slug>/. Re-running skips
sources already present at the pinned commit.

    python3 scripts/accuracy-bi/fetch.py
"""

import io
import json
import sys
import tarfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
WORK = ROOT / ".accuracy" / "bi"


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
            print(f"+ {url}", file=sys.stderr, flush=True)
            out = dest / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(urllib.request.urlopen(url, timeout=300).read())
        stamp.write_text(src["commit"])
        return dest
    url = f"https://codeload.github.com/{src['repo']}/tar.gz/{src['commit']}"
    print(f"+ {url}", file=sys.stderr, flush=True)
    data = urllib.request.urlopen(url, timeout=300).read()
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
    for src in sources:
        fetch(src)
    return 0


if __name__ == "__main__":
    sys.exit(main())
