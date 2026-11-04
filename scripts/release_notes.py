#!/usr/bin/env python3
"""Print the CHANGELOG.md section for a version (e.g. `0.11.0` or `v0.11.0`)."""
import re
import sys

version = sys.argv[1].lstrip("v")
text = open(sys.argv[2] if len(sys.argv) > 2 else "CHANGELOG.md", encoding="utf-8").read()
match = re.search(
    rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.S | re.M
)
if not match:
    sys.exit(f"no CHANGELOG section for {version}")
body = match.group(1).strip()
print(body)
print()
print(
    "**Install:** `brew install sipemu/tap/dbt-lineage` · `cargo binstall dbt-lineage` · "
    "`docker pull ghcr.io/sipemu/dbt-lineage:" + version + "` · "
    "GitHub Action `sipemu/dbt-lineage-viewer@v" + version.split(".")[0] + "`"
)
