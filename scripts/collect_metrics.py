#!/usr/bin/env python3
"""Append one JSON line of adoption metrics to metrics.jsonl.

Release downloads, stars/forks, crates.io and ghcr pulls work with the
default GITHUB_TOKEN; repo traffic (views, clones, referrers) needs a token
with push access in TRAFFIC_TOKEN and is skipped otherwise.
"""
import datetime
import json
import os
import sys
import urllib.request

REPO = os.environ.get("GITHUB_REPOSITORY", "sipemu/dbt-lineage-viewer")


def get(url, token=None):
    req = urllib.request.Request(url, headers={"User-Agent": "dbt-lineage-metrics"})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main(out):
    token = os.environ.get("GITHUB_TOKEN")
    row = {"date": datetime.date.today().isoformat()}

    repo = get(f"https://api.github.com/repos/{REPO}", token)
    row["stars"], row["forks"] = repo["stargazers_count"], repo["forks_count"]

    downloads = {}
    page = 1
    while True:
        releases = get(f"https://api.github.com/repos/{REPO}/releases?per_page=100&page={page}", token)
        for rel in releases:
            n = sum(a["download_count"] for a in rel["assets"] if not a["name"].endswith(".sha256"))
            downloads[rel["tag_name"]] = n
        if len(releases) < 100:
            break
        page += 1
    row["release_downloads"] = downloads
    row["release_downloads_total"] = sum(downloads.values())

    try:
        crate = get("https://crates.io/api/v1/crates/dbt-lineage")["crate"]
        row["crates_downloads"], row["crates_recent"] = crate["downloads"], crate["recent_downloads"]
    except Exception as e:  # keep the row even if one source fails
        row["crates_error"] = str(e)

    traffic = os.environ.get("TRAFFIC_TOKEN")
    if traffic:
        base = f"https://api.github.com/repos/{REPO}/traffic"
        views, clones = get(f"{base}/views", traffic), get(f"{base}/clones", traffic)
        row["views_14d"], row["views_unique_14d"] = views["count"], views["uniques"]
        row["clones_14d"], row["clones_unique_14d"] = clones["count"], clones["uniques"]
        row["referrers"] = {r["referrer"]: r["uniques"] for r in get(f"{base}/popular/referrers", traffic)}

    with open(out, "a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")
    print(json.dumps(row, indent=2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "metrics.jsonl")
