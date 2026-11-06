#!/usr/bin/env python3
"""Append one JSON line of adoption metrics to metrics.jsonl.

Release downloads, stars/forks, crates.io and ghcr pulls work with the
default GITHUB_TOKEN; repo traffic (views, clones, referrers) needs a token
with push access in TRAFFIC_TOKEN and is skipped otherwise.

Distribution channels: VS Code Marketplace and Open VSX installs, PyPI
(dbt-lineage-cli, via pypistats.org), the MCP Registry listing and Glama.
Glama's API needs GLAMA_API_KEY (without it only the listing is checked);
its terms require attribution and a link to the listing wherever the data
is shown. The GitHub Action has no install count of its own: it downloads
the release binary, so it is in release_downloads.
"""
import datetime
import json
import os
import sys
import urllib.request

REPO = os.environ.get("GITHUB_REPOSITORY", "sipemu/dbt-lineage-viewer")
VSCODE_EXT = "sipemu.dbt-lineage-viewer"
PYPI = "dbt-lineage-cli"
MCP_NAME = "io.github.sipemu/dbt-lineage"
GLAMA = "sipemu/dbt-lineage-viewer"


def get(url, token=None, data=None, headers=None):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": "dbt-lineage-metrics", **(headers or {})})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def vscode(row):
    query = {"filters": [{"criteria": [{"filterType": 7, "value": VSCODE_EXT}]}], "flags": 914}
    res = get(
        "https://marketplace.visualstudio.com/_apis/public/gallery/extensionquery",
        data=json.dumps(query).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json;api-version=3.0-preview.1"},
    )
    ext = res["results"][0]["extensions"][0]
    stats = {s["statisticName"]: s["value"] for s in ext.get("statistics", [])}
    # The Marketplace reports `install` only once it has counted some.
    row["vscode_installs"] = int(stats.get("install", 0))
    row["vscode_downloads"] = int(stats.get("downloadCount", 0))
    row["vscode_version"] = ext["versions"][0]["version"]


def openvsx(row):
    ext = get(f"https://open-vsx.org/api/{VSCODE_EXT.replace('.', '/', 1)}")
    row["openvsx_downloads"], row["openvsx_version"] = ext["downloadCount"], ext["version"]


def pypi(row):
    row["pypi_version"] = get(f"https://pypi.org/pypi/{PYPI}/json")["info"]["version"]
    recent = get(f"https://pypistats.org/api/packages/{PYPI}/recent")["data"]
    row["pypi_last_day"], row["pypi_last_week"], row["pypi_last_month"] = (
        recent["last_day"], recent["last_week"], recent["last_month"])


def mcp_registry(row):
    servers = get(f"https://registry.modelcontextprotocol.io/v0/servers?search={MCP_NAME}")["servers"]
    versions = [s["server"]["version"] for s in servers if s["server"]["name"] == MCP_NAME]
    row["mcp_registry_version"] = versions[-1] if versions else None


def glama(row):
    key = os.environ.get("GLAMA_API_KEY")
    if key:
        server = get(f"https://glama.ai/api/mcp/v1/servers/{GLAMA}", key)
        row["glama"] = {k: v for k, v in server.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
    else:
        req = urllib.request.Request(f"https://glama.ai/mcp/servers/{GLAMA}", method="HEAD",
                                     headers={"User-Agent": "Mozilla/5.0 dbt-lineage-metrics"})
        with urllib.request.urlopen(req, timeout=30) as r:
            row["glama_listed"] = r.status == 200


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

    for name, fetch in [("vscode", vscode), ("openvsx", openvsx), ("pypi", pypi),
                        ("mcp_registry", mcp_registry), ("glama", glama)]:
        try:
            fetch(row)
        except Exception as e:  # keep the row even if one source fails
            row[f"{name}_error"] = str(e)

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
