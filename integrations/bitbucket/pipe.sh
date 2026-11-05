#!/usr/bin/env bash
# Bitbucket Pipe entrypoint: report, sticky PR comment, Code Insights, exit code.
# Variables (all optional): PROJECT_DIR, BASE, MANIFEST, FAIL_ON, COMMENT,
# INSIGHTS, BITBUCKET_ACCESS_TOKEN (repository access token, pull request write).
set -uo pipefail

PROJECT_DIR="${PROJECT_DIR:-.}"
BASE="${BASE:-auto}"
MANIFEST="${MANIFEST:-}"
FAIL_ON="${FAIL_ON:-lint-errors}"
COMMENT="${COMMENT:-true}"
INSIGHTS="${INSIGHTS:-true}"

cd "${BITBUCKET_CLONE_DIR:-.}"
args=(-p "$PROJECT_DIR")
[ -n "$MANIFEST" ] && args+=(--manifest "$MANIFEST")

rc=0
dbt-lineage ci report "${args[@]}" --base "$BASE" --fail-on "$FAIL_ON" \
  --out dbt-lineage-report.md --outputs dbt-lineage.env || rc=$?
[ -f dbt-lineage-report.md ] && cat dbt-lineage-report.md

if [ "$COMMENT" = "true" ] && [ -f dbt-lineage-report.md ] && [ -n "${BITBUCKET_PR_ID:-}" ]; then
  dbt-lineage ci comment --file dbt-lineage-report.md \
    || echo "dbt-lineage: PR comment skipped (set BITBUCKET_ACCESS_TOKEN to a repository access token)"
fi

# Code Insights report + inline annotations on the commit. Inside Pipelines
# the local proxy (localhost:29418) authenticates, so no token is needed.
if [ "$INSIGHTS" = "true" ] && [ -n "${BITBUCKET_COMMIT:-}" ]; then
  dbt-lineage ci annotations --format bitbucket --post "${args[@]}" --out dbt-lineage-insights.json \
    || echo "dbt-lineage: Code Insights report skipped"
fi
exit $rc
