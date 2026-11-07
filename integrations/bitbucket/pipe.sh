#!/usr/bin/env bash
# Bitbucket Pipe entrypoint: PR report, sticky PR comment, Code Insights
# report + annotations, exit code from FAIL_ON.
#
# Variables (all optional, defaults below; see README.md):
#   BITBUCKET_ACCESS_TOKEN  repository access token (pull requests: write)
#   PROJECT_DIR BASE MANIFEST FAIL_ON COMMENT INSIGHTS REPORT_ID DEBUG
set -uo pipefail

source "$(dirname "$0")/common.sh"

# --- variables ---------------------------------------------------------------
PROJECT_DIR=${PROJECT_DIR:="."}
BASE=${BASE:="auto"}
MANIFEST=${MANIFEST:=""}
FAIL_ON=${FAIL_ON:="lint-errors"}
COMMENT=${COMMENT:="true"}
INSIGHTS=${INSIGHTS:="true"}
REPORT_ID=${REPORT_ID:="dbt-lineage"}
DEBUG=${DEBUG:="false"}
BITBUCKET_ACCESS_TOKEN=${BITBUCKET_ACCESS_TOKEN:=""}

enable_debug

case "$FAIL_ON" in
  lint-errors | plan-empty | manifest-stale | breaking-changes | never) ;;
  *) fail "FAIL_ON must be one of lint-errors, plan-empty, manifest-stale, breaking-changes, never (got '$FAIL_ON')" ;;
esac
for flag in COMMENT INSIGHTS DEBUG; do
  case "${!flag}" in
    true | false) ;;
    *) fail "$flag must be true or false (got '${!flag}')" ;;
  esac
done
[[ "$REPORT_ID" =~ ^[A-Za-z0-9._-]{1,100}$ ]] \
  || fail "REPORT_ID may only contain letters, digits, '.', '_' and '-' (got '$REPORT_ID')"

cd "${BITBUCKET_CLONE_DIR:-.}" || fail "cannot enter BITBUCKET_CLONE_DIR=${BITBUCKET_CLONE_DIR:-}"
[ -d "$PROJECT_DIR" ] || fail "PROJECT_DIR '$PROJECT_DIR' does not exist in the clone"

# --- Pipelines proxy -----------------------------------------------------------
# Pipes run in their own container: the step's auth proxy at localhost:29418
# is reachable as host.docker.internal:29418 from here.
# PIPELINES_PROXY_ADDR overrides the address (tests).
PIPELINES_PROXY_ADDR=${PIPELINES_PROXY_ADDR:="host.docker.internal:29418"}
if [ -z "${BITBUCKET_PROXY:-}" ] && [ -z "$BITBUCKET_ACCESS_TOKEN" ] && [ -n "${BITBUCKET_BUILD_NUMBER:-}" ]; then
  export BITBUCKET_PROXY="http://${PIPELINES_PROXY_ADDR}"
  debug "Code Insights through the Pipelines proxy $BITBUCKET_PROXY"
fi
# The clone may route git over the same proxy (http.<url>.proxy =
# localhost:29418); point those entries at the host for `git fetch` of the
# PR destination branch, without touching .git/config.
n=0
while read -r key value; do
  case "$value" in
    *localhost:29418* | *127.0.0.1:29418*)
      export "GIT_CONFIG_KEY_$n=$key"
      export "GIT_CONFIG_VALUE_$n=$(echo "$value" | sed -E "s#(localhost|127\.0\.0\.1):29418#${PIPELINES_PROXY_ADDR}#")"
      n=$((n + 1))
      ;;
  esac
done < <(git config --get-regexp '^http\..*proxy$' 2>/dev/null || true)
[ "$n" -gt 0 ] && export GIT_CONFIG_COUNT=$n && debug "rewrote $n git proxy setting(s) for the pipe container"

# A token is used for the PR comment and, when set, for Code Insights too.
[ -n "$BITBUCKET_ACCESS_TOKEN" ] && export BITBUCKET_ACCESS_TOKEN || unset BITBUCKET_ACCESS_TOKEN

args=(-p "$PROJECT_DIR")
[ -n "$MANIFEST" ] && args+=(--manifest "$MANIFEST")

# --- report --------------------------------------------------------------------
info "dbt-lineage $(dbt-lineage --version 2>/dev/null | awk '{print $NF}'): project '$PROJECT_DIR', base '$BASE'"
rc=0
dbt-lineage ci report "${args[@]}" --base "$BASE" --fail-on "$FAIL_ON" \
  --out dbt-lineage-report.md --outputs dbt-lineage.env || rc=$?
[ -f dbt-lineage-report.md ] && cat dbt-lineage-report.md

# --- sticky PR comment ---------------------------------------------------------
if [ "$COMMENT" = "true" ] && [ -f dbt-lineage-report.md ]; then
  if [ -z "${BITBUCKET_PR_ID:-}" ]; then
    info "Not a pull-request pipeline: no PR comment."
  elif [ -z "${BITBUCKET_ACCESS_TOKEN:-}" ]; then
    warning "PR comment skipped: set BITBUCKET_ACCESS_TOKEN to a repository access token with pull request write scope."
  else
    dbt-lineage ci comment --file dbt-lineage-report.md \
      || warning "PR comment failed (see above)."
  fi
fi

# --- Code Insights ---------------------------------------------------------------
if [ "$INSIGHTS" = "true" ]; then
  if [ -z "${BITBUCKET_COMMIT:-}" ]; then
    info "BITBUCKET_COMMIT is not set: no Code Insights report."
  else
    dbt-lineage ci annotations --format bitbucket --post --report-id "$REPORT_ID" "${args[@]}" \
      --out dbt-lineage-insights.json \
      || warning "Code Insights report failed (see above)."
  fi
fi

if [ "$rc" -eq 0 ]; then
  success "dbt-lineage finished."
else
  fail "dbt-lineage failed (FAIL_ON=$FAIL_ON, exit code $rc)."
fi
