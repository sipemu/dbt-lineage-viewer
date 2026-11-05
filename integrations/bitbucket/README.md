# dbt-lineage — Bitbucket Pipe

Lineage diff, rebuild selector and lint for every pull request, posted as one sticky PR comment.

```yaml
# bitbucket-pipelines.yml
pipelines:
  pull-requests:
    '**':
      - step:
          name: dbt-lineage
          clone:
            depth: full
          script:
            - pipe: docker://ghcr.io/sipemu/dbt-lineage-pipe:v0
              variables:
                BITBUCKET_ACCESS_TOKEN: $DBT_LINEAGE_BB_TOKEN   # repository access token
                # PROJECT_DIR: dbt
                # FAIL_ON: lint-errors
          artifacts:
            - dbt-lineage-report.md
```

| Variable | Default | Description |
| --- | --- | --- |
| `BITBUCKET_ACCESS_TOKEN` | — | Repository access token with pull request write scope, for the comment |
| `PROJECT_DIR` | `.` | dbt project directory |
| `BASE` | `auto` | Base ref; `auto` = PR destination branch |
| `MANIFEST` | — | Optional compiled `manifest.json` |
| `FAIL_ON` | `lint-errors` | `lint-errors`, `plan-empty`, `manifest-stale`, `breaking-changes`, or `never` |
| `COMMENT` | `true` | Post the sticky PR comment |
| `INSIGHTS` | `true` | Publish a Code Insights report with inline annotations on the commit |

**Code Insights.** Lint findings appear as a *dbt-lineage* report on the commit and as annotations in the PR diff (errors as `BUG`/`HIGH`, warnings as `CODE_SMELL`/`MEDIUM`, info as `LOW`; at most 1,000 per report, as Bitbucket allows). Without a token the request goes through the Pipelines proxy at `localhost:29418`, which adds authentication; with `BITBUCKET_ACCESS_TOKEN` set, the public API is called directly. Override the proxy with `BITBUCKET_PROXY`. Outside the pipe: `dbt-lineage ci annotations --format bitbucket --post`.

Bitbucket renders Markdown tables but not Mermaid, so the impact diagram shows as a code block.
