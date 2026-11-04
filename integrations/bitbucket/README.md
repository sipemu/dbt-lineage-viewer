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
| `FAIL_ON` | `lint-errors` | `lint-errors`, `plan-empty`, `manifest-stale`, or `never` |
| `COMMENT` | `true` | Post the sticky PR comment |

Bitbucket renders Markdown tables but not Mermaid, so the impact diagram shows as a code block.
