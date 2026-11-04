# dbt-lineage — GitLab CI/CD component

Lineage diff, impact diagram, rebuild selector and lint for every merge request, with no warehouse and no Python.

```yaml
# .gitlab-ci.yml
include:
  - component: gitlab.com/sipemu/dbt-lineage/dbt-lineage@~latest
    inputs:
      project-dir: dbt        # optional
      fail-on: lint-errors    # optional
```

What you get on each MR pipeline:

- **Sticky MR note** with the lineage diff (including SQL body changes), a Mermaid impact diagram, the `dbt build -s "…"` selector, and lint findings. It needs a CI/CD variable `GITLAB_TOKEN` holding a project access token with `api` scope, because `CI_JOB_TOKEN` cannot post notes.
- **Code Quality** findings inline in the MR diff (`artifacts:reports:codequality`).
- **dotenv outputs** `selector` and `failed` for later jobs (`artifacts:reports:dotenv`).

| Input | Default | Description |
| --- | --- | --- |
| `stage` | `test` | Pipeline stage |
| `version` | `v0` | Tag of `ghcr.io/sipemu/dbt-lineage` |
| `project-dir` | `.` | dbt project directory |
| `base` | `auto` | Base ref; `auto` = MR target branch |
| `manifest` | `""` | Optional compiled `manifest.json` |
| `fail-on` | `lint-errors` | `lint-errors`, `plan-empty`, `manifest-stale`, or `never` |
| `comment` | `true` | Post the sticky MR note |

Source and issues: [github.com/sipemu/dbt-lineage-viewer](https://github.com/sipemu/dbt-lineage-viewer).
