# dbt-lineage — Azure Pipelines template

> **Not yet tested on Azure DevOps.** The template uses only documented features (step templates, `resources.repositories`, logging commands, `System.AccessToken`); feedback and fixes welcome.

On every PR build it:

- writes the dbt-lineage report (lineage diff incl. SQL body changes, column impact, breaking changes, rebuild selector, lint) and publishes it as the `dbt-lineage-report` artifact;
- logs lint findings as build errors/warnings with file paths (`##vso[task.logissue]`);
- posts the report as one sticky PR comment, updated on later runs;
- fails the step on the `failOn` conditions.

## Usage

Reference the template from this GitHub repository (Azure needs a GitHub service connection to read it) and pin a release tag:

```yaml
# azure-pipelines.yml
pr: [main]
pool: { vmImage: ubuntu-latest }

resources:
  repositories:
    - repository: dbt_lineage
      type: github
      name: sipemu/dbt-lineage-viewer
      endpoint: my-github-connection   # your GitHub service connection
      ref: refs/tags/v0.14.0

steps:
  - checkout: self
    fetchDepth: 0                      # diff needs the target branch
  - template: integrations/azure/dbt-lineage.yml@dbt_lineage
    parameters:
      projectDir: dbt                  # optional, default "."
      failOn: lint-errors,breaking-changes
```

| Parameter | Default | Description |
| --- | --- | --- |
| `projectDir` | `.` | dbt project directory |
| `base` | `auto` | Base ref; `auto` = PR target branch (`System.PullRequest.TargetBranch`) |
| `failOn` | `lint-errors` | `lint-errors`, `plan-empty`, `manifest-stale`, `breaking-changes`, or `never` |
| `comment` | `true` | Post the sticky PR comment |
| `image` | `ghcr.io/sipemu/dbt-lineage:v0` | Container image |

**Permissions.** The comment is posted with `System.AccessToken`; grant the project's build service account *Contribute to pull requests* on the repository. Without it the step logs a warning and continues.

**Without the template**, see the inline variant in [`docs/CI.md`](../../docs/CI.md#azure-pipelines).
