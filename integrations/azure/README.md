# dbt-lineage — Azure Pipelines template

> **Tested on Azure DevOps** (Azure Repos, Microsoft-hosted `ubuntu-latest`, Nov 2026): build warnings, report artifact and the sticky PR comment (posted once, then updated in place).

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
      ref: refs/tags/v0.19.0

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

**PR builds.** Azure Repos ignores the YAML `pr:` trigger; add the pipeline as a *Build validation* branch policy on the target branch. The PR comment and `--base auto` need the PR variables, which Azure sets only for policy-triggered builds — a build queued manually on `refs/pull/<id>/merge` runs the report but skips the comment.

**Without the template**, see the inline variant in [`docs/CI.md`](../../docs/CI.md#azure-pipelines).
