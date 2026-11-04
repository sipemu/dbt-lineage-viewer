# CI integrations

`dbt-lineage ci report` renders one Markdown report per pull/merge request (lineage diff including SQL body changes, Mermaid impact diagram, rebuild selector, manifest freshness, lint), and `dbt-lineage ci comment` posts it as one sticky comment that later runs update. Every platform below is a thin wrapper around those two commands.

| Platform | Integration | Comment token | Inline findings |
| --- | --- | --- | --- |
| GitHub Actions | `uses: sipemu/dbt-lineage-viewer@v0` | `GITHUB_TOKEN` (automatic) | SARIF via code scanning |
| GitLab CI | CI/CD component, [`integrations/gitlab`](../integrations/gitlab) | `GITLAB_TOKEN`: project access token, `api` scope | Code Quality (`lint -o codeclimate`) |
| Bitbucket Pipelines | Pipe `ghcr.io/sipemu/dbt-lineage-pipe:v0`, [`integrations/bitbucket`](../integrations/bitbucket) | `BITBUCKET_ACCESS_TOKEN`: repository access token | — |
| Azure Pipelines | container step (below) | `SYSTEM_ACCESSTOKEN` | — |

`DBT_LINEAGE_TOKEN` overrides the platform token everywhere.

## Base branch

`--base auto` (the default) reads the PR/MR target branch from `GITHUB_BASE_REF`, `CI_MERGE_REQUEST_TARGET_BRANCH_NAME`, `BITBUCKET_PR_DESTINATION_BRANCH` or `SYSTEM_PULLREQUEST_TARGETBRANCH`. If the checkout has no local branch of that name, `origin/<branch>` is used, and fetched if missing (shallow clones fetch only its tip). Full-depth checkouts are still recommended.

## GitHub Actions

See the [README](../README.md#as-a-github-action) and the live [demo PR](https://github.com/sipemu/dbt-lineage-demo/pulls).

## GitLab CI

```yaml
include:
  - component: gitlab.com/sipemu/dbt-lineage/dbt-lineage@~latest
```

Or without the component:

```yaml
dbt-lineage:
  image: { name: ghcr.io/sipemu/dbt-lineage:v0, entrypoint: [""] }
  rules: [{ if: $CI_PIPELINE_SOURCE == "merge_request_event" }]
  variables: { GIT_DEPTH: "0" }
  script:
    - dbt-lineage ci report --out dbt-lineage-report.md || rc=$?
    - dbt-lineage lint -o codeclimate > gl-code-quality-report.json || true
    - dbt-lineage ci comment --file dbt-lineage-report.md
    - exit ${rc:-0}
  artifacts:
    when: always
    reports: { codequality: gl-code-quality-report.json }
```

## Bitbucket Pipelines

See [`integrations/bitbucket/README.md`](../integrations/bitbucket/README.md).

## Azure Pipelines

Not yet tested on Azure; feedback welcome.

```yaml
pr: [main]
pool: { vmImage: ubuntu-latest }
steps:
  - checkout: self
    fetchDepth: 0
  - script: |
      docker run --rm -v "$(Build.SourcesDirectory):/workspace" \
        -e TF_BUILD -e SYSTEM_PULLREQUEST_TARGETBRANCH -e SYSTEM_PULLREQUEST_PULLREQUESTID \
        -e SYSTEM_COLLECTIONURI -e SYSTEM_TEAMPROJECT -e BUILD_REPOSITORY_ID -e SYSTEM_ACCESSTOKEN \
        --entrypoint sh ghcr.io/sipemu/dbt-lineage:v0 \
        -c 'dbt-lineage ci report --out report.md; rc=$?; dbt-lineage ci comment --file report.md; exit $rc'
    env:
      SYSTEM_ACCESSTOKEN: $(System.AccessToken)
```

The build service account needs "Contribute to pull requests" on the repository.

## Outputs and exit codes

- `--outputs <file>` appends `selector=…` and `failed=…` (`$GITHUB_OUTPUT`, GitLab dotenv).
- `--fail-on lint-errors,plan-empty,manifest-stale,breaking-changes` (default `lint-errors`; `never` to only report) makes `ci report` exit 1 after writing the report, so the comment is still posted.
