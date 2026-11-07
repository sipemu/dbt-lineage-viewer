# CI integrations

`dbt-lineage ci report` renders one Markdown report per pull/merge request (lineage diff including SQL body changes, Mermaid impact diagram, rebuild selector, manifest freshness, lint), `dbt-lineage ci comment` posts it as one sticky comment that later runs update, and `dbt-lineage ci annotations` turns lint findings into the platform's inline annotations. Every platform below is a thin wrapper around these commands.

| Platform | Integration | Comment token | Inline findings |
| --- | --- | --- | --- |
| GitHub Actions | `uses: sipemu/dbt-lineage-viewer@v0` | `GITHUB_TOKEN` (automatic) | SARIF via code scanning |
| GitLab CI | CI/CD component, [`integrations/gitlab`](../integrations/gitlab) | `GITLAB_TOKEN`: project access token, `api` scope | Code Quality (`ci annotations --format codeclimate`) |
| Bitbucket Pipelines | Pipe `ghcr.io/sipemu/dbt-lineage-pipe:0.18.0` (or `:v0`), [`integrations/bitbucket`](../integrations/bitbucket) | `BITBUCKET_ACCESS_TOKEN`: repository access token | Code Insights report + annotations (`ci annotations --format bitbucket --post`) |
| Azure Pipelines | step template, [`integrations/azure`](../integrations/azure) | `SYSTEM_ACCESSTOKEN` | build warnings/errors (`ci annotations --format azure`) |

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
    - dbt-lineage ci annotations --format codeclimate --out gl-code-quality-report.json || true
    - dbt-lineage ci comment --file dbt-lineage-report.md
    - exit ${rc:-0}
  artifacts:
    when: always
    reports: { codequality: gl-code-quality-report.json }
```

## Bitbucket Pipelines

See [`integrations/bitbucket/README.md`](../integrations/bitbucket/README.md) (variables, examples, Code Insights details) and the complete [`bitbucket-pipelines.yml` example](../integrations/bitbucket/examples/bitbucket-pipelines.yml).

## Azure Pipelines

Tested on Azure DevOps (Azure Repos, policy-triggered PR builds). Grant the build service *Contribute to pull requests*; see [`integrations/azure/README.md`](../integrations/azure/README.md) for the template. The step template in [`integrations/azure`](../integrations/azure) runs the report, logs lint findings as build issues, posts the PR comment and publishes the report as an artifact:

```yaml
resources:
  repositories:
    - repository: dbt_lineage
      type: github
      name: sipemu/dbt-lineage-viewer
      endpoint: my-github-connection   # GitHub service connection
      ref: refs/tags/v0.14.0
steps:
  - checkout: self
    fetchDepth: 0
  - template: integrations/azure/dbt-lineage.yml@dbt_lineage
    parameters: { projectDir: dbt, failOn: lint-errors }
```

Inline, without the template:

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
        -c 'dbt-lineage ci report --out report.md; rc=$?; dbt-lineage ci annotations --format azure; dbt-lineage ci comment --file report.md; exit $rc'
    env:
      SYSTEM_ACCESSTOKEN: $(System.AccessToken)
```

The build service account needs "Contribute to pull requests" on the repository.

## Inline annotations

`dbt-lineage ci annotations --format <f> [-p DIR] [--out FILE]` renders the lint findings (including `private-ref`) with repository-relative file paths:

| Format | Platform | Notes |
| --- | --- | --- |
| `sarif` | GitHub code scanning | upload with `github/codeql-action/upload-sarif` |
| `codeclimate` | GitLab Code Quality | `artifacts:reports:codequality` |
| `bitbucket` | Bitbucket Code Insights | `--post` replaces the commit's report (id `--report-id`, default `dbt-lineage`) and creates up to 1,000 annotations in batches of 100, with stable `external_id`s; result `FAILED` when there are error findings |
| `azure` | Azure Pipelines | `##vso[task.logissue]` lines on stdout |

Bitbucket authentication: `DBT_LINEAGE_TOKEN` or `BITBUCKET_ACCESS_TOKEN` (Bearer, `https://api.bitbucket.org`); without a token, inside Pipelines the request goes through the proxy that runs alongside every step at `localhost:29418` (plain `http://api.bitbucket.org`, auth added by the proxy). `BITBUCKET_PROXY` overrides the proxy address (a pipe container reaches it as `http://host.docker.internal:29418`, which the pipe sets); with a token the proxy is not used. `BITBUCKET_API_URL` replaces `https://api.bitbucket.org` for the comment and Code Insights calls (tests, gateways).

## Outputs and exit codes

- `--outputs <file>` appends `selector=…` and `failed=…` (`$GITHUB_OUTPUT`, GitLab dotenv).
- `--fail-on lint-errors,plan-empty,manifest-stale,breaking-changes,verify-missed` (default `lint-errors`; `never` to only report) makes `ci report` exit 1 after writing the report, so the comment is still posted.
- `--verify` compares the parsed lineage with `target/manifest.json` (or `--manifest`) and adds a **Lineage verified against dbt** section. Run `dbt parse` in an earlier step; it needs your adapter and `profiles.yml` but never connects to the warehouse.
- `--base-manifest <path>` takes the base branch's `manifest.json` from `dbt compile` (for example the production manifest kept for slim CI). When the PR checkout also has a compiled `target/manifest.json` (run `dbt compile` in the job), column impact reads dbt-compiled SQL for every model whose file matches the manifest's checksum, which covers columns generated by Jinja loops and macros. With compiled SQL on one side only, both sides fall back to parsing, so the comparison stays consistent.
