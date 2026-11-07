# Bitbucket Pipelines Pipe: dbt-lineage

Report dbt lineage changes, the rebuild selector and lint findings to the pull request, without a warehouse: one sticky PR comment, plus a Code Insights report with inline annotations on the commit.

## YAML Definition

Add the following snippet to the script section of your `bitbucket-pipelines.yml` file:

```yaml
- pipe: docker://ghcr.io/sipemu/dbt-lineage-pipe:0.19.0
  variables:
    BITBUCKET_ACCESS_TOKEN: $DBT_LINEAGE_BB_TOKEN # Optional (needed for the PR comment).
    # PROJECT_DIR: '<string>' # Optional.
    # BASE: '<string>' # Optional.
    # MANIFEST: '<string>' # Optional.
    # FAIL_ON: '<string>' # Optional.
    # COMMENT: '<boolean>' # Optional.
    # INSIGHTS: '<boolean>' # Optional.
    # REPORT_ID: '<string>' # Optional.
    # DEBUG: '<boolean>' # Optional.
```

Pin the full version as above for repeatable builds. `:0.19` follows patch releases, `:v0` every 0.x release, and `:latest` the newest release. Once the pipe is mirrored to Bitbucket (see [Publishing](#publishing-maintainers)), `pipe: sipemu/dbt-lineage-pipe:0.19.0` works too.

## Variables

| Variable | Usage |
| --- | --- |
| `BITBUCKET_ACCESS_TOKEN` | Repository access token with **Pull requests: Write** scope. Needed for the PR comment. When set, Code Insights is posted with it as well. Default: none. |
| `PROJECT_DIR` | dbt project directory, relative to the repository root. Default: `.` |
| `BASE` | Base ref for the diff. `auto` uses `BITBUCKET_PR_DESTINATION_BRANCH`. Default: `auto` |
| `MANIFEST` | Optional compiled `manifest.json` (more exact lineage). Default: none. |
| `FAIL_ON` | When the step fails: `lint-errors`, `plan-empty`, `manifest-stale`, `breaking-changes` or `never`. Default: `lint-errors` |
| `COMMENT` | Post or update the sticky PR comment. Default: `true` |
| `INSIGHTS` | Publish the Code Insights report and annotations on the commit. Default: `true` |
| `REPORT_ID` | Code Insights report id (`[A-Za-z0-9._-]`, at most 100 characters). Use one per dbt project when a repository has several. Default: `dbt-lineage` |
| `DEBUG` | Print the commands the pipe runs. Default: `false` |

## Details

The pipe runs `dbt-lineage ci report` on the clone and compares the PR head with its destination branch. It parses the dbt project's SQL and YAML and does not connect to a warehouse. The report covers changed models and columns, downstream impact, a `dbt build --select` selector for the changed nodes, and lint findings. It is printed to the log and written to `dbt-lineage-report.md`. The step's outputs go to `dbt-lineage.env`.

**PR comment.** In a `pull-requests` pipeline, `dbt-lineage ci comment` creates the comment and updates it on later runs, so the PR keeps one comment. A hidden marker identifies it. The comments API requires a token (`BITBUCKET_ACCESS_TOKEN`). Without one the pipe skips the comment and logs a warning.

**Code Insights.** `dbt-lineage ci annotations --format bitbucket --post` publishes a report named *dbt-lineage* on `BITBUCKET_COMMIT`:

- Report: `report_type: BUG` and `result: FAILED` when there are error findings, otherwise `PASSED`. Error, warning and info counts are shown as `NUMBER` data fields.
- Annotations: errors are `BUG`/`HIGH`, warnings `CODE_SMELL`/`MEDIUM`, info `CODE_SMELL`/`LOW`, all with `result: FAILED`. Each points at the finding's file, relative to the repository root (it includes `PROJECT_DIR`), on line 1. Errors come first. Bitbucket allows up to 1,000 annotations per report, and the pipe sends them in requests of at most 100. When there are more than 1,000 findings, the report details say so.
- Re-runs: the report id is fixed per commit and each annotation's `external_id` is stable. The previous report and its annotations are deleted before the new report is created, so re-running a step replaces the results instead of duplicating them.
- Authentication: without a token, requests go through the Pipelines auth proxy. From inside a pipe container it is reached at `http://host.docker.internal:29418`, which the pipe sets as `BITBUCKET_PROXY`. With a token, the public API (`https://api.bitbucket.org`) is called with Bearer auth.

**Base branch.** With `BASE: auto`, the PR destination branch is used. If the clone has no local branch of that name, `origin/<branch>` is fetched. With the default clone depth (50) only its tip is fetched. Git proxy settings that point at `localhost:29418` are redirected to the host for this fetch, via `GIT_CONFIG_*`, so `.git/config` is not changed. `clone: depth: full` remains the most robust choice.

Bitbucket renders Markdown tables but not Mermaid, so the impact diagram shows as a code block.

The step exits non-zero according to `FAIL_ON`. A failed comment or Code Insights request only logs a warning.

## Prerequisites

- A `pull-requests` pipeline, for the PR comment. Code Insights works on any commit.
- For the PR comment: a [repository access token](https://support.atlassian.com/bitbucket-cloud/docs/repository-access-tokens/) with the **Pull requests: Write** scope. Store it as a secured repository variable, for example `DBT_LINEAGE_BB_TOKEN`.
- Code Insights needs no setup inside Pipelines. Reports show in the PR's *Reports* panel, and annotations show inline in the diff for files the PR changes.

## Examples

Basic example: comment and Code Insights on every pull request.

```yaml
pipelines:
  pull-requests:
    '**':
      - step:
          name: dbt-lineage
          clone:
            depth: full
          script:
            - pipe: docker://ghcr.io/sipemu/dbt-lineage-pipe:0.19.0
              variables:
                BITBUCKET_ACCESS_TOKEN: $DBT_LINEAGE_BB_TOKEN
          artifacts:
            - dbt-lineage-report.md
```

Advanced example: dbt project in a subdirectory, a compiled manifest, failing on breaking changes, and no comment (Code Insights only, through the Pipelines proxy, so no token is needed):

```yaml
- pipe: docker://ghcr.io/sipemu/dbt-lineage-pipe:0.19.0
  variables:
    PROJECT_DIR: 'warehouse/dbt'
    MANIFEST: 'warehouse/dbt/target/manifest.json'
    FAIL_ON: 'breaking-changes'
    COMMENT: 'false'
    REPORT_ID: 'dbt-lineage-warehouse'
```

A complete `bitbucket-pipelines.yml` is in [`examples/bitbucket-pipelines.yml`](examples/bitbucket-pipelines.yml).

Outside the pipe, the same commands work in any step that uses the `ghcr.io/sipemu/dbt-lineage` image: `dbt-lineage ci report`, `ci comment`, and `ci annotations --format bitbucket --post`. A regular step reaches the proxy at `localhost:29418`, which is the default.

## Support

Report bugs and request features in the [issue tracker](https://github.com/sipemu/dbt-lineage-viewer/issues). Please include the pipe version and the step log with `DEBUG: 'true'`.

## Development

| File | Purpose |
| --- | --- |
| `pipe.yml` | Pipe metadata: name, image (pinned to the release version), category, variables with defaults |
| `pipe.sh` | Entrypoint: validates variables, runs `ci report`, `ci comment`, `ci annotations` |
| `common.sh` | `info` / `success` / `warning` / `fail` / `debug`, as in Atlassian's pipes toolkit |
| `Dockerfile` | `ghcr.io/sipemu/dbt-lineage:<version>` (Alpine, static binary) plus the scripts |
| `bitbucket-pipelines.yml` | CI for the mirrored pipe repository on Bitbucket (test, then push on tags) |
| `test/test_pipe.py`, `test/mock_bitbucket.py` | End-to-end test: builds the image, runs the entrypoint on a fixture repo against a mocked Bitbucket API and Pipelines proxy |

Run the end-to-end test locally (Docker and Python 3 needed; it is skipped without Docker):

```sh
python3 integrations/bitbucket/test/test_pipe.py
# or
cargo test --release --test bitbucket_pipe_test -- --ignored
```

`PIPE_IMAGE=<image>` tests an existing image. `BASE_IMAGE=<image>` builds the pipe on a given base instead of building the repository's root `Dockerfile`. `cargo test` checks that `pipe.yml`, the `Dockerfile` and this README pin the crate version, and that every variable is defaulted in `pipe.sh` and documented here.

**Versioning.** The pipe uses semantic versioning and shares its version with dbt-lineage. Each `vX.Y.Z` tag publishes `ghcr.io/sipemu/dbt-lineage-pipe:X.Y.Z`, `:X.Y`, `:vX` and `:latest` (`.github/workflows/release-binaries.yml`). The release bump must update the version in `pipe.yml`, the `Dockerfile` default and this README. Changes are listed in the repository's [CHANGELOG](../../CHANGELOG.md).

## Publishing (maintainers)

The image is published automatically. These steps are manual and need a Bitbucket account:

1. **Mirror the pipe repository on Bitbucket.** Atlassian lists pipes hosted on Bitbucket, and `pipe: <workspace>/<repo>:<tag>` reads `pipe.yml` from there. Create a public repository `sipemu/dbt-lineage-pipe`, the URL in `pipe.yml`'s `repository`. Copy the contents of `integrations/bitbucket/` into it, along with `LICENSE` and a `CHANGELOG.md`. Then push a `X.Y.Z` tag for every release. The included `bitbucket-pipelines.yml` runs the end-to-end test and checks the image. It needs the `BASE_IMAGE` repository variable or Docker access to ghcr.io.
2. **Image registry.** Atlassian's guide uses Docker Hub (`account/repo:tag`). The pipe works from ghcr.io with `docker://`. If the review asks for Docker Hub, also push the image to `docker.io/sipemu/dbt-lineage-pipe` from the release workflow, using `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` secrets, and change `image:` in `pipe.yml`.
3. **Verify in a live workspace.** Run the basic example on a test PR. Check the sticky comment, the *dbt-lineage* report in the PR's *Reports* panel, and inline annotations on changed files. Check both the proxy path (no token) and the token path.
4. **Submit to the Pipes directory.** Fork [`bitbucketpipelines/official-pipes`](https://bitbucket.org/bitbucketpipelines/official-pipes) and add `pipes/dbt-lineage.yml`:

   ```yaml
   repositoryPath: 'sipemu/dbt-lineage-pipe'
   version: '0.19.0'
   ```

   Commit it, run `python build_run.py pipes/dbt-lineage.yml` to validate, and open a pull request. The review checks the README structure, semantic version tags, CI and tests, an image under 1 GB, pinned base images, a support channel, and a commitment to maintain the pipe. Atlassian currently accepts only *vendors*, meaning the authors or maintainers of the product the pipe integrates. dbt-lineage qualifies as its own vendor. Questions go to `pipelines-feedback@atlassian.com`.
5. Optionally add a maintainer/vendor `email` to `pipe.yml`. The submission form asks for a contact.
