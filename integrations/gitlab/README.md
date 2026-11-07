# dbt-lineage — GitLab CI/CD component

Lineage diff, impact diagram, rebuild selector and lint for every merge request, with no warehouse and no Python.

## Usage

```yaml
# .gitlab-ci.yml
include:
  - component: gitlab.com/sipemu/dbt-lineage/dbt-lineage@~latest
    inputs:
      project-dir: dbt        # optional
      fail-on: lint-errors    # optional
```

Pin a release (`@0.19.0`), a minor (`@0.19`) or a major (`@0`) instead of `~latest` for reproducible pipelines. The job runs on merge request pipelines only.

What you get on each MR pipeline:

- **Sticky MR note** with the lineage diff (including SQL body changes), a Mermaid impact diagram, the `dbt build -s "…"` selector, and lint findings. It needs a CI/CD variable `GITLAB_TOKEN` holding a project (or group) access token with `api` scope and at least the Reporter role, because `CI_JOB_TOKEN` cannot post notes. Mark it *masked*; if you also mark it *protected*, MR pipelines from unprotected branches cannot read it and the note is skipped (the job still passes). Set `comment: false` to skip the note.
- **Code Quality** findings inline in the MR diff and in the MR widget (`artifacts:reports:codequality`). Each finding has a stable fingerprint (rule + node), so GitLab shows only findings the MR introduces or fixes. Paths are relative to the repository root, also when the dbt project lives in a subdirectory.
- **dotenv outputs** `selector` and `failed` for later jobs (`artifacts:reports:dotenv`), e.g. `dbt build -s "$selector"` in a job with `needs: [dbt-lineage]`.
- **The report** as a job artifact, linked from the MR as *dbt-lineage report*.

The job clones with `GIT_DEPTH: "0"` and fetches the MR target branch when it is missing, so shallow-clone project settings don't break the diff.

## Inputs

| Input | Type | Default | Description |
| --- | --- | --- | --- |
| `stage` | string | `test` | Pipeline stage |
| `version` | string | `v0` | Tag of the `ghcr.io/sipemu/dbt-lineage` image (`v0`, `0.19`, `0.19.0`, `latest`) |
| `project-dir` | string | `.` | dbt project directory, relative to the repository root |
| `base` | string | `auto` | Base ref; `auto` = MR target branch |
| `manifest` | string | `""` | Optional compiled `manifest.json` |
| `fail-on` | string | `lint-errors` | Comma-separated `lint-errors`, `plan-empty`, `manifest-stale`, `breaking-changes`, or `never` |
| `comment` | boolean | `true` | Post the sticky MR note |

To change anything else (rules, tags, `needs`), define a job named `dbt-lineage` in your `.gitlab-ci.yml`; GitLab merges its keys into the component's job.

## Publishing to the CI/CD Catalog (maintainers)

This directory is the root of the catalog project `gitlab.com/sipemu/dbt-lineage`: `templates/dbt-lineage.yml` (the component), this README, `.gitlab-ci.yml` (tests the component and releases it) and `tests/project` (a small dbt project the pipeline runs the component on). The source of truth stays in [github.com/sipemu/dbt-lineage-viewer](https://github.com/sipemu/dbt-lineage-viewer); a mirror of the whole repository would not work, because the catalog only looks at `templates/` in the project root.

One-time setup:

1. On gitlab.com, create the public project `sipemu/dbt-lineage` (no README, so the first push is accepted as-is).
2. *Settings → General → Naming, description, topics*: set the **project description** (shown in the catalog, and required to publish), e.g. "dbt lineage diff, impact diagram, rebuild selector and lint on every merge request". Add topics such as `dbt`, `lineage`.
3. *Settings → General → Visibility, project features, permissions*: turn on **CI/CD Catalog project** and save.
4. Push this directory as the project's default branch, from the GitHub checkout:

   ```sh
   git subtree split --prefix=integrations/gitlab -b gitlab-component
   git push git@gitlab.com:sipemu/dbt-lineage.git gitlab-component:main
   ```

   The pipeline on `main` runs the component from that commit on `tests/project` and checks its report, dotenv and Code Quality output. It pulls `ghcr.io/sipemu/dbt-lineage:v0`, which is public.

Each release (after the container image for it is published on ghcr.io):

1. Re-run the two commands above to sync `main`.
2. Tag the commit with a plain semantic version, without a `v` prefix (the catalog rejects other tags), and push the tag:

   ```sh
   git push git@gitlab.com:sipemu/dbt-lineage.git gitlab-component:refs/tags/0.19.0
   ```

   The tag pipeline tests the component again, then `create-release` (the `release` keyword) creates the GitLab release, which publishes that version to the CI/CD Catalog. Create releases through this pipeline rather than the UI: the catalog publishes versions released by a CI job.
3. Check the version on the project's catalog page (*Search or go to → Explore → CI/CD Catalog*).

Before pushing, `python3 scripts/check-gitlab-component.py` (needs `jsonschema` and `pyyaml`) validates the component and `.gitlab-ci.yml` against GitLab's CI schema and the catalog's input rules; with a gitlab.com token, `glab ci lint integrations/gitlab/.gitlab-ci.yml` additionally lints against the server.

Source and issues: [github.com/sipemu/dbt-lineage-viewer](https://github.com/sipemu/dbt-lineage-viewer).
