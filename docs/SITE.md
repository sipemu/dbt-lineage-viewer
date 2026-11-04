# Static lineage site

`dbt-lineage site` writes a self-contained static website for a dbt project: no server, no CDN, no Python. Publish the output directory to GitHub Pages, GitLab Pages or any static host.

```sh
dbt-lineage site -p path/to/project --out site            # SQL-parse mode
dbt-lineage site --manifest target/manifest.json --out site
dbt-lineage site --title "Analytics DAG"                   # default: dbt project name
```

Live example: [sipemu.github.io/dbt-lineage-demo](https://sipemu.github.io/dbt-lineage-demo/) (built by [this workflow](https://github.com/sipemu/dbt-lineage-demo/blob/main/.github/workflows/lineage-site.yml)).

## What's in it

- **Index** (`index.html`) — search across models, sources, seeds, snapshots, exposures, metrics and semantic models by name, description, tag and path; filter by type. Press `/` to focus the search. The search runs in the browser over a JSON index embedded in the page; without JavaScript the full list is still shown.
- **One page per node** (`nodes/<unique_id>.html`) with:
  - description, materialization, tags, file path;
  - a lineage diagram of the node and everything up to two hops up- and downstream (each box links to its page);
  - upstream and downstream lists, plus attached tests;
  - a column table with **column-level lineage**: which upstream columns each column comes from and which downstream columns use it, linked column to column;
  - the model's SQL with Jinja and keyword highlighting (seeds show a CSV preview).

Light and dark themes follow the system setting; pages work at phone width.

Tests and unresolved (phantom) references are listed on their neighbours' pages but get no page of their own.

## Performance

On a synthetic 1,500-model project (500 sources, 2,000 pages) the whole site — graph, column lineage, pages — is generated in about 0.3 s on a 20-core machine.

## GitHub Pages

```yaml
# .github/workflows/lineage-site.yml
name: Lineage site

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - name: Install dbt-lineage
        env:
          VERSION: 0.12.1
        run: |
          curl -fsSL "https://github.com/sipemu/dbt-lineage-viewer/releases/download/v$VERSION/dbt-lineage-v$VERSION-x86_64-unknown-linux-gnu.tar.gz" \
            | tar -xz --strip-components=1
          sudo mv dbt-lineage /usr/local/bin/
      - run: dbt-lineage site -p . --out site
      - uses: actions/upload-pages-artifact@v5
        with:
          path: site

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v5
```

Release archives carry the version in their file name, so pin one (or use `cargo binstall dbt-lineage`). Alternatively run the container image: `docker run --rm -v "$PWD:/workspace" ghcr.io/sipemu/dbt-lineage:v0 site --out site`.

Enable Pages once under *Settings → Pages → Source: GitHub Actions*.

## GitLab Pages

```yaml
pages:
  image: { name: ghcr.io/sipemu/dbt-lineage:v0, entrypoint: [""] }
  script:
    - dbt-lineage site -p . --out public
  artifacts:
    paths: [public]
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
```
