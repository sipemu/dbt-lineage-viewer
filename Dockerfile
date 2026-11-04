# Container image for CI systems that run jobs in containers (GitLab CI,
# Bitbucket Pipelines, Azure). Static musl binary on Alpine, plus git for
# `diff` / `plan` / `ci report`.
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --locked --no-default-features \
    && strip target/release/dbt-lineage

FROM alpine:3
RUN apk add --no-cache git bash ca-certificates \
    # CI checkouts are often owned by another uid than the container user.
    && git config --system --add safe.directory '*'
COPY --from=build /src/target/release/dbt-lineage /usr/local/bin/dbt-lineage
WORKDIR /workspace
ENTRYPOINT ["dbt-lineage"]
CMD ["--help"]
