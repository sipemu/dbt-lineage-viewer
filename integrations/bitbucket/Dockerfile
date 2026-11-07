# Bitbucket Pipe image: the dbt-lineage image (static musl binary on Alpine,
# with bash and git) plus the pipe entrypoint. The release workflow passes the
# matching versioned base image; the default is pinned, never `latest`, so a
# rebuild of the same pipe version gives the same image.
ARG BASE_IMAGE=ghcr.io/sipemu/dbt-lineage:0.18.0
FROM ${BASE_IMAGE}
COPY pipe.sh common.sh /
RUN chmod 0755 /pipe.sh /common.sh
ENTRYPOINT ["/pipe.sh"]
