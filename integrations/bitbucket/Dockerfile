# Bitbucket Pipe image: the dbt-lineage image plus the pipe entrypoint.
ARG BASE_IMAGE=ghcr.io/sipemu/dbt-lineage:latest
FROM ${BASE_IMAGE}
COPY pipe.sh /pipe.sh
ENTRYPOINT ["/pipe.sh"]
CMD []
