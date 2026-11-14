# syntax=docker/dockerfile:1
# The one Kermodes image (architecture.md §16.1): Python slim and the kermodes wheel, which runs
# every role (`kermodes weir`, `kermodes ui`, ...). Built once per release and promoted as is.
# Image names are fully qualified: CI builds with buildah, which does not guess registries.

# The image CI checks with (ci/Dockerfile): the same Rust, Bun, and uv build the release.
ARG CI_IMAGE=git.rcolclough.com/rcolclough/kermodes-ci:rust1.98.1-bun1.4.2-sccache0.19.0

# The wheel, with the web app and notebook widget it serves from kermodes/static. With sccache's
# credentials as build secrets (the release job passes them), compiles share CI's cache on
# rustfs; secrets never land in a layer.
FROM ${CI_IMAGE} AS wheel
ARG SCCACHE_ENDPOINT=http://rustfs-svc.rustfs.svc.cluster.local:9000
WORKDIR /src
COPY . .
RUN --mount=type=secret,id=sccache_id --mount=type=secret,id=sccache_key \
    if [ -s /run/secrets/sccache_id ]; then \
        export RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0 SCCACHE_BUCKET=kermodes-sccache \
            SCCACHE_ENDPOINT="$SCCACHE_ENDPOINT" SCCACHE_REGION=us-east-1 \
            SCCACHE_S3_USE_SSL=false SCCACHE_IGNORE_SERVER_IO_ERROR=1 \
            AWS_ACCESS_KEY_ID="$(cat /run/secrets/sccache_id)" \
            AWS_SECRET_ACCESS_KEY="$(cat /run/secrets/sccache_key)"; \
    fi && \
    (cd web && bun install --frozen-lockfile && bun run build) && \
    uv build --wheel --out-dir /dist && \
    if [ -n "${RUSTC_WRAPPER:-}" ]; then sccache --show-stats; fi

FROM docker.io/library/python:3.14-slim-trixie AS runtime
ARG IMAGE_CREATED
ARG IMAGE_REVISION
ARG IMAGE_SOURCE
ARG IMAGE_VERSION
LABEL org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.source="${IMAGE_SOURCE}" \
      org.opencontainers.image.title="Kermodes" \
      org.opencontainers.image.version="${IMAGE_VERSION}"
COPY --from=wheel /dist/ /tmp/dist/
# git: `kermodes control` syncs projects with it.
RUN apt-get update && \
    apt-get install --yes --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir /tmp/dist/*.whl && \
    rm -rf /tmp/dist && \
    useradd --create-home --uid 10001 kermodes
USER 10001
WORKDIR /home/kermodes
ENTRYPOINT ["kermodes"]
