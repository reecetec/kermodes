# syntax=docker/dockerfile:1
# The one Kermodes image (architecture.md §16.1): Python slim and the kermodes wheel, which runs
# every role (`kermodes weir`, `kermodes ui`, ...). Built once per release and promoted as is.

ARG RUST_VERSION=1.98.1

# The web app and the notebook widget, which the wheel serves from kermodes/static.
FROM oven/bun:1.4.2 AS web
WORKDIR /src/web
COPY web/ ./
RUN bun install --frozen-lockfile && bun run build

FROM ghcr.io/astral-sh/uv:0.13.7-python3.14-trixie-slim AS wheel
ARG RUST_VERSION
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && \
    apt-get install --yes --no-install-recommends build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --default-toolchain "$RUST_VERSION"
WORKDIR /src
COPY . .
COPY --from=web /src/python/kermodes/static python/kermodes/static
RUN uv build --wheel --out-dir /dist

FROM python:3.14-slim-trixie AS runtime
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
RUN pip install --no-cache-dir /tmp/dist/*.whl && \
    rm -rf /tmp/dist && \
    useradd --create-home --uid 10001 kermodes
USER 10001
WORKDIR /home/kermodes
ENTRYPOINT ["kermodes"]
