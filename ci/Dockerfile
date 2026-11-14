# syntax=docker/dockerfile:1
# The image CI jobs and the release's wheel build run in: what each run installed before (about
# 35 s), baked once. Rebuilt by .gitea/workflows/ci-image.yaml when this file changes; the tag
# (CI_IMAGE there and in ci.yaml) names what is in it, so bump it with any change here.
# Image names are fully qualified: CI builds with buildah, which does not guess registries.

FROM ghcr.io/astral-sh/uv:0.13.7-python3.14-trixie-slim

ARG RUST_VERSION=1.98.1
ARG BUN_VERSION=1.4.2
ARG SCCACHE_VERSION=0.19.0
ARG SCCACHE_SHA256=aa73c7270c997894038c0d8f0d297865f57569e1687883322bc431713d4bb9dd

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    BUN_INSTALL=/usr/local/bun \
    UV_LINK_MODE=copy \
    PATH=/usr/local/cargo/bin:/usr/local/bun/bin:$PATH

# postgresql: the Postgres catalog's tests run against one started in the job's own container.
RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
        build-essential ca-certificates curl git nodejs postgresql unzip && \
    rm -rf /var/lib/apt/lists/* && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --profile minimal --default-toolchain "$RUST_VERSION" \
        --component clippy,rustfmt && \
    curl -fsSL https://bun.sh/install | bash -s "bun-v$BUN_VERSION" && \
    curl -fsSL -o /tmp/sccache.tar.gz \
        "https://github.com/mozilla/sccache/releases/download/v$SCCACHE_VERSION/sccache-v$SCCACHE_VERSION-x86_64-unknown-linux-musl.tar.gz" && \
    echo "$SCCACHE_SHA256  /tmp/sccache.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/sccache.tar.gz -C /tmp && \
    install -m 0755 "/tmp/sccache-v$SCCACHE_VERSION-x86_64-unknown-linux-musl/sccache" /usr/local/bin/sccache && \
    rm -rf /tmp/sccache* && \
    rustc --version && cargo clippy --version && bun --version && sccache --version
