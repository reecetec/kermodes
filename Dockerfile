# syntax=docker/dockerfile:1
# The one Kermodes image (architecture.md §16.1): Python slim and the kermodes wheel, which runs
# every role (`kermodes weir`, `kermodes ui`, ...). It installs the wheel CI built for the
# candidate (dist/, manylinux x86_64), the very file a release later publishes; a release
# retags this image, never rebuilds it. For a local image: `uv build --wheel --out-dir dist`.

# uv, as CI's: environment builds read projects' uv.lock and install with it (§10.6).
FROM ghcr.io/astral-sh/uv:0.13.7 AS uv

FROM docker.io/library/python:3.14-slim-trixie
ARG IMAGE_CREATED
ARG IMAGE_REVISION
ARG IMAGE_SOURCE
ARG IMAGE_VERSION
LABEL org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.source="${IMAGE_SOURCE}" \
      org.opencontainers.image.title="Kermodes" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${IMAGE_VERSION}"
COPY dist/ /tmp/dist/
COPY --from=uv /uv /usr/local/bin/uv
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
