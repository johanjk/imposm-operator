# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# Runner image: imposm (cgo: GEOS, LevelDB) + imposm-operator, non-root.
# Base images by tag and digest (Dependabot updates both; see
# .github/dependabot.yml).
# imposm is pinned to a master commit, not v0.14.2: v0.14.2 updates chained
# generalized tables (e.g. roads_gen0 <- roads_gen1) from the base table in
# diff mode, so they drift from a fresh import after the first diff. Fixed
# upstream after v0.14.2 (GeneralizedTableSpec.InsertSQL uses
# SourceGeneralized). Move back to a tag once a release contains the fix.
ARG IMPOSM_REF=3d25f252a2d7627e1a2600b281c46cc8ec183698

FROM golang:1.26.10-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS imposm
ARG IMPOSM_REF
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgeos-dev libleveldb-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git init -q . && git remote add origin https://github.com/omniscale/imposm3.git \
 && git fetch -q --depth 1 origin "${IMPOSM_REF}" && git checkout -q FETCH_HEAD
# Fixes not (yet) upstream, see containers/patches/*.patch.
COPY containers/patches /patches
RUN for p in /patches/*.patch; do git apply --verbose "$p"; done
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X github.com/omniscale/imposm3.Version=${IMPOSM_REF}" \
      -o /out/imposm ./cmd/imposm

FROM golang:1.26.10-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS operator
ARG VERSION=dev
# set by docker/metadata-action in CI (the image version)
ARG DOCKER_META_VERSION
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api api
COPY cmd cmd
COPY internal internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${DOCKER_META_VERSION:-${VERSION}}" \
      -o /out/imposm-operator ./cmd/imposm-operator

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgeos-c1v5 libleveldb1d ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 65532 --user-group --no-create-home --home-dir /var/lib/imposm-operator/data --shell /usr/sbin/nologin runner \
 && install -d -o 65532 -g 65532 /var/lib/imposm-operator /var/lib/imposm-operator/data
COPY --from=imposm /out/imposm /usr/local/bin/imposm
COPY --from=operator /out/imposm-operator /usr/local/bin/imposm-operator
USER 65532:65532
WORKDIR /var/lib/imposm-operator/data
ENV HOME=/var/lib/imposm-operator/data
ENTRYPOINT ["/usr/local/bin/imposm-operator"]
CMD ["run"]
LABEL org.opencontainers.image.source=https://github.com/johanjk/imposm-operator \
      org.opencontainers.image.description="imposm-operator runner (imposm + sync engine)"
