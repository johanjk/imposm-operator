# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
# Controller image: static binary on distroless, non-root. Also serves the
# test fixture server used by the e2e suite.
# Base images by tag and digest (Dependabot updates both; see
# .github/dependabot.yml).
# Pure Go: built on the build machine's platform, cross-compiled for the
# target (no emulation for multi-platform builds).
FROM --platform=$BUILDPLATFORM golang:1.27.2-bookworm@sha256:5cf287a799e6b94384bad13d16b14904c531f51ba65792237e122ce42b392f61 AS build
ARG TARGETOS TARGETARCH
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
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=${DOCKER_META_VERSION:-${VERSION}}" \
      -o /out/imposm-operator ./cmd/imposm-operator

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/imposm-operator /imposm-operator
USER 65532:65532
ENTRYPOINT ["/imposm-operator"]
CMD ["controller"]
LABEL org.opencontainers.image.source=https://github.com/johanjk/imposm-operator \
      org.opencontainers.image.licenses=MIT
