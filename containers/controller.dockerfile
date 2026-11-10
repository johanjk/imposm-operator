# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
# Controller image: static binary on distroless, non-root. Also serves the
# test fixture server used by the e2e suite.
# Base images by tag and digest (Dependabot updates both; see
# .github/dependabot.yml).
FROM golang:1.26.10-bookworm@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS build
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

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/imposm-operator /imposm-operator
USER 65532:65532
ENTRYPOINT ["/imposm-operator"]
CMD ["controller"]
LABEL org.opencontainers.image.source=https://github.com/johanjk/imposm-operator \
      org.opencontainers.image.licenses=MIT
