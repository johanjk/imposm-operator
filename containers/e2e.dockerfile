# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
# e2e helper image: fixture server (imposm-operator fixture-server), the frozen
# Monaco fixture with reference.rows.json.gz and reference.tiles, and the
# tablesums and expirecheck checkers.
# Base images by tag and digest (Dependabot updates both; see
# .github/dependabot.yml).
FROM --platform=$BUILDPLATFORM golang:1.27.2-bookworm@sha256:5cf287a799e6b94384bad13d16b14904c531f51ba65792237e122ce42b392f61 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api api
COPY cmd cmd
COPY internal internal
COPY hack/tablesums hack/tablesums
COPY hack/expirecheck hack/expirecheck
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/imposm-operator ./cmd/imposm-operator && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/tablesums ./hack/tablesums && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/expirecheck ./hack/expirecheck

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/ /usr/local/bin/
COPY test/fixtures/monaco /fixtures/monaco
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/imposm-operator", "fixture-server", "--dir", "/fixtures/monaco"]
