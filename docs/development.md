# Development

Go is the only build requirement for the unit tests; the chaos suites need
Docker, the e2e needs kind and Helm (or nix + KVM for `make e2e-vm`). See
[design.md](design.md#testing) for what each tier covers.

```sh
make build test      # unit, scenario and envtest controller tests
make fuzz            # all fuzz targets (FUZZTIME=30s each)
make chaos           # chaos suites (Docker; builds the runner image)
make e2e             # kind + CloudNativePG + Chaos Mesh, all modes
make e2e-vm          # the same inside a NixOS VM (nix + KVM): for rootless
                     # Docker, where Chaos Mesh network/IO faults can't work
hack/minutely-smoketest.sh   # live minutely diffs (openstreetmap.fr) vs osmium
make generate        # deepcopy, CRD, JSON schemas
make fixtures reference   # rebuild the Monaco fixture and reference.rows.json.gz
make docker          # controller, runner and e2e images (docker buildx bake)
```

## Images and releases

The images are built with `docker buildx bake` from
[`docker-bake.hcl`](../docker-bake.hcl): `docker buildx bake` builds the
controller and runner for the local platform, `docker buildx bake e2e` the
e2e helper image, and `make docker*` does the same with the Makefile's tags.
Base images are pinned by tag and digest in `containers/*.dockerfile`, and
Dependabot ([`.github/dependabot.yml`](../.github/dependabot.yml)) updates
them, the Go modules and the actions monthly.

CI ([`ci.yaml`](../.github/workflows/ci.yaml)) runs the tests, builds the
images (nothing pushed), and lints the workflows (actionlint, zizmor); its
`check` job is the one required check. On master and on `v*` tags,
[`deployments.yaml`](../.github/workflows/deployments.yaml) waits for that
check and pushes `ghcr.io/<owner>/imposm-operator` and
`imposm-operator-runner` for amd64 and arm64 (bake group `release`), tagged
by docker/metadata-action (`sha-…`, the branch, `X.Y.Z` and `X.Y` for tags),
with BuildKit provenance and SBOM attestations. Charts are published by
[`charts.yaml`](../.github/workflows/charts.yaml).
