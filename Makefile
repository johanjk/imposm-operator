GO        ?= go
TAG       ?= dev
REGISTRY  ?= ghcr.io/johanjk
CONTROLLER_IMAGE ?= $(REGISTRY)/imposm-operator:$(TAG)
RUNNER_IMAGE     ?= $(REGISTRY)/imposm-operator-runner:$(TAG)
E2E_IMAGE        ?= $(REGISTRY)/imposm-operator-e2e:$(TAG)
CONTROLLER_GEN   ?= $(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0
SETUP_ENVTEST    ?= $(GO) run sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.24
FUZZTIME  ?= 30s
export CGO_ENABLED ?= 0

FUZZ_TARGETS = \
	./internal/osm:FuzzParseState ./internal/osm:FuzzSeqPath ./internal/osm:FuzzParseSeqPath \
	./internal/osm:FuzzParseMD5 ./internal/osm:FuzzPBFHeader ./internal/osm:FuzzSummarizeChanges \
	./internal/pgconf:FuzzPgpassParse ./internal/pgconf:FuzzPgpassMatch \
	./internal/mapping:FuzzMappingMerge ./internal/imposm:FuzzArgs ./internal/spec:FuzzSpecDecode \
	./internal/expire:FuzzSplitLines ./internal/limitto:FuzzValidate \
	./internal/mapping:FuzzTableRenames ./internal/engine:FuzzParseProgressLog \
	./internal/runner:FuzzFitTerminationMessage \
	./internal/controller:FuzzApplyResult ./internal/controller:FuzzRunnerEgress \
	./internal/strutil:FuzzTruncate

.PHONY: all build generate test envtest fuzz chaos chaos-orchestration chaos-imposm \
	docker docker-controller docker-runner docker-e2e helm-lint e2e e2e-vm controller-manifests fixtures reference smoke clean

all: generate build test helm-lint

build:
	$(GO) build -o bin/imposm-operator ./cmd/imposm-operator

## generate: deepcopy, CRD, JSON schemas and the chart's CRD copy
generate:
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:dir=config/crd
	$(GO) run ./hack/genschema

## envtest: download kube-apiserver/etcd for the controller tests
envtest:
	$(SETUP_ENVTEST) use -p path --bin-dir $(CURDIR)/testbin >/dev/null

## test: unit, scenario and controller (envtest) tests
test: envtest
	$(GO) vet ./...
	$(GO) test ./...

## fuzz: run every fuzz target for FUZZTIME
fuzz:
	@set -e; for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; fn=$${t##*:}; echo "== $$fn"; \
		$(GO) test $$pkg -run '^$$' -fuzz "^$$fn$$" -fuzztime $(FUZZTIME); \
	done

## chaos: both in-process chaos suites (need docker; chaos-imposm needs the runner image)
chaos: chaos-orchestration chaos-imposm

chaos-orchestration:
	$(GO) test -tags chaos -run 'TestOrchestrationChaos|TestMoveTablesOnPostgres' -count=1 -v -timeout 20m ./internal/chaos

chaos-imposm: docker-runner
	RUNNER_IMAGE=$(RUNNER_IMAGE) $(GO) test -tags chaos -run TestImposmChaos -count=1 -v -timeout 40m ./internal/chaos

docker: docker-controller docker-runner docker-e2e

# Images are built with docker buildx bake (docker-bake.hcl), as in CI.
BAKE = docker buildx bake --load --set '*.args.VERSION=$(TAG)'

docker-controller:
	$(BAKE) --set 'controller.tags=$(CONTROLLER_IMAGE)' controller

docker-runner:
	$(BAKE) --set 'runner.tags=$(RUNNER_IMAGE)' runner

docker-e2e:
	$(BAKE) --set 'e2e.tags=$(E2E_IMAGE)' e2e

helm-lint:
	helm lint charts/imposm-operator-crds
	@for m in crd configmap; do for s in cluster namespace; do \
		helm lint charts/imposm-operator --quiet --set mode=$$m --set rbac.scope=$$s >/dev/null || exit 1; \
		helm template t charts/imposm-operator --set mode=$$m --set rbac.scope=$$s >/dev/null \
		|| exit 1; echo "linted and rendered mode=$$m scope=$$s"; done; done

## e2e: kind + CloudNativePG + Chaos Mesh, crd and configmap mode
##   CHAOS=1 (default) all experiments, CHAOS=pod pod kills only (rootless docker), CHAOS=0 none
e2e: docker
	TAG=$(TAG) CHAOS=$(or $(CHAOS),1) test/e2e/run.sh all

## controller-manifests: the objects the controller creates for a few
##   representative syncs, as YAML for the chart linters (build/controller-manifests)
controller-manifests:
	rm -rf build/controller-manifests
	CONTROLLER_MANIFESTS=$(CURDIR)/build/controller-manifests $(GO) test -count=1 -run '^TestControllerManifests$$' ./internal/controller/

## e2e-vm: `make e2e` inside a NixOS VM with rootful Docker, for hosts where
##   kind can't run NetworkChaos/IOChaos (needs nix + KVM; see hack/e2e-vm/run.sh)
e2e-vm:
	hack/e2e-vm/run.sh

## fixtures: rebuild the frozen Monaco fixture (needs osmium); then run `make reference`
fixtures:
	hack/fixtures.sh

## reference: regenerate test/fixtures/monaco/reference.rows.json.gz with the real runner image
reference: docker-runner
	RUNNER_IMAGE=$(RUNNER_IMAGE) hack/reference.sh

clean:
	rm -rf bin testbin
