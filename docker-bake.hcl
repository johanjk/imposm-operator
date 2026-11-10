# Image builds: `docker buildx bake` (controller and runner), `docker buildx
# bake e2e`, or the Makefile's docker targets. CI builds the default group
# on every push and pull request; .github/workflows/deployments.yaml pushes
# the release group to GHCR once CI passed on master or a v* tag.

variable "REGISTRY" {
  default = "ghcr.io/johanjk"
}

variable "TAG" {
  default = "dev"
}

# The version compiled into the binaries (main.version). In CI the image
# version from docker/metadata-action (DOCKER_META_VERSION) takes over.
variable "VERSION" {
  default = "dev"
}

group "default" {
  targets = ["controller", "runner"]
}

# The published images, for both architectures. A local multi-platform
# build emulates the foreign one for the cgo imposm stage (slow); CI builds
# each architecture natively (deployments.yaml sets the platform).
group "release" {
  targets = ["controller-release", "runner-release"]
}

# Overridden in CI by docker/metadata-action's bake files (tags and labels),
# one per image (its bake-target input).
target "docker-metadata-action-controller" {
  tags = ["${REGISTRY}/imposm-operator:${TAG}"]
}

target "docker-metadata-action-runner" {
  tags = ["${REGISTRY}/imposm-operator-runner:${TAG}"]
}

target "_common" {
  context = "."
  args = {
    VERSION = VERSION
  }
}

# The images for the platform building them, e.g. for local use.
target "controller" {
  inherits   = ["_common", "docker-metadata-action-controller"]
  dockerfile = "containers/controller.dockerfile"
}

target "runner" {
  inherits   = ["_common", "docker-metadata-action-runner"]
  dockerfile = "containers/runner.dockerfile"
}

# Fixture server, the frozen Monaco fixture and the e2e checkers (not
# published).
target "e2e" {
  inherits   = ["_common"]
  dockerfile = "containers/e2e.dockerfile"
  tags       = ["${REGISTRY}/imposm-operator-e2e:${TAG}"]
}

target "controller-release" {
  inherits  = ["controller"]
  platforms = ["linux/amd64", "linux/arm64"]
}

target "runner-release" {
  inherits  = ["runner"]
  platforms = ["linux/amd64", "linux/arm64"]
}
