# Documentation

- [Deployment options](deployment.md): the two modes (`crd`, `configmap`),
  what each needs, how to install and operate them, and running the runner
  without the operator.
- [Database](database.md): the rights a sync's user needs, setup SQL, and
  read-only access for consumers.
- [Features](features.md): upstreams, actions, expire tiles, re-imports and
  renames, storage, monitoring, security, several regions, passwords.
- [Design](design.md): architecture, the sync state machine, workspaces,
  trust model, credentials and the test tiers.
- [Monitoring](monitoring.md): Prometheus metrics, alerts and what to do
  when they fire.
- [State machines](state-machine.md): Mermaid diagrams of the import
  lifecycle, production during a re-import, the sync phase and one run.
- [Development](development.md): building and testing.
- [OSM metadata, changesets and audit trails](design-osm-metadata.md):
  `spec.metadata`: changeset ids, element versions, history and changesets
  next to imposm's tables (package `internal/osmmeta`).
- [Problems found by the tests](imposm-issues.md): imposm bugs the tests
  found, and how the operator works around them.

The spec reference is [`api/v1alpha1/types.go`](../api/v1alpha1/types.go)
(JSON Schema: [`config/schema/`](../config/schema/imposmsyncspec.schema.json));
[`examples/`](../examples) has `ImposmSync` resources (minimal, with expire
tiles), a ConfigMap sync, Helm values for Geofabrik regions, the planet and
openstreetmap.fr, and a CloudNativePG database.
