# Features

- Spec reference: [`api/v1alpha1/types.go`](../api/v1alpha1/types.go), JSON Schema in
  [`config/schema/`](../config/schema/imposmsyncspec.schema.json).
- Upstreams: any mirror laid out like planet.openstreetmap.org or Geofabrik
  (a PBF, an optional `.md5`, an osmosis replication directory), daily,
  hourly or minutely. Examples: Geofabrik regions with daily diffs
  ([`imposmsync.yaml`](../examples/imposmsync.yaml),
  [`imposmsync-regions.yaml`](../examples/imposmsync-regions.yaml)), the planet with
  hourly or minutely diffs ([`imposmsync-planet.yaml`](../examples/imposmsync-planet.yaml);
  `source.replicationBaseURL` takes the diffs from another server than the
  snapshot, e.g. a nearby mirror for the snapshot),
  and openstreetmap.fr regions with minutely diffs
  ([`imposmsync-osmfr.yaml`](../examples/imposmsync-osmfr.yaml)). Downloads resume after
  interruptions, and never splice two versions of a moving `-latest` file.
- Actions: `sync`, `deploy`, `revert-deploy`, `remove-backup`, `reimport`.
  Trigger them with the annotation `imposm.johanjk.github.io/action` on the
  sync, or `imposm-operator run --action` when running the runner yourself.
- Expire tiles for cache invalidation (`imposm.expireTiles`): imposm's
  standard expire-tile files (`z/x/y` lines) on a shared volume, for any tool
  that reads them, and/or a Redis stream in zaptiles' format. Only tiles of
  committed changes are sent, at least once; `0/0/0` is sent when the changes
  are unknown (e.g. a new import was deployed).
- Mappings in imposm's format, referenced from ConfigMaps and split into
  parts. Validated before any import: unknown table and column types, a
  filter on a key imposm would not load (imposm keeps only the keys the
  mapping names unless `tags.load_all` is set, so such a filter never
  matches) is `MappingValid=False`; a `hstore_tags`/`jsonb_tags` column
  without `tags.load_all` (it holds only the mapped keys) a warning in the
  condition's message. After each run, production tables without rows are
  listed in `status.emptyTables` (condition `EmptyTables`). The runner's
  imposm adds a `jsonb_tags` column type (all tags, or those in
  `args.include`, as a JSONB object; no hstore extension needed), registers
  generalized tables with their SRID, and leaves `type=multipolygon` out of
  polygons built from multipolygon relations, see
  [`containers/patches/`](../containers/patches/) and
  [imposm-issues.md](imposm-issues.md).
- OSM metadata (`spec.metadata`): each element's version, timestamp and
  changeset, optionally every version (a full audit trail of what changed)
  and the changesets themselves, in a schema of their own (`osm_history` for production in `osm`)
  joinable to imposm's rows (and a view per table with them joined, e.g.
  `osm_history.osm_roads`), with retention. Versions and timestamps work
  with any source; changesets need one with changeset ids (the public
  Geofabrik downloads have none). See
  [design-osm-metadata.md](design-osm-metadata.md).
- Re-imports without growing lag: while a new import is built (a mapping,
  SRID, prefix, limitTo or schema change, or the `reimport` action),
  production keeps receiving diffs with its own frozen configuration and
  cache (`deploy.syncDuringReimport: auto|always|never`, default `auto`: when
  the volume has room for a second imposm cache). Size `storage` for two
  caches to keep that during re-imports.
- Renames without re-import: changing only the schemas, the table prefix or
  a table's name (same definition) moves and renames the existing tables.
  After a deploy, tables that left the mapping or the schemas are dropped
  (only this sync's, never with CASCADE); a new import that would overwrite
  production's tables is refused.
- Storage for the imposm cache and downloads (`storage`): a PVC the operator
  creates (default), an existing PVC (`existingClaim`), or a persistent
  volume source (`volume`: `persistentVolumeClaim`, `csi` or `nfs`), e.g. an
  inline CSI volume:
  ```yaml
  storage:
    volume:
      csi: {driver: nfs.csi.k8s.io, volumeAttributes: {server: nfs.example, share: /imposm/norway}}
  ```
  The expire-tile files volume (`imposm.expireTiles.files.volume`) takes the
  same three kinds. Other volume sources (hostPath, emptyDir, Secrets, ...)
  are not accepted, and in crd and configmap mode `csi` and `nfs` need the
  admin's `runnerPolicy.allowedVolumeTypes` (and `allowedCSIDrivers`): see
  the trust model in [design.md](design.md#trust-model).
- Status of the data: `status.currentSequence` and `currentTimestamp`
  (the replication time the data is up to), and per run
  `status.lastRunDiffRanges`: the diffs it applied (sequences, and the
  replication times from the data before them to the data after), also in
  the `LastRunSucceeded` message, e.g. `applied 1 diff to production: 4966
  (changes up to 2026-11-09T21:22:17Z)`.
- Monitoring: the controller exports per-sync Prometheus metrics
  (`imposm_sync_lag_seconds`, `_current_sequence`, `_ready`, `_running`,
  `_consecutive_failures`, `_storage_available_bytes`, `_expire_pending`,
  `_runs_total`, …). With the Prometheus Operator installed the chart also
  creates a ServiceMonitor and alerts (lagging, failing, storage low, expire
  tiles stuck, conflict, production paused, re-import headroom, unused
  tables). See [monitoring.md](monitoring.md).
- Hardened by default: non-root, read-only root filesystem, no capabilities;
  each sync's runner pods have their own ServiceAccount (`<sync>-imposm-runner`,
  no RBAC bindings) and mount no token; NetworkPolicies for every pod,
  toggled by `networkPolicy.ingress` / `networkPolicy.egress` (controller:
  only metrics/probes in, only DNS and the Kubernetes API out; runners, one
  policy per sync: nothing in, out only DNS, the sync's database and Redis
  ports to `networkPolicy.runner.internalTo` (default: any pod, any address
  but link-local), and its upstream's ports on public addresses); a PodDisruptionBudget
  and spreading over nodes and zones for the controller; metrics over HTTPS
  with Kubernetes authn/authz when the operator has cluster scope.
- Runner pods: the image is the operator admin's (chart values / controller
  flags) and the service account the operator's own per-sync one, never the
  sync's choice. A sync sets resources, pull secrets and policy, and, as the
  admin's `runnerPolicy` allows, `runner.labels` and `runner.annotations`
  (key prefixes; none by default), node selection and tolerations (allowed
  by default); the admin can add a node selector and tolerations to every
  runner. A spec asking for more is `Invalid`, with reason `RunnerPolicy` on its `SpecValid` and `Ready` conditions.
- Several regions in one database: separate schemas per sync (recommended),
  one sync over a larger extract with `imposm.limitTo` polygons, or
  `imposm.tablePrefix`. Syncs that would write the same tables are refused
  (phase `Conflict`).
- Passwords: `database.pgpassSecretRef` (libpq password file, e.g. CNPG's
  `<cluster>-app` secret, key `pgpass`) or `database.passwordSecretRef`.
- Connection from a Secret: `database.secretRef` names a Secret with the
  keys `host`, `port`, `dbname`, `user` and `sslmode` (each optional, the
  keys of CNPG's `<cluster>-app` secret), which win over the spec's fields;
  `host`, `database` and `user` may then be left out of the spec. Only the
  runner reads it (mounted as files). `database.port` stays in the spec, as
  the runner's NetworkPolicy opens it: a different port in the Secret fails
  the run with a message to set it. The password still comes from
  `pgpassSecretRef` or `passwordSecretRef`, which may name the same Secret:
  ```yaml
  database:
    secretRef: {name: osm-app}
    pgpassSecretRef: {name: osm-app, key: pgpass}
  ```
