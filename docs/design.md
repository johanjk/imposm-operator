# Design

## Components

```
                 ┌──────────────── always running (crd / configmap modes) ───────────────┐
 ImposmSync CR ─▶│ controller: renders spec+merged mapping ConfigMap, data PVC, CronJob; │
 or labelled CM  │ starts action Jobs; reads run results from pod termination messages  │
                 └───────────────────────────────┬───────────────────────────────────────┘
                                                 │ CronJob / Job (on demand)
                                                 ▼
                 ┌──────────── runner pod: imposm-operator run + imposm ─────────────┐
 mirror  ◀─HTTP──│ engine: download, import, deploy, diffs; records state; writes   │──▶ PostGIS
 (Geofabrik)     │ JSON result to /dev/termination-log                               │    (CNPG)
                 └───────────────────── data PVC: imposm cache, downloads ───────────┘
```

- The **controller** has no database credentials and runs no imposm. It only
  needs namespaced access to ConfigMaps, PVCs, CronJobs, Jobs and Pods (plus
  ImposmSyncs in CRD mode).
- The **runner** has no Kubernetes API access (token not mounted). It does one
  bounded unit of work per run and exits.
- The chart installs only the operator; syncs and mapping ConfigMaps are
  created separately.

## One spec, two front-ends

`api/v1alpha1.ImposmSyncSpec` is the only spec type. `internal/spec` defaults
and validates it identically for:

| mode | where the spec lives | status | needs |
|------|----------------------|--------|-------|
| `crd` | `ImposmSync` resource | `.status`, printer columns, conditions | the CRD, from the separate `imposm-operator-crds` chart (installed once by an admin); the operator itself can be namespace-scoped |
| `configmap` | ConfigMap labelled `imposm.johanjk.github.io/sync=true`, key `spec.yaml` | sibling ConfigMap `<name>-status` (the user's ConfigMap is never written, so GitOps tools see no drift) | a namespaced Role |

ConfigMaps lack admission validation, a status subresource and printer
columns. To compensate, the controller validates with the same code as the
CRD's markers, reports `SpecValid`/`MappingValid` conditions in the status
ConfigMap, and `config/schema/imposmsyncspec.schema.json` (generated from the
CRD) can validate specs in CI or in an editor.

## Mapping

imposm's runtime config (connection, cachedir, srid, expire tiles) is
generated from typed spec fields. The mapping is referenced
(`mapping.sources[].configMapRef`), not embedded in the CR. It is free-form,
can be large, and is often shared by several syncs. The parts (every key of
every referenced ConfigMap) are merged **strictly** (`internal/mapping`):

- a table or generalized table defined in two parts is an error;
- conflicting scalars are an error;
- lists are unioned.

The merged result is validated and hashed. The hash decides when a re-import
is needed. The controller hands the merged mapping to the runner inline, so
the runner needs no Kubernetes access.

## Engine state machine

Diagrams: [state-machine.md](state-machine.md).

```
pending ─download─▶ downloaded ─imposm -read─▶ read ─imposm -write─▶ written
   ─optimize─▶ optimized ─(catch up diffs in the import schema)─▶ ready
   ─deployproduction─▶ deployed ─(diffs in production, chunked, bounded)─▶ …
```

- Every completed step is recorded in `<state schema>.import`, so a killed run
  resumes at the last finished step. Downloads go to a `.part` file that is
  resumed with `Range` plus `If-Range` (the ETag or Last-Modified of the first
  response): when a `-latest` name has moved on to a new file in between, the
  server sends the new file whole instead of its tail, which the md5 would
  only catch after the whole download (95 GB for the planet).
- The snapshot and the diffs can come from different servers
  (`source.replicationBaseURL`), e.g. the planet from a mirror and the diffs
  from planet.openstreetmap.org. Changing where the diffs come from
  re-imports, as another server may be another sequence space.
- The first import is deployed as soon as it is written. Later imports (a
  mapping or source change, cache loss, or the `reimport` action) catch up in
  the import schema while production keeps serving, then deploy.
- Production keeps receiving diffs meanwhile (`deploy.syncDuringReimport`,
  default `auto`). Every import has its own workspace,
  `<data>/imports/<id>/`: its imposm cache, checkpoint and chunk marker, and
  its configuration frozen when it started (merged mapping, SRID, table
  prefix, limitTo, schemas). Each run first applies diffs to production with
  the deployed import's workspace, then advances the new import with its
  own, so a spec change never reaches an import already built or being
  built. The status keeps reporting production's sequence and lag, with the
  new import's progress in the message. Production pauses until the deploy
  instead (and the message says why) when `syncDuringReimport` is `never`;
  with `auto` when the volume has no room for a second cache (about 1.2
  times the current one, checked before the new import builds its cache);
  when the replication source changed; or when the deployed import's cache
  or configuration is not available. After the deploy the previous import's
  workspace is removed. Snapshots and diffs are shared; a diff is kept until
  every active import has applied it.
- Renames do not re-import. When only the schemas, the table prefix or table
  names change (a table renamed in the mapping with an identical definition,
  generalized tables' sources renamed along; anything ambiguous re-imports),
  the existing tables are moved (`ALTER TABLE ... SET SCHEMA`) and renamed in
  one transaction, with the indexes and owned sequences named after them;
  the import's frozen configuration and claims follow. A move whose target
  exists is refused (phase Conflict).
- If production matches the spec again while a new import is being built
  (the change was reverted, or reduced to renames), the new import is
  abandoned and production carries on; it kept syncing meanwhile.
- Tables are cleaned up, but only ever tables this sync claimed, and never
  with CASCADE (a table a user's view depends on is kept, stays claimed and
  is reported):
  - after a deploy, every claimed table the deployed import does not use is
    dropped: tables that left the mapping (imposm's deploy only rotates the
    new mapping's tables), and everything in schemas or under a prefix no
    longer used;
  - an import abandoned before its deploy has its import-schema tables
    dropped when the next one starts;
  - the backup is dropped after each deploy unless `deploy.keepBackup`.
- A new import is refused (phase Conflict) when its import schema holds
  tables production is serving or keeping as backup: it would overwrite
  them. Production keeps syncing with its frozen configuration meanwhile.
- Diffs run in chunks of `diffChunkSize`, with at most `maxDiffsPerRun` per
  run, through `imposm diff -commit-latest` (one transaction per chunk; see
  [imposm-issues.md](imposm-issues.md) for why). Every chunk is a `diff_batch` row.
- A missing diff upstream (a gap) stops the run with an error; it is never
  skipped.
- **Cache checkpoints.** imposm writes its cache before the database
  transaction commits, so an interrupted chunk leaves the two out of step.
  Before each chunk the engine clones the cache to `cache.checkpoint`, using
  reflink when the volume supports it (XFS, btrfs, ZFS: near-instant even for
  planet-sized caches) and a copy when the cache is below
  `imposm.cacheCheckpointLimit`. It then writes `chunk.inprogress`. If the
  chunk does not commit, or a later run finds the marker, the cache is
  restored before retrying. With no checkpoint available, the import is
  re-done in the import schema. For large extracts, use a reflink-capable
  storage class. Network file systems (NFS/SMB inline CSI volumes) have no
  reflink, so the checkpoint falls back to copying below the limit.
- **Commit probe.** imposm commits a chunk and only then writes
  `last.state.txt`. A kill between the two looks exactly like a kill before
  the commit, but needs the opposite treatment: rolling the cache back would
  replay the chunk's creates and duplicate rows. When a chunk ends without a
  recorded commit, the engine checks the database using the chunk's net
  effect (`osm.SummarizeChanges`):
  - an object the chunk created is present → committed (keep the cache,
    advance `last.state.txt`);
  - an object the chunk deleted is still present, or a created node that
    must be in a filter-free point table is absent → not committed (restore
    the checkpoint);
  - otherwise it is undecidable: roll back if imposm exited by itself (its
    transaction rolled back), re-import if it was killed.

  Lookups only use tables of the matching geometry type, so a new way can
  never match an old node with the same id.

### Reconciliation with imposm

imposm's `last.state.txt` (in the cache) is the authority on what the cache
and database contain:

| observation | meaning | action |
|-------------|---------|--------|
| last.state > recorded | imposm committed, then we crashed before recording | record the gap as applied ("reconciled") |
| last.state < recorded | the cache is older than the database (restored volume, DB rollback) | re-import |
| no last.state / cache marker mismatch | cache lost or belongs to another import | re-import (production keeps serving) |

### Status and progress

```
$ kubectl get imposmsync
NAME     PHASE     LAG     ACTIVITY                                       LAST RUN   AGE
norway   Syncing   6d0h    diff 4655-4657 (2/4), ways 173k                12m        2d
monaco   Syncing   23h4m   idle, next run 21:17 UTC                       43m        2d
$ kubectl get imposmsync -o wide   # + sequence, upstream, re-import progress, backup, storage, pending expire batches
```

- **Live progress (`status.activeRun`).** While a sync Job runs, the runner
  prints throttled progress records (`IMPOSM_OPERATOR_PROGRESS {json}`) on
  stderr. They carry:
  - the step, and download bytes done/total (also for resumed downloads);
  - diff chunk i/n and its sequences;
  - imposm's own step and node/way/relation counters, parsed from imposm's
    output.

  The controller reads the tail of the runner pod's log (`get pods/log`)
  every 15 s and copies the newest record into the status, so the runner
  still needs no Kubernetes API access. A pod that has not started yet is
  reported with its waiting reason (`pending: ImagePullBackOff`,
  `unschedulable: …`). The condition `Running` is true while a Job is
  active.
- **`status.activity`** is the one-line summary in the default
  `kubectl get` output: the running step, otherwise the next scheduled run,
  the waiting deploy, or the last error. **`status.lag`** is human readable.
- **From finished runs:**
  - last run duration and diffs applied, consecutive failures;
  - snapshot size;
  - **storage**: bytes used on the data volume, plus free and total and the
    production cache size, with the `StorageLow` condition below 10% free;
  - `nextRunTime`.
- **`status.production`** is the import that serves the production schema:
  its id, deploy time, applied sequence and timestamp, whether a backup is
  kept (`backupAvailable`, so `revert-deploy` works), and whether it applies
  diffs (`syncing`). While paused it carries `pausedReason`
  (`Disabled`, `NoRoom`, `SourceChanged`, `CacheUnavailable`,
  `Failed`, `Reverted`) and a message. A run that fails before it reads the
  state (database or upstream unreachable) leaves the earlier report in
  place.
- **`status.reimport`** exists while a new import is built next to
  production: its id, phase, why it started (`MappingChanged`, `SRIDChanged`,
  `LimitToChanged`, `SourceChanged`, `ConfigChanged`, `ReimportAction`,
  `CacheLost`, `Recovery`), start time, sequence and a short `progress`
  (`download`, `read`, `write`, `optimize`, `catch-up 4660/4663`,
  `awaiting deploy`). The sync's phase is then `Reimporting`; `Ready` stays
  true as long as production syncs.
- **`status.unusedTables`** lists tables the sync no longer uses but could
  not drop because something depends on them (a view, a foreign key).
- **Conditions** besides `Ready`, `LastRun`, `Running`, `MappingValid` and
  `StorageLow`:
  - `SpecApplied`: production runs the current spec; false with reason
    `Importing`, `Reimporting`, `Pending` (waits for a run or a deploy),
    `Reverted`, `TablesClaimed`, `ImportSchemaConflict` or
    `RenameTargetExists`;
  - `ProductionSyncing`: production applies diffs, with the pause reason
    when not;
  - `ReimportHeadroom`: the volume fits a second imposm cache (1.2× the
    production cache), so a re-import keeps production syncing under
    `syncDuringReimport: auto`;
  - `UnusedTablesKept`: `status.unusedTables` is not empty.
- **Events** on the sync (the ImposmSync, or the ConfigMap in ConfigMap
  mode), recorded once per finished run: `ImportStarted`,
  `ReimportStarted`, `ImportDeployed`, `ImportAbandoned`, `TablesRenamed`,
  `TablesDropped`, `UnusedTablesKept`, `RunFailed`, `RunKilled`, and on
  status transitions `ProductionPaused`, `ProductionResumed` and
  `Conflict`. `kubectl describe imposmsync` shows them.
- In ConfigMap mode the same status is in `<name>-status`.

### Metrics and alerts

The full reference with alert runbooks is [monitoring.md](monitoring.md).

The controller sets per-sync gauges from the status it computes (labels
`namespace`, `name`, `source`):

- lag, current and upstream sequence;
- ready, running, phase;
- last run, last success, last run duration, next run;
- consecutive failures, pending expire batches;
- storage used / available / total;
- `reimport_running`, `production_paused{reason}`, `unused_tables`,
  `reimport_headroom`.

`imposm_sync_runs_total{result}` and `imposm_sync_diffs_applied_total` count
finished runs. Two safeguards keep each run applied exactly once:

- Counters advance only after the status write that records the run
  succeeded, and the last counted Job is remembered in memory, because the
  informer cache can return an object before its status patch arrives.
- Status writes use an optimistic lock (`resourceVersion`), so a reconcile
  working from a stale object fails with a conflict and retries instead of,
  for example, counting a failed run twice.

Deleting a sync removes its series. Only the leader replica reconciles, so
only it exports `imposm_sync_*` series; scrape every pod (the ServiceMonitor
does), not one through the Service.

The chart serves metrics through a Service. With `rbac.scope=cluster` it uses
HTTPS with Kubernetes authentication and authorization (TokenReview and
SubjectAccessReview; scrapers need the `<release>-metrics-reader`
ClusterRole). With namespace scope it uses plain HTTP, restricted by
NetworkPolicy. When the Prometheus Operator API exists, a ServiceMonitor and a
PrometheusRule are rendered, with these alerts:

- `ImposmSyncLagging` (lag over 36 h by default);
- `ImposmSyncFailing` (3 failed runs in a row);
- `ImposmSyncStorageLow` (less than 10% free);
- `ImposmSyncExpireTilesPending` (an expire batch undelivered for 1 h);
- `ImposmSyncConflict` (phase `Conflict` or `Invalid`);
- `ImposmSyncProductionPaused` (production applies no diffs for 6 h);
- `ImposmSyncNoReimportHeadroom` (info: a re-import would pause
  production);
- `ImposmSyncUnusedTablesKept` (info).

### Expire tiles

`imposm.expireTiles` makes imposm write the tiles each diff touches, in
imposm's standard expire-tile format (`<YYYYMMDD>/<time>.tiles`, one `z/x/y`
per line, zooms up to `zoom`), for tile cache invalidation. The files sink
delivers that format unchanged, for any tool that reads imposm expire files;
the Redis sink is zaptiles' stream format.

```
imposm diff ─▶ expiretiles-staging/ ─(chunk committed)─▶ expiretiles-outbox/<entry>/
                    │ (rollback / import-schema catch-up: discarded)        │
                                                          ┌─────────────────┴──────────────┐
                                                   files sink (shared volume)      redis sink (stream)
```

- **Only committed tiles.** A chunk's tiles move from staging to the outbox
  (one atomic rename) only once the chunk committed to *production*. They are
  discarded when the chunk is rolled back, and for the catch-up of an
  undeployed import (whose tables nobody serves).
- **At least once.** The outbox lives on the data volume. Every entry is
  delivered to every sink (marker files per sink) and removed only when all
  sinks took it; delivery is retried at the start and end of every run.
  Duplicates are harmless, since invalidation is idempotent. A sink outage
  never blocks syncing; it shows as `status.expirePending` and the
  `ExpireTilesDelivered` condition.
- **Unknown changes → `0/0/0`.** The tile `0/0/0` covers the whole map, so
  invalidators that expand tiles to their descendants (zaptiles does)
  invalidate everything. It is queued (before the action, so a crash can only add an
  invalidation, never lose one):
  - when an import is deployed or reverted (production is replaced as a
    whole);
  - when a chunk's commit is only proven by the commit probe, because imposm
    may have died before writing its expire file.
- **Sinks:**
  - `files`: copies into a shared volume (`files.volume`: any volume source,
    e.g. an RWX PVC or CSI share your invalidator reads) in imposm's standard
    layout. Files are written under a temporary name and renamed, so
    a reader never sees a partial `.tiles` file, and existing files are never
    overwritten.
  - `redis`: `XADD <stream> MAXLEN ~ <maxLen> * <field> "<z/x/y lines>"`, one
    message per file (split at 1 MiB): zaptiles' stream format (its
    `[redis]` mode).
    The URL can come from a Secret (`urlSecretRef`), the password too
    (`passwordSecretRef`).
  - neither: files stay in the data volume under `expiretiles/`.
- imposm only supports expire zooms 6–18 (anything else silently becomes 14),
  so the spec enforces that range.

### Several syncs in one database

imposm's deploy replaces whole tables, so two syncs must never write the
same tables. Every run first **claims** its tables in the database-wide
registry `imposm_operator.table_claim` (always in that schema, whatever the
state schema). A claim is the concrete set `schema.<prefix><table>` over the
import, production and backup schemas, for every table and generalized table
of the mapping. If the set overlaps another sync's claim, the run stops
before imposm runs, with phase `Conflict`, the other owner, the clashing
tables, and the SQL to drop the claim if that sync no longer exists. While a
new import catches up, the old deployed tables stay claimed; they are
released right after the deploy.

Ways to put several regions into one database:

1. **Separate schemas (recommended):** give each sync its own
   `database.schemas` (import / production / backup) and, for a combined
   view, create `UNION ALL` views. Each region keeps its own lifecycle.
2. **One sync over a larger extract with `imposm.limitTo`:** e.g. Europe
   plus its replication, limited to the Norway and Monaco polygons (GeoJSON in
   EPSG:4326, inline or from a ConfigMap; `cacheBufferDegrees` keeps the
   ways crossing the edge complete). imposm clips the import and every diff to
   the polygons. Borders between adjacent regions stay consistent, at the cost
   of downloading and caching the larger extract.
3. **`imposm.tablePrefix` (not the golden path):** `<prefix>_<table>`
   instead of imposm's `osm_<table>`, for syncs that must share schemas.
   Unset by default. With a prefix the operator passes imposm a libpq
   key/value connection string: imposm's URL form quotes the value
   (`prefix='a'`) and uses it literally.

The **import identity** (`status.importHash`) is a hash of the mapping, SRID,
table prefix and limitTo. Changing any of them re-imports while production
keeps serving.

### Locking

A `flock` on the volume (protects the LevelDB cache even when Postgres is
down) plus a Postgres session advisory lock per target, verified before every
step. CronJobs use `concurrencyPolicy: Forbid`. A scheduled run that finds a
lock taken reports `Busy` and exits 0 (the next run catches up). A requested
action (deploy, revert-deploy, reimport, remove-backup) waits for the locks
instead, within its deadline, so a request is never dropped.

revert-deploy needs a backup: the import records whether the backup schema
holds the production data it replaced (`deploy.keepBackup` at deploy time,
cleared by remove-backup). Without one, and while a newer import is being
built in the import schema (which the revert would overwrite), it is refused.

## Trust model

Whoever may write a sync (an `ImposmSync`, or a labelled ConfigMap in
configmap mode) makes the controller create pods in that namespace. What that
grants is limited on purpose:

- the runner image comes from the operator's admin (chart values, controller
  flags), never from the sync; each sync's runner pods run as their own
  ServiceAccount `<sync>-imposm-runner` (created by the operator, no RBAC
  bindings, owned by the sync) and mount no token, so they never share an
  identity such as the namespace's `default` account;
- volumes are only `persistentVolumeClaim`, `csi` or `nfs`: no hostPath, no
  Secret or ConfigMap mounts beyond the ones the operator renders;
- labels and annotations under the operator's prefixes are reserved;
- the chart's `runnerPolicy` (the controller's `--runner-policy-config`)
  limits the rest of what a spec decides about its pods, and a spec asking
  for more is `Invalid` (condition reason `RunnerPolicy`). By default specs may set no runner labels or
  annotations (they could match other NetworkPolicies' selectors, opt out of
  a mesh, attach Multus networks or drop an AppArmor profile) and no NFS or
  inline CSI volumes (they reach storage the namespace has no claim on, NFS
  from the node's address); node selectors and tolerations are allowed
  unless `allowTenantPlacement` is false, and the admin can add its own;
- the controller never takes over an object it did not create: a generated
  name (`<sync>-imposm`, `-imposm-run`, `-imposm-runner`, `-imposm-data`,
  `-status`) taken by another object makes the sync `Conflict`
  (`NameTaken`) instead of adopting, changing and later garbage-collecting
  it;
- mapping references are capped (32 sources, 64 keys, no repeats), so a spec
  cannot make the controller load more than about 32 MiB.

What it still grants, as any pod in the namespace would: the runner reads the
Secrets named in the spec (`passwordSecretRef`, `pgpassSecretRef`, the Redis
refs) and sends the database password to the host the spec names. Give sync
write access only to those who may use that namespace's database Secrets.

Runner egress is restricted per sync (ports derived from the spec, upstream
on public addresses only), which keeps a compromised runner away from cloud
metadata and internal HTTP services. The database and Redis ports go to
`networkPolicy.runner.internalTo`: by default any pod and any address but
link-local (cloud metadata), because NetworkPolicy cannot match the DNS name
the spec gives; narrow it to the database's namespace or CIDR where you can.
A database or Redis port equal to an upstream port (80, 443, the source
URLs' ports) is refused, so the internal rule cannot open internal HTTP
services. The runner's HTTP client also refuses redirects from https to
http and from a public host to a literal non-public address, and diffs are
capped (2 GiB compressed, 16 GiB uncompressed), so a hostile mirror cannot
fill the volume.

The controller's own RBAC is what the operator needs, but it is broad: with
`rbac.scope=cluster` its token can create Jobs (and so pods that mount any
Secret or use any ServiceAccount) in every namespace, even without any
`secrets` permission. Prefer `rbac.scope=namespace` (one release per
namespace) where tenants are not trusted, and treat the controller's
ServiceAccount token like a cluster credential.

## Credentials

- Spec: `database.passwordSecretRef` (→ `PGPASSWORD`) or
  `database.pgpassSecretRef` (→ `PGPASSFILE`). CloudNativePG's `<cluster>-app`
  secret carries a ready `pgpass` key.
- `internal/pgconf` implements libpq's password-file rules: `*` wildcards,
  `\:` and `\\` escapes, first match wins, sockets match `localhost`, and
  files readable by group or others are ignored.
- Kubernetes Secret volumes are root-owned, so a non-root pod reads them
  through `fsGroup`. The runner therefore accepts group-readable (never
  world-readable) password files when `--pgpass-allow-group-readable` is set,
  which the controller and chart set automatically.
- imposm gets the password only through its process environment, never in its
  config file or the logs.

## Testing

| layer | what | where |
|-------|------|-------|
| unit | parsers, pgpass, mapping merge, arg builder, spec validation, process-group kill | `internal/*/*_test.go` |
| scenario | engine against the fixture server, fake imposm (models the uncommitted-cache and commit-window hazards, verified by mutation), in-memory store: first import, daily diffs, bounded backlog, gaps, a crash at every fault point with and without the commit probe, lost records, mapping change, cache loss, cache-behind-DB, manual deploy/revert, header-less snapshot, corrupt snapshot | `internal/engine` |
| fuzz | `state.txt`, sequence paths, md5, PBF header, pgpass parse/match, mapping merge (order independence, idempotence), imposm args (no flag injection), spec decode (every accepted spec yields valid upstream, DB config and imposm args) | `make fuzz` |
| controller | envtest: both sources, rendering, status from runs, actions, mapping errors, CRD validation | `internal/controller` |
| chaos, orchestration | real Postgres, fake imposm, 15% HTTP faults of seven kinds, random imposm crashes, backend kills every 100–300 ms, a Postgres restart, three racing runners | `make chaos-orchestration` |
| chaos, real imposm | runner image + PostGIS + Redis: pod SIGKILL at random times, imposm SIGKILL mid-import/mid-diff, backend kills during diffs, sequence gap, truncated diff. Every scenario must match the reference rows (`reference.rows.json.gz`; same rows and attributes, geometries within 5 cm), and both expire sinks (files and Redis stream) must hold every tile of the fault-free run unless an extra `0/0/0` covers them | `make chaos-imposm` |
| e2e | kind + CloudNativePG + Chaos Mesh in all three modes, with expire tiles to a PVC and a Redis stream: runner kill, controller leader kill, packet loss to Postgres, slow/lossy mirror, cache I/O latency, then the reference rows (`reference.rows.json.gz`) and the reference expire tiles (`reference.tiles`, checked on the PVC and in the stream by `hack/expirecheck`). `CHAOS=pod` runs only the pod kills. Network and I/O experiments need a rootful Docker host with legacy iptables (Linux 6.18+ may be built without it) and netem: rootless kind cannot load those modules, and IOChaos cannot get `/dev/fuse` through the device cgroup. `make e2e-vm` runs the whole thing in a NixOS VM (`hack/e2e-vm`) for such hosts. Not run in GitHub CI (about 20 minutes) | `make e2e`, `make e2e-vm` |

### Larger scale: Norway

`hack/scaletest.sh` runs the same pipeline on a country, e.g. a Norway fixture
built with `REGION=norway OUT=… hack/fixtures.sh` (not committed, about
2.7 GB):

- the yearly `norway-260101` snapshot plus 10 "days" derived from today's
  `norway-latest`: 4.57 million changed objects;
- then a direct import of `norway-latest` as a second sync in the same
  database;
- then a server-side comparison (`tablesums -compare-schema`).

The first "day" of node changes and the first of way changes take minutes,
the others seconds. That is the split, not the operator: day 1 holds 656k
modifications of existing nodes, which imposm must resolve to their ways and
relations, while days 2–4 are mostly new nodes nothing depends on yet.

Generalized tables are compared within their simplification tolerance: two
simplifications of inputs that differ by centimetres can choose vertices up
to the tolerance apart. Ways that are both roads and railways exposed
imposm's duplicated generalized rows (see [imposm-issues.md](imposm-issues.md)).

### Minutely diffs from a live upstream

Every other real-data test uses daily diffs. `hack/minutely-smoketest.sh`
covers minutely ones against a live upstream (openstreetmap.fr, default
Corse):

- sync "live" imports the extract from download.openstreetmap.fr and catches
  up its minutely diffs, then runs a few more times minutes apart, as in
  production;
- the expected data is built independently: the snapshot the runner
  downloaded plus the same diffs, merged by `osmium apply-changes`, imported
  as a second sync from a fixture server pinned to the same sequence;
- both are compared with `tablesums -compare-schema`.

First run (2026-11-09): import plus 437 minutely diffs in one run (155 s),
then three rounds of 5, 2 and 2 new minutes (5 s each); 446 diffs in all,
equivalent to the direct import within 10 cm. The snapshot header names
`http://…/replication/./europe/france/corse/minute` while the spec uses
`https://…/replication/europe/france/corse/minute/`; the engine accepted the
header's sequence after matching it against the configured stream by
timestamp.

### Fixture and reference

Geofabrik keeps only about 130 days of daily diffs and only yearly snapshots,
so an old snapshot cannot be chained to real diffs. The fixture
(`test/fixtures/monaco`) therefore uses:

- the real `monaco-260101.osm.pbf` (sequence 4651), and
- `osmium derive-changes` to the 2026-11-07 `monaco-latest.osm.pbf`, split
  into 10 referentially consistent daily diffs (4652–4661).

That is ten months of real edits with synthetic day boundaries.

The reference (`reference.rows.json.gz`) is a row dump of the production
tables after the fault-free run. Results are compared **tolerantly**: row
counts and attributes must be equal, geometries within 5 cm, float columns
(areas) within 0.1%. They cannot be compared byte for byte, because imposm
rounds coordinates differently depending on how diffs are grouped into
transactions: a direct import, one diff per run, and chunked
`-commit-latest` runs all differ in the last digit. `hack/reference.sh` also
checks the reference against a direct import of the target PBF, and records
the fault-free run's expire tiles as `reference.tiles`.
