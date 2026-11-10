# Monitoring

The controller exports Prometheus metrics for each sync. The chart can also install a ServiceMonitor and a set of alerts.

## Scraping

| | `rbac.scope=cluster` | `rbac.scope=namespace` |
|---|---|---|
| Endpoint | HTTPS on port 8443 (`metrics`) | plain HTTP on port 8080 (`metrics`) |
| Access | Kubernetes authentication and authorization: TokenReview, then SubjectAccessReview for `get` on `/metrics` | restricted by the controller's NetworkPolicy |
| Scraper needs | a binding to the `<release>-metrics-reader` ClusterRole | to be allowed by the NetworkPolicy ingress |

`metrics.secure` overrides the choice (`auto`, `true`, `false`). The chart creates a `<release>-metrics` Service. With the Prometheus Operator installed (`serviceMonitor.enabled: auto`), it also creates a ServiceMonitor that scrapes every 60 s (`serviceMonitor.interval`).

Only the leader replica reconciles, so only it exports the `imposm_sync_*` series. Scrape every pod, as the ServiceMonitor does (it targets the endpoints), not one pod through the Service. A leader change briefly ends the series on one pod and starts them on another.

Example: let a Prometheus running as `monitoring/prometheus-k8s` scrape the secure endpoint:

```sh
kubectl create clusterrolebinding imposm-metrics-reader \
  --clusterrole=imposm-operator-metrics-reader \
  --serviceaccount=monitoring:prometheus-k8s
```

## Metrics

Every series has the labels `namespace` and `name` (of the ImposmSync, or of the ConfigMap in `configmap` mode) and `source` (`crd` or `configmap`). The values mirror the sync's status. They are updated on every reconcile, at least every 15 s while a run is active. When a sync is deleted, its series are removed.

### Data freshness

| Metric | Type | Meaning |
|---|---|---|
| `imposm_sync_lag_seconds` | gauge | How far production's newest applied data is behind the upstream head (`status.lagSeconds`). |
| `imposm_sync_current_sequence` | gauge | Newest replication sequence applied to production. |
| `imposm_sync_upstream_sequence` | gauge | Upstream head sequence that the last run saw. |
| `imposm_sync_diffs_applied_total` | counter | Replication diffs applied, summed over finished runs. |

### Health and runs

| Metric | Type | Meaning |
|---|---|---|
| `imposm_sync_ready` | gauge | 1 while the `Ready` condition is true: production serves current data and the last run succeeded. This includes a re-import running next to production. |
| `imposm_sync_phase{phase}` | gauge | 1 for the current `status.phase`: `Pending`, `Importing`, `Syncing`, `Reimporting`, `AwaitingDeploy`, `Reverted`, `Failed`, `Conflict`, `Invalid`, `Suspended`. Only the current phase has a series. |
| `imposm_sync_running` | gauge | 1 while a run's Job is active. |
| `imposm_sync_runs_total{result}` | counter | Finished runs, `result` = `success` or `failure`. A run killed without a result (deadline, OOM, eviction) counts as a failure. Runs that found another run holding the lock are not counted. |
| `imposm_sync_consecutive_failures` | gauge | Failed runs since the last success. |
| `imposm_sync_last_run_timestamp_seconds` | gauge | When the last run finished (Unix time). |
| `imposm_sync_last_success_timestamp_seconds` | gauge | When the last successful run finished. |
| `imposm_sync_last_run_duration_seconds` | gauge | Duration of the last finished run. |
| `imposm_sync_next_run_timestamp_seconds` | gauge | Next scheduled run. The series is absent while the sync is suspended. |

### Re-imports and production

| Metric | Type | Meaning |
|---|---|---|
| `imposm_sync_reimport_running` | gauge | 1 while a new import is built next to production (`status.reimport` is set). |
| `imposm_sync_production_paused{reason}` | gauge | 1 while production applies no diffs. `reason` is `Disabled`, `NoRoom`, `SourceChanged`, `CacheUnavailable`, `Failed` or `Reverted` (see [state-machine.md](state-machine.md#production-during-a-re-import)). The series is absent while production syncs. |
| `imposm_sync_reimport_headroom` | gauge | 1 when the data volume has room for a second imposm cache (1.2× production's cache). Then a re-import under `syncDuringReimport: auto` keeps production syncing; at 0 it would pause production until the new import is deployed. The series is absent until production's cache size is known. |
| `imposm_sync_unused_tables` | gauge | Tables the sync no longer uses but kept because something depends on them (`status.unusedTables`). |
| `imposm_sync_empty_tables` | gauge | Production tables without rows (`status.emptyTableCount`; `status.emptyTables` lists up to 11). |

### Storage and expire tiles

| Metric | Type | Meaning |
|---|---|---|
| `imposm_sync_storage_used_bytes` | gauge | Bytes the sync uses on its data volume: caches, downloads, diffs. |
| `imposm_sync_storage_available_bytes` | gauge | Free bytes on the data volume. |
| `imposm_sync_storage_total_bytes` | gauge | Size of the data volume. |
| `imposm_sync_expire_pending` | gauge | Expire-tile batches not yet delivered to every sink. |

The storage values come from the last finished run, which measures the volume. They are absent until a run has finished.

### Counting guarantees

`runs_total` and `diffs_applied_total` advance exactly once per run:

- They advance only after the status write that records the run has succeeded.
- The controller remembers the last counted Job in memory.

A controller restart or leader change resets the counters, as usual for Prometheus counters, so use `rate()` / `increase()`.

## Alerts

With the Prometheus Operator installed (`prometheusRule.enabled: auto`), the chart creates a PrometheusRule with the group `imposm-operator`. Thresholds are chart values under `prometheusRule`. `prometheusRule.labels` is added to the PrometheusRule object, for example to match your Prometheus' `ruleSelector`.

| Alert | Severity | Fires when | Value |
|---|---|---|---|
| `ImposmSyncLagging` | warning | `imposm_sync_lag_seconds` is above `lag` for 30 m | `lag: 36h` (Go duration) |
| `ImposmSyncFailing` | critical | `consecutive_failures` ≥ `consecutiveFailures` | `consecutiveFailures: 3` |
| `ImposmSyncStorageLow` | warning | free / total < `storageFreeRatio` for 15 m | `storageFreeRatio: 0.1` |
| `ImposmSyncExpireTilesPending` | warning | `expire_pending` > 0 for `expirePendingFor` | `expirePendingFor: 1h` |
| `ImposmSyncConflict` | warning | phase `Conflict` or `Invalid` for 5 m | |
| `ImposmSyncProductionPaused` | warning | `production_paused` for `productionPausedFor` | `productionPausedFor: 6h` |
| `ImposmSyncNoReimportHeadroom` | info | `reimport_headroom` == 0 for 1 h | |
| `ImposmSyncUnusedTablesKept` | info | `unused_tables` > 0 for 1 h | |
| `ImposmSyncEmptyTables` | info | `empty_tables` > 0 for 1 h | |

Pick `lag` to match the update frequency. The default fits daily diffs; use something like `3h` for hourly diffs and `30m` for minutely ones. A full import or a long re-import with production paused also raises lag, so `ImposmSyncProductionPaused` usually fires first.

### What to do

- **Lagging:** check `kubectl get imposmsync` (activity, last run) and the `LastRunSucceeded` condition. A backlog catches up at `maxDiffsPerRun` diffs per run; raise it, or schedule runs more often.
- **Failing:** `status.message` and the `RunFailed` / `RunKilled` events (`kubectl describe imposmsync <name>`) give the error. For killed runs, check the Job's pod: deadline (`runner.activeDeadline`), OOM (`runner.resources`), eviction.
- **StorageLow:** grow `storage.size` (the PVC's StorageClass must allow expansion). The imposm cache is the largest item (`status.storage.cacheBytes`); a re-import needs room for a second one.
- **ExpireTilesPending:** a sink is unreachable (the Redis URL or password, or the files volume). Batches stay in the outbox and are delivered once the sink is back. The `ExpireTilesDelivered` condition has the error.
- **Conflict / Invalid:** the `SpecApplied` condition's reason says which:
  - `TablesClaimed`: another sync writes the same tables;
  - `ImportSchemaConflict`: `schemas.import` holds tables in use;
  - `RenameTargetExists`: a renamed table's new name is taken;
  - `Ready` reason `NameTaken`: an object with one of the sync's generated names exists and was not created for it (the controller does not take it over); rename the sync or remove the object;
  - `Invalid`: the spec or the mapping is invalid (`SpecValid` / `MappingValid`);
  - `RunnerPolicy` (phase `Invalid`, on `SpecValid` and `Ready`): the spec is valid but asks for more than the admin's `runnerPolicy` allows (runner labels or annotations, NFS or CSI volumes, placement); the message names each field. Change the spec, or the chart's `runnerPolicy`.

  Production keeps syncing during a conflict where it can.
- **ProductionPaused:** `status.production.pausedReason` and its message say why. `NoRoom` clears once the new import is deployed; to avoid it next time, grow `storage.size` until `ReimportHeadroom` is true. `Disabled` is `syncDuringReimport: never`. `Reverted` waits for the `deploy` or `reimport` action. `Failed`: production's own diffs failed while a new import is built; the import goes on and replaces production, the run reports the error.
- **NoReimportHeadroom:** informational. The next re-import (a mapping, SRID, limitTo or source change) will pause production until its deploy. Grow `storage.size` if that is not acceptable.
- **EmptyTables:** `status.emptyTables` lists production tables without rows. Usually the mapping matches nothing in the region (a value the data does not use, or a tag that is tagged differently there); check the table's mapping and filters against the data (e.g. taginfo for the region). A table that is expected to be empty in this region can be left as is.
- **UnusedTablesKept:** `status.unusedTables` lists them. Drop each table (and the view or foreign key that uses it) by hand once it is no longer needed.

## Useful queries

```promql
# syncs more than an hour behind
imposm_sync_lag_seconds > 3600

# failure ratio over a day
sum by (namespace, name) (increase(imposm_sync_runs_total{result="failure"}[1d]))
  / sum by (namespace, name) (increase(imposm_sync_runs_total[1d]))

# diffs applied per hour
sum by (namespace, name) (rate(imposm_sync_diffs_applied_total[1h])) * 3600

# days until the data volume is full, from the last 6 hours' trend
imposm_sync_storage_available_bytes
  / -deriv(imposm_sync_storage_available_bytes[6h]) / 86400 > 0

# syncs whose next re-import would pause production
imposm_sync_reimport_headroom == 0
```

## Beyond metrics

- `kubectl get imposmsync` shows the phase, lag, a one-line activity and the time since the last run. `-o wide` adds sequences, re-import progress, backup, storage and pending expire batches.
- `kubectl describe imposmsync` shows the conditions and events (see [design.md](design.md#status-and-progress)).
- In `configmap` mode, the status is in the `<name>-status` ConfigMap, and events are recorded on the sync's ConfigMap.
- The controller also exports controller-runtime's standard metrics: `controller_runtime_reconcile_*`, `workqueue_*`, and the Go process and runtime metrics.
