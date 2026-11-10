# State machines

Three state machines drive a sync:

- **Imports** are stored in the `imposm_operator.import` table, one row per snapshot import.
- **Production** is the deployed import. It either applies diffs or is paused.
- **The sync's phase** is `status.phase`. Each run's result sets it, and the controller adjusts it.

A run (`imposm-operator run`) is a bounded unit of work. It moves these machines forward and stops at the first error, so the next run resumes from the recorded state. The code is in `internal/engine` (`engine.go`, `tables.go`, `status.go`) and `internal/state`.

## Import lifecycle

The diagram below shows `state.ImportPhase`. Each step is recorded once it is done, so a run killed at any point resumes at the step it was on.

```mermaid
stateDiagram-v2
    [*] --> pending: new import<br/>(first import, spec change,<br/>reimport action, cache lost)

    state "building (import schema)" as building {
        pending --> downloaded: download PBF<br/>(resumable, checksum)
        downloaded --> read: imposm import -read<br/>(makes room for the cache)
        read --> written: imposm import -write
        written --> optimized: imposm import -optimize<br/>(or skipped)
        optimized --> optimized: catch up diffs<br/>in the import schema<br/>(maxDiffsPerRun per run)
        optimized --> ready: at upstream head,<br/>or the first import
        read --> downloaded: cache does not match
        written --> downloaded: cache does not match
        optimized --> downloaded: cache does not match
    }

    ready --> ready: deploy.auto false (AwaitingDeploy):<br/>keeps catching up
    ready --> deployed: deploy (auto or action)<br/>import → production → backup

    deployed --> deployed: apply diffs
    deployed --> reverted: revert-deploy action<br/>(backup kept)
    reverted --> deployed: deploy action
    reverted --> superseded: reimport action

    building --> superseded: a newer import is created,<br/>or production matches the spec again<br/>(its import-schema tables are dropped)
    building --> needs_reimport: cache lost mid-build
    needs_reimport --> superseded: replaced by a new import
    deployed --> superseded: a newer import is deployed
    superseded --> [*]
```

Notes:

- **Renames do not re-import.** When only the schema names, the table prefix or the table names change (`TablesHash` is unchanged), `relocate` moves the import's tables with `ALTER TABLE … SET SCHEMA / RENAME` and refreezes its configuration. The import keeps its phase. If a rename target already exists, the sync is in `Conflict` (`RenameTargetExists`).
- **A lost cache under a deployed import** (or a reverted one) leaves that import deployed and serving. The import's last error is set, and a new import starts with reason `CacheLost`.
- **The import schema must be free.** `checkImportSchemaFree` refuses to start an import into a schema that holds tables in use (for example the production schema). The sync is then in `Conflict` (`ImportSchemaConflict`), and production keeps syncing.
- **After a deploy**, the tables the new mapping no longer has are dropped (`dropOrphans`). Tables that something depends on are kept and listed in `status.unusedTables`.

## Production during a re-import

While a new import is built, the deployed import can keep applying diffs. It uses its own frozen configuration, mapping and cache in `imports/<id>/`. This depends on `deploy.syncDuringReimport`.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> syncing: first deploy
    syncing --> paused: re-import starts and<br/>production cannot keep up
    paused --> syncing: new import deployed<br/>(production is the new import)
    syncing --> syncing: re-import deployed

    state paused {
        Disabled: Disabled<br/>syncDuringReimport never
        NoRoom: NoRoom<br/>auto, no room for<br/>a second cache (1.2×)
        SourceChanged: SourceChanged<br/>replication URL changed
        CacheUnavailable: CacheUnavailable<br/>production cache missing<br/>or out of step
        Failed: Failed<br/>production's diffs failed<br/>(the new import goes on)
    }
    syncing --> Reverted: revert-deploy
    Reverted --> syncing: deploy action
```

`status.production.syncing` and `pausedReason` report this state, together with the `ProductionSyncing` condition and the `imposm_sync_production_paused{reason}` metric. The `ReimportHeadroom` condition shows in advance whether an `auto` re-import would pause. When a pause starts and when it ends, the controller records `ProductionPaused` and `ProductionResumed` events.

## Sync phase (`status.phase`)

```mermaid
stateDiagram-v2
    [*] --> Pending: created
    Pending --> Importing: first run
    Importing --> Syncing: first import deployed
    Importing --> AwaitingDeploy: ready, deploy.auto false

    Syncing --> Reimporting: spec change needs a new import<br/>(mapping, SRID, limitTo, source,<br/>reimport action, cache lost)
    Reimporting --> Syncing: new import deployed,<br/>or production matches the spec again
    Reimporting --> AwaitingDeploy: ready, deploy.auto false
    AwaitingDeploy --> Syncing: deploy action

    Syncing --> Reverted: revert-deploy
    Reverted --> Syncing: deploy action
    Reverted --> Reimporting: reimport action

    Syncing --> Conflict: tables claimed by another sync,<br/>import schema in use,<br/>rename target exists
    Reimporting --> Conflict
    Conflict --> Syncing: spec fixed

    Syncing --> Failed: run failed
    Importing --> Failed
    Reimporting --> Failed
    Failed --> Syncing: next run succeeds

    state "any" as any
    any --> Invalid: spec or mapping invalid<br/>(controller)
    Invalid --> Pending: fixed
    any --> Suspended: spec.suspend
    Suspended --> Pending: resumed
```

- **`Ready` is true while production serves current data:** in `Syncing`, `Reimporting` and `AwaitingDeploy`, as long as production is not paused and the last run succeeded.
- **`SpecApplied`** says whether production already runs the current spec. When it does not, the reason is one of `Importing`, `Reimporting`, `Pending`, `Reverted`, `TablesClaimed`, `ImportSchemaConflict` or `RenameTargetExists`.
- **The controller sets `Invalid`, `Suspended` and `Pending` itself.** All other phases come from the result of the newest finished run. A run killed before it reported anything counts as failed (the `RunKilled` event).

## One run

```mermaid
flowchart TD
    start([run]) --> lock{advisory lock<br/>for the target}
    lock -- held by another run --> busy([Busy: skipped])
    lock -- acquired --> load[load the newest import<br/>and the deployed import]
    load --> claim{"claim the spec's tables<br/>(only ever adds)"}
    claim -- claimed by another sync --> conflict([Conflict: TablesClaimed])
    claim --> head[read upstream state.txt]
    head --> action{action}
    action -- remove-backup --> rmb([drop the backup schema])
    action -- revert-deploy --> rev([imposm revert → Reverted])
    action -- sync / deploy / reimport --> relocate[relocate: renames only?<br/>move tables in two steps, refreeze]
    relocate -- rename target taken --> conflict3([Conflict: RenameTargetExists,<br/>production keeps syncing])
    relocate --> resume[resumeProduction: production<br/>matches the spec again?<br/>abandon the import being built]
    resume --> stale{newest import stale,<br/>needs_reimport, or<br/>reimport action?}
    stale -- yes --> guard{import schema free?}
    guard -- no --> conflict2([Conflict: ImportSchemaConflict,<br/>production keeps syncing])
    guard -- yes --> newimp[newImport: record why,<br/>supersede the old one,<br/>then drop its abandoned tables]
    stale -- no --> ws
    newimp --> ws["open the import's workspace<br/>imports/&lt;id&gt;"]
    ws --> prod[syncProduction: the deployed import<br/>applies diffs with its own workspace,<br/>or pauses with a reason]
    prod --> pipe[pipeline: advance the import<br/>one recorded step at a time]
    pipe -- cache lost --> again[new import, reason CacheLost<br/>once per run]
    again --> pipe
    pipe -- deployed --> orphans[drop orphaned tables,<br/>free the old workspace]
    orphans --> diffs
    pipe -- already deployed --> orphans2["retry dropping unused tables<br/>(kept ones stay claimed)"]
    orphans2 --> diffs[apply diffs in chunks<br/>up to maxDiffsPerRun]
    pipe -- building / awaiting deploy --> finish
    diffs --> finish[finish: production, reimport,<br/>SpecApplied, unused tables, events]
    finish --> out([result → termination message<br/>→ controller status, events, metrics])
```
