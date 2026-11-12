# OSM metadata, changesets and audit trails

Status: implemented (`spec.metadata`), except logging in to sources that
require it (see [Sources](#sources)).

Which changeset last touched a row, what that changeset was (its comment,
source and editor), and optionally every version an element went through:
for every table a sync imports. The operator keeps this in tables of its
own, updated in the same runs as imposm's, joinable on `osm_id`.

## Goals

- For every element a sync's tables are built from: its `version`,
  `timestamp` and `changeset`, current with production.
- Optionally, the changesets themselves: comment, source, editor, tags,
  bounding box, open and close time.
- Optionally, history: every version an element went through since the
  sync started, either its metadata only (`history.mode: versions`) or a
  full audit trail (`history.mode: full`): what each version changed (tags,
  node position, way nodes, relation members), by which changeset,
  append-only.
- No change to imposm, its cache or its tables; a sync without `metadata`
  works exactly as before, and changing `metadata` never re-imports.
- The same guarantees as the diffs: resumable after a kill, never ahead of
  production, at most one run behind it, no silent gaps.

## Non-goals

- Who edited: `user` and `uid` are dropped while parsing and never stored.
  A changeset id is enough to look the author up on openstreetmap.org.
- Logging in to sources that require an account (Geofabrik's internal
  server). The source must be reachable without credentials, e.g. a mirror
  of it inside the organisation.
- Untagged nodes by default (see [Scope](#scope)): a way's geometry changes
  when one of its nodes moves, but the way's own changeset does not.
- Element history before the sync started (full-history extracts are a
  separate product, and large).

## Why imposm cannot do it

- imposm discards metadata when it reads a PBF or a diff, and its LevelDB
  cache has no place for it (open upstream:
  [imposm3#58](https://github.com/omniscale/imposm3/issues/58),
  [#259](https://github.com/omniscale/imposm3/issues/259)). Adding it would
  change the cache format, and every way and relation rebuild would need the
  metadata of an element that did not change in the diff.
- Its row model does not fit: one element can be several rows (a way that is
  a road and a footpath route, generalized copies, clipped parts), and a
  way's row is rewritten when only a node moved. Metadata belongs to the
  element, once.

## Package

[`internal/osmmeta`](../internal/osmmeta) does the work and is
self-contained: it imports nothing from the operator. Give it a snapshot
(`.osm.pbf`), replication diffs (`.osc.gz`) and the changeset replication,
and it maintains the tables. The operator's engine
([`internal/engine/metadata.go`](../internal/engine/metadata.go)) only
decides when, feeding it the snapshot and diffs it already downloads for
imposm.

| File | What |
|---|---|
| `osmmeta.go` | types, `Config`, the `Store` interface |
| `pbf.go` | snapshot reader (`github.com/paulmach/osm/osmpbf`, parallel decoding) |
| `osc.go` | streaming `.osc` reader (bounded memory on large diffs) |
| `changesets.go` | changeset replication: `state.yaml`, files, `SyncChangesets` |
| `postgres.go` | the tables, bulk snapshot load, idempotent diff chunks, retention |
| `osmmetatest/` | writes PBFs, osmChange and changeset files with metadata for tests |

## Sources

The public Geofabrik downloads strip metadata (GDPR): the Norway PBF and
diffs carry `version` and `timestamp` only, no `changeset`, `uid` or `user`
(checked on `norway-updates/000/004/967.osc.gz`). Data with metadata:

| What | Where | Access |
|---|---|---|
| Regional snapshots and diffs with full metadata | Geofabrik's internal server, `https://osm-internal.download.geofabrik.de/` | an OpenStreetMap login, not supported: use a mirror reachable without one |
| Planet snapshot and diffs with full metadata | `https://planet.openstreetmap.org/` | public |
| Changesets (comment, tags, bbox) | `https://planet.openstreetmap.org/replication/changesets/` (minutely, own sequence numbers) | public |

A source without changeset ids (the public Geofabrik downloads) is enough
for versions and timestamps: the element metadata and history are kept with
`changeset` NULL, and the join function and changes view work as usual. Only
changesets need the ids to join by: with `changesets.enabled`, the runner
checks the data, not the URL, and refuses a snapshot or diff chunk whose
elements carry no changeset ids (`MetadataValid=False`, "changesets are on
but the source has no changeset ids"); nothing is written. imposm's tables
are not affected either way.

## Spec

```yaml
metadata:
  elements: tagged                # none | tagged (default) | all
  schema: osm_history             # default: <production schema>_history; one per sync
  history:
    mode: none                    # none (default) | versions | full
    retention:                    # all optional; nothing set: keep everything
      maxAge: 3650d               # versions replaced longer ago are deleted
      maxVersions: 0              # per element, newest kept; 0: all
      deleted: 730d               # history of elements deleted longer ago
      content: 365d               # full only: content columns cleared after this
    indexTags: false              # full only: GIN index on tags, see Indexes
  changesets:
    enabled: false
    replicationURL: https://planet.openstreetmap.org/replication/changesets/
    bbox: ""                      # minLon,minLat,maxLon,maxLat; default: the snapshot header's
    retention: ""                 # e.g. 730d; empty: keep all
    indexText: false              # full-text index on comments, see Indexes
    maxFilesPerRun: 1500          # one file per minute upstream
```

Durations are `<n>d` or `<n>h`. Validation refuses: nothing kept (`elements:
none` without changesets), history without elements, `maxAge`/`maxVersions`
without history, `content`/`indexTags` without `full`, a `schema` equal to
one of `database.schemas` (imposm's deploy rotates whole schemas),
credentials in `replicationURL`, and changesets without element metadata
and without `bbox` (the snapshot's bbox is only read with element metadata).
The runner's NetworkPolicy opens the changeset replication's port.

## Schema

In `metadata.schema`, by default the production schema with `_history`
(`osm_history` for a sync deploying to `osm`), the same way a sync's
production schema is its own. The first sync to use the schema claims it
(`position` row `owner`: the sync's state schema and name, as its table
claims); another sync configured with the same schema is refused
(`MetadataCurrent=False`, "schema … is used by …") and writes nothing, nor
touches the first sync's views or join function. The schema and its
`position` table must belong to the sync's database user (or a role it is a
member of): another user's schema is refused before anything is written to
it, as its owner could have put a trigger there that would run with the
sync user's rights. For a sync deploying to `osm`:

```sql
-- latest version per element
CREATE TABLE osm_history.element_meta (
  osm_type "char" NOT NULL,             -- 'n', 'w', 'r'
  osm_id bigint NOT NULL,
  version integer NOT NULL,
  "timestamp" timestamptz NOT NULL,
  changeset bigint,
  deleted boolean NOT NULL DEFAULT false,
  sequence bigint NOT NULL,             -- replication sequence it came from (0: snapshot)
  PRIMARY KEY (osm_type, osm_id));

-- history.mode versions or full: every version seen, append-only
CREATE TABLE osm_history.element_history (
  osm_type "char", osm_id bigint, version integer,
  "timestamp" timestamptz NOT NULL, changeset bigint,
  action text NOT NULL,                 -- create | modify | delete | baseline
  sequence bigint NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  -- full only: the element as it was in this version
  tags jsonb, lon double precision, lat double precision,
  nodes bigint[],                       -- ways: node ids in order
  members jsonb,                        -- relations: [{"type","ref","role"}, ...]
  PRIMARY KEY (osm_type, osm_id, version));

-- when a version was replaced (written with the next version; the history
-- itself is never updated, except retention's content clearing)
CREATE TABLE osm_history.history_superseded (
  osm_type "char", osm_id bigint, version integer, superseded_at timestamptz NOT NULL,
  PRIMARY KEY (osm_type, osm_id, version));
CREATE TABLE osm_history.history_breaks (id, sequence_from, sequence_to, reason, at);
CREATE TABLE osm_history.history_retention (id, at, rule, cutoff, rows_deleted, rows_cleared);

CREATE TABLE osm_history.changesets (
  id bigint PRIMARY KEY, created_at timestamptz NOT NULL, closed_at timestamptz,
  open boolean NOT NULL, num_changes integer,
  comment text,                         -- tags->>'comment', for convenience
  tags jsonb NOT NULL,                  -- created_by, source, hashtags, ...
  bbox geometry(Polygon, 4326));

-- where each stream is: elements, changesets, pruned, content, missing, bbox
CREATE TABLE osm_history.position (stream text PRIMARY KEY, sequence bigint,
  "timestamp" timestamptz, source text, updated_at timestamptz);
```

Deleted elements keep a row with `deleted = true` (version and changeset of
the deletion) until `retention.deleted` or the next snapshot load, so "what
deleted this switch" can be answered.

### Indexes

Chosen from the queries the tables serve, and nothing more: every index on
`element_meta` costs on each upsert, and the history is the largest table.

```sql
-- element_meta: PK (osm_type, osm_id) serves the joins and the upserts
(changeset)                              -- "what did changeset X touch", changeset retention
("timestamp")                            -- "edited since"
("timestamp") WHERE deleted              -- retention.deleted
-- element_history: PK serves one element's trail in order, the changes
-- view (previous version = next PK entry) and ON CONFLICT DO NOTHING
(changeset)
("timestamp") WHERE action <> 'baseline' -- "edits in a period"; baselines are not edits
USING brin (sequence)                    -- "what came with diff N"
USING gin (tags jsonb_path_ops) WHERE action <> 'baseline'   -- indexTags only
-- history_superseded
(superseded_at)                          -- retention
-- changesets: PK (id) for the joins
USING gist (bbox)                        -- changesets in an area
(closed_at)                              -- recent changesets, retention
(id) WHERE open                          -- the few still open
USING gin (to_tsvector('simple', coalesce(comment, '')))     -- indexText only
```

- **BRIN on `sequence`**: rows are appended in sequence order, so a BRIN
  index is a few pages instead of a B-tree over every row.
- **The `timestamp` index skips baselines.** A baseline row carries the
  timestamp of an edit that may be years old; leaving them out keeps the
  index small and "edits in a period" correct.
- **Retention uses `history_superseded`, not the history.** `maxAge` and
  `content` need "replaced longer ago than X", the next version's
  timestamp. Finding that in the history means a self-join over every old
  row, every day; the superseded table has one small row per replaced
  version, indexed on that time. The newest version of an element has no
  superseded row, which is the "never delete the current state" rule for
  free.
- **The GIN indexes are opt-in**: largest and slowest to maintain, and only
  needed to search by tag over all history or by comment text.
- **Bulk load.** A snapshot is copied (`COPY`) into an unlogged staging
  table, the new `element_meta` is built from it without indexes, the
  indexes are created afterwards, and it is swapped in by rename, in one
  transaction.
- **Vacuum.** `element_meta` is updated in place by every diff; daily diffs
  touch well under 1 % of its rows, so autovacuum's defaults are fine. After
  a retention pass that removed rows, the operator runs `VACUUM (ANALYZE)`
  on the tables it touched.

### Partitioning

Not used. A regional sync's tables stay well within what single PostgreSQL
tables handle (Norway: element metadata ~17 M rows; a `full` history grows
by ~20 M rows, ~8 GB, a year), and the usual reasons do not apply:

- **Retention as `DROP PARTITION`** works when rows expire by their own
  time. Here they expire by the time they were *replaced*, never if they are
  an element's current version, and `deleted` expires whole elements: a
  month of history never becomes droppable as a block.
- **Time partitioning breaks replay idempotency.** The primary key would
  have to include the partition key, so `(osm_type, osm_id, version)` would
  no longer be unique, and a replayed chunk could insert a version twice.
- **Queries** go by element, changeset or sequence, which the indexes serve
  directly.

If a planet-sized sync ever needs it, the safe partitioning is `LIST
(osm_type)`: the primary keys already start with `osm_type`, so uniqueness
and replays keep working, and the node partition (largest with `elements:
all`) gets its own indexes and vacuum, without changing queries.

### Joining imposm rows

imposm's `osm_id` depends on the geometry type and the mapping's
`use_single_id_space`. The operator creates one function per sync, with
the views, from the mapping of the deployed import (not the spec's: after a
mapping change production keeps the old ids until the re-import is
deployed). It calls PostGIS by its schema, so it works whatever the
caller's `search_path`:

```sql
osm_history.element(id bigint, geom geometry) RETURNS TABLE (osm_type "char", osm_id bigint)
```

| `use_single_id_space` | node | way | relation (polygons, `relation` and `relation_member` tables) |
|---|---|---|---|
| false (default) | `id` | `id` | `-id` |
| true | `id` | `-id` | `-1e17 - id` |

(From imposm's `wayID`/`relID`.) Without a single id space a node and a way
can have the same `osm_id`, so the function also takes the row's geometry:
points are nodes, other positive ids ways.

Example:

```sql
SELECT t.osm_id, t.name, m.version, m."timestamp", c.comment
FROM osm.osm_roads t
CROSS JOIN LATERAL osm_history.element(t.osm_id, t.geometry) e
JOIN osm_history.element_meta m ON (m.osm_type, m.osm_id) = (e.osm_type, e.osm_id)
LEFT JOIN osm_history.changesets c ON c.id = m.changeset
WHERE t.ref = 'E 6';
```

### Views

With element metadata, every production table gets a view of the same name
in the metadata schema: the table's columns plus `osm_version`,
`osm_timestamp` and `osm_changeset`, joined as above (`LEFT JOIN`: a row
without metadata yet keeps NULLs).

```sql
SELECT name, osm_version, osm_timestamp FROM osm_history.osm_stations WHERE name = 'Oslo S';
```

- The id and geometry columns come from the mapping (`type: id`,
  `geometry`/`validated_geometry`; a generalized table has its source's).
  A table without an id column gets no view; one without geometry takes
  positive ids as ways.
- A view runs with its owner's (the sync user's) rights: readers need
  `USAGE` on the metadata schema and `SELECT` on the views, not access to
  the production schema.
- A view follows its table when the table is moved, and imposm drops
  backup and import tables without `CASCADE`: a view left on a replaced
  table would fail the next deploy. So the operator drops the views before
  imposm's deploy and revert, and recreates them on the new production
  tables right after (readers see no view for those seconds). Every run
  also checks them: each view's comment holds the sync (the schema's
  owner) and a fingerprint of its definition and table, and a view is
  replaced only when it changed (e.g. after a re-import or a rename).
- After a change of `metadata.schema`, the sync's views in the previous
  schema are dropped (found by their comment); its tables stay until
  dropped by hand.
- A snapshot load replaces `element_meta`, which the views join: it drops
  and recreates them in its own transaction, so readers never miss them.
- Views the operator did not create are never replaced or dropped; a table
  whose name one takes gets no view, is reported in
  `status.metadata.message`, and the other tables keep theirs.
- While a revert is in place there are no views (production holds the
  previous import's tables until the next deploy).
- After `spec.metadata` is removed, the sync's views (found by their
  comment) are dropped before each deploy, so they cannot hold replaced
  tables; the metadata schema and its tables stay until dropped by hand
  (`DROP SCHEMA osm_history CASCADE`).

## Scope

`elements` decides which elements get rows:

- `tagged`: every way and relation, and nodes with at least one tag (about
  10 % of the nodes; every node an imposm point table can hold). A node that
  loses its tags keeps its rows: it was kept before.
- `all`: also untagged nodes, about ten times the rows. Answers "which
  changeset moved a node of this way".

Filtering further by the mapping (only elements that match a table) is not
done: imposm decides matches, including through relation membership, and
duplicating that logic would drift from it.

## Run order and consistency

The element metadata has its own position (`position` table, stream
`elements`, with the source it belongs to) and always follows
**production**, never an import being built. Each run, after production's
diffs:

```
no position, or another source ──▶ load the snapshot on the volume
                                   (download it if missing; wait while it is
                                   newer than production)
position < production's sequence ──▶ apply the missing diffs in chunks
                                   (from the volume, fetched again if gone)
changesets enabled ──▶ SyncChangesets towards the replication head
once a day ──▶ retention, and the count of missing changesets
```

- **Diff chunks are transactions**: the chunk is copied into a temporary
  table, then `element_meta` is upserted (a row is only replaced by the
  same or a newer version), history rows are inserted with `ON CONFLICT DO
  NOTHING`, the superseded rows written, and the position advanced. A
  replayed chunk changes nothing; a chunk that leaves a gap after the
  position (or comes from another source) is refused.
- **Kills**: a run killed between imposm's commit and the metadata
  transaction leaves the metadata behind production; the next run catches
  up. The next diffs the metadata needs (at most `maxDiffsPerRun`, so a
  metadata that keeps failing cannot fill the volume) are kept on the
  volume until it has passed them; others are fetched again.
- **Failures never fail the run**: imposm's tables are fine. The status
  reports `MetadataCurrent=False` with the reason, and the next run retries.
- **Revert**: after `revert-deploy` production's sequence is older than the
  metadata's; the metadata waits ("ahead of production") and continues once
  production has passed it again. Until then it describes newer data than
  production.
- **Changesets** have their own replication and position (stream
  `changesets`, with the area as its source). A first sync starts a day
  before the production snapshot (changesets still open then), estimated at
  one file per minute. A changeset appears again when it closes; the newest
  state wins. A missing file below the head stops the changeset sync with an
  error, like a diff gap; element metadata keeps going. At most
  `maxFilesPerRun` files per run.
- The metadata transaction is separate from imposm's, so a reader can see a
  row updated by imposm a moment before its metadata row.

## Snapshots and re-imports

- **First import, or enabling `metadata` on an existing sync**: the
  snapshot on the volume is loaded (or downloaded), then the diffs from its
  sequence to production's are applied. imposm's tables are not re-imported.
- **Re-imports** (mapping, SRID, prefix changes): the elements did not
  change, so the metadata is kept and keeps following production.
- **Source changes** (another snapshot or replication URL): the metadata
  waits until the new import is deployed, then is rebuilt from that
  snapshot. A kept history records the break (`history_breaks`) and gets
  baseline rows for the new snapshot's elements; their previous versions
  are recorded as replaced by them, so retention covers them.
- **Disabling** stops writing; the tables stay (dropping an audit trail is
  an admin's deliberate `DROP`).

### Audit trail

History starts at the snapshot: each element's state then is one
`baseline` row (not an edit: its timestamp and changeset are those of that
version's edit, which may be years old); every later version is appended as
diffs arrive, with the `.osc` action. Every version in a diff is recorded,
including intermediate ones when a diff holds several versions of one
element.

With `history.mode: full` each row holds the element's content in that
version, so any two versions can be compared without OpenStreetMap's API,
also after the element was deleted there. The view
`osm_history.element_changes` shows what each version changed
against the previous kept one: `tags_added`, `tags_removed`, `tags_changed`
(`{"key": [old, new]}`), `moved_m` (nodes), `nodes_changed` (ways),
`members_changed` (relations). The previous version is looked up per row
by primary key, so filters on changeset, time or element use their indexes:

```sql
SELECT osm_type, osm_id, version, "timestamp", tags_changed, moved_m
FROM osm_history.element_changes WHERE changeset = 123456789;
```

`history.mode: versions` stores the same rows without the content columns.

### Retention

Without `history.retention` nothing is deleted. Each setting is optional
and they combine:

| Setting | Deletes or clears | Measured from |
|---|---|---|
| `maxAge` | versions replaced longer ago than this | the **next** version's timestamp: an element unchanged for ten years keeps its only row |
| `maxVersions` | all but the newest N versions of an element | |
| `deleted` | the whole history and metadata of an element deleted in OSM longer ago | the deletion's timestamp |
| `content` (`full`) | the content columns of replaced versions; the row stays, so the trail degrades to `versions` | the next version's timestamp |
| `changesets.retention` | changesets closed longer ago that no kept metadata or history row refers to | `closed_at` |

- **The current state stays complete**: an element's newest row is never
  deleted or cleared while the element exists.
- **The changes view compares against the previous *kept* version**: a
  pruned version is not shown as a change.
- **Every pruning is logged** in `history_retention` (rule, cutoff, rows),
  so the trail shows what was removed instead of looking like the versions
  never existed.
- **Bounded**: retention runs once a day, after the diffs and changesets,
  in batches of 50 000 rows within 2 minutes per run; a large first pruning
  continues over the next runs. Content clearing remembers how far it got.

## Status, metrics, alerts

- `status.metadata`: `sequence`, `timestamp`, `elements`, `historyRows`,
  `historyOldest` (oldest kept edit), `changesetSequence`,
  `changesetTimestamp`, `changesets`, `changesetsMissing` (changesets of
  element metadata from the last 30 days, older than 48 hours, not kept: a
  replication gap or outside the bbox), `lastPruned`, `prunedRows`,
  `message`.
- Conditions `MetadataValid` (False: changesets are on but the source has
  no changeset ids) and
  `MetadataCurrent` (False: building, behind or failing, with the reason).
- Metrics `imposm_sync_metadata_lag_seconds`,
  `imposm_sync_changesets_missing`, `imposm_sync_history_pruned_rows`;
  alerts `ImposmSyncMetadataBehind` (over 2 hours behind for 2 hours) and
  `ImposmSyncChangesetsMissing`.

## Database rights

The sync user creates `metadata.schema` (`CREATE` on the database, or the
schema created for it beforehand) and owns its tables. Readers need `USAGE`
on the schema and `SELECT` on the tables (`EXECUTE` on the join function is
granted to `PUBLIC` by default); see [database.md](database.md).

## Testing

- Unit (`internal/osmmeta`): the `.osc`, PBF and changeset readers, the
  changeset sync (budget, area, gaps), helpers.
- PostGIS (`internal/chaos/osmmeta_pg_test.go`, `make chaos`): snapshot
  load (refused without changeset ids when changesets are on, kept with
  NULL changesets when they are off), diffs with replays and a refused
  gap, untagged nodes (left out with `tagged`, kept with `all`), the
  changes view, the join function, changesets reopening and closing,
  every retention rule, a source change with its history break and
  superseded rows, a schema owned by another user
  (refused), another sync's views left alone, and a view name taken by
  another object (that table skipped, the others kept).
- Engine (`internal/engine/metadata_test.go`): the metadata follows
  production run by run, catches up after a failure from kept diffs,
  reports a source without changeset ids while changesets are on without
  failing the sync, starts on an
  existing sync, syncs changesets within their budget, keeps at most one
  run's diffs while it fails, and drops the views before each deploy once
  `metadata` is removed.
- The fixtures with metadata are generated (`osmmetatest`): the public
  Monaco extract has no changeset ids.

## Sizing (Norway, estimate)

| Table | Rows | Size with indexes |
|---|---|---|
| `element_meta`, `tagged` | 17.3 M (measured) | 2.0 GB (measured) |
| `element_meta`, `all` | ~120 M | ~12 GB |
| `element_history`, `versions`, per year | ~20 M | ~2 GB |
| `element_history`, `full`, baseline | ~15 M (`tagged`) | ~6 GB |
| `element_history`, `full`, per year | ~20 M | ~8 GB |
| `changesets`, Norway bbox, per year | ~300 k | ~0.3 GB |

`element_meta` measured on the public Norway extract (November 2026: 4.3 M
nodes, 12.2 M ways, 0.8 M relations; no changeset ids, which a source with
metadata adds as one `bigint` per row). The rest are estimates.

## Open questions

1. Is `tagged` the right default, or do users need node moves (`all`)?
2. Changeset backfill: the planet changeset dump (`changesets-latest.osm.bz2`)
   would give the changesets of the snapshot's elements. Worth a one-off
   job, or is "changesets from the sync's start on" enough?
3. Does the audit trail need tamper evidence (e.g. a hash chain over rows),
   or are append-only writes and grants enough?
