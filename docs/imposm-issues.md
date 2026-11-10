# Problems found by the tests

The comparison against the reference rows (`reference.rows.json.gz`) and against a direct import of the
target data found three **imposm** problems that affect any imposm deployment, and one in the operator itself:

1. **Chained generalized tables drift in diff mode (imposm v0.14.2).**
   `GeneralizedTableSpec.InsertSQL` read from the base table instead of the
   source generalized table, so e.g. `roads_gen0` (from `roads_gen1`) was
   refilled from `roads`, ignoring `roads_gen1`'s filter. After 10 Monaco diffs
   `osm_roads_gen0` had 1260 rows instead of 515. Fixed upstream after v0.14.2;
   the runner image pins that commit until a release contains it.
2. **imposm's cache and the database are not updated atomically.** imposm
   updates its LevelDB cache while processing a diff, before the database
   transaction commits. If imposm is killed or the database connection drops
   mid-diff, the database rolls back but the cache keeps part of the change.
   Re-applying the diff onto that cache duplicates rows (seen as extra rows in
   `osm_buildings` and `osm_landusages`). With the default commit-per-diff
   setting, imposm makes this worse by retrying the failed diff inside the same
   process. The operator therefore:
   - runs `imposm diff -commit-latest`, so imposm exits on a database error
     instead of retrying on its dirty cache;
   - checkpoints the cache before every diff chunk (reflink clone, or a copy
     below `imposm.cacheCheckpointLimit`), leaves an in-progress marker, and
     restores the checkpoint when a chunk fails or a run finds a leftover
     marker. Without a checkpoint, an interrupted chunk triggers a re-import
     while production keeps serving;
   - probes the database when a chunk ends without a recorded commit,
     because imposm commits first and writes `last.state.txt` afterwards. A
     kill in between must keep the cache: rolling it back made the replay
     duplicate the chunk's creates (13 extra `osm_housenumbers` in one chaos
     run).
3. **Generalized rows duplicated in diff mode (imposm master, unfixed).**
   An element that matches the same table more than once (a way that is both
   a road and a disused railway: two rows in `osm_roads`) is queued once per
   match for the generalized-table update, and every queued entry re-inserts
   its rows. Duplicates double at each generalization level (2 in
   `roads_gen1`, 4 in `roads_gen0`). Monaco has no such way; the Norway
   scale test found 16 extra rows. The runner build applies
   `containers/patches/0001-dedupe-generalized-update-ids.patch` (deduplicate
   the queued ids) until it is fixed upstream.
4. **A completed download could wedge a sync.** If the runner was killed after
   a download finished but before the file was renamed, resuming it returned
   HTTP 416, which was treated as permanent. A 416 on resume now means "the
   part file may be complete": it is verified (md5 / gzip / size) and used or
   discarded.
