#!/usr/bin/env bash
# Larger-scale smoke test with the real runner image:
#   1. import the fixture's snapshot and apply its daily diffs one run at a
#      time (sync "diffs", schemas public/import/backup), timing every run;
#   2. import the fixture's target PBF directly as a second sync in the same
#      database (schemas direct/direct_import/direct_backup), which the table
#      guard must allow;
#   3. compare both server-side: equal rows and attributes, geometries within
#      5 cm (generalized tables: plus their simplification tolerance), areas
#      within 0.1%.
#
#   FIXTURE=/path/to/fixture (built by REGION=norway hack/fixtures.sh) hack/scaletest.sh
#
# Env: RUNNER_IMAGE, WORK (scratch dir, default mktemp), KEEP=1 keeps containers,
#      TOLERANCE (metres, default 0.1: in EPSG:3857 one unit of imposm's 1e-7
#      degree rounding is 3-6 cm at 60-70 degrees north).
set -euo pipefail
FIXTURE=${FIXTURE:?set FIXTURE to a fixture directory}
RUNNER_IMAGE=${RUNNER_IMAGE:-ghcr.io/johanjk/imposm-operator-runner:dev}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
. "$FIXTURE/fixture.env"
REPLICATION_DIR=${REPLICATION_DIR:-monaco-updates}
WORK=${WORK:-$(mktemp -d)}
id=scale-$$
mkdir -p "$WORK"
log() { printf '[scale %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
# shellcheck source=hack/lib.sh
. hack/lib.sh
cleanup() {
	[ "${KEEP:-0}" = 1 ] && { log "kept containers $id-pg $id-fx and network $id"; return; }
	lib_cleanup
}
trap cleanup EXIT

CGO_ENABLED=0 go build -o "$WORK/tablesums" ./hack/tablesums
PG_SHM=2g pg_start -c shared_buffers=2GB -c maintenance_work_mem=1GB -c max_wal_size=8GB -c checkpoint_timeout=30min \
	-c work_mem=64MB
docker run -d --name "$id-fx" --network "$id" --network-alias fx -v "$FIXTURE:/fixtures/region:ro" \
	--entrypoint imposm-operator "$RUNNER_IMAGE" fixture-server --dir /fixtures/region \
	--replication-dir "$REPLICATION_DIR" --head "$SNAPSHOT_SEQUENCE" >/dev/null

setup() { # name pbf checksum schemas
	runner_dir "$WORK/$1" "source: {baseURL: \"http://fx:8080/\", pbfPath: \"$2\", replicationPath: $REPLICATION_DIR/, checksum: $3}
database: {host: pg, database: osm, user: osm, sslMode: disable, schemas: $4}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
imposm: {expireTiles: {zoom: 14, files: {volume: {persistentVolumeClaim: {claimName: expiretiles}}}}}
maxDiffsPerRun: 1000"
}
run() { # name -> prints result json, appends timing
	local start=$SECONDS out
	out=$(runner_run "$WORK/$1" "$1" "$WORK/$1/runner.log")
	echo "$out" | python3 -c "import json,sys; r=json.load(sys.stdin); print(f\"{r['phase']:<10} seq={r.get('currentSequence')} diffs={r.get('diffsApplied')} {r.get('error','')}\")" | sed "s/^/  $(($SECONDS - start))s  /"
	echo "$out" | grep -q '"error"' && { echo "run failed, see $WORK/$1/runner.log" >&2; exit 1; } || true
}

setup diffs "$SNAPSHOT" md5 "{import: import, production: public, backup: backup}"
log "import of $SNAPSHOT (sequence $SNAPSHOT_SEQUENCE)"
run diffs
for s in $(seq $((SNAPSHOT_SEQUENCE + 1)) "$HEAD_SEQUENCE"); do
	fx_head "$s"
	log "diff $s: $(zcat "$FIXTURE/$REPLICATION_DIR/$(seqpath "$s").osc.gz" | grep -oE '<(node|way|relation) ' | wc -l) objects"
	run diffs
done

setup direct "$TARGET" none "{import: direct_import, production: direct, backup: direct_backup}"
log "direct import of $TARGET as a second sync in the same database"
run direct

log "comparing public (snapshot + diffs) with direct (target import)"
docker run --rm --network "$id" --user "$HOST_USER" -v "$WORK:/w" -v "$ROOT/examples/mapping:/mapping:ro" --entrypoint /w/tablesums "$RUNNER_IMAGE" \
	-conn "host=pg user=osm password=secret dbname=osm sslmode=disable" -schema public -compare-schema direct -tolerance "${TOLERANCE:-0.1}" \
	-mapping-dir /mapping

log "sizes"
docker exec "$id-pg" psql -U osm -d osm -Atc "SELECT 'database ' || pg_size_pretty(pg_database_size('osm'))"
docker exec "$id-pg" psql -U osm -d osm -Atc "SELECT relname || ' ' || n_live_tup FROM pg_stat_user_tables WHERE schemaname='public' AND relname IN ('osm_buildings','osm_roads','osm_landusages','osm_housenumbers') ORDER BY 1"
du -sh "$WORK/diffs/data/cache" "$WORK/diffs/data/downloads" 2>/dev/null || true
find "$WORK/diffs/expiretiles" -name '*.tiles' | wc -l | sed 's/^/expire files: /'
