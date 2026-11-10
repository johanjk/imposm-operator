#!/usr/bin/env bash
# Minutely-diff smoke test against a live upstream, with the real runner image:
#   1. sync "live": import an openstreetmap.fr extract and catch up its
#      minutely diffs straight from download.openstreetmap.fr (one run for the
#      backlog, then ROUNDS more runs a few minutes apart, as in production);
#   2. build the expected result independently: the very snapshot the runner
#      downloaded + the same minutely diffs, merged by osmium apply-changes;
#   3. sync "direct": import that merged PBF from a local fixture server whose
#      head is pinned to the sequence "live" reached;
#   4. compare both server-side (tablesums: rows, attributes, geometries within
#      TOLERANCE metres, generalized tables within their simplification).
#
#   hack/minutely-smoketest.sh                       # Corse (~40 MB)
#   REGION=europe/france/bretagne hack/minutely-smoketest.sh
#
# Needs docker, go, osmium (e.g. nix shell nixpkgs#osmium-tool) and internet.
# Env: RUNNER_IMAGE, WORK (default mktemp), REGION, ROUNDS (default 3),
#      PAUSE between rounds (seconds, default 150), TOLERANCE (default 0.1),
#      KEEP=1 keeps the containers.
set -euo pipefail
REGION=${REGION:-europe/france/corse}
UPSTREAM=${UPSTREAM:-https://download.openstreetmap.fr}
RUNNER_IMAGE=${RUNNER_IMAGE:-ghcr.io/johanjk/imposm-operator-runner:dev}
ROUNDS=${ROUNDS:-3}
PAUSE=${PAUSE:-150}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
WORK=${WORK:-$(mktemp -d)}
id=minutely-$$
mkdir -p "$WORK"
log() { printf '[minutely %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
# shellcheck source=hack/lib.sh
. hack/lib.sh
cleanup() {
	[ "${KEEP:-0}" = 1 ] && { log "kept containers $id-pg $id-fx and network $id"; return; }
	lib_cleanup
}
trap cleanup EXIT
command -v osmium >/dev/null || { echo "osmium not found (nix shell nixpkgs#osmium-tool)" >&2; exit 1; }

CGO_ENABLED=0 go build -o "$WORK/tablesums" ./hack/tablesums
pg_start -c shared_buffers=1GB -c maintenance_work_mem=512MB -c max_wal_size=4GB

setup() { # name source-yaml schemas
	runner_dir "$WORK/$1" "source: $2
database: {host: pg, database: osm, user: osm, sslMode: disable, schemas: $3}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
imposm: {expireTiles: {zoom: 14, files: {volume: {persistentVolumeClaim: {claimName: expiretiles}}}}}
maxDiffsPerRun: 5000
diffChunkSize: 60"
}
seq_of=""
run() { # name -> prints a summary, sets seq_of
	local start=$SECONDS out
	out=$(runner_run "$WORK/$1" "$1" "$WORK/$1/runner.log")
	echo "$out" | python3 -c "import json,sys; r=json.load(sys.stdin); print(f\"{r['phase']:<10} seq={r.get('currentSequence')} upstream={r.get('upstreamSequence')} diffs={r.get('diffsApplied')} {r.get('error','')}\")" |
		sed "s/^/  $((SECONDS - start))s  /"
	if echo "$out" | grep -q '"error"'; then echo "run failed, see $WORK/$1/runner.log" >&2; exit 1; fi
	seq_of=$(echo "$out" | python3 -c "import json,sys; print(json.load(sys.stdin)['currentSequence'])")
}

# 1. live
setup live "{baseURL: \"$UPSTREAM/\", pbfPath: extracts/$REGION.osm.pbf, replicationPath: replication/$REGION/minute/, updateFrequency: minute}" \
	"{import: import, production: public, backup: backup}"
log "live: import $UPSTREAM/extracts/$REGION.osm.pbf and catch up its minutely diffs"
run live
for r in $(seq 1 "$ROUNDS"); do
	log "live: round $r of $ROUNDS after ${PAUSE}s (new minutes upstream)"
	sleep "$PAUSE"
	run live
done
head=$seq_of
snapshot=$(find "$WORK/live/data" -name '*.osm.pbf' -size +1k | head -1)
base=$(osmium fileinfo -g header.option.osmosis_replication_sequence_number "$snapshot")
log "live reached $head; snapshot $snapshot is at $base ($((head - base)) minutely diffs)"

# 2. expected: snapshot + the same diffs via osmium
fx="$WORK/fx"
mkdir -p "$fx"
log "fetching diffs $((base + 1))..$head for the reference"
seq $((base + 1)) "$head" | while read -r s; do
	p=$(seqpath "$s")
	mkdir -p "$fx/minute/$(dirname "$p")"
	echo "$UPSTREAM/replication/$REGION/minute/$p.osc.gz $fx/minute/$p.osc.gz"
	echo "$UPSTREAM/replication/$REGION/minute/$p.state.txt $fx/minute/$p.state.txt"
done | xargs -P 2 -n 2 sh -c 'curl -fsS --retry 5 -A "imposm-operator-smoketest (+https://github.com/johanjk/imposm-operator)" -o "$1" "$0"'
ts=$(sed -n 's/^timestamp=//p' "$fx/minute/$(seqpath "$head").state.txt" | tr -d '\\')
cp "$fx/minute/$(seqpath "$head").state.txt" "$fx/minute/state.txt"
mapfile -t diffs < <(for s in $(seq $((base + 1)) "$head"); do echo "$fx/minute/$(seqpath "$s").osc.gz"; done)
osmium apply-changes --overwrite -o "$fx/target.osm.pbf" \
	--output-header="osmosis_replication_sequence_number=$head" \
	--output-header="osmosis_replication_timestamp=$ts" \
	--output-header="osmosis_replication_base_url=http://fx:8080/minute" \
	"$snapshot" "${diffs[@]}"
log "reference $(du -h "$fx/target.osm.pbf" | cut -f1) at $head ($ts)"

# 3. direct import of the reference, head pinned
docker run -d --name "$id-fx" --network "$id" --network-alias fx -v "$fx:/fixtures/region:ro" \
	--entrypoint imposm-operator "$RUNNER_IMAGE" fixture-server --dir /fixtures/region \
	--replication-dir minute --head "$head" >/dev/null
sleep 2
setup direct "{baseURL: \"http://fx:8080/\", pbfPath: target.osm.pbf, replicationPath: minute/, updateFrequency: minute, checksum: none}" \
	"{import: direct_import, production: direct, backup: direct_backup}"
log "direct import of the reference as a second sync in the same database"
run direct
[ "$seq_of" = "$head" ] || { echo "direct sync ended at $seq_of, not $head" >&2; exit 1; }

# 4. compare
log "comparing public (live: snapshot + minutely diffs) with direct (reference import)"
docker run --rm --network "$id" --user "$HOST_USER" -v "$WORK:/w" -v "$ROOT/examples/mapping:/mapping:ro" --entrypoint /w/tablesums "$RUNNER_IMAGE" \
	-conn "host=pg user=osm password=secret dbname=osm sslmode=disable" -schema public -compare-schema direct \
	-tolerance "${TOLERANCE:-0.1}" -mapping-dir /mapping
find "$WORK/live/expiretiles" -name '*.tiles' | wc -l | sed 's/^/expire files: /'
log "PASS"
