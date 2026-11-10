#!/usr/bin/env bash
# Regenerate test/fixtures/monaco/reference.rows.json.gz: import the snapshot and apply
# all daily diffs with the real runner image (no faults), checksum the
# production tables (row dump), and cross-check the result against a direct import of
# the fixture's target PBF (geometries within 5 cm, areas within 0.1%).
# Also writes reference.tiles: the expire tiles (imposm.expireTiles, zoom 14)
# of the fault-free run.
set -euo pipefail
RUNNER_IMAGE=${RUNNER_IMAGE:-ghcr.io/johanjk/imposm-operator-runner:dev}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
. test/fixtures/monaco/fixture.env
id=ref-$$
work=$(mktemp -d)
# shellcheck source=hack/lib.sh
. hack/lib.sh
cleanup() {
	lib_cleanup
	docker run --rm -v "$work:/w" --user "$HOST_USER" --entrypoint /bin/sh "$RUNNER_IMAGE" -c 'rm -rf /w/*' >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT

CGO_ENABLED=0 go build -o "$work/tablesums" ./hack/tablesums
CGO_ENABLED=0 go build -o "$work/expirecheck" ./hack/expirecheck
pg_start
docker run -d --name "$id-fx" --network "$id" --network-alias fx -v "$PWD/test/fixtures/monaco:/fixtures/monaco:ro" \
	--entrypoint imposm-operator "$RUNNER_IMAGE" fixture-server --head "$SNAPSHOT_SEQUENCE" >/dev/null
docker exec "$id-pg" psql -U osm -d osm -h 127.0.0.1 -qc 'create database direct' >/dev/null

setup() { # dir database pbf checksum
	runner_dir "$work/$1" "source: {baseURL: \"http://fx:8080/\", pbfPath: \"$3\", replicationPath: monaco-updates/, checksum: $4}
database: {host: pg, database: $2, user: osm, sslMode: disable}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
imposm: {expireTiles: {zoom: 14, files: {volume: {persistentVolumeClaim: {claimName: expiretiles}}}}}"
}
run() { runner_run "$work/$1" reference; } # dir
check() { # json expected-seq
	echo "$1" | grep -q "\"currentSequence\":$2," || { echo "run failed: $1" >&2; exit 1; }
	echo "$1" | grep -q '"error"' && { echo "run failed: $1" >&2; exit 1; }
	true
}

setup diffs osm "$SNAPSHOT" md5
check "$(run diffs)" "$SNAPSHOT_SEQUENCE"
for s in $(seq $((SNAPSHOT_SEQUENCE + 1)) "$HEAD_SEQUENCE"); do
	fx_head "$s"
	check "$(run diffs)" "$s"
	echo "applied $s"
done

setup direct direct "$TARGET" none
run direct >/dev/null

ts() { docker run --rm --network "$id" --user "$HOST_USER" -v "$work:/w" -v "$PWD/test/fixtures/monaco:/fx" --entrypoint /w/tablesums "$RUNNER_IMAGE" "$@"; }
ts -conn "host=pg user=osm password=secret dbname=osm sslmode=disable" \
	-tolerant-with "host=pg user=osm password=secret dbname=direct sslmode=disable" -tolerance 0.05
ts -conn "host=pg user=osm password=secret dbname=osm sslmode=disable" -head "$HEAD_SEQUENCE" -dump /fx/reference.rows.json.gz
docker run --rm --user "$HOST_USER" -v "$work:/w" -v "$PWD/test/fixtures/monaco:/fx" --entrypoint /w/expirecheck "$RUNNER_IMAGE" \
	-write -dir /w/diffs/expiretiles -reference /fx/reference.tiles
