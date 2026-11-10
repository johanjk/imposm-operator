#!/usr/bin/env bash
# Build a frozen test fixture (default: Monaco; REGION=norway OUT=... for a
# larger scale test, not committed).
#
# Geofabrik only keeps ~130 days of daily diffs, and its only old snapshots are
# the yearly ones (monaco-YY0101.osm.pbf), so a real old snapshot can't be
# chained to real daily diffs. Instead:
#
#   snapshot = monaco-260101.osm.pbf           (real file, real header: seq 4651)
#   target   = monaco-latest.osm.pbf           (real file, downloaded now)
#   diffs    = osmium derive-changes snapshot -> target, split by hack/splitosc
#              into N referentially consistent "days", numbered seq+1..seq+N
#
# so the diffs contain ~10 months of real edits, and snapshot + all diffs must
# equal a direct import of target.
#
# Requires: curl, go, osmium (run inside `nix shell nixpkgs#osmium-tool` or set
# OSMIUM="docker run --rm -v $PWD:$PWD -w $PWD <image> osmium").
set -euo pipefail

REGION=${REGION:-monaco}
SNAPSHOT=${SNAPSHOT:-$REGION-260101.osm.pbf}
BASE=${BASE:-https://download.geofabrik.de/europe}
DAYS=${DAYS:-10}
OUT=${OUT:-test/fixtures/$REGION}
OSMIUM=${OSMIUM:-osmium}
UA="imposm-operator-fixtures (+https://github.com/johanjk/imposm-operator)"

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fetch() { curl -fsSL --retry 3 --max-time 300 -A "$UA" -o "$2" "$1"; }

echo ">> downloading snapshot $SNAPSHOT and current target"
fetch "$BASE/$SNAPSHOT" "$work/snapshot.osm.pbf"
fetch "$BASE/$SNAPSHOT.md5" "$work/snapshot.md5"
(cd "$work" && sed "s/$SNAPSHOT/snapshot.osm.pbf/" snapshot.md5 | md5sum -c -)
fetch "$BASE/$REGION-latest.osm.pbf" "$work/target.osm.pbf"
fetch "$BASE/$REGION-latest.osm.pbf.md5" "$work/target.md5"
(cd "$work" && sed "s/$REGION-latest.osm.pbf/target.osm.pbf/" target.md5 | md5sum -c -)
# (the md5 files come from the same server: they catch corruption, not
# tampering; review fixture changes like code)

seq=$(go run ./hack/pbfheader "$work/snapshot.osm.pbf" | sed -n 's/^sequence=//p')
ts=$(go run ./hack/pbfheader "$work/snapshot.osm.pbf" | sed -n 's/^timestamp=//p')
target_date=$(go run ./hack/pbfheader "$work/target.osm.pbf" | sed -n 's/^timestamp=\(..........\).*/\1/p')
echo ">> snapshot sequence=$seq timestamp=$ts, target date=$target_date"

echo ">> deriving changes"
$OSMIUM derive-changes --overwrite -o "$work/change.osc" "$work/snapshot.osm.pbf" "$work/target.osm.pbf"
go run ./hack/splitosc -n "$DAYS" -in "$work/change.osc" -out "$work/days"

rm -rf "$OUT"
mkdir -p "$OUT/$REGION-updates" "$OUT/target"
cp "$work/snapshot.osm.pbf" "$OUT/$SNAPSHOT"
cp "$work/snapshot.md5" "$OUT/$SNAPSHOT.md5"
cp "$work/target.osm.pbf" "$OUT/target/$REGION-$target_date.osm.pbf"

epoch=$(date -u -d "$ts" +%s)
state() { # seq epoch
	local s=$1 t
	t=$(date -u -d "@$2" +%Y-%m-%dT%H\\:%M\\:%SZ)
	printf '#%s\nsequenceNumber=%d\ntimestamp=%s\n' "$(date -u -d "@$2")" "$s" "$t"
}
state "$seq" "$epoch" >"$OUT/$REGION-updates/snapshot.state.txt"
for i in $(seq 1 "$DAYS"); do
	s=$((seq + i))
	p=$(printf '%09d' "$s")
	dir="$OUT/$REGION-updates/${p:0:3}/${p:3:3}"
	mkdir -p "$dir"
	cp "$work/days/$(printf '%02d' "$i").osc.gz" "$dir/${p:6:3}.osc.gz"
	state "$s" $((epoch + i * 86400)) >"$dir/${p:6:3}.state.txt"
done
# the snapshot's own state file, so timestamp -> sequence search can terminate
p=$(printf '%09d' "$seq")
mkdir -p "$OUT/$REGION-updates/${p:0:3}/${p:3:3}"
mv "$OUT/$REGION-updates/snapshot.state.txt" "$OUT/$REGION-updates/${p:0:3}/${p:3:3}/${p:6:3}.state.txt"
h=$(printf '%09d' $((seq + DAYS)))
cp "$OUT/$REGION-updates/${h:0:3}/${h:3:3}/${h:6:3}.state.txt" "$OUT/$REGION-updates/state.txt"

cat >"$OUT/ATTRIBUTION" <<ATTR
Map data © OpenStreetMap contributors, available under the Open Database
License (ODbL) v1.0: https://www.openstreetmap.org/copyright

Extracts by Geofabrik GmbH (https://download.geofabrik.de/).
The files under $REGION-updates/ are derived from $SNAPSHOT and the
$target_date $REGION-latest.osm.pbf with osmium derive-changes, split into
$DAYS consecutive change files by hack/splitosc. Their timestamps are synthetic.
ATTR

cat >"$OUT/fixture.env" <<ENV
SNAPSHOT=$SNAPSHOT
SNAPSHOT_SEQUENCE=$seq
HEAD_SEQUENCE=$((seq + DAYS))
DAYS=$DAYS
TARGET=target/$REGION-$target_date.osm.pbf
REPLICATION_DIR=$REGION-updates
ENV

(cd "$OUT" && find . -type f ! -name SHA256SUMS | LC_ALL=C sort | xargs sha256sum >SHA256SUMS)
du -sh "$OUT"
