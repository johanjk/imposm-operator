# shellcheck shell=bash
# Shared helpers for the Docker-based scripts (scaletest.sh, reference.sh,
# minutely-smoketest.sh). Source after `cd` to the repository root, with $id
# (container and network name prefix) and $RUNNER_IMAGE set.

: "${id:?set id (container and network prefix) before sourcing hack/lib.sh}"

# The PostGIS test image; internal/chaos reads the same variable.
POSTGIS_IMAGE=${POSTGIS_IMAGE:-postgis/postgis:18-3.7@sha256:c5d1ba4292ff7b4471ff41cd515c677633797b72cdd2a7bb691cadaab91f1a1f}
CURL_IMAGE=${CURL_IMAGE:-curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69}

# lib_cleanup removes the containers and the network.
lib_cleanup() {
	docker rm -f "$id-pg" "$id-fx" >/dev/null 2>&1 || true
	docker network rm "$id" >/dev/null 2>&1 || true
}

# pg_start [postgres -c args...]: network plus PostGIS (user/db osm, password
# secret, alias pg), ready on return. PG_SHM sets --shm-size (default 1g).
pg_start() {
	docker network create "$id" >/dev/null
	docker run -d --name "$id-pg" --network "$id" --network-alias pg --shm-size="${PG_SHM:-1g}" \
		-e POSTGRES_PASSWORD=secret -e POSTGRES_USER=osm -e POSTGRES_DB=osm "$POSTGIS_IMAGE" "$@" >/dev/null
	pg_wait
}

# pg_wait waits for three successful queries in a row: the image restarts the
# server once after initdb, so a single success is not enough.
pg_wait() {
	local ok=0
	for _ in $(seq 1 120); do
		if docker exec "$id-pg" psql -U osm -d osm -h 127.0.0.1 -Atc 'select 1' >/dev/null 2>&1; then
			ok=$((ok + 1))
			[ "$ok" -ge 3 ] && return 0
		else
			ok=0
		fi
		sleep 1
	done
	echo "postgres did not become ready" >&2
	return 1
}

# runner_dir DIR SPEC: the runner's mounts in DIR (etc/ with the example
# mapping, a pgpass for pg, and SPEC as spec.yaml; data/; expiretiles/).
runner_dir() {
	mkdir -p "$1/etc/mapping/osm-mapping" "$1/data" "$1/expiretiles"
	cp examples/mapping/[0-9]*.yaml "$1/etc/mapping/osm-mapping/"
	printf 'pg:5432:*:osm:secret\n' >"$1/etc/pgpass"
	chmod 600 "$1/etc/pgpass"
	printf '%s\n' "$2" >"$1/etc/spec.yaml"
}

# runner_run DIR NAME [LOG]: one runner run; prints its JSON result, appends
# the log to LOG (default: discarded).
runner_run() {
	docker run --rm --network "$id" --user 0:0 -e PGPASSFILE=/etc/imposm-operator/pgpass \
		-v "$1/etc:/etc/imposm-operator:ro" -v "$1/data:/var/lib/imposm-operator/data" \
		-v "$1/expiretiles:/expiretiles" \
		"$RUNNER_IMAGE" run --name "$2" --termination-log "" 2>>"${3:-/dev/null}" | tail -1
}

# fx_head SEQ moves the fixture server's head.
fx_head() {
	docker run --rm --network "$id" "$CURL_IMAGE" -fsS --retry 5 -X POST "http://fx:8080/_control/head?seq=$1" >/dev/null
}

# seqpath SEQ: the replication path AAA/BBB/CCC of a sequence.
seqpath() { printf '%09d' "$1" | sed 's|\(...\)\(...\)\(...\)|\1/\2/\3|'; }
