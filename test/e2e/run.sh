#!/usr/bin/env bash
# End-to-end and chaos tests on kind with CloudNativePG and Chaos Mesh.
#
#   test/e2e/run.sh [crd|configmap|all]
#
# For each mode: deploy a PostGIS cluster (CNPG), the fixture server with the
# frozen Monaco fixture, the chart, and one sync. Import the snapshot, then
# move the fixture head forward through the 10 daily diffs while running
# Chaos Mesh experiments (runner pod kill, controller kill, network loss to
# Postgres and to the mirror, I/O latency on the cache volume). After each
# experiment the sync must catch up; at the end the production tables must
# match test/fixtures/monaco/reference.rows.json.gz (same rows, geometries
# within 5 cm), and the expire tiles on the PVC and in the Redis stream must
# cover test/fixtures/monaco/reference.tiles.
#
# Uses its own kubeconfig (test/e2e/.kubeconfig, or E2E_KUBECONFIG); your
# ~/.kube/config and current context are never touched. To inspect a kept
# cluster: KUBECONFIG=test/e2e/.kubeconfig kubectl get pods -A
#
# Env: CLUSTER (imposm-e2e), TAG (dev), KEEP=1 keeps the cluster (a failed
#      run always keeps it, to inspect), REUSE=1 runs on a cluster left by an
#      earlier run instead of re-creating it, KIND (kind binary),
#      CHAOS=1   all Chaos Mesh experiments. Needs a rootful Docker host (e.g.
#                GitHub's runners or a VM): in rootless Docker, NetworkChaos
#                fails ("can't initialize iptables table": chaos-daemon uses
#                legacy iptables and netem, and cannot load kernel modules
#                from a user namespace; `sudo modprobe ip_tables
#                iptable_filter iptable_mangle sch_netem` on the host may
#                help) and IOChaos cannot get /dev/fuse through the device
#                cgroup at all
#      CHAOS=pod only PodChaos (runner and controller kills): works everywhere
#      CHAOS=0   no Chaos Mesh; just apply the 10 days one by one
set -euo pipefail

MODES=${1:-all}
[ "$MODES" = all ] && MODES="crd configmap"
CLUSTER=${CLUSTER:-imposm-e2e}
TAG=${TAG:-dev}
CHAOS=${CHAOS:-1}
# Pinned: Chaos Mesh's daemon runs privileged on the host.
CNPG_CHART_VERSION=${CNPG_CHART_VERSION:-0.29.1}
CHAOS_MESH_CHART_VERSION=${CHAOS_MESH_CHART_VERSION:-2.8.8}
CURL_IMAGE=${CURL_IMAGE:-curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69}
KIND=${KIND:-kind}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
# shellcheck disable=SC1091
. test/fixtures/monaco/fixture.env
# A private kubeconfig: never change the user's ~/.kube/config or context.
export KUBECONFIG=${E2E_KUBECONFIG:-$ROOT/test/e2e/.kubeconfig}
IMAGES="ghcr.io/johanjk/imposm-operator:$TAG ghcr.io/johanjk/imposm-operator-runner:$TAG ghcr.io/johanjk/imposm-operator-e2e:$TAG"

log() { printf '\n\033[1m[e2e %s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf '\033[31m[e2e] FAIL: %s\033[0m\n' "$*" >&2; exit 1; }

setup_cluster() {
	# A cluster left by an earlier (failed) run carries its state, e.g. an
	# experiment that was never removed: start clean unless asked not to.
	if [ "${REUSE:-0}" != 1 ] && $KIND get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		log "deleting the cluster left by an earlier run (REUSE=1 keeps it)"
		$KIND delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG"
	fi
	if ! $KIND get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		log "creating kind cluster $CLUSTER"
		$KIND create cluster --name "$CLUSTER" --config test/e2e/kind.yaml --wait 5m --kubeconfig "$KUBECONFIG"
	else
		$KIND export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG"
	fi
	for img in $IMAGES; do $KIND load docker-image --name "$CLUSTER" "$img"; done
	log "installing CloudNativePG"
	helm repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
	helm repo update cnpg >/dev/null
	helm upgrade --install cnpg cnpg/cloudnative-pg --version "$CNPG_CHART_VERSION" -n cnpg-system --create-namespace --wait --timeout 10m
	if [ "$CHAOS" != 0 ]; then
		log "installing Chaos Mesh"
		helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
		helm repo update chaos-mesh >/dev/null
		helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh --version "$CHAOS_MESH_CHART_VERSION" -n chaos-mesh --create-namespace --timeout 10m \
			--set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
			--set dashboard.create=false
		# Rootless Docker/kind: the daemon cannot bind /lib/modules read-write
		# ("cannot clear locked flags MS_RDONLY"); it only reads it anyway.
		i=$(kubectl -n chaos-mesh get ds chaos-daemon -o jsonpath='{range .spec.template.spec.containers[0].volumeMounts[*]}{.name}{"\n"}{end}' | grep -n '^lib-modules$' | cut -d: -f1)
		[ -n "$i" ] && kubectl -n chaos-mesh patch ds chaos-daemon --type=json \
			-p "[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/volumeMounts/$((i - 1))/readOnly\",\"value\":true}]" >/dev/null
		if ! kubectl -n chaos-mesh rollout status deploy/chaos-controller-manager --timeout=10m; then
			kubectl -n chaos-mesh get pods -o wide
			kubectl -n chaos-mesh describe pods -l app.kubernetes.io/component=controller-manager | sed -n '/^Events:/,$p' | tail -30
			die "Chaos Mesh controller did not start"
		fi
		if [ "$CHAOS" = 1 ]; then
			kubectl -n chaos-mesh rollout status ds/chaos-daemon --timeout=5m ||
				die "chaos-daemon is not running (rootless kind?); use CHAOS=pod"
		fi
	fi
}

deploy() { # ns mode
	local ns=$1 mode=$2
	log "[$mode] deploying into namespace $ns"
	kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n "$ns" apply -f examples/cnpg-cluster.yaml
	kubectl -n "$ns" apply -k examples/mapping
	kubectl -n "$ns" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: fx}
spec:
  selector: {matchLabels: {app: fx}}
  template:
    metadata: {labels: {app: fx}}
    spec:
      containers:
        - name: fx
          image: ghcr.io/johanjk/imposm-operator-e2e:$TAG
          imagePullPolicy: Never
          args: ["--head", "$SNAPSHOT_SEQUENCE"]
          ports: [{containerPort: 8080}]
          # the mirror is harness, not under test: stay in the Service through the
          # delay experiments (a 1s probe timeout drops it from the endpoints)
          readinessProbe: {httpGet: {path: /_control/head, port: 8080}, timeoutSeconds: 10, failureThreshold: 30}
---
apiVersion: v1
kind: Service
metadata: {name: fx}
spec:
  selector: {app: fx}
  ports: [{port: 8080, targetPort: 8080}]
YAML
	# Expire-tile outputs: a PVC (imposm's standard files) and a Redis stream
	# (zaptiles' format).
	kubectl -n "$ns" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: redis}
spec:
  selector: {matchLabels: {app: redis}}
  template:
    metadata: {labels: {app: redis}}
    spec:
      containers:
        - name: redis
          image: redis:7.4-alpine@sha256:c5d34a98396a4775e223c74e929162c35c5a2182901ba26b50ade939941ce14b
          ports: [{containerPort: 6379}]
          readinessProbe: {exec: {command: [redis-cli, ping]}}
---
apiVersion: v1
kind: Service
metadata: {name: redis}
spec:
  selector: {app: redis}
  ports: [{port: 6379, targetPort: 6379}]
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: expiretiles}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 100Mi}}
YAML
	kubectl -n "$ns" rollout status deploy/redis --timeout=5m
	kubectl -n "$ns" wait --for=condition=Ready cluster/osm --timeout=10m
	# postgis comes from an image volume and is created by the Database resource
	kubectl -n "$ns" wait --for=jsonpath='{.status.applied}'=true database.postgresql.cnpg.io/osm --timeout=5m
	kubectl -n "$ns" rollout status deploy/fx --timeout=5m
	if [ "$mode" = crd ]; then
		helm upgrade --install imposm-operator-crds charts/imposm-operator-crds --wait
	fi
	helm upgrade --install imposm charts/imposm-operator -n "$ns" --wait --timeout 5m \
		--set mode="$mode" --set rbac.scope=namespace \
		--set image.controller.tag="$TAG" --set image.controller.pullPolicy=Never \
		--set image.runner.tag="$TAG" --set image.runner.pullPolicy=Never \
		--set controller.replicas=2 \
		-f test/e2e/values.yaml
	apply_sync "$ns" "$mode"
}

# apply_sync creates the sync from test/e2e/sync-spec.yaml, as a user would:
# an ImposmSync, or a ConfigMap labelled as a sync.
apply_sync() { # ns mode
	case $2 in
	crd)
		{
			printf 'apiVersion: imposm.johanjk.github.io/v1alpha1\nkind: ImposmSync\nmetadata: {name: monaco}\nspec:\n'
			grep -v '^#' test/e2e/sync-spec.yaml | sed 's/^/  /'
		} | kubectl -n "$1" apply -f - ;;
	configmap)
		kubectl -n "$1" create configmap monaco --from-file=spec.yaml=test/e2e/sync-spec.yaml --dry-run=client -o yaml |
			kubectl label --local -f - imposm.johanjk.github.io/sync=true -o yaml | kubectl -n "$1" apply -f - ;;
	esac
}

set_head() { # ns seq
	kubectl -n "$1" run "head-$RANDOM" --rm -i --restart=Never --quiet --image="$CURL_IMAGE" -- \
		-fsS --retry 10 --retry-all-errors --retry-delay 3 -X POST "http://fx:8080/_control/head?seq=$2" >/dev/null
}

trigger() { # ns mode
	case $2 in
	crd) kubectl -n "$1" annotate imposmsync monaco imposm.johanjk.github.io/action=sync --overwrite >/dev/null ;;
	configmap) kubectl -n "$1" annotate configmap monaco imposm.johanjk.github.io/action=sync --overwrite >/dev/null ;;
	esac
}

current_seq() { # ns mode
	case $2 in
	crd) kubectl -n "$1" get imposmsync monaco -o jsonpath='{.status.currentSequence}' ;;
	configmap) kubectl -n "$1" get configmap monaco-status -o jsonpath='{.data.status\.yaml}' 2>/dev/null |
		sed -n 's/^currentSequence: //p' ;;
	esac
}

runner_running() { # ns -> waits until a runner pod is Running
	for _ in $(seq 1 120); do
		if kubectl -n "$1" get pods -l imposm.johanjk.github.io/name=monaco,app.kubernetes.io/component=sync \
			--field-selector=status.phase=Running -o name 2>/dev/null | grep -q .; then return 0; fi
		sleep 1
	done
	return 1
}

wait_seq() { # ns mode seq timeout
	local ns=$1 mode=$2 want=$3 deadline=$((SECONDS + ${4:-600})) cur
	while [ $SECONDS -lt $deadline ]; do
		cur=$(current_seq "$ns" "$mode" || true)
		[ "${cur:-0}" -ge "$want" ] && { log "[$mode] at sequence $cur"; return 0; }
		# no runner active: start another run
		if ! kubectl -n "$ns" get pods -l imposm.johanjk.github.io/name=monaco \
			--field-selector=status.phase!=Succeeded,status.phase!=Failed -o name 2>/dev/null | grep -q .; then
			trigger "$ns" "$mode"
		fi
		sleep 10
	done
	die "[$mode] sequence ${cur:-none} did not reach $want"
}

chaos() { # ns name yaml-kind spec...
	local ns=$1 name=$2 kind=$3
	shift 3
	kubectl -n "$ns" apply -f - <<YAML
apiVersion: chaos-mesh.org/v1alpha1
kind: $kind
metadata: {name: $name}
spec:
$*
YAML
	[ "$kind" = PodChaos ] && return 0
	# a network/IO fault that never lands would make the experiment pass
	# without testing anything: require AllInjected, else show why and fail
	if ! kubectl -n "$ns" wait "$kind/$name" --for=condition=AllInjected --timeout=60s >/dev/null; then
		kubectl -n "$ns" get "$kind" "$name" \
			-o jsonpath='{range .status.experiment.containerRecords[*]}{.id}: {.events[-1:].message}{"\n"}{end}' >&2 || true
		unchaos "$ns" "$kind" "$name"
		return 1
	fi
}

# delete an experiment; if chaos-mesh cannot recover it (finalizer stuck), drop
# the finalizer after a minute instead of waiting forever
unchaos() { # ns kind name
	kubectl -n "$1" delete "$2" "$3" --ignore-not-found --timeout=60s >/dev/null 2>&1 && return 0
	echo "[e2e] $2/$3 not recovered within 60s, removing its finalizer" >&2
	kubectl -n "$1" patch "$2" "$3" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
}

RUNNER_SEL="      imposm.johanjk.github.io/name: monaco
      app.kubernetes.io/component: sync"

experiments() { # ns mode
	local ns=$1 mode=$2 head=$SNAPSHOT_SEQUENCE
	step() { head=$((head + $1)); [ $head -gt "$HEAD_SEQUENCE" ] && head=$HEAD_SEQUENCE; set_head "$ns" $head; }

	log "[$mode] experiment: kill the runner pod mid-run"
	step 2; trigger "$ns" "$mode"; runner_running "$ns" && sleep 2
	chaos "$ns" kill-runner PodChaos "  action: pod-kill
  mode: all
  selector:
    labelSelectors:
$RUNNER_SEL"
	sleep 5; unchaos "$ns" podchaos kill-runner
	wait_seq "$ns" "$mode" $head

	log "[$mode] experiment: kill the controller leader"
	step 2
	chaos "$ns" kill-controller PodChaos "  action: pod-kill
  mode: one
  selector:
    labelSelectors:
      app.kubernetes.io/component: controller"
	trigger "$ns" "$mode"; sleep 10; unchaos "$ns" podchaos kill-controller
	wait_seq "$ns" "$mode" $head

	if [ "$CHAOS" = pod ]; then
		log "[$mode] CHAOS=pod: skipping network and I/O experiments"
		set_head "$ns" "$HEAD_SEQUENCE"
		wait_seq "$ns" "$mode" "$HEAD_SEQUENCE"
		return
	fi

	log "[$mode] experiment: 70% packet loss between runner and Postgres"
	step 2
	chaos "$ns" pg-loss NetworkChaos "  action: loss
  mode: all
  loss: {loss: '70', correlation: '50'}
  # direction 'to' (the default, the only one allowed without a target):
  # loss on everything the Postgres pods send, i.e. every reply to the runner
  duration: 45s
  selector:
    labelSelectors:
      cnpg.io/cluster: osm" || die "[$mode] pg-loss NetworkChaos was not injected"
	trigger "$ns" "$mode"; sleep 45; unchaos "$ns" networkchaos pg-loss
	wait_seq "$ns" "$mode" $head

	log "[$mode] experiment: slow, lossy mirror"
	step 2
	chaos "$ns" fx-delay NetworkChaos "  action: delay
  mode: all
  delay: {latency: 1500ms, jitter: 500ms}
  loss: {loss: '25'}
  duration: 45s
  selector:
    labelSelectors:
      app: fx" || die "[$mode] fx-delay NetworkChaos was not injected"
	trigger "$ns" "$mode"; sleep 45; unchaos "$ns" networkchaos fx-delay
	wait_seq "$ns" "$mode" $head

	log "[$mode] experiment: I/O latency on the cache volume during diffs"
	# IOChaos only attaches to pods that exist when it is created, and a Monaco
	# run takes seconds: hold the runner in its downloads with a slow mirror,
	# attach IOChaos to it, then release the mirror so imposm applies the diffs
	# while every second cache/download I/O is delayed
	step 2
	chaos "$ns" fx-hold NetworkChaos "  action: delay
  mode: all
  delay: {latency: 4s}
  duration: 10m
  selector:
    labelSelectors:
      app: fx" || die "[$mode] fx-hold NetworkChaos was not injected"
	trigger "$ns" "$mode"
	runner_running "$ns" || die "[$mode] no runner pod started"
	chaos "$ns" cache-io IOChaos "  action: latency
  mode: all
  # a slow disk, not a dead one: leveldb does thousands of small I/Os per
  # diff, 200ms made a two-diff Monaco run take ten minutes
  delay: 20ms
  percent: 50
  volumePath: /var/lib/imposm-operator/data
  path: /var/lib/imposm-operator/data/**
  duration: 5m
  selector:
    labelSelectors:
$RUNNER_SEL" || die "[$mode] cache-io IOChaos was not injected into the runner"
	unchaos "$ns" networkchaos fx-hold
	wait_seq "$ns" "$mode" $head 900
	unchaos "$ns" iochaos cache-io
	set_head "$ns" "$HEAD_SEQUENCE"
	wait_seq "$ns" "$mode" "$HEAD_SEQUENCE"
}

verify() { # ns mode
	local ns=$1 mode=$2
	log "[$mode] comparing production tables with the reference rows"
	kubectl -n "$ns" delete job verify --ignore-not-found >/dev/null
	kubectl -n "$ns" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata: {name: verify}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext: {runAsUser: 65532, fsGroup: 65532}
      containers:
        - name: verify
          image: ghcr.io/johanjk/imposm-operator-e2e:$TAG
          imagePullPolicy: Never
          command: [/usr/local/bin/tablesums]
          # generalized tables are compared within their simplification tolerance
          args: [-conn, "host=osm-rw user=osm dbname=osm sslmode=require", -compare-rows, /fixtures/monaco/reference.rows.json.gz, -mapping-dir, /mapping]
          env: [{name: PGPASSFILE, value: /pg/pgpass}]
          volumeMounts: [{name: pgpass, mountPath: /pg}, {name: mapping, mountPath: /mapping}]
      volumes:
        - name: pgpass
          secret: {secretName: osm-app, defaultMode: 0440, items: [{key: pgpass, path: pgpass}]}
        - name: mapping
          configMap: {name: osm-mapping}
YAML
	if ! kubectl -n "$ns" wait --for=condition=complete job/verify --timeout=5m; then
		kubectl -n "$ns" logs job/verify || true
		die "[$mode] tables differ from the reference rows"
	fi
	kubectl -n "$ns" logs job/verify

	log "[$mode] checking expire tiles on the PVC and in the Redis stream"
	kubectl -n "$ns" delete job verify-expire --ignore-not-found >/dev/null
	kubectl -n "$ns" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata: {name: verify-expire}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext: {runAsUser: 65532, fsGroup: 65532}
      containers:
        - name: verify
          image: ghcr.io/johanjk/imposm-operator-e2e:$TAG
          imagePullPolicy: Never
          command: [/usr/local/bin/expirecheck]
          args: [-reference, /fixtures/monaco/reference.tiles, -dir, /expiretiles,
                 -redis, "redis://redis:6379", -stream, expiretiles, -field, tiles]
          volumeMounts: [{name: tiles, mountPath: /expiretiles, readOnly: true}]
      volumes:
        - name: tiles
          persistentVolumeClaim: {claimName: expiretiles}
YAML
	if ! kubectl -n "$ns" wait --for=condition=complete job/verify-expire --timeout=5m; then
		kubectl -n "$ns" logs job/verify-expire || true
		die "[$mode] expire tiles incomplete"
	fi
	kubectl -n "$ns" logs job/verify-expire
}

setup_cluster
for mode in $MODES; do
	ns="osm-$mode"
	deploy "$ns" "$mode"
	log "[$mode] initial import"
	trigger "$ns" "$mode"
	if [ "$mode" = crd ]; then
		# Live progress: the activity column must show what the runner does.
		seen=""
		for _ in $(seq 1 120); do
			a=$(kubectl -n "$ns" get imposmsync monaco -o jsonpath='{.status.activity}' 2>/dev/null || true)
			case "$a" in "" | idle*) ;; *) echo "$seen" | grep -qxF "$a" || { seen="$seen$a"$'\n'; echo "  activity: $a"; } ;; esac
			[ "$(current_seq "$ns" "$mode")" = "$SNAPSHOT_SEQUENCE" ] && break
			sleep 1
		done
		# At least one step parsed from the runner's progress log, not just the
		# pod state: proves log reading and the pods/log RBAC.
		echo "$seen" | grep -vE '^(pending|running)' | grep -q . ||
			die "[$mode] status.activity never showed a step from the runner's progress log"
		kubectl -n "$ns" get imposmsync monaco
		kubectl -n "$ns" get imposmsync monaco -o wide
		# Metrics from every controller pod (only the leader reconciles, so
		# only it has imposm_sync_* series; Prometheus scrapes each pod).
		# NetworkPolicy must allow the scrape.
		# Retried like a scraper would: a fresh client pod can race the
		# NetworkPolicy and the first status write.
		ips=$(kubectl -n "$ns" get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[*].status.podIP}')
		m=$(kubectl -n "$ns" run "metrics-$RANDOM" --rm -i --restart=Never --quiet --image="$CURL_IMAGE" --command -- \
			sh -c "for _ in 1 2 3 4 5 6 7 8 9 10; do
				out=\$(for ip in $ips; do curl -sS --max-time 5 http://\$ip:8080/metrics; done)
				echo \"\$out\" | grep -q '^imposm_sync_current_sequence{' && { echo \"\$out\"; exit 0; }
				sleep 3
			done" | grep '^imposm_sync_' || true)
		echo "$m" | grep -q '^imposm_sync_current_sequence{.*name="monaco".*} ' ||
			die "[$mode] imposm_sync metrics missing on all controller pods"
		echo "$m" | grep -E '^imposm_sync_(current_sequence|lag_seconds|ready)\{' | sed 's/^/  /'
	fi
	wait_seq "$ns" "$mode" "$SNAPSHOT_SEQUENCE" 900
	if [ "$CHAOS" != 0 ]; then
		experiments "$ns" "$mode"
	else
		for s in $(seq $((SNAPSHOT_SEQUENCE + 1)) "$HEAD_SEQUENCE"); do
			set_head "$ns" "$s"
			wait_seq "$ns" "$mode" "$s"
		done
	fi
	verify "$ns" "$mode"
	log "[$mode] PASS"
done
[ "${KEEP:-0}" = 1 ] || { log "deleting cluster"; $KIND delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG"; }
