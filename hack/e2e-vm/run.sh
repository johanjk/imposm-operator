#!/usr/bin/env bash
# Run the full e2e (every Chaos Mesh experiment) in a NixOS VM with rootful
# Docker, for hosts where kind can't do NetworkChaos/IOChaos (rootless Docker,
# kernels without legacy iptables).
#
#   hack/e2e-vm/run.sh                 up, test, down
#   hack/e2e-vm/run.sh up|down|ssh     manage the VM
#   hack/e2e-vm/run.sh test [VAR=..]   sync the repo into the VM and run
#                                      `make e2e` there (e.g. CHAOS=pod)
#   hack/e2e-vm/run.sh destroy         stop the VM and delete its disk and key
#
# Needs nix (flakes) and KVM. State lives in $E2E_VM_STATE (default
# ~/.cache/imposm-operator/e2e-vm: disk image ~25 GB after a run, ssh key,
# console log). The VM's docker image cache survives `down`, so later runs
# are faster. Uses 10 GB RAM and 8 cores; ssh on 127.0.0.1:$E2E_VM_SSH_PORT.
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$DIR/../.." && pwd)
STATE=${E2E_VM_STATE:-${XDG_CACHE_HOME:-$HOME/.cache}/imposm-operator/e2e-vm}
PORT=${E2E_VM_SSH_PORT:-2222}
SSH=(ssh -F /dev/null -i "$STATE/id" -p "$PORT" -o StrictHostKeyChecking=no
	-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=30 root@127.0.0.1)

log() { printf '\033[1m[e2e-vm] %s\033[0m\n' "$*" >&2; }

running() { [ -f "$STATE/pid" ] && kill -0 "$(cat "$STATE/pid")" 2>/dev/null; }

up() {
	mkdir -p "$STATE/shared"
	[ -f "$STATE/id" ] || ssh-keygen -q -t ed25519 -N '' -C imposm-operator-e2e-vm -f "$STATE/id"
	cp "$STATE/id.pub" "$STATE/shared/id.pub"
	if running; then log "VM already running"; return; fi
	log "building the VM"
	# path: so the flake is used as files on disk, tracked by git or not
	nix build "path:$DIR#nixosConfigurations.vm.config.system.build.vm" -o "$STATE/result"
	log "booting (console: $STATE/console.log)"
	(
		cd "$STATE"
		NIX_DISK_IMAGE="$STATE/nixos.qcow2" SHARED_DIR="$STATE/shared" \
			QEMU_NET_OPTS="hostfwd=tcp:127.0.0.1:$PORT-:22" \
			setsid "$STATE/result/bin/run-nixos-vm" >"$STATE/console.log" 2>&1 </dev/null &
		echo $! >"$STATE/pid"
	)
	for _ in $(seq 1 120); do
		"${SSH[@]}" -o ConnectTimeout=3 true 2>/dev/null && { log "VM is up"; return; }
		running || { tail -20 "$STATE/console.log" >&2; log "VM exited during boot"; exit 1; }
		sleep 3
	done
	log "VM did not come up within 6 minutes"; exit 1
}

down() {
	running || { rm -f "$STATE/pid"; return; }
	log "powering off"
	"${SSH[@]}" poweroff 2>/dev/null || true
	for _ in $(seq 1 60); do running || { rm -f "$STATE/pid"; return; }; sleep 2; done
	kill "$(cat "$STATE/pid")" 2>/dev/null || true
	rm -f "$STATE/pid"
}

test_e2e() {
	running || { log "VM not running (hack/e2e-vm/run.sh up)"; exit 1; }
	log "syncing the repo"
	rsync -a --delete -e "${SSH[*]:0:${#SSH[@]}-1}" \
		--exclude .git --exclude /bin --exclude /testbin --exclude /test/e2e/.kubeconfig \
		"$ROOT/" "root@127.0.0.1:/root/imposm-operator/"
	local args=""
	[ $# -gt 0 ] && args=$(printf ' %q' "$@")
	log "make e2e$args"
	"${SSH[@]}" "cd /root/imposm-operator && make e2e$args"
}

case ${1:-all} in
up) up ;;
down) down ;;
ssh) shift; exec "${SSH[@]}" "$@" ;;
test) shift; test_e2e "$@" ;;
destroy) down; rm -rf "$STATE"; log "removed $STATE" ;;
all)
	up
	trap down EXIT
	test_e2e
	;;
*) sed -n '2,15p' "$0" >&2; exit 2 ;;
esac
