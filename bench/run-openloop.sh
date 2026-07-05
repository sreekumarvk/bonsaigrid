#!/usr/bin/env bash
# Open-loop, coordinated-omission-correct benchmark in the cgroup-isolated harness.
# Unlike run-all-isolated.sh (closed-loop, connection ladder), this sweeps an
# OFFERED-RATE ladder and measures latency from each request's ideal send time, so
# the tail reflects real queueing delay once the server saturates — the number a
# closed-loop test cannot see. Realistic workload: Zipf keyspace (real hit/miss),
# configurable read/write ratio and object size.
#
#   memcached / redis                 -> real incumbents
#   bonsaigrid-mc / bonsaigrid-redis  -> BonsaiGrid via its memcached / RESP protocol
#   bonsaigrid                        -> BonsaiGrid via the official Hazelcast client
#
# Usage:
#   bench/run-openloop.sh
#   RATIO=1:9 DATA_SIZE=1024 bench/run-openloop.sh bonsaigrid-mc memcached
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"; LOADDIR="$ROOT/bench/loadgen"

SERVER_CPUS="${SERVER_CPUS:-0-7}"; CLIENT_CPUS="${CLIENT_CPUS:-8-19}"; SERVER_MEM="${SERVER_MEM:-4g}"
RATES="${RATES:-25000,50000,100000,200000,400000,600000,800000,1000000}"
CONNS="${CONNS:-50}"; STAGE_SECS="${STAGE_SECS:-5}"; WARMUP_SECS="${WARMUP_SECS:-3}"
RATIO="${RATIO:-1:9}"; DATA_SIZE="${DATA_SIZE:-128}"; KEY_MAX="${KEY_MAX:-5000000}"; ZIPF_S="${ZIPF_S:-1.05}"
DOCKER="${DOCKER:-docker}"; IMG_GO="${IMG_GO:-golang:1.24}"; IMG_LOADGEN="${IMG_LOADGEN:-alpine:3}"
IMG_REDIS="${IMG_REDIS:-redis:7.4-alpine}"; IMG_MEMCACHED="${IMG_MEMCACHED:-memcached:1.6-alpine}"
_OSID=ubuntu; _OSVER=24.04; [ -r /etc/os-release ] && . /etc/os-release && _OSID="${ID:-ubuntu}" && _OSVER="${VERSION_ID:-24.04}"
IMG_BONSAI_BASE="${IMG_BONSAI_BASE:-${_OSID}:${_OSVER}}"; MC_MEM_MB="${MC_MEM_MB:-4096}"

BACKENDS_DEFAULT="memcached bonsaigrid-mc redis bonsaigrid-redis"
POS=(); for a in "$@"; do case "$a" in -h|--help) sed -n '2,15p' "$0"; exit 0;; *) POS+=("$a");; esac; done
[ "${#POS[@]}" -gt 0 ] && BACKENDS="${POS[*]}" || BACKENDS="${BACKENDS:-$BACKENDS_DEFAULT}"

log(){ printf '\n\033[1;36m==>\033[0m %s\n' "$*"; }
info(){ printf '    %s\n' "$*"; }
warn(){ printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }
ncpus(){ local s=0 p a b; local IFS=','; for p in $1; do [[ $p == *-* ]] && { a=${p%-*}; b=${p#*-}; s=$((s+b-a+1)); } || s=$((s+1)); done; echo $s; }
SERVER_NCPU=$(ncpus "$SERVER_CPUS")

cleanup(){ for c in bench_memcached bench_redis bench_bonsaigrid; do $DOCKER rm -f "$c" >/dev/null 2>&1 || true; done; }
trap cleanup EXIT INT TERM
wait_port(){ timeout "${3:-30}" bash -c "until (exec 3<>/dev/tcp/$1/$2) 2>/dev/null; do sleep 0.3; done" 2>/dev/null; }

start_server(){ case "$1" in
  memcached) $DOCKER run -d --name bench_memcached --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_MEMCACHED" memcached -m "$MC_MEM_MB" -p 11211 -t "$SERVER_NCPU" >/dev/null; wait_port 127.0.0.1 11211 ;;
  redis) $DOCKER run -d --name bench_redis --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_REDIS" redis-server --port 6379 --save '' --appendonly no >/dev/null; wait_port 127.0.0.1 6379 ;;
  bonsaigrid|bonsaigrid-mc|bonsaigrid-redis) $DOCKER run -d --name bench_bonsaigrid --network host --cpuset-cpus="$SERVER_CPUS" \
      --memory="$SERVER_MEM" --security-opt seccomp=unconfined -v "$ROOT":/w -w /w -e BONSAI_CORES="$SERVER_NCPU" \
      "$IMG_BONSAI_BASE" ./target/release/server >/dev/null; wait_port 127.0.0.1 5701 30 || { $DOCKER logs bench_bonsaigrid 2>&1|tail -5; die "bonsaigrid :5701"; } ;;
esac; }
stop_server(){ case "$1" in memcached) $DOCKER rm -f bench_memcached;; redis) $DOCKER rm -f bench_redis;;
  bonsaigrid|bonsaigrid-mc|bonsaigrid-redis) $DOCKER rm -f bench_bonsaigrid;; esac >/dev/null 2>&1 || true; }

command -v "$DOCKER" >/dev/null 2>&1 && $DOCKER info >/dev/null 2>&1 || die "docker not reachable"
if [[ " $BACKENDS " == *bonsaigrid* ]]; then cargo build --release -q -p server || die "cargo build -p server"; fi
log "Building Go load generator ($IMG_GO)"
GOCACHE_DIR="${GOCACHE_DIR:-$HOME/.cache/bonsai-bench/gocache}"; GOMOD_DIR="${GOMOD_DIR:-$HOME/.cache/bonsai-bench/gomod}"
mkdir -p "$GOCACHE_DIR" "$GOMOD_DIR"
$DOCKER run --rm -v "$ROOT":/src -w /src/bench/loadgen --user "$(id -u):$(id -g)" \
  -v "$GOCACHE_DIR":/gocache -v "$GOMOD_DIR":/gomod \
  -e HOME=/tmp -e GOPATH=/gomod -e GOCACHE=/gocache -e CGO_ENABLED=0 -e GOFLAGS=-mod=mod \
  "$IMG_GO" go build -buildvcs=false -o loadgen . || die "loadgen build failed"

log "open-loop: rates=$RATES conns=$CONNS ratio=$RATIO data=${DATA_SIZE}B keyspace=$KEY_MAX zipfS=$ZIPF_S"
info "server cpuset=$SERVER_CPUS mem=$SERVER_MEM · client cpuset=$CLIENT_CPUS"

run_loadgen(){ local t="$1"; rm -f "$LOADDIR/results-open-$t.json"
  $DOCKER run --rm --network host --cpuset-cpus="$CLIENT_CPUS" --user "$(id -u):$(id -g)" \
    -v "$ROOT":/src -w /src/bench/loadgen -e HOME=/tmp \
    -e MODE=open -e TARGET="$t" -e RATES="$RATES" -e CONNS="$CONNS" -e STAGE_SECS="$STAGE_SECS" \
    -e WARMUP_SECS="$WARMUP_SECS" -e RATIO="$RATIO" -e DATA_SIZE="$DATA_SIZE" -e KEY_MAX="$KEY_MAX" \
    -e ZIPF_S="$ZIPF_S" -e OUT="results-open-$t.json" "$IMG_LOADGEN" ./loadgen; }

for t in $BACKENDS; do
  log "$t"; start_server "$t"
  if run_loadgen "$t"; then info "wrote results-open-$t.json"; else warn "loadgen failed for $t"; fi
  stop_server "$t"
done

log "Merging + plotting"
python3 bench/openloop_report.py "$LOADDIR" $BACKENDS
