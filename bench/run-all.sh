#!/usr/bin/env bash
# Four-backend cache benchmark orchestrator.
#
# Launches each caching system, runs the Go load generator against it, collects
# per-backend results, and merges them into combined.json. Each backend is
# benched in isolation (started, measured, torn down) so idle peers add no noise.
#
#   memcached  -> docker (memcached image)         :11211
#   redis      -> docker (redis image)             :6379
#   hazelcast  -> docker (hazelcast image)         :5702
#   bonsaigrid -> native release binary (host)      :5701
#
# The load generator is pure Go but `go` is not required on the host: it is built
# once inside a golang container into a static binary that runs on the host.
#
# Usage:
#   bench/run-all.sh                         # all four, default ramp
#   bench/run-all.sh bonsaigrid memcached    # subset, in this order
#   LEVELS=1,8,64 STAGE_SECS=6 bench/run-all.sh
#   KEEP_UP=1 bench/run-all.sh bonsaigrid    # leave services running for poking
#
# Key env knobs (all optional):
#   BACKENDS LEVELS STAGE_SECS WARMUP_SECS HZ_CONNS MAP_NAME MC_MEM_MB
#   DOCKER IMG_REDIS IMG_MEMCACHED IMG_HAZELCAST IMG_GO  KEEP_UP
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

# ---- configuration (env-overridable) ---------------------------------------
BACKENDS_DEFAULT="memcached redis hazelcast bonsaigrid"
LEVELS="${LEVELS:-1,2,4,8,16,32,64,128}"
STAGE_SECS="${STAGE_SECS:-4}"
WARMUP_SECS="${WARMUP_SECS:-2}"
HZ_CONNS="${HZ_CONNS:-128}"          # pooled connections for the hz-protocol client
MAP_NAME="${MAP_NAME:-bench}"
MC_MEM_MB="${MC_MEM_MB:-4096}"

DOCKER="${DOCKER:-docker}"
IMG_REDIS="${IMG_REDIS:-redis:7.4-alpine}"
IMG_MEMCACHED="${IMG_MEMCACHED:-memcached:1.6-alpine}"
IMG_HAZELCAST="${IMG_HAZELCAST:-hazelcast/hazelcast:5.5}"
IMG_GO="${IMG_GO:-golang:1.24}"

P_BONSAI=5701; P_HZ=5702; P_REDIS=6379; P_MC=11211
LOADDIR="$ROOT/bench/loadgen"
KEEP_UP="${KEEP_UP:-0}"

# ---- args: positional list overrides BACKENDS ------------------------------
POS=()
for a in "$@"; do
  case "$a" in
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    --keep-up) KEEP_UP=1 ;;
    -*)        echo "unknown flag: $a" >&2; exit 2 ;;
    *)         POS+=("$a") ;;
  esac
done
if [ "${#POS[@]}" -gt 0 ]; then BACKENDS="${POS[*]}"; else BACKENDS="${BACKENDS:-$BACKENDS_DEFAULT}"; fi

# ---- pretty logging --------------------------------------------------------
log()  { printf '\n\033[1;36m==>\033[0m %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# ---- teardown --------------------------------------------------------------
BONSAI_PID=""
declare -a STARTED_CONTAINERS=()

stop_container() { # name
  local c="$1"
  $DOCKER rm -f "$c" >/dev/null 2>&1 || true
  # drop from tracking list
  local keep=(); local x
  for x in "${STARTED_CONTAINERS[@]:-}"; do [ -n "$x" ] && [ "$x" != "$c" ] && keep+=("$x"); done
  STARTED_CONTAINERS=("${keep[@]:-}")
}
stop_bonsai() {
  [ -n "$BONSAI_PID" ] && kill "$BONSAI_PID" 2>/dev/null || true
  [ -n "$BONSAI_PID" ] && wait "$BONSAI_PID" 2>/dev/null || true
  BONSAI_PID=""
}
cleanup() {
  if [ "$KEEP_UP" = "1" ]; then warn "KEEP_UP=1 — leaving services up (clean up with: $DOCKER rm -f ${STARTED_CONTAINERS[*]:-}; kill ${BONSAI_PID:-})"; return; fi
  log "Tearing down"
  stop_bonsai
  local c
  for c in "${STARTED_CONTAINERS[@]:-}"; do [ -n "$c" ] && $DOCKER rm -f "$c" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT INT TERM

# ---- readiness helpers -----------------------------------------------------
wait_port() { # host port timeout_secs
  local host="$1" port="$2" to="${3:-30}"
  timeout "$to" bash -c "until (exec 3<>/dev/tcp/$host/$port) 2>/dev/null; do sleep 0.3; done" 2>/dev/null
}
wait_hz_ready() { # container timeout_secs — Hazelcast prints '... is STARTED'
  local c="$1" to="${2:-120}" i=0
  while [ "$i" -lt "$((to*2))" ]; do
    $DOCKER logs "$c" 2>&1 | grep -q "is STARTED" && return 0
    sleep 0.5; i=$((i+1))
  done
  return 1
}

needs_docker() { case " $BACKENDS " in *" redis "*|*" memcached "*|*" hazelcast "*) return 0;; esac; return 1; }

# ---- preflight -------------------------------------------------------------
log "Configuration"
info "backends     : $BACKENDS"
info "levels       : $LEVELS   stage=${STAGE_SECS}s warmup=${WARMUP_SECS}s"
info "hz_conns     : $HZ_CONNS   map=$MAP_NAME"
info "results dir  : $LOADDIR"

command -v "$DOCKER" >/dev/null 2>&1 || die "docker not found (set DOCKER= or install it)"
$DOCKER info >/dev/null 2>&1 || die "cannot reach the docker daemon (need sudo, or add your user to the 'docker' group?)"
command -v cargo >/dev/null 2>&1 || die "cargo not found (needed to build the BonsaiGrid server)"

# ---- build BonsaiGrid (only if selected) -----------------------------------
if [[ " $BACKENDS " == *" bonsaigrid "* ]]; then
  log "Building BonsaiGrid server (release)"
  cargo build --release -q -p server || die "cargo build -p server failed"
  [ -x "$ROOT/target/release/server" ] || die "server binary missing after build"
fi

# ---- build the Go load generator (static, in a container) ------------------
log "Building Go load generator via $IMG_GO (static binary)"
# Persist the Go module + build cache in a user-owned host dir so only the first
# run pays the module download.
GOCACHE_DIR="${GOCACHE_DIR:-$HOME/.cache/bonsai-bench/gocache}"
GOMOD_DIR="${GOMOD_DIR:-$HOME/.cache/bonsai-bench/gomod}"
mkdir -p "$GOCACHE_DIR" "$GOMOD_DIR"
# Run as the host user so the binary and any module/go.sum writes are owned by us
# (not root), and point GOPATH/GOCACHE/HOME at writable dirs for the non-root uid.
$DOCKER run --rm -v "$ROOT":/src -w /src/bench/loadgen \
  --user "$(id -u):$(id -g)" \
  -v "$GOCACHE_DIR":/gocache -v "$GOMOD_DIR":/gomod \
  -e HOME=/tmp -e GOPATH=/gomod -e GOCACHE=/gocache \
  -e CGO_ENABLED=0 -e GOFLAGS=-mod=mod \
  "$IMG_GO" go build -buildvcs=false -o loadgen . \
  || die "loadgen build failed (first run needs network to fetch Go modules)"
[ -x "$LOADDIR/loadgen" ] || die "loadgen binary not produced"
info "built $LOADDIR/loadgen"

# ---- per-backend start / stop ----------------------------------------------
start_backend() {
  local t="$1"
  case "$t" in
    memcached)
      log "Starting memcached ($IMG_MEMCACHED, ${MC_MEM_MB}MB) on :$P_MC"
      $DOCKER run -d --name bench_memcached --network host "$IMG_MEMCACHED" \
        memcached -m "$MC_MEM_MB" -p "$P_MC" >/dev/null || die "memcached start failed"
      STARTED_CONTAINERS+=(bench_memcached)
      wait_port 127.0.0.1 "$P_MC" 30 || die "memcached did not open :$P_MC" ;;
    redis)
      log "Starting redis ($IMG_REDIS, no persistence) on :$P_REDIS"
      $DOCKER run -d --name bench_redis --network host "$IMG_REDIS" \
        redis-server --port "$P_REDIS" --save '' --appendonly no >/dev/null || die "redis start failed"
      STARTED_CONTAINERS+=(bench_redis)
      wait_port 127.0.0.1 "$P_REDIS" 30 || die "redis did not open :$P_REDIS" ;;
    hazelcast)
      log "Starting hazelcast ($IMG_HAZELCAST) on :$P_HZ (JVM boot ~20s)"
      $DOCKER run -d --name bench_hazelcast --network host \
        -e HZ_NETWORK_PORT_PORT="$P_HZ" -e HZ_NETWORK_PORT_AUTOINCREMENT=false \
        -e HZ_NETWORK_JOIN_MULTICAST_ENABLED=false -e HZ_CLUSTERNAME=dev \
        "$IMG_HAZELCAST" >/dev/null || die "hazelcast start failed"
      STARTED_CONTAINERS+=(bench_hazelcast)
      wait_port 127.0.0.1 "$P_HZ" 120 || die "hazelcast did not open :$P_HZ"
      wait_hz_ready bench_hazelcast 120 || warn "hazelcast 'is STARTED' not seen; proceeding anyway"
      sleep 2 ;;
    bonsaigrid)
      log "Starting BonsaiGrid (native release) on :$P_BONSAI"
      "$ROOT/target/release/server" >/tmp/bench_bonsaigrid.log 2>&1 &
      BONSAI_PID=$!
      wait_port 127.0.0.1 "$P_BONSAI" 30 || die "BonsaiGrid did not open :$P_BONSAI (see /tmp/bench_bonsaigrid.log)"
      head -1 /tmp/bench_bonsaigrid.log | sed 's/^/    /' ;;
    *) die "unknown backend: $t" ;;
  esac
}
stop_backend() {
  case "$1" in
    memcached) stop_container bench_memcached ;;
    redis)     stop_container bench_redis ;;
    hazelcast) stop_container bench_hazelcast ;;
    bonsaigrid) stop_bonsai ;;
  esac
}

# ---- run the ramp against one backend --------------------------------------
bench_one() {
  local t="$1"
  start_backend "$t"
  log "Load generating against $t"
  # Remove any stale result (older runs left root-owned files); the loadgen writes
  # a fresh one owned by us. Deletion works because $LOADDIR is user-owned.
  rm -f "$LOADDIR/results-$t.json"
  if ( cd "$LOADDIR" && \
       TARGET="$t" LEVELS="$LEVELS" STAGE_SECS="$STAGE_SECS" WARMUP_SECS="$WARMUP_SECS" \
       HZ_CONNS="$HZ_CONNS" MAP_NAME="$MAP_NAME" OUT="results-$t.json" \
       ./loadgen ); then
    info "wrote $LOADDIR/results-$t.json"
  else
    warn "loadgen failed for $t (leaving previous results-$t.json untouched)"
  fi
  stop_backend "$t"
}

# ---- main ------------------------------------------------------------------
for t in $BACKENDS; do bench_one "$t"; done

# ---- merge into combined.json + summary ------------------------------------
log "Merging results -> combined.json"
python3 - "$LOADDIR" $BACKENDS <<'PY'
import json, os, sys
outdir, backends = sys.argv[1], sys.argv[2:]
combined = {}
rows = []
for b in backends:
    p = os.path.join(outdir, f"results-{b}.json")
    if not os.path.exists(p):
        print(f"    (skip {b}: no results file)"); continue
    stages = json.load(open(p)).get("stages", [])
    combined[b] = stages
    if stages:
        last = stages[-1]
        rows.append((b, last["level"], last["set"]["rps"], last["set"]["p99_us"]))
json.dump(combined, open(os.path.join(outdir, "combined.json"), "w"), indent=2)
print(f"    wrote {os.path.join(outdir,'combined.json')} with {list(combined)}")
if rows:
    print("\n    peak stage (highest level):")
    print(f"    {'backend':<12}{'conns':>7}{'ops/s':>12}{'p99 (set)':>12}")
    for b, lvl, rps, p99 in rows:
        us = f"{p99/1000:.1f} ms" if p99 >= 1000 else f"{int(p99)} µs"
        print(f"    {b:<12}{lvl:>7}{int(rps):>12,}{us:>12}")
PY

log "Done"
info "Results: $LOADDIR/results-*.json  ->  $LOADDIR/combined.json"
info "The dashboard (bench/deploy/dashboard.html) embeds its data inline;"
info "update those arrays from combined.json to refresh it."
