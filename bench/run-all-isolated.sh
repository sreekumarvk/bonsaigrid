#!/usr/bin/env bash
# Four-backend cache benchmark with cgroup isolation (fair comparison).
#
# Every server AND the load generator runs in its own Docker container (= its own
# cgroup v2), with:
#   * disjoint cpuset masks  — servers on one set of CPUs, the client on another,
#     so the client and the server under test never contend for the same cores;
#   * equal CPU budget       — every server gets the SAME cpuset (same core count);
#   * equal memory budget    — every server gets the SAME --memory cap.
#
# This removes the co-location confound: in the non-isolated run the CPU-heavy
# Hazelcast Go client and the server fought over the same cores, so throughput
# reflected scheduling, not the server. Here each server gets a fixed, private
# CPU/mem budget and the client is quarantined to its own cores.
#
#   memcached  -> docker  :11211   (memcached -t = #server cpus)
#   redis      -> docker  :6379
#   hazelcast  -> docker  :5702
#   bonsaigrid -> docker  :5701    (host binary in a glibc-matched image,
#                                   seccomp=unconfined so io_uring works)
#   loadgen    -> docker            (pinned to the CLIENT cpuset)
#
# Usage:
#   bench/run-all-isolated.sh
#   bench/run-all-isolated.sh bonsaigrid memcached
#   SERVER_CPUS=0-7 CLIENT_CPUS=8-19 SERVER_MEM=4g bench/run-all-isolated.sh
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

# ---- isolation config (env-overridable) ------------------------------------
SERVER_CPUS="${SERVER_CPUS:-0-7}"      # cpuset mask for the server under test
CLIENT_CPUS="${CLIENT_CPUS:-8-19}"     # cpuset mask for the load generator
SERVER_MEM="${SERVER_MEM:-4g}"         # memory cap per server
CLIENT_MEM="${CLIENT_MEM:-8g}"         # memory cap for the client

# ---- workload config -------------------------------------------------------
BACKENDS_DEFAULT="memcached redis hazelcast bonsaigrid"
LEVELS="${LEVELS:-1,2,4,8,16,32,64,128}"
STAGE_SECS="${STAGE_SECS:-4}"
WARMUP_SECS="${WARMUP_SECS:-2}"
HZ_CONNS="${HZ_CONNS:-128}"
MAP_NAME="${MAP_NAME:-bench}"
MC_MEM_MB="${MC_MEM_MB:-4096}"
SAMPLE_MS="${SAMPLE_MS:-250}"        # server cgroup CPU/mem sampling interval

DOCKER="${DOCKER:-docker}"
IMG_REDIS="${IMG_REDIS:-redis:7.4-alpine}"
IMG_MEMCACHED="${IMG_MEMCACHED:-memcached:1.6-alpine}"
IMG_HAZELCAST="${IMG_HAZELCAST:-hazelcast/hazelcast:5.5}"
IMG_GO="${IMG_GO:-golang:1.24}"
IMG_LOADGEN="${IMG_LOADGEN:-alpine:3}"   # runs the static loadgen binary
# BonsaiGrid base image: match the host libc so the host-built binary runs.
_OSID=ubuntu; _OSVER=24.04; [ -r /etc/os-release ] && . /etc/os-release && _OSID="${ID:-ubuntu}" && _OSVER="${VERSION_ID:-24.04}"
IMG_BONSAI_BASE="${IMG_BONSAI_BASE:-${_OSID}:${_OSVER}}"

P_BONSAI=5701; P_HZ=5702; P_REDIS=6379; P_MC=11211
LOADDIR="$ROOT/bench/loadgen"
KEEP_UP="${KEEP_UP:-0}"

# ---- args ------------------------------------------------------------------
POS=()
for a in "$@"; do
  case "$a" in
    -h|--help) sed -n '2,33p' "$0"; exit 0 ;;
    --keep-up) KEEP_UP=1 ;;
    -*) echo "unknown flag: $a" >&2; exit 2 ;;
    *) POS+=("$a") ;;
  esac
done
if [ "${#POS[@]}" -gt 0 ]; then BACKENDS="${POS[*]}"; else BACKENDS="${BACKENDS:-$BACKENDS_DEFAULT}"; fi

log()  { printf '\n\033[1;36m==>\033[0m %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

cpuset_count() { # "0-7,10" -> integer core count
  local spec="$1" n=0 part a b; local IFS=','
  for part in $spec; do
    if [[ "$part" == *-* ]]; then a="${part%-*}"; b="${part#*-}"; n=$((n + b - a + 1)); else n=$((n+1)); fi
  done
  echo "$n"
}
SERVER_NCPU="$(cpuset_count "$SERVER_CPUS")"

# ---- teardown --------------------------------------------------------------
ALL_NAMES="bench_bonsaigrid bench_hazelcast bench_redis bench_memcached"
cleanup() {
  if [ "$KEEP_UP" = "1" ]; then warn "KEEP_UP=1 — leaving containers up ($ALL_NAMES)"; return; fi
  log "Tearing down"
  for c in $ALL_NAMES; do $DOCKER rm -f "$c" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT INT TERM

wait_port() { local host="$1" port="$2" to="${3:-30}"; timeout "$to" bash -c "until (exec 3<>/dev/tcp/$host/$port) 2>/dev/null; do sleep 0.3; done" 2>/dev/null; }
wait_hz_ready() { local c="$1" to="${2:-120}" i=0; while [ "$i" -lt "$((to*2))" ]; do $DOCKER logs "$c" 2>&1 | grep -q "is STARTED" && return 0; sleep 0.5; i=$((i+1)); done; return 1; }

# ---- server resource sampling (cgroup v2, read directly) -------------------
resolve_cgroup() { # container name -> unified cgroup dir (or empty)
  local id; id=$($DOCKER inspect -f '{{.Id}}' "$1" 2>/dev/null) || return 0
  find /sys/fs/cgroup -maxdepth 5 -type d -name "*$id*" 2>/dev/null | head -1
}
# Background loop: append "epoch_ms cpu_pct_of_budget mem_mb" every SAMPLE_MS.
# cpu_pct is % of the server's cpu budget (SERVER_NCPU cores); mem is the working
# set (memory.current minus reclaimable file cache), like docker/k8s report.
sample_loop() {
  local cg="$1" ncpu="$2" out="$3" iv; iv=$(awk "BEGIN{print ${SAMPLE_MS}/1000}")
  local pu pt u t now mc inact memmb cores pct
  pu=$(awk '/^usage_usec/{print $2}' "$cg/cpu.stat" 2>/dev/null || echo 0)
  pt=$(date +%s%3N)
  while :; do
    sleep "$iv"
    u=$(awk '/^usage_usec/{print $2}' "$cg/cpu.stat" 2>/dev/null) || break
    now=$(date +%s%3N)
    mc=$(cat "$cg/memory.current" 2>/dev/null || echo 0)
    inact=$(awk '/^inactive_file/{print $2}' "$cg/memory.stat" 2>/dev/null || echo 0)
    memmb=$(awk "BEGIN{printf \"%.1f\",($mc-$inact)/1048576}")
    cores=$(awk "BEGIN{dt=$now-$pt; if(dt<=0)dt=1; printf \"%.4f\",($u-$pu)/(dt*1000)}")
    pct=$(awk "BEGIN{printf \"%.1f\",$cores/$ncpu*100}")
    echo "$now $pct $memmb" >> "$out"
    pu=$u; pt=$now
  done
}
# Inject per-stage CPU/mem aggregates into a results file using its stage windows.
inject_resources() {
  python3 - "$1" "$2" <<'PY'
import json, sys
res, samp = sys.argv[1], sys.argv[2]
d = json.load(open(res))
S = []
for ln in open(samp):
    p = ln.split()
    if len(p) == 3:
        try: S.append((int(p[0]), float(p[1]), float(p[2])))
        except ValueError: pass
def agg(lo, hi):
    xs = [s for s in S if lo <= s[0] <= hi]
    if not xs: return None
    c = [x[1] for x in xs]; m = [x[2] for x in xs]
    return {"cpu": {"avg_pct": round(sum(c)/len(c), 1), "max_pct": round(max(c), 1)},
            "mem": {"avg_mb": round(sum(m)/len(m), 1), "max_mb": round(max(m), 1)},
            "res_samples": len(xs)}
n = 0
for st in d.get("stages", []):
    a = agg(st.get("t_start_ms", 0), st.get("t_end_ms", 0))
    if a: st.update(a); n += 1
json.dump(d, open(res, "w"), indent=2)
print(f"    injected CPU/mem into {n}/{len(d.get('stages',[]))} stages ({len(S)} samples)")
PY
}

# ---- preflight -------------------------------------------------------------
log "Isolation configuration"
info "server cpuset : $SERVER_CPUS  (${SERVER_NCPU} cpus)   mem=$SERVER_MEM"
info "client cpuset : $CLIENT_CPUS                       mem=$CLIENT_MEM"
info "backends      : $BACKENDS"
info "workload      : levels=$LEVELS stage=${STAGE_SECS}s warmup=${WARMUP_SECS}s hz_conns=$HZ_CONNS"
info "bonsai image  : $IMG_BONSAI_BASE (io_uring via seccomp=unconfined)"

# Environment checks + stale-container cleanup (also runnable standalone).
DOCKER="$DOCKER" bash "$ROOT/bench/preflight.sh" || die "preflight failed"
[ "$SERVER_NCPU" -ge 1 ] || die "SERVER_CPUS parsed to 0 cpus"

# ---- build BonsaiGrid + loadgen --------------------------------------------
if [[ " $BACKENDS " == *" bonsaigrid "* ]]; then
  log "Building BonsaiGrid server + bench tool (release)"
  cargo build --release -q -p server -p bench || die "cargo build -p server -p bench failed"
fi
log "Building Go load generator via $IMG_GO (static binary)"
GOCACHE_DIR="${GOCACHE_DIR:-$HOME/.cache/bonsai-bench/gocache}"
GOMOD_DIR="${GOMOD_DIR:-$HOME/.cache/bonsai-bench/gomod}"
mkdir -p "$GOCACHE_DIR" "$GOMOD_DIR"
$DOCKER run --rm -v "$ROOT":/src -w /src/bench/loadgen \
  --user "$(id -u):$(id -g)" \
  -v "$GOCACHE_DIR":/gocache -v "$GOMOD_DIR":/gomod \
  -e HOME=/tmp -e GOPATH=/gomod -e GOCACHE=/gocache -e CGO_ENABLED=0 -e GOFLAGS=-mod=mod \
  "$IMG_GO" go build -buildvcs=false -o loadgen . || die "loadgen build failed (need network for modules?)"
[ -x "$LOADDIR/loadgen" ] || die "loadgen binary not produced"

# ---- per-backend server (each in its own cgroup + cpuset) ------------------
start_backend() {
  local t="$1"
  case "$t" in
    memcached)
      log "memcached  cpuset=$SERVER_CPUS mem=$SERVER_MEM threads=$SERVER_NCPU"
      $DOCKER run -d --name bench_memcached --network host \
        --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
        "$IMG_MEMCACHED" memcached -m "$MC_MEM_MB" -p "$P_MC" -t "$SERVER_NCPU" >/dev/null || die "memcached start failed"
      wait_port 127.0.0.1 "$P_MC" 30 || die "memcached did not open :$P_MC" ;;
    redis)
      log "redis      cpuset=$SERVER_CPUS mem=$SERVER_MEM"
      $DOCKER run -d --name bench_redis --network host \
        --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
        "$IMG_REDIS" redis-server --port "$P_REDIS" --save '' --appendonly no >/dev/null || die "redis start failed"
      wait_port 127.0.0.1 "$P_REDIS" 30 || die "redis did not open :$P_REDIS" ;;
    hazelcast)
      log "hazelcast  cpuset=$SERVER_CPUS mem=$SERVER_MEM (JVM boot ~20s)"
      $DOCKER run -d --name bench_hazelcast --network host \
        --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
        -e HZ_NETWORK_PORT_PORT="$P_HZ" -e HZ_NETWORK_PORT_AUTOINCREMENT=false \
        -e HZ_NETWORK_JOIN_MULTICAST_ENABLED=false -e HZ_CLUSTERNAME=dev \
        -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0" \
        "$IMG_HAZELCAST" >/dev/null || die "hazelcast start failed"
      wait_port 127.0.0.1 "$P_HZ" 120 || die "hazelcast did not open :$P_HZ"
      wait_hz_ready bench_hazelcast 120 || warn "hazelcast 'is STARTED' not seen; proceeding"; sleep 2 ;;
    bonsaigrid)
      log "bonsaigrid cpuset=$SERVER_CPUS mem=$SERVER_MEM cores=$SERVER_NCPU (containerized)"
      $DOCKER run -d --name bench_bonsaigrid --network host \
        --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
        --security-opt seccomp=unconfined \
        -v "$ROOT":/w -w /w -e BONSAI_CORES="$SERVER_NCPU" \
        "$IMG_BONSAI_BASE" ./target/release/server >/dev/null || die "bonsaigrid start failed"
      wait_port 127.0.0.1 "$P_BONSAI" 30 || { $DOCKER logs bench_bonsaigrid 2>&1 | tail -5; die "bonsaigrid did not open :$P_BONSAI"; }
      $DOCKER logs bench_bonsaigrid 2>&1 | grep -m1 "listening" | sed 's/^/    /' || true ;;
    *) die "unknown backend: $t" ;;
  esac
}
stop_backend() { case "$1" in
    memcached) $DOCKER rm -f bench_memcached >/dev/null 2>&1 || true ;;
    redis)     $DOCKER rm -f bench_redis >/dev/null 2>&1 || true ;;
    hazelcast) $DOCKER rm -f bench_hazelcast >/dev/null 2>&1 || true ;;
    bonsaigrid)$DOCKER rm -f bench_bonsaigrid >/dev/null 2>&1 || true ;;
  esac; }

# ---- run the loadgen (pinned to the CLIENT cpuset) -------------------------
run_loadgen() {
  local t="$1"
  rm -f "$LOADDIR/results-$t.json"
  $DOCKER run --rm --network host \
    --cpuset-cpus="$CLIENT_CPUS" --memory="$CLIENT_MEM" \
    --user "$(id -u):$(id -g)" \
    -v "$ROOT":/src -w /src/bench/loadgen -e HOME=/tmp \
    -e TARGET="$t" -e LEVELS="$LEVELS" -e STAGE_SECS="$STAGE_SECS" -e WARMUP_SECS="$WARMUP_SECS" \
    -e HZ_CONNS="$HZ_CONNS" -e MAP_NAME="$MAP_NAME" -e OUT="results-$t.json" \
    "$IMG_LOADGEN" ./loadgen
}

# Thin-client server-ceiling reference: drive the SAME isolated BonsaiGrid with the
# native raw-protocol bench client (no official Hazelcast-client tax), client pinned
# to the CLIENT cpuset. Regenerates results-bonsaigrid-fair.json every run (the
# dashboard's dashed reference line) so it is never stale.
run_ladder() {
  local out="$LOADDIR/results-bonsaigrid-fair.json"
  command -v taskset >/dev/null 2>&1 || { warn "taskset not found; skipping thin-client reference"; return; }
  [ -x "$ROOT/target/release/bench" ] || { warn "bench binary missing; skipping thin-client reference"; return; }
  log "Thin-client reference: bench ladder (client on cpuset $CLIENT_CPUS)"
  if LEVELS="$LEVELS" BENCH_ADDR="127.0.0.1:$P_BONSAI" \
       taskset -c "$CLIENT_CPUS" "$ROOT/target/release/bench" ladder "$STAGE_SECS" 128 > "$out" 2>/dev/null; then
    info "wrote $(basename "$out")"
  else
    warn "bench ladder failed; keeping the previous thin-client reference"
  fi
}

bench_one() {
  local t="$1"
  start_backend "$t"
  # start sampling the server's cgroup for the duration of the load
  local cg samp="/tmp/bench_res_$t.txt" spid=""
  cg="$(resolve_cgroup "bench_$t")"
  if [ -n "$cg" ] && [ -r "$cg/cpu.stat" ]; then
    : > "$samp"; sample_loop "$cg" "$SERVER_NCPU" "$samp" & spid=$!
  else
    warn "could not read cgroup for bench_$t; CPU/mem not sampled"
  fi
  log "Load generating against $t (client on cpuset $CLIENT_CPUS)"
  if run_loadgen "$t"; then info "wrote results-$t.json"; else warn "loadgen failed for $t"; fi
  if [ -n "$spid" ]; then kill "$spid" 2>/dev/null; wait "$spid" 2>/dev/null || true
    [ -f "$LOADDIR/results-$t.json" ] && inject_resources "$LOADDIR/results-$t.json" "$samp"
    rm -f "$samp"
  fi
  [ "$t" = "bonsaigrid" ] && run_ladder   # refresh the thin-client reference while the server is up
  stop_backend "$t"
}

# ---- main ------------------------------------------------------------------
for t in $BACKENDS; do bench_one "$t"; done

log "Merging results -> combined.json"
python3 - "$LOADDIR" $BACKENDS <<'PY'
import json, os, sys
outdir, backends = sys.argv[1], sys.argv[2:]
combined, rows = {}, []
for b in backends:
    p = os.path.join(outdir, f"results-{b}.json")
    if not os.path.exists(p): print(f"    (skip {b}: no results)"); continue
    stages = json.load(open(p)).get("stages", [])
    combined[b] = stages
    if stages:
        last = stages[-1]; rows.append((b, last["level"], last["set"]["rps"], last["set"]["p99_us"]))
json.dump(combined, open(os.path.join(outdir, "combined.json"), "w"), indent=2)
print(f"    wrote combined.json with {list(combined)}")
if rows:
    print("\n    peak stage (isolated, equal CPU+mem budget):")
    print(f"    {'backend':<12}{'conns':>7}{'ops/s':>12}{'p99 set':>11}")
    for b, lvl, rps, p99 in sorted(rows, key=lambda r:-r[2]):
        us = f"{p99/1000:.1f} ms" if p99 >= 1000 else f"{int(p99)} us"
        print(f"    {b:<12}{lvl:>7}{int(rps):>12,}{us:>11}")
PY

# Regenerate the self-contained dashboard from this run (data → HTML, no hand-editing).
python3 bench/gen_dashboard.py "$LOADDIR/combined.json" "$LOADDIR/results-bonsaigrid-fair.json" 2>/dev/null \
  && info "dashboard baked: bench/deploy/dashboard.html" || true

log "Done"
info "Each server ran on cpuset $SERVER_CPUS (${SERVER_NCPU} cpus, $SERVER_MEM); client on $CLIENT_CPUS."
info "Results: $LOADDIR/combined.json · dashboard: bench/deploy/dashboard.html · track: bench/track.sh"
