#!/usr/bin/env bash
# Industry-standard cache benchmark with memtier_benchmark (Redis Labs), in the same
# cgroup-isolated harness as run-all-isolated.sh. memtier drives each backend through
# its real wire protocol with the SAME thin client, and reports the metrics the
# industry cares about: throughput, p50/p99/p99.9 latency, hit/miss ratio, and
# network KB/sec. One tool, apples-to-apples across:
#
#   memcached      -> docker,   memcache_text  :11211
#   redis          -> docker,   redis (RESP)   :6379
#   bonsaigrid-mc  -> docker,   memcache_text  :5701   (BonsaiGrid's memcached protocol)
#   bonsaigrid-redis-> docker,  redis (RESP)   :5701   (BonsaiGrid's RESP protocol)
#
# Servers run on cpuset SERVER_CPUS (equal cpu/mem); memtier on CLIENT_CPUS.
# Per-level raw JSON lands in bench/loadgen/memtier/; memtier_report.py merges it.
#
# Usage:
#   bench/run-memtier.sh
#   bench/run-memtier.sh memcached bonsaigrid-mc
#   RATIO=9:1 DATA_SIZE=1024 bench/run-memtier.sh     # 90/10 read/write, 1 KB objects
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

SERVER_CPUS="${SERVER_CPUS:-0-7}"; CLIENT_CPUS="${CLIENT_CPUS:-8-19}"; SERVER_MEM="${SERVER_MEM:-4g}"
LEVELS="${LEVELS:-1,2,4,8,16,32,64,128}"
STAGE_SECS="${STAGE_SECS:-5}"
RATIO="${RATIO:-1:1}"                 # set:get, memtier's --ratio (1:1 = 50/50)
DATA_SIZE="${DATA_SIZE:-128}"         # object size in bytes
KEY_MAX="${KEY_MAX:-1000000}"         # keyspace (drives hit/miss ratio)
KEY_PATTERN="${KEY_PATTERN:-R:R}"     # random get/set; G:G = gaussian hotspot
DOCKER="${DOCKER:-docker}"
IMG_MEMTIER="${IMG_MEMTIER:-redislabs/memtier_benchmark:latest}"
IMG_REDIS="${IMG_REDIS:-redis:7.4-alpine}"; IMG_MEMCACHED="${IMG_MEMCACHED:-memcached:1.6-alpine}"
_OSID=ubuntu; _OSVER=24.04; [ -r /etc/os-release ] && . /etc/os-release && _OSID="${ID:-ubuntu}" && _OSVER="${VERSION_ID:-24.04}"
IMG_BONSAI_BASE="${IMG_BONSAI_BASE:-${_OSID}:${_OSVER}}"
MC_MEM_MB="${MC_MEM_MB:-4096}"
OUTDIR="$ROOT/bench/loadgen"; MTDIR="$OUTDIR/memtier"; KEEP_UP="${KEEP_UP:-0}"

BACKENDS_DEFAULT="memcached redis bonsaigrid-mc bonsaigrid-redis"
POS=(); for a in "$@"; do case "$a" in -h|--help) sed -n '2,20p' "$0"; exit 0;; *) POS+=("$a");; esac; done
[ "${#POS[@]}" -gt 0 ] && BACKENDS="${POS[*]}" || BACKENDS="${BACKENDS:-$BACKENDS_DEFAULT}"

log(){ printf '\n\033[1;36m==>\033[0m %s\n' "$*"; }
info(){ printf '    %s\n' "$*"; }
warn(){ printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }
ncpus(){ local s=0 p a b; local IFS=','; for p in $1; do [[ $p == *-* ]] && { a=${p%-*}; b=${p#*-}; s=$((s+b-a+1)); } || s=$((s+1)); done; echo $s; }
CLIENT_NCPU=$(ncpus "$CLIENT_CPUS"); SERVER_NCPU=$(ncpus "$SERVER_CPUS")

ALL="bench_memcached bench_redis bench_bonsaigrid"
cleanup(){ [ "$KEEP_UP" = 1 ] && return; for c in $ALL; do $DOCKER rm -f "$c" >/dev/null 2>&1 || true; done; }
trap cleanup EXIT INT TERM
wait_port(){ timeout "${3:-30}" bash -c "until (exec 3<>/dev/tcp/$1/$2) 2>/dev/null; do sleep 0.3; done" 2>/dev/null; }

# server (container + port + memtier protocol) for a backend
proto_of(){ case "$1" in memcached|bonsaigrid-mc) echo memcache_text;; redis|bonsaigrid-redis) echo redis;; esac; }
port_of(){ case "$1" in memcached) echo 11211;; redis) echo 6379;; bonsaigrid-mc|bonsaigrid-redis) echo 5701;; esac; }
start_server(){ case "$1" in
  memcached) $DOCKER run -d --name bench_memcached --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_MEMCACHED" memcached -m "$MC_MEM_MB" -p 11211 -t "$SERVER_NCPU" >/dev/null; wait_port 127.0.0.1 11211 ;;
  redis) $DOCKER run -d --name bench_redis --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_REDIS" redis-server --port 6379 --save '' --appendonly no >/dev/null; wait_port 127.0.0.1 6379 ;;
  bonsaigrid-mc|bonsaigrid-redis) $DOCKER run -d --name bench_bonsaigrid --network host --cpuset-cpus="$SERVER_CPUS" \
      --memory="$SERVER_MEM" --security-opt seccomp=unconfined -v "$ROOT":/w -w /w -e BONSAI_CORES="$SERVER_NCPU" \
      "$IMG_BONSAI_BASE" ./target/release/server >/dev/null; wait_port 127.0.0.1 5701 30 || { $DOCKER logs bench_bonsaigrid 2>&1|tail -5; die "bonsaigrid did not open :5701"; } ;;
esac; }
stop_server(){ case "$1" in memcached) $DOCKER rm -f bench_memcached >/dev/null 2>&1;; redis) $DOCKER rm -f bench_redis >/dev/null 2>&1;;
  bonsaigrid-mc|bonsaigrid-redis) $DOCKER rm -f bench_bonsaigrid >/dev/null 2>&1;; esac || true; }

command -v "$DOCKER" >/dev/null 2>&1 && $DOCKER info >/dev/null 2>&1 || die "docker not reachable"
if [[ " $BACKENDS " == *bonsaigrid* ]]; then cargo build --release -q -p server || die "cargo build -p server failed"; fi
mkdir -p "$MTDIR"; rm -f "$MTDIR"/*.json
log "memtier config: ratio=$RATIO data=$DATA_SIZE keyspace=$KEY_MAX pattern=$KEY_PATTERN levels=$LEVELS stage=${STAGE_SECS}s"
info "servers cpuset=$SERVER_CPUS mem=$SERVER_MEM · client cpuset=$CLIENT_CPUS ($CLIENT_NCPU cpus)"

run_level(){ # backend proto port level
  local t="$1" proto="$2" port="$3" level="$4"
  local threads=$(( level < CLIENT_NCPU ? level : CLIENT_NCPU )); [ "$threads" -lt 1 ] && threads=1
  local clients=$(( (level + threads - 1) / threads ))
  taskset -c "$CLIENT_CPUS" true 2>/dev/null # noop; memtier is pinned via docker cpuset below
  $DOCKER run --rm --network host --cpuset-cpus="$CLIENT_CPUS" -v "$MTDIR":/out "$IMG_MEMTIER" \
    --protocol="$proto" --server=127.0.0.1 --port="$port" \
    --clients="$clients" --threads="$threads" --ratio="$RATIO" --data-size="$DATA_SIZE" \
    --key-pattern="$KEY_PATTERN" --key-maximum="$KEY_MAX" --test-time="$STAGE_SECS" \
    --hide-histogram --json-out-file="/out/${t}-${level}.json" >/dev/null 2>&1 \
    || warn "memtier $t level=$level failed"
}

for t in $BACKENDS; do
  proto=$(proto_of "$t"); port=$(port_of "$t")
  [ -n "$proto" ] || { warn "unknown backend $t"; continue; }
  log "$t ($proto :$port)"; start_server "$t"
  IFS=',' read -ra LV <<< "$LEVELS"
  for level in "${LV[@]}"; do
    printf '    level=%s ... ' "$level"
    run_level "$t" "$proto" "$port" "$level"
    python3 - "$MTDIR/${t}-${level}.json" <<'PY' 2>/dev/null || echo "(no result)"
import json,sys
d=json.load(open(sys.argv[1]))["ALL STATS"]; T=d["Totals"]; G=d["Gets"]
h=G.get("Hits/sec",0); m=G.get("Misses/sec",0); hr=100*h/(h+m) if (h+m)>0 else 0
p=T.get("Percentile Latencies",{})
print("%.0f ops/s  p50=%.2fms p99=%.2fms p99.9=%.2fms  hit=%.1f%%  net=%.0fKB/s"%(
    T["Ops/sec"], p.get("p50.00",0), p.get("p99.00",0), p.get("p99.90",0), hr,
    T.get("KB/sec RX",0)+T.get("KB/sec TX",0)))
PY
  done
  stop_server "$t"
done

log "Merging memtier results"
python3 bench/memtier_report.py "$MTDIR" $BACKENDS
