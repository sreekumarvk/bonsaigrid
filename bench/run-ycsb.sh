#!/usr/bin/env bash
# YCSB (Yahoo! Cloud Serving Benchmark) standardized-workload matrix, via go-ycsb,
# in the cgroup-isolated harness. YCSB's core workloads model real applications the
# throughput ladder and memtier's fixed ratios don't: B read-heavy, C read-only,
# D read-latest (recency skew), F read-modify-write — all over a Zipfian keyspace.
#
# go-ycsb has a redis driver (no memcache one), so this covers the RESP pair:
#   redis            -> real Redis
#   bonsaigrid-redis -> BonsaiGrid via its RESP protocol
# (the memcached-protocol pair is covered by memtier + the open-loop bench.)
#
# Usage:
#   bench/run-ycsb.sh
#   RECORDS=1000000 OPS=1000000 THREADS=64 bench/run-ycsb.sh
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"; LOADDIR="$ROOT/bench/loadgen"; WL="$ROOT/bench/ycsb/workloads"

SERVER_CPUS="${SERVER_CPUS:-0-7}"; CLIENT_CPUS="${CLIENT_CPUS:-8-19}"; SERVER_MEM="${SERVER_MEM:-4g}"
RECORDS="${RECORDS:-500000}"; OPS="${OPS:-500000}"; THREADS="${THREADS:-50}"
FIELDLEN="${FIELDLEN:-100}"; WORKLOADS="${WORKLOADS:-a b c d f}"
DOCKER="${DOCKER:-docker}"; IMG_GO="${IMG_GO:-golang:1.24}"; IMG_RUN="${IMG_RUN:-alpine:3}"
IMG_REDIS="${IMG_REDIS:-redis:7.4-alpine}"; IMG_MEMCACHED="${IMG_MEMCACHED:-memcached:1.6-alpine}"; MC_MEM_MB="${MC_MEM_MB:-4096}"
_OSID=ubuntu; _OSVER=24.04; [ -r /etc/os-release ] && . /etc/os-release && _OSID="${ID:-ubuntu}" && _OSVER="${VERSION_ID:-24.04}"
IMG_BONSAI_BASE="${IMG_BONSAI_BASE:-${_OSID}:${_OSVER}}"
YBIN="${YBIN:-$ROOT/bench/ycsb/go-ycsb}"

BACKENDS_DEFAULT="memcached bonsaigrid-mc redis bonsaigrid-redis"
POS=(); for a in "$@"; do case "$a" in -h|--help) sed -n '2,16p' "$0"; exit 0;; *) POS+=("$a");; esac; done
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
# go-ycsb driver + port per backend (memcache driver is patched in at build time).
driver_of(){ case "$1" in redis|bonsaigrid-redis) echo redis;; memcached|bonsaigrid-mc) echo memcache;; esac; }
port_of(){ case "$1" in memcached) echo 11211;; redis) echo 6379;; bonsaigrid-mc|bonsaigrid-redis) echo 5701;; esac; }
start_server(){ case "$1" in
  memcached) $DOCKER run -d --name bench_memcached --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_MEMCACHED" memcached -m "$MC_MEM_MB" -p 11211 -t "$SERVER_NCPU" >/dev/null; wait_port 127.0.0.1 11211 ;;
  redis) $DOCKER run -d --name bench_redis --network host --cpuset-cpus="$SERVER_CPUS" --memory="$SERVER_MEM" \
      "$IMG_REDIS" redis-server --port 6379 --save '' --appendonly no >/dev/null; wait_port 127.0.0.1 6379 ;;
  bonsaigrid-mc|bonsaigrid-redis) $DOCKER run -d --name bench_bonsaigrid --network host --cpuset-cpus="$SERVER_CPUS" \
      --memory="$SERVER_MEM" --security-opt seccomp=unconfined -v "$ROOT":/w -w /w -e BONSAI_CORES="$SERVER_NCPU" \
      "$IMG_BONSAI_BASE" ./target/release/server >/dev/null; wait_port 127.0.0.1 5701 30 || die "bonsaigrid :5701" ;;
esac; }
stop_server(){ case "$1" in memcached) $DOCKER rm -f bench_memcached;; redis) $DOCKER rm -f bench_redis;;
  bonsaigrid-mc|bonsaigrid-redis) $DOCKER rm -f bench_bonsaigrid;; esac >/dev/null 2>&1 || true; }

command -v "$DOCKER" >/dev/null 2>&1 && $DOCKER info >/dev/null 2>&1 || die "docker not reachable"
if [[ " $BACKENDS " == *bonsaigrid* ]]; then cargo build --release -q -p server || die "cargo build -p server"; fi

# Build go-ycsb (static) once, cached at bench/ycsb/go-ycsb. We patch in a memcache
# driver (bench/ycsb/patch/) that go-ycsb lacks, so the matrix can target the
# memcached-protocol backends too. Delete the cached binary to force a rebuild.
if [ ! -x "$YBIN" ]; then
  log "Building go-ycsb (static, + memcache driver) via $IMG_GO"
  GOCACHE_DIR="${GOCACHE_DIR:-$HOME/.cache/bonsai-bench/gocache}"; mkdir -p "$GOCACHE_DIR" "$(dirname "$YBIN")"
  $DOCKER run --rm -v "$(dirname "$YBIN")":/out -v "$ROOT":/src -v "$GOCACHE_DIR":/go \
    -e CGO_ENABLED=0 -e GOFLAGS=-mod=mod "$IMG_GO" sh -c '
      git clone --depth 1 -q https://github.com/pingcap/go-ycsb /s &&
      mkdir -p /s/db/memcache &&
      cp /src/bench/ycsb/patch/memcache_db.go /s/db/memcache/db.go &&
      cp /src/bench/ycsb/patch/memcache_register.go /s/cmd/go-ycsb/zz_memcache.go &&
      cd /s && go get github.com/bradfitz/gomemcache/memcache &&
      go build -o /out/go-ycsb ./cmd/go-ycsb' \
    || die "go-ycsb build failed"
  info "built $YBIN"
fi

log "YCSB matrix: records=$RECORDS ops=$OPS threads=$THREADS fieldlen=${FIELDLEN}B workloads=[$WORKLOADS]"
info "server cpuset=$SERVER_CPUS mem=$SERVER_MEM · client cpuset=$CLIENT_CPUS"
mkdir -p "$LOADDIR/ycsb"; rm -f "$LOADDIR/ycsb"/*.txt

# run go-ycsb (load or run) in the client cpuset against a backend, picking the
# right driver (redis / memcache) and connection property for it.
ydriver(){ # phase backend port workload_file out
  local phase="$1" t="$2" port="$3" wlf="$4" out="$5" drv connp
  drv=$(driver_of "$t")
  case "$drv" in
    redis)    connp="redis.addr=127.0.0.1:$port" ;;
    memcache) connp="memcache.hosts=127.0.0.1:$port" ;;
  esac
  $DOCKER run --rm --network host --cpuset-cpus="$CLIENT_CPUS" -v "$ROOT":/src -v "$YBIN":/go-ycsb "$IMG_RUN" \
    /go-ycsb "$phase" "$drv" -P "/src/bench/ycsb/workloads/$wlf" \
    -p "$connp" -p threadcount="$THREADS" \
    -p recordcount="$RECORDS" -p operationcount="$OPS" -p fieldcount=1 -p fieldlength="$FIELDLEN" \
    2>/dev/null | tee "$out" >/dev/null
}

for t in $BACKENDS; do
  port=$(port_of "$t"); [ -n "$port" ] || { warn "unknown backend $t"; continue; }
  log "$t (redis driver :$port)"; start_server "$t"
  info "load $RECORDS records ..."
  ydriver load "$t" "$port" workloada "/tmp/ycsb_load_$t.txt"
  for w in $WORKLOADS; do
    printf '    workload %s ... ' "$w"
    ydriver run "$t" "$port" "workload$w" "$LOADDIR/ycsb/${t}-${w}.txt"
    grep '^TOTAL' "$LOADDIR/ycsb/${t}-${w}.txt" | tail -1 | \
      sed -E 's/.*OPS: ([0-9.]+).* 99th\(us\): ([0-9]+), 99\.9th\(us\): ([0-9]+).*/\1 ops\/s  p99=\2us p99.9=\3us/' \
      || echo "(no result)"
  done
  stop_server "$t"
done

log "Merging + plotting"
python3 bench/ycsb_report.py "$LOADDIR/ycsb" $BACKENDS
