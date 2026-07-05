#!/usr/bin/env bash
# One command to run every benchmark suite and build the combined report page.
#
# Runs, in sequence (each self-contained, each in the same cgroup-isolated harness):
#   1. run-all-isolated.sh  fair four-backend throughput ladder (+ GC, resources)
#   2. run-memtier.sh       industry-standard tool, real protocols
#   3. run-openloop.sh      coordinated-omission-correct capacity (the elbow)
#   4. run-ycsb.sh          YCSB workload matrix A–F (RESP pair)
# then bakes bench/deploy/index.html — one page linking every dashboard.
#
# A failing suite is reported and skipped, not fatal. This is a LONG run
# (~20 min at defaults); use STAGE_SECS / RATES / RECORDS to shorten, or SKIP steps.
#
# Config (shared across all four; pass once):
#   SERVER_CPUS=0-3 CLIENT_CPUS=4-7 bench/benchmark-all.sh     # smaller machine
#   SKIP="ycsb openloop" bench/benchmark-all.sh                # run a subset
#   bench/benchmark-all.sh -h
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

[ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] && { sed -n '2,21p' "$0"; exit 0; }

# Pin the shared isolation config once so every suite uses the same budgets.
export SERVER_CPUS="${SERVER_CPUS:-0-7}"
export CLIENT_CPUS="${CLIENT_CPUS:-8-19}"
export SERVER_MEM="${SERVER_MEM:-4g}"
SKIP="${SKIP:-}"

log(){ printf '\n\033[1;35m########\033[0m \033[1m%s\033[0m\n' "$*"; }
info(){ printf '    %s\n' "$*"; }
warn(){ printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }

skip(){ case " $SKIP " in *" $1 "*) info "skip $1"; return 0;; *) return 1;; esac; }
step(){ # key label script...
  local key="$1" label="$2"; shift 2
  skip "$key" && return 0
  log "$label"
  if "$@"; then info "✓ $label done"; else warn "✗ $label FAILED — continuing"; fi
}

log "Running all benchmark suites · server=$SERVER_CPUS client=$CLIENT_CPUS mem=$SERVER_MEM · SKIP=[${SKIP:-none}]"

step isolated "1/4 · fair four-backend (closed-loop)"          bench/run-all-isolated.sh
step memtier  "2/4 · memtier_benchmark"                        bench/run-memtier.sh
step openloop "3/4 · open-loop (coordinated-omission-correct)" bench/run-openloop.sh
step ycsb     "4/4 · YCSB workload matrix"                     bench/run-ycsb.sh

log "Building combined report"
if python3 bench/gen_index.py; then
  info "Report:   bench/deploy/index.html"
  info "Serve it: (cd \"$ROOT\" && python3 -m http.server) then open /bench/deploy/index.html"
else
  warn "no results to report"
fi
