#!/usr/bin/env bash
# Baseline/regression benchmark for BonsaiGrid. Reusable across increments.
# Usage: bench/run.sh [label]
# Measures: put+get latency (p50/p99) + throughput, and memory density
# (server RSS delta per stored entry).
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

LABEL="${1:-current}"
ADDR="127.0.0.1:5701"
LOAD_COUNT="${LOAD_COUNT:-200000}"
LOAD_VALSZ="${LOAD_VALSZ:-100}"
LAT_N="${LAT_N:-50000}"

echo "Building release binaries..."
cargo build --release -q -p server -p bench

server_rss_kb() { grep VmRSS "/proc/$1/status" | awk '{print $2}'; }

run_server() {
  ./target/release/server >/tmp/bonsai_bench_server.log 2>&1 &
  echo $!
}

echo "## BonsaiGrid benchmark — $LABEL"
echo

# --- Latency / throughput ---
SRV=$(run_server); sleep 1
echo "### Latency (sequential put+get, n=$LAT_N)"
BENCH_ADDR="$ADDR" ./target/release/bench latency "$LAT_N"
kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true
echo

# --- Memory density ---
SRV=$(run_server); sleep 1
RSS_BEFORE=$(server_rss_kb "$SRV")
BENCH_ADDR="$ADDR" ./target/release/bench load "$LOAD_COUNT" "$LOAD_VALSZ" >/dev/null
sleep 1
RSS_AFTER=$(server_rss_kb "$SRV")
kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true

DELTA_KB=$(( RSS_AFTER - RSS_BEFORE ))
PER_ENTRY=$(awk "BEGIN{printf \"%.1f\", ($DELTA_KB*1024)/$LOAD_COUNT}")
RAW_PER_ENTRY=$(( LOAD_VALSZ + 15 ))  # value + ~15-byte key
echo "### Memory density ($LOAD_COUNT entries x $LOAD_VALSZ-byte values)"
echo "rss_before_kb $RSS_BEFORE"
echo "rss_after_kb  $RSS_AFTER"
echo "delta_kb      $DELTA_KB"
echo "bytes/entry   $PER_ENTRY   (raw payload ~$RAW_PER_ENTRY B)"
