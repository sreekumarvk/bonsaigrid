#!/usr/bin/env bash
# End-to-end correctness check over the real client wire protocol.
#
# Starts the BonsaiGrid server and, for each write op the clients actually use,
# writes N unique keys with distinct values and reads every one back, comparing
# bytes. This is the integration test that would have caught the MapSet (69376)
# no-op: the benchmark loadgen discards GET values, so a server that acks writes
# without storing them looked fine. Run in CI alongside `cargo test`.
#
# Usage: bench/verify-correctness.sh [count]
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-50000}"
PORT="${PORT:-5701}"
echo "Building server + bench (release)..."
cargo build --release -q -p server -p bench

fail=0
check() { # op (put|set)
  local op="$1" srv out
  ./target/release/server >/tmp/vc_"$op".log 2>&1 &
  srv=$!
  # wait for the port
  timeout 30 bash -c "until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do sleep 0.2; done"
  out=$(BENCH_ADDR="127.0.0.1:$PORT" ./target/release/bench verify "$N" 128 0 "$op")
  kill "$srv" 2>/dev/null || true; wait "$srv" 2>/dev/null || true
  echo "--- $op ---"; echo "$out" | sed -n '2,6p'
  if ! echo "$out" | grep -q "PASS"; then
    echo "  >>> FAIL: $op round-trip did not return stored data"; fail=1
  fi
}

check put   # MapPut  (65792)
check set   # MapSet  (69376) — the op IMap.set/SetWithTTL sends

if [ "$fail" -ne 0 ]; then
  echo "CORRECTNESS: FAILED"; exit 1
fi
echo "CORRECTNESS: all write ops store and return data faithfully ($N keys each)"
