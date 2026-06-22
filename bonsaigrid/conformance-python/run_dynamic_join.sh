#!/usr/bin/env bash
# Start a 2-member bootstrap cluster; the smoke launches a 3rd member that joins
# at runtime and must receive its partitions via migration. Then tear down.
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

cargo build -q -p server
PIDS=()
for IDX in 0 1; do
  BONSAI_MEMBERS=2 BONSAI_MEMBER_INDEX="$IDX" \
    ./target/debug/server >"/tmp/bonsai_dj_$IDX.log" 2>&1 &
  PIDS+=($!)
done
sleep 2
conformance-python/.venv/bin/python conformance-python/dynamic_join_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
pkill -9 -x server 2>/dev/null
exit $RC
