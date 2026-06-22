#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."
cargo build -q -p server
PIDS=()
for IDX in 0 1 2; do
  BONSAI_MEMBERS=3 BONSAI_BACKUPS=1 BONSAI_MEMBER_INDEX="$IDX" \
    ./target/debug/server >"/tmp/bonsai_ha_$IDX.log" 2>&1 &
  PIDS+=($!)
  echo "${PIDS[$IDX]}" > "/tmp/bonsai_ha$IDX.pid"
done
sleep 2
conformance-python/.venv/bin/python conformance-python/structure_ha_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
pkill -9 -x server 2>/dev/null
exit $RC
