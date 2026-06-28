#!/usr/bin/env bash
# Launch a 3-member BonsaiGrid cluster, run the smart-client cluster smoke, tear down.
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

cargo build -q -p server
PIDS=()
for IDX in 0 1 2; do
  BONSAI_MEMBERS=3 BONSAI_MEMBER_INDEX="$IDX" ./target/debug/server >"/tmp/bonsai_member_$IDX.log" 2>&1 &
  PIDS+=($!)
done
sleep 1.5
conformance-python/.venv/bin/python conformance-python/cluster_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
exit $RC
