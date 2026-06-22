#!/usr/bin/env bash
# 3-member cluster (K=1); the smoke SIGKILLs member 0 with no manual promote and
# relies on heartbeat detection + auto-failover. Then tear down.
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

cargo build -q -p server
PIDS=()
for IDX in 0 1 2; do
  BONSAI_MEMBERS=3 BONSAI_BACKUPS=1 BONSAI_MEMBER_INDEX="$IDX" \
    ./target/debug/server >"/tmp/bonsai_af_$IDX.log" 2>&1 &
  PIDS+=($!)
done
echo "${PIDS[0]}" > /tmp/bonsai_m0.pid
sleep 2
conformance-python/.venv/bin/python conformance-python/auto_failover_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
exit $RC
