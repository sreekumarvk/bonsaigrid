#!/usr/bin/env bash
# Launch a 3-member BonsaiGrid cluster (K=1 sync backups), run the failover smoke
# (which promotes + kills member 0), then tear down.
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

cargo build -q -p server
PIDS=()
for IDX in 0 1 2; do
  BONSAI_MEMBERS=3 BONSAI_BACKUPS=1 BONSAI_MEMBER_INDEX="$IDX" \
    ./target/debug/server >"/tmp/bonsai_fo_$IDX.log" 2>&1 &
  PIDS+=($!)
done
# Member 0's PID, so the smoke can kill exactly that process (env-var prefixes
# are not part of argv, so pkill -f cannot match it).
echo "${PIDS[0]}" > /tmp/bonsai_m0.pid
sleep 2
conformance-python/.venv/bin/python conformance-python/cluster_failover_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
exit $RC
