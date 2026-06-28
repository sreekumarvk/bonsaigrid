#!/usr/bin/env bash
# 3-member cluster with quorum=2; the smoke kills two members and checks that the
# survivor rejects writes (below quorum) but still serves reads. Then tear down.
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

cargo build -q -p server
PIDS=()
for IDX in 0 1 2; do
  BONSAI_MEMBERS=3 BONSAI_BACKUPS=1 BONSAI_QUORUM=2 BONSAI_MEMBER_INDEX="$IDX" \
    ./target/debug/server >"/tmp/bonsai_q_$IDX.log" 2>&1 &
  PIDS+=($!)
  echo "${PIDS[$IDX]}" > "/tmp/bonsai_q$IDX.pid"
done
sleep 2
conformance-python/.venv/bin/python conformance-python/quorum_smoke.py
RC=$?
for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
exit $RC
