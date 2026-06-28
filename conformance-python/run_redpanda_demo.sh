#!/usr/bin/env bash
# Run the Redpanda + BonsaiGrid streaming demo end to end.
#  - ensures a Redpanda broker is running in Docker on 127.0.0.1:9092
#  - (re)creates the source/sink topics
#  - builds + starts a single-node BonsaiGrid server
#  - runs the demo (SQL mappings + INSERT + streaming JOB, produce + verify)
set -uo pipefail
cd "$(dirname "$0")/.."   # bonsaigrid/

# 1) Redpanda
if ! docker ps --format '{{.Names}}' | grep -qx redpanda; then
  echo "starting Redpanda..."
  docker rm -f redpanda >/dev/null 2>&1
  docker run -d --name redpanda -p 9092:9092 -p 9644:9644 \
    docker.redpanda.com/redpandadata/redpanda:latest \
    redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
    --node-id 0 --check=false \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://127.0.0.1:9092 >/dev/null
  sleep 8
fi
docker exec redpanda rpk topic delete pizzastream recommender_pizzastream >/dev/null 2>&1
docker exec redpanda rpk topic create pizzastream recommender_pizzastream >/dev/null 2>&1

# 2) BonsaiGrid (single node)
cargo build -q -p server
pkill -9 -x server 2>/dev/null
BONSAI_CORES=1 ./target/debug/server >/tmp/bonsai_demo.log 2>&1 &
SRV=$!
sleep 2

# 3) the demo
conformance-python/.venv/bin/python conformance-python/redpanda_demo.py
RC=$?

kill -9 "$SRV" 2>/dev/null
exit $RC
