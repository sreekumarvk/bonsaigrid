#!/usr/bin/env bash
# Track benchmark results in Bencher (continuous benchmarking).
#
# Two layers, one tracker:
#   * macro (system): the loadgen's combined.json -> BMF -> `bencher run`
#   * micro (rust):   `bencher run --adapter rust_criterion "cargo bench -p store"`
#
# This script handles the macro layer. It always writes BMF locally; it uploads
# to Bencher only when a project + token are configured, so it is safe to run
# without an account (you still get bmf.json + local Criterion HTML reports).
#
# Works fully local (SQLite) or against Bencher Cloud — set BENCHER_HOST:
#
#   Self-hosted (local SQLite, no account):
#     bencher up --detach --api-volume bencher_data:/var/lib/bencher/data
#     export BENCHER_HOST=http://localhost:6610 BENCHER_PROJECT=bonsaigrid-bench
#     bench/track.sh            # uploads (unclaimed project; no token needed)
#     # graphs at http://localhost:3000 ; stop with `bencher down`
#
#   Bencher Cloud (public repos free):
#     export BENCHER_PROJECT=<slug> BENCHER_API_TOKEN=<token>
#     bench/track.sh
#
#   BENCHER_TESTBED=<hardware-label>   # optional, defaults to the hostname
#
# Usage:
#   bench/track.sh                       # use the current combined.json
#   bench/track.sh path/to/combined.json
set -euo pipefail
cd "$(dirname "$0")/.."

COMBINED="${1:-bench/loadgen/combined.json}"
BMF="bench/loadgen/bmf.json"
[ -f "$COMBINED" ] || { echo "no results at $COMBINED — run bench/run-all-isolated.sh first" >&2; exit 1; }

python3 bench/to_bmf.py "$COMBINED" > "$BMF"
echo "wrote $BMF ($(python3 -c "import json;print(len(json.load(open('$BMF'))))") benchmarks)"

if ! command -v bencher >/dev/null 2>&1; then
  echo "bencher CLI not installed — BMF ready to upload later."
  echo "  install: curl --proto '=https' --tlsv1.2 -sSfL https://bencher.dev/download/install-cli.sh | sh"
  exit 0
fi

HOST="${BENCHER_HOST:-https://api.bencher.dev}"
if [ -z "${BENCHER_PROJECT:-}" ]; then
  echo "BENCHER_PROJECT not set — validating BMF via dry-run (no upload)."
  bencher run --host "$HOST" --adapter json --file "$BMF" --dry-run
  exit 0
fi

# Upload for this branch+commit with a throughput regression gate: --err fails the
# command if a result drops below the lower boundary of the historical distribution.
# --token is required for Bencher Cloud / claimed projects; omitted for a local
# unclaimed self-hosted project.
args=(--host "$HOST" --project "$BENCHER_PROJECT"
      --branch "$(git rev-parse --abbrev-ref HEAD)"
      --hash "$(git rev-parse HEAD)"
      --testbed "${BENCHER_TESTBED:-$(hostname)}"
      --adapter json
      --threshold-measure throughput --threshold-test t_test
      --threshold-lower-boundary 0.95 --thresholds-reset --err
      --file "$BMF")
[ -n "${BENCHER_API_TOKEN:-}" ] && args+=(--token "$BENCHER_API_TOKEN")
bencher run "${args[@]}"
