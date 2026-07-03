#!/usr/bin/env bash
# Preflight for the benchmark harness: verify required tooling is present and clear
# stale state, so a fresh run starts clean. Exits non-zero if a hard requirement is
# missing. Run standalone before a run, or let bench/run-all-isolated.sh call it.
#
#   bench/preflight.sh
set -uo pipefail

DOCKER="${DOCKER:-docker}"
fail=0
ok()   { printf '  \033[1;32mok\033[0m    %s\n' "$*"; }
bad()  { printf '  \033[1;31mFAIL\033[0m  %s\n' "$*"; fail=1; }
warn() { printf '  \033[1;33mwarn\033[0m  %s\n' "$*"; }

echo "Preflight:"

# --- required tooling ---
have_docker=0
if command -v "$DOCKER" >/dev/null 2>&1; then
  if $DOCKER info >/dev/null 2>&1; then ok "docker daemon reachable"; have_docker=1
  else bad "docker installed but daemon unreachable (need sudo, or add your user to the 'docker' group)"; fi
else
  bad "docker not found (set DOCKER=… or install it)"
fi
command -v cargo   >/dev/null 2>&1 && ok "cargo present"   || bad "cargo not found (builds the BonsaiGrid server + bench tool)"
command -v python3 >/dev/null 2>&1 && ok "python3 present" || bad "python3 not found (merges results + generates the dashboard)"

# --- clear stale benchmark containers from a prior run ---
if [ "$have_docker" -eq 1 ]; then
  stale=$($DOCKER ps -aq --filter name=bench_ 2>/dev/null)
  if [ -n "$stale" ]; then
    echo "$stale" | xargs -r $DOCKER rm -f >/dev/null 2>&1 && ok "removed stale bench_ containers"
  else
    ok "no stale bench_ containers"
  fi
fi

# --- warn if a server port is already bound (a stray server would clash) ---
for p in 5701 5702 6379 11211; do
  if timeout 1 bash -c "true <>/dev/tcp/127.0.0.1/$p" 2>/dev/null; then
    warn "port $p is in use — a prior server may still be running (it will clash with the run)"
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "preflight: FAILED — fix the above and re-run"
  exit 1
fi
echo "preflight: ok"
