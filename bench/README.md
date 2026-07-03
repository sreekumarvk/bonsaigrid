# Benchmarks

Two layers of benchmark, one tracker. **Data and presentation are separate:**
benchmarks emit JSON; a tracker (Bencher) graphs and gates it over commits.
Nothing is hand-authored.

| Layer | What it measures | Tool |
|-------|------------------|------|
| **Macro** (system) | client→server throughput, latency percentiles, server CPU/mem, correctness | Go loadgen (`bench/loadgen`), cgroup-isolated |
| **Micro** (in-process) | slab allocator + store index put/get | Criterion (`crates/store/benches`) |
| **Tracking** | history per branch/commit, regression gates, graphs | Bencher (local SQLite or cloud) |

```
run-all-isolated.sh ─► combined.json ─► to_bmf.py ─► track.sh ─┐
cargo bench -p store ─► target/criterion/ ────────────────────► Bencher ─► graphs + gates
```

---

## Prerequisites

- **Docker** (daemon reachable; `docker info` works). Used for Redis/Memcached/Hazelcast,
  the BonsaiGrid container, the Go loadgen build/run, and the Bencher server.
- **cargo** / Rust toolchain (builds the BonsaiGrid server, the `bench` tool, and Criterion).
- **`go` is NOT required on the host** — the loadgen is built inside a `golang` container.
- **`bencher` CLI** — optional, only for tracking (`curl --proto '=https' --tlsv1.2 -sSfL https://bencher.dev/download/install-cli.sh | sh`).

Fixed loopback ports: BonsaiGrid `5701`, Hazelcast `5702`, Redis `6379`,
Memcached `11211`; Bencher API `6610`, console `3000`.

`bench/preflight.sh` checks this environment (docker/cargo/python3), clears stale
`bench_` containers, and warns on bound ports. It runs automatically at the start of
`run-all-isolated.sh`; run it standalone to check before a fresh run.

---

## Quick start

```bash
# 1. Run the fair, four-backend benchmark (all servers + client in isolated cgroups)
bench/run-all-isolated.sh                      # → bench/loadgen/combined.json

# 2. Confirm correctness end-to-end (writes N keys, reads each back, compares bytes)
bench/verify-correctness.sh                    # PASS for put + set over the wire

# 3. Micro-benchmark the Rust hot path
cargo bench -p store --bench hotpath           # → target/criterion/**/report/index.html

# 4. Track it over time in a fully-local Bencher (SQLite, no account) — see "Tracking"
bencher up --detach --api-volume bencher_data:/var/lib/bencher/data
BENCHER_HOST=http://localhost:6610 BENCHER_PROJECT=bonsaigrid-bench bench/track.sh
#   graphs → http://localhost:3000/perf/bonsaigrid-bench
```

---

## The macro benchmark

Two runners. **Use the isolated one for real numbers.**

### `run-all-isolated.sh` — fair comparison (recommended)

Every server **and** the load client runs in its own container (its own cgroup v2)
on a **disjoint cpuset**, with equal CPU/memory caps, so the CPU-heavy client can't
starve the server under test. Per-server CPU and memory are sampled from each
container's cgroup and injected per stage. BonsaiGrid runs containerized too
(glibc-matched image + `seccomp=unconfined` so io_uring works).

```bash
bench/run-all-isolated.sh                      # all four, default ramp
bench/run-all-isolated.sh bonsaigrid memcached # subset, in order
SERVER_CPUS=0-7 CLIENT_CPUS=8-19 SERVER_MEM=4g bench/run-all-isolated.sh
```

Key env vars: `SERVER_CPUS` (default `0-7`), `CLIENT_CPUS` (`8-19`), `SERVER_MEM`
(`4g`), `CLIENT_MEM` (`8g`), `LEVELS` (`1,2,4,8,16,32,64,128`), `STAGE_SECS` (`4`),
`WARMUP_SECS` (`2`), `HZ_CONNS` (`128`), `SAMPLE_MS` (`250`), `MC_MEM_MB` (`4096`),
image tags `IMG_REDIS`/`IMG_MEMCACHED`/`IMG_HAZELCAST`/`IMG_GO`, `DOCKER`, `KEEP_UP=1`.

### `run-all.sh` — quick, co-located (not fair)

Same backends but Docker for the caches + **native** BonsaiGrid + host loadgen, all
sharing cores. Faster to run, but throughput reflects the Linux scheduler, not the
server — the CPU-heavy Hazelcast client and the server contend. Use only for a smoke
check; cite `run-all-isolated.sh` for results.

### Output

`bench/loadgen/results-<backend>.json` and merged `bench/loadgen/combined.json`.
Each stage records:

```json
{ "level": 128,
  "set": { "rps": 285271, "p50_us": 125, "p90_us": 473, "p99_us": 1127 },
  "get": { ... },
  "errors": 0, "mismatch": 0,
  "cpu": { "avg_pct": 64.4, "max_pct": 66.1 },
  "mem": { "avg_mb": 1631.2, "max_mb": 1660.0 },
  "t_start_ms": ..., "t_end_ms": ... }
```

`rps` is the per-op rate (set = get, one of each per closed-loop iteration).
`cpu` is % of the server's cpuset budget; `mem` is the working set (MB).

---

## Correctness

The loadgen is **self-validating**: every GET is compared against the value just
SET, and a miss or wrong value is counted as `mismatch`. **`mismatch` must be 0** —
a nonzero value means the run is invalid (e.g. a server that acks writes without
storing them). This is what caught the `MapSet` no-op bug.

- **Unit tests:** `cargo test -p server` (round-trips for put/set/get/remove,
  quorum gating, etc.).
- **Integration test (over the wire):** `bench/verify-correctness.sh [count]` starts
  the server and, for each write op the clients use (`MapPut`, `MapSet`), writes N
  unique keys and reads every one back, comparing bytes. Wire it into CI next to
  `cargo test`.
- **Ad-hoc:** `target/release/bench verify <count> <valsz> <ttl_ms> <put|set>`.

---

## The micro benchmark

Criterion measures the in-process cost the macro test can't isolate (slab + index):

```bash
cargo bench -p store --bench hotpath
```

Reports (with min/mean/max, violin/line plots) land in `target/criterion/`.
Criterion auto-compares against the previous run and prints `change: … (p = …)`, so
local regressions show immediately. Add more `benches/*.rs` as the hot path grows
(serialization codecs, partition hashing, …).

### Server-isolated reference (`bench ladder`)

The macro benchmark drives BonsaiGrid through the **official Hazelcast client**,
which is heavyweight (that's a real client tax, not a server limit). For a
client-isolated number, `bench ladder` drives the same server with a thin
raw-protocol client:

```bash
./target/release/server &                      # or BONSAI_CORES=8 ./target/release/server &
BENCH_ADDR=127.0.0.1:5701 ./target/release/bench ladder 4 128  # → JSON, ~425k ops/s on 8 cores
```

---

## Tracking (Bencher)

Runs locally; graphs in a web dashboard. Install once:

```bash
curl --proto '=https' --tlsv1.2 -sSfL https://bencher.dev/download/install-cli.sh | sh
```

### Fully local — self-hosted, SQLite, no account

The CLI runs the whole server; the API persists **all history to a local SQLite DB**
(`/var/lib/bencher/data/bencher.db`).

```bash
bencher up --detach --api-volume bencher_data:/var/lib/bencher/data   # API :6610, console :3000

export BENCHER_HOST=http://localhost:6610
export BENCHER_PROJECT=bonsaigrid-bench        # auto-created "unclaimed" on first run (no token)
export BENCHER_TESTBED=$(hostname)

bench/track.sh                                 # macro → BMF → local SQLite
bencher run --host "$BENCHER_HOST" --project "$BENCHER_PROJECT" \
  --adapter rust_criterion "cargo bench -p store --bench hotpath"   # micro

# graphs: http://localhost:3000/perf/bonsaigrid-bench   (claim the project via the printed link)
bencher down                                   # stop the server (the SQLite volume persists)
```

`bench/to_bmf.py` maps each `(backend, level)` to a Bencher *benchmark* with measures
`throughput`, `latency-set-p99`, `latency-get-p99`, `cpu`, `memory`, and `mismatch`.
`bench/track.sh` uploads with a `t_test` lower-boundary **regression gate on
throughput** (`--err` fails the command on a drop) — wire it into CI.

### Alternative — Bencher Cloud (free for public repos)

```bash
export BENCHER_PROJECT=<slug> BENCHER_API_TOKEN=<token>   # BENCHER_HOST defaults to cloud
bench/track.sh
```

Without `BENCHER_PROJECT`, `bench/track.sh` just writes `bench/loadgen/bmf.json` and
runs `bencher run --dry-run` to validate — safe with no server at all.

---

## The self-contained dashboard

`bench/deploy/dashboard.html` is a shareable one-page view of the latest
run (throughput / SET+GET latency / CPU / memory, plus the native-driver reference
line). **Nothing in it is hand-written** — the charts, headline tiles, and summary
are all computed from the data:

- **Every run regenerates it.** `run-all-isolated.sh` calls `gen_dashboard.py`, which
  bakes the run's `combined.json` into the file between the `__BENCH_DATA__` markers.
  Regenerate manually with `bench/gen_dashboard.py bench/loadgen/combined.json`.
- **Served over http it also reloads `combined.json` live:**
  ```bash
  python3 -m http.server        # then open /bench/deploy/dashboard.html
  ```
  Opened as a `file://` it uses the baked snapshot (which already matches the run
  that generated it).

The **Correctness** tile turns red with the mismatch count if any GET didn't match —
so an invalid run is obvious at a glance. Bencher is the primary *history* tracker;
this is the per-run comparison view.

---

## Files

| Path | What |
|------|------|
| `run-all-isolated.sh` | fair four-backend run (cgroup + cpuset isolation, CPU/mem sampling) |
| `run-all.sh` | quick co-located run (not fair) |
| `preflight.sh` | env checks + stale-container cleanup (auto-run by `run-all-isolated.sh`) |
| `verify-correctness.sh` | end-to-end write/read-back correctness check over the wire |
| `to_bmf.py` | `combined.json` → Bencher Metric Format |
| `track.sh` | convert + upload to Bencher (local SQLite or cloud) with a regression gate |
| `gen_dashboard.py` | bake `combined.json` into the self-contained dashboard (auto-run each run) |
| `loadgen/` | the Go load generator (`main.go`, per-backend `store_*.go`) |
| `loadgen/combined.json` | merged results (checked in) |
| `deploy/dashboard.html` | secondary self-contained dashboard |
| `crates/store/benches/hotpath.rs` | Criterion micro-benchmarks |
| `crates/bench/` | raw-protocol driver: `ladder` (server-isolated) + `verify` |

---

## Caveats

- **Cross-client comparison.** BonsaiGrid and Hazelcast are driven through the same
  heavyweight official Hazelcast client; Redis/Memcached use their thin native
  clients. Cross-backend numbers are therefore *directional*. The controlled results
  are: BonsaiGrid vs Hazelcast (identical client), and `bench ladder` (client
  isolated) for the server ceiling.
- **First run pulls images and Go modules** (needs network; modules cache to
  `~/.cache/bonsai-bench`). A full four-backend run is ~5–8 min including pulls.
- **Ctrl-C is safe** — a trap tears down all containers on exit.
