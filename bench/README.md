# Benchmarks

New here? Read **[QUICKSTART.md](QUICKSTART.md)** first — it's the two-minute version.

A suite of benchmarks that each stress a different axis, one tracker, and one combined
report. **Data and presentation are separate:** every benchmark emits JSON; the
dashboards and the Bencher tracker render it. Nothing is hand-authored.

## At a glance

| Benchmark | What it stresses | Tool | Dashboard |
|---|---|---|---|
| `run-all-isolated.sh` | fair four-backend throughput/latency ladder + server CPU/mem + JVM GC | Go loadgen, cgroup-isolated | `deploy/dashboard.html` |
| `run-memtier.sh` | industry-standard tool — p99.9 tail, hit/miss, network | memtier_benchmark | `deploy/memtier.html` |
| `run-openloop.sh` | coordinated-omission-correct capacity (the latency elbow) | Go loadgen (open-loop) | `deploy/openloop.html` |
| `run-ycsb.sh` | YCSB core workloads A–F over a Zipfian keyspace | go-ycsb (+ patched memcache driver) | `deploy/ycsb.html` |
| `cargo bench -p store` | in-process slab + index hot path | Criterion | `target/criterion/` |
| **`benchmark-all.sh`** | **runs all of the above, bakes one report** | — | **`deploy/index.html`** |
| `track.sh` | history per commit + regression gates | Bencher (local SQLite/cloud) | Bencher web |

Backends: **BonsaiGrid** (`:5701`) — driven via the Hazelcast client, via its
**memcached** protocol (`bonsaigrid-mc`), and via its **RESP** protocol
(`bonsaigrid-redis`) — against real **Memcached** (`:11211`), **Redis** (`:6379`), and
**Hazelcast** (`:5702`). memtier, the open-loop bench, and YCSB drive BonsaiGrid with
the *same thin client* as an incumbent, giving apples-to-apples server-vs-server
numbers; the isolated macro run uses the heavyweight Hazelcast client (see Caveats).

```
run-all-isolated.sh ─► combined.json ─┐
run-memtier.sh      ─► memtier-*.json ─┤
run-openloop.sh     ─► openloop-*.json ├─► gen_index.py ─► deploy/index.html (one report)
run-ycsb.sh         ─► ycsb-*.json ────┘         └► to_bmf.py ─► track.sh ─► Bencher
cargo bench -p store ─► target/criterion/ ───────────────────────────────► Bencher
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
# 0. Run EVERY benchmark suite and build one combined report page (~20 min).
#    Runs the four suites below in sequence, then bakes bench/deploy/index.html —
#    a self-contained report with an executive summary + every benchmark's charts inline.
bench/benchmark-all.sh                          # → bench/deploy/index.html
#   smaller machine:      SERVER_CPUS=0-3 CLIENT_CPUS=4-7 bench/benchmark-all.sh
#   subset / faster:      SKIP="ycsb openloop" STAGE_SECS=3 bench/benchmark-all.sh
#   view it:              (python3 -m http.server) then open /bench/deploy/index.html

# --- or run a single suite ---

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

## Standardized workloads — YCSB (`run-ycsb.sh`)

The Yahoo! Cloud Serving Benchmark's **core workloads** model real applications that
the throughput ladder and memtier's fixed ratios don't: **A** update-heavy (50/50),
**B** read-heavy (95/5), **C** read-only, **D** read-latest (recency skew), **F**
read-modify-write — all over a **Zipfian** keyspace. Driven by **`go-ycsb`**, so
**BonsaiGrid via both its RESP and memcached protocols** goes head-to-head with real
Redis and Memcached on a standard, recognizable workload set.

```bash
bench/run-ycsb.sh                                  # all four backends, workloads A–F
RECORDS=1000000 OPS=1000000 THREADS=64 bench/run-ycsb.sh
WORKLOADS="b c f" bench/run-ycsb.sh                # a subset
bench/run-ycsb.sh redis bonsaigrid-redis           # a subset of backends
```

`ycsb_report.py` builds the matrix (`ycsb-combined.json`) and bakes
`bench/deploy/ycsb.html`. go-ycsb ships no memcache driver, so a ~120-line one
(`bench/ycsb/patch/`) is compiled in when the `go-ycsb` binary is built — once,
static, cached at `bench/ycsb/go-ycsb` (git-ignored; delete it to rebuild).

## Open-loop benchmark — coordinated-omission-correct (`run-openloop.sh`)

The closed-loop harness (above) reports peak ops/sec but **understates tail latency**:
each worker waits for its own reply before sending again, so a server stall simply
slows the whole loop — the requests that *would* have been sent during the stall are
never counted (**coordinated omission**). `bench/run-openloop.sh` fixes this with the
wrk2 method: a dispatcher issues requests at a fixed **offered rate**, and each
request's latency is measured from its **ideal send time**. When the server
saturates, the backlog shows up as an exploding tail — the **latency elbow**.

```bash
bench/run-openloop.sh                                   # 4 backends, offered-rate ladder
RATIO=1:9 DATA_SIZE=1024 bench/run-openloop.sh          # 90% reads, 1 KB objects
```

It also fixes the **workload realism** gaps: a **Zipf keyspace** (`KEY_MAX`, `ZIPF_S`)
so GETs of cold keys genuinely miss (a real **hit/miss ratio**), a configurable
read/write **`RATIO`**, and **`DATA_SIZE`** objects. Values stay per-key-deterministic,
so every hit is still verified (0 mismatches). Config: `RATES`, `CONNS`, `STAGE_SECS`,
`WARMUP_SECS`, plus the isolation vars. The MODE=open path lives in the Go loadgen
(`openloop.go` / `workload.go`, unit-tested).

`openloop_report.py` merges `results-open-*.json` → `openloop-combined.json`, reports
**usable throughput** (highest offered rate holding p99 under `SLO_US`, default 10 ms),
and bakes `bench/deploy/openloop.html` (the elbow: p99/p99.9 vs offered load, and
achieved-vs-offered with the ideal line).

## Standard-tool benchmark — memtier_benchmark

`bench/run-memtier.sh` drives every backend with **`memtier_benchmark`** (the Redis
Labs industry standard) through its real wire protocol, in the same cgroup-isolated
harness. BonsaiGrid is benched via **both** its memcached and RESP protocols, so the
comparison is apples-to-apples against real Memcached and Redis — same thin client.
It captures the pillars the loadgen doesn't: **p99.9 tail latency, hit/miss ratio,
and network throughput**.

```bash
bench/run-memtier.sh                              # all four; → bench/loadgen/memtier-combined.json
RATIO=9:1 DATA_SIZE=1024 bench/run-memtier.sh     # 90/10 read/write, 1 KB objects
bench/run-memtier.sh memcached bonsaigrid-mc      # subset
```

Backends: `memcached`, `redis`, `bonsaigrid-mc` (memcached protocol → :5701),
`bonsaigrid-redis` (RESP → :5701). Config: `RATIO` (set:get), `DATA_SIZE`,
`KEY_MAX` (keyspace → drives hit ratio), `KEY_PATTERN` (`R:R` random, `G:G`
gaussian hotspot), `LEVELS`, `STAGE_SECS`, plus the isolation vars.

`memtier_report.py` merges the per-level JSON into `memtier-combined.json`, writes
`memtier-bmf.json` for Bencher, prints a summary, and bakes
`bench/deploy/memtier.html` (throughput / p99.9 / hit-ratio / network, live-loading
+ embedded snapshot).

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
| **`QUICKSTART.md`** | **two-minute getting-started guide (read first)** |
| `benchmark-all.sh` | run every suite in sequence, then bake the combined report |
| `gen_index.py` | bake all `*-combined.json` into `deploy/index.html` (the report) |
| `run-all-isolated.sh` | fair four-backend run (cgroup + cpuset isolation, CPU/mem + GC sampling) |
| `run-all.sh` | quick co-located run (not fair) |
| `run-memtier.sh` / `memtier_report.py` | memtier_benchmark run + merge/bake `deploy/memtier.html` |
| `run-openloop.sh` / `openloop_report.py` | open-loop (CO-correct) run + merge/bake `deploy/openloop.html` |
| `run-ycsb.sh` / `ycsb_report.py` | YCSB matrix + merge/bake `deploy/ycsb.html` |
| `ycsb/workloads/` | YCSB core workload files (A–F) |
| `ycsb/patch/` | memcache driver compiled into `go-ycsb` at build time |
| `preflight.sh` | env checks (docker/cargo/python3, CPU governor) + stale-container cleanup |
| `verify-correctness.sh` | end-to-end write/read-back correctness check over the wire |
| `to_bmf.py` / `track.sh` | `combined.json` → Bencher Metric Format → upload with a regression gate |
| `gen_dashboard.py` | bake `combined.json` into `deploy/dashboard.html` (auto-run each run) |
| `loadgen/` | the Go load generator: `main.go`, `workload.go`, `openloop.go`, per-backend `store_*.go` |
| `loadgen/*-combined.json` | merged results per benchmark (checked in) |
| `deploy/index.html` | the one-page combined report (all suites, charts inline) |
| `deploy/{dashboard,memtier,openloop,ycsb}.html` | per-benchmark self-contained dashboards |
| `crates/store/benches/hotpath.rs` | Criterion micro-benchmarks |
| `crates/bench/` | raw-protocol driver: `ladder` (server-isolated) + `verify` |

---

## Caveats

- **Which comparisons are apples-to-apples.** In `run-all-isolated.sh`, BonsaiGrid and
  Hazelcast share the same heavyweight official Hazelcast client while Redis/Memcached
  use thin native clients, so *cross-backend* numbers there are directional (the
  controlled pair is BonsaiGrid vs Hazelcast). The **memtier, open-loop, and YCSB**
  benchmarks remove that asymmetry — they drive BonsaiGrid with the *same thin client*
  as the incumbent (memcached protocol vs Memcached, RESP vs Redis), so those are true
  server-vs-server results. `bench ladder` is the client-isolated server ceiling.
- **First run pulls images and builds** (needs network; Go/Rust caches to
  `~/.cache/bonsai-bench`). A single four-backend suite is ~5–8 min including pulls;
  `benchmark-all.sh` (all four) is ~20 min.
- **Ctrl-C is safe** — every runner traps and tears down its containers on exit.
