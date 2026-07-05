# Benchmark quick start

The two-minute version. For the full reference see [README.md](README.md).

## 1. You need two things

- **Docker** — daemon running (`docker info` works). Everything else (Redis,
  Memcached, Hazelcast, the BonsaiGrid container, the Go loadgen, go-ycsb, memtier)
  runs in containers.
- **Rust / `cargo`** — builds the BonsaiGrid server.

`go` and `node` are **not** required on the host — the Go tools are built inside
containers. `python3` is used to merge results and bake the dashboards.

## 2. Check your CPU count first ⚠️

The harness pins the server and the load client to **disjoint** CPU sets so the
client can't steal cycles from the server. The defaults assume a **~20-core** machine:
server on cores `0-7`, client on `8-19`. On a smaller box you **must** shrink them, or
the two overlap and the numbers are meaningless:

```bash
# 8-core laptop, for example: 4 cores server, 3 cores client
SERVER_CPUS=0-3 CLIENT_CPUS=4-7 bench/benchmark-all.sh
```

Pick any two non-overlapping ranges that fit your machine; give the client at least as
many cores as the server.

## 3. Run everything, get one page

```bash
bench/benchmark-all.sh
```

This runs all four benchmark suites in sequence and bakes a single self-contained
report at **`bench/deploy/index.html`** — an executive summary plus every benchmark's
charts. It's a long run (~20 min at defaults). To view it:

```bash
python3 -m http.server            # from the repo root
# then open http://localhost:8000/bench/deploy/index.html
```

(Opening the file directly also works — it falls back to the baked-in snapshot.)

Useful knobs:

```bash
SKIP="ycsb openloop" bench/benchmark-all.sh        # run a subset of suites
STAGE_SECS=3 RATES="50000,200000,500000" \
  bench/benchmark-all.sh                            # shorter stages / fewer points
```

## 4. …or run just one suite

Each writes its own `bench/loadgen/*-combined.json` and bakes its own dashboard under
`bench/deploy/`. They all honor `SERVER_CPUS` / `CLIENT_CPUS` / `SERVER_MEM`.

| I want to measure… | Command | Dashboard |
|---|---|---|
| Fair four-backend throughput/latency | `bench/run-all-isolated.sh` | `deploy/dashboard.html` |
| Standard-tool numbers (p99.9, hit/miss, network) | `bench/run-memtier.sh` | `deploy/memtier.html` |
| **Honest capacity** (tail latency under load) | `bench/run-openloop.sh` | `deploy/openloop.html` |
| YCSB workloads A–F | `bench/run-ycsb.sh` | `deploy/ycsb.html` |
| The Rust hot path (no network) | `cargo bench -p store --bench hotpath` | `target/criterion/` |

Just want **one** number? Run `bench/run-openloop.sh` — it reports *usable throughput*
(the highest load that still holds p99 under a 10 ms SLO), which is the capacity that
actually holds in production. The "peak ops/sec" from the other runs sits past that
elbow.

## 5. Where results land

- `bench/loadgen/*-combined.json` — the merged data (checked in).
- `bench/deploy/*.html` — self-contained dashboards; `index.html` is the combined report.
- Re-bake the report from existing data without re-running: `python3 bench/gen_index.py`.

## Troubleshooting

- **"cpu governor = powersave" warning** — for stable latency, set it to
  `performance`: `sudo cpupower frequency-set -g performance` (preflight tries to do
  this if it can).
- **"port … is in use"** — a stray server/container from a previous run. `bench/preflight.sh`
  clears `bench_*` containers; kill any leftover `target/release/server` process.
- **First run is slow** — it pulls Docker images and builds the server + Go tools once
  (cached afterward under `~/.cache/bonsai-bench`). Needs network.
- **Ctrl-C** — safe; every runner tears down its containers on exit.
