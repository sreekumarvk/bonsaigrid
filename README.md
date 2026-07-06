# BonsaiGrid

A high-performance, resource-efficient reimplementation of Apache Hazelcast on a
zero-allocation, thread-per-core, shared-nothing Rust runtime. **Goal:** a genuine
drop-in replacement — unmodified Hazelcast clients connect and work, and operators
keep their existing metrics/monitoring — with markedly better latency and memory
density underneath.

**New here?** [**QUICKSTART.md**](QUICKSTART.md) gets a client talking to a server
(and a benchmark running) in a few minutes.

Design philosophy and the cross-core routing architecture live under
`docs/superpowers/specs/`; the platform-parity roadmap is
`docs/hazelcast-platform-gap-roadmap.md`.

---

## Status

The server speaks the **Hazelcast Open Client Protocol** (fixtures version 2.10) in
two modes:

- **Single node** (default): thread-per-core, io_uring, per-core TPC ports.
- **Multi-node** (`BONSAI_MEMBERS=K`): K processes form a cluster; each owns
  partitions `{p : p % K == index}`. A stock **smart** client routes each key to its
  owner — verified by 1000 keys round-tripping across a 3-member cluster.

All five major platform-parity gaps are **shipped or substantially shipped** (see
`docs/hazelcast-platform-gap-roadmap.md` for the box-by-box status):

- **Distributed architecture** — thread-per-core shared-nothing, io_uring, membership
  / heartbeat / master-election / migration, cross-core routing, zero-allocation hot
  path.
- **Fast data store** — sync K-backup replication, strict-majority quorum, owner-only
  reads, HLC time-ordered merge (deterministic-simulation verified).
- **Data structures** — IMap, MultiMap, Queue, List, Set, Ringbuffer, PNCounter,
  Topic, Flake-ID, locks, CardinalityEstimator (HyperLogLog); entry listeners,
  transactions, entry processors.
- **Security** — resource+action RBAC, hashed-credential auth, client kTLS, member
  mTLS, client-cert-as-principal.
- **Persistence** — WAL + snapshots + crash recovery for IMap and every structure.
- **CP subsystem (Raft)** — AtomicLong, AtomicReference, CountDownLatch, Semaphore,
  FencedLock, CPMap; CP sessions, named CP groups, read-index (lease) linearizable
  reads.
- **Streaming / SQL** — distributed scatter/gather SQL, event-time windowing,
  stream-stream joins; Kafka, JDBC, CDC, file, and socket connectors.
- **Geo / WAN** — asynchronous active-active cross-cluster replication with HLC
  convergence.

### Protocols and operator surface

Besides the Hazelcast client protocol, the same server port also speaks **memcached**
and **RESP (Redis)** (protocol-detected from the first bytes), plus Hazelcast's REST
health endpoints — so existing health checks / k8s probes / load balancers work
unchanged:

```
GET /hazelcast/health/node-state         -> ACTIVE
GET /hazelcast/health/cluster-size       -> <member count>
GET /hazelcast/health                    -> {"nodeState":"ACTIVE",...,"clusterSize":N}
```

Server-side **partition computation** (MurmurHash3) matches the client exactly
(verified: 1000/1000 keys across a cluster).

---

## Layout

| Path | What |
|------|------|
| `crates/protocol` | frame envelope + little-endian primitive codecs |
| `crates/codecs` | auth, cluster-view, map, CP, and nested member/partition codecs |
| `crates/serialization` | compact serialization, schema service |
| `crates/store` | single-node slab-backed opaque-blob map (`Data` never deserialized) + aux structures + HLL |
| `crates/server` | reactor, handshake + dispatch, member thread, connectors (kafka/jdbc/cdc), memcache/RESP/REST |
| `crates/member` | member-to-member io_uring transport + replication |
| `crates/spsc` | lock-free single-producer/single-consumer ring (cross-core + WAN capture) |
| `crates/raft` | from-scratch Raft core + CP state machines |
| `crates/security` | RBAC, identity providers, kTLS/mTLS |
| `crates/persistence` | WAL + snapshots + recovery |
| `crates/wan` | geo/WAN outbound queue + publisher/consumer |
| `crates/jet` | streaming operators, windowing, joins, source/sink connectors |
| `crates/query` | SQL parser + predicate engine + json-flat |
| `crates/jni` | embedded-server JNI bindings |
| `crates/bench` | raw-protocol driver: `ladder` (server-isolated) + `verify` |
| `conformance-python` / `conformance-java` | stock-client end-to-end oracles |
| `tests/golden` | Hazelcast's committed 2.10 conformance fixture |
| `bench/` | benchmark suites, loadgen, dashboards (see [Benchmarks](#benchmarks)) |
| `hazelcast/` | read-only Apache Hazelcast checkout — protocol reference only |

---

## Build & test

```bash
cargo test                      # unit + golden-vector conformance (byte-exact)
cargo run -p server             # bind 127.0.0.1:5701
cargo run --release -p server   # release build (use this for anything performance-related)
```

Common server env vars: `BONSAI_MEMBERS` (cluster size), `BONSAI_CORES`,
`BONSAI_CP=1` (CP subsystem), `BONSAI_PERSISTENCE=none|async|sync`,
`BONSAI_WAN_TARGETS=...` (geo/WAN). See the roadmap and per-crate docs for the full set.

---

## Conformance testing

Two layers ensure we match the **immutable client contract**, not just our own idea
of it:

1. **Golden-vector codec conformance** (`cargo test`) — encode/decode validated
   byte-for-byte against Hazelcast's own committed
   `2.10.protocol.compatibility.binary`.
2. **Behavioural conformance** — a real, unmodified Hazelcast client runs ported
   `IMap` scenarios against the running server.

### Python smoke test (fast, JVM-free)

Runs an unmodified `hazelcast-python-client` against BonsaiGrid as a quick
liveness/compat check:

```bash
python3 -m venv conformance-python/.venv
conformance-python/.venv/bin/pip install -r conformance-python/requirements.txt
cargo run -p server &                                    # in the repo root
conformance-python/.venv/bin/python conformance-python/smoke.py    # prints PYTHON SMOKE OK
```

### Java parity harness (canonical oracle)

The canonical "are we building the right thing" check: the real, unmodified Hazelcast
**Java** client driving ported `IMap` scenarios. The number of passing scenarios is
the **parity score**. Requires **JDK 17+** (any Hazelcast 5.x client does):

```bash
sudo apt-get install -y openjdk-17-jdk        # if needed
cargo run -p server &                          # repo root
cd conformance-java && mvn test
```

The Rust golden-vector tests and the Python smoke test fully gate correctness when a
JDK 17 environment isn't available.

---

## Benchmarks

A suite of benchmarks that each stress a different axis, one tracker, and one combined
report. **Data and presentation are separate:** every benchmark emits JSON; the
dashboards and the Bencher tracker render it. Nothing is hand-authored.

> For a two-minute getting-started walkthrough, see the **Benchmarking** section of
> [QUICKSTART.md](QUICKSTART.md).

### At a glance

| Benchmark | What it stresses | Tool | Dashboard |
|---|---|---|---|
| `run-all-isolated.sh` | fair four-backend throughput/latency ladder + server CPU/mem + JVM GC | Go loadgen, cgroup-isolated | `bench/deploy/dashboard.html` |
| `run-memtier.sh` | industry-standard tool — p99.9 tail, hit/miss, network | memtier_benchmark | `bench/deploy/memtier.html` |
| `run-openloop.sh` | coordinated-omission-correct capacity (the latency elbow) | Go loadgen (open-loop) | `bench/deploy/openloop.html` |
| `run-ycsb.sh` | YCSB core workloads A–F over a Zipfian keyspace | go-ycsb (+ patched memcache driver) | `bench/deploy/ycsb.html` |
| `cargo bench -p store` | in-process slab + index hot path | Criterion | `target/criterion/` |
| **`benchmark-all.sh`** | **runs all of the above, bakes one report** | — | **`bench/deploy/index.html`** |
| `track.sh` | history per commit + regression gates | Bencher (local SQLite/cloud) | Bencher web |

Backends: **BonsaiGrid** (`:5701`) — driven via the Hazelcast client, via its
**memcached** protocol (`bonsaigrid-mc`), and via its **RESP** protocol
(`bonsaigrid-redis`) — against real **Memcached** (`:11211`), **Redis** (`:6379`), and
**Hazelcast** (`:5702`). memtier, the open-loop bench, and YCSB drive BonsaiGrid with
the *same thin client* as an incumbent, giving apples-to-apples server-vs-server
numbers; the isolated macro run uses the heavyweight Hazelcast client (see Caveats).

### Prerequisites

- **Docker** (daemon reachable; `docker info` works). Used for Redis/Memcached/
  Hazelcast, the BonsaiGrid container, the Go loadgen build/run, and the Bencher server.
- **cargo** / Rust toolchain (builds the server, the `bench` tool, and Criterion).
- **`go` is NOT required on the host** — the loadgen is built inside a `golang` container.
- **`bencher` CLI** — optional, only for tracking.

Fixed loopback ports: BonsaiGrid `5701`, Hazelcast `5702`, Redis `6379`, Memcached
`11211`; Bencher API `6610`, console `3000`. `bench/preflight.sh` checks the
environment, clears stale `bench_` containers, and warns on bound ports (it runs
automatically at the start of `run-all-isolated.sh`).

### Running

```bash
# Run EVERY suite and build one combined report page (~20 min):
bench/benchmark-all.sh                          # → bench/deploy/index.html
#   smaller machine:   SERVER_CPUS=0-3 CLIENT_CPUS=4-7 bench/benchmark-all.sh
#   subset / faster:   SKIP="ycsb openloop" STAGE_SECS=3 bench/benchmark-all.sh
#   view it:           python3 -m http.server   # then open /bench/deploy/index.html

# --- or a single suite ---
bench/run-all-isolated.sh                       # fair four-backend run → bench/loadgen/combined.json
bench/run-memtier.sh                            # memtier: p99.9 tail, hit/miss, network
bench/run-openloop.sh                           # coordinated-omission-correct capacity
bench/run-ycsb.sh                               # YCSB workloads A–F
cargo bench -p store --bench hotpath            # Rust hot path (no network)
bench/verify-correctness.sh                     # end-to-end write/read-back over the wire
```

**Use `run-all-isolated.sh` for real numbers.** Every server **and** the load client
runs in its own container (its own cgroup v2) on a **disjoint cpuset**, with equal
CPU/memory caps, so the CPU-heavy client can't starve the server under test. BonsaiGrid
runs containerized too (glibc-matched image + `seccomp=unconfined` so io_uring works).
`run-all.sh` is a quick co-located variant that shares cores — smoke check only, not
fair.

Key env vars: `SERVER_CPUS` (default `0-7`), `CLIENT_CPUS` (`8-19`), `SERVER_MEM`
(`4g`), `CLIENT_MEM` (`8g`), `LEVELS` (`1,2,4,8,16,32,64,128`), `STAGE_SECS` (`4`),
`WARMUP_SECS` (`2`), `HZ_CONNS` (`128`), plus per-suite knobs (`RATIO`, `DATA_SIZE`,
`KEY_MAX`, `WORKLOADS`, `RATES`, …).

Each stage of `run-all-isolated.sh` records per-op rate, p50/p90/p99 latency, errors,
`mismatch`, and sampled server CPU/mem into `bench/loadgen/results-<backend>.json` and
merged `combined.json`.

### The suites

- **`run-ycsb.sh`** — YCSB **core workloads** (A update-heavy, B read-heavy, C
  read-only, D read-latest, F read-modify-write) over a **Zipfian** keyspace via
  `go-ycsb`, so BonsaiGrid (RESP + memcached) goes head-to-head with real Redis and
  Memcached. go-ycsb ships no memcache driver, so a small one (`bench/ycsb/patch/`) is
  compiled in at build time.
- **`run-openloop.sh`** — fixes **coordinated omission**: a dispatcher issues requests
  at a fixed **offered rate** and measures each request's latency from its **ideal send
  time**, so a saturated server shows the **latency elbow**. Reports **usable
  throughput** (highest offered rate holding p99 under `SLO_US`, default 10 ms). Adds a
  Zipf keyspace, configurable read/write `RATIO`, and `DATA_SIZE` objects.
- **`run-memtier.sh`** — drives every backend with **`memtier_benchmark`** through its
  real wire protocol in the same isolated harness, capturing **p99.9 tail latency,
  hit/miss ratio, and network throughput**.

### Correctness

The loadgen is **self-validating**: every GET is compared against the value just SET,
and a miss or wrong value is counted as `mismatch`. **`mismatch` must be 0** — a
nonzero value means the run is invalid (a server that acks writes without storing
them). Unit tests: `cargo test -p server`. Over the wire:
`bench/verify-correctness.sh [count]`.

### Server-isolated reference (`bench ladder`)

The macro benchmark drives BonsaiGrid through the **official Hazelcast client**, which
is heavyweight (a real client tax, not a server limit). For a client-isolated ceiling,
`bench ladder` drives the same server with a thin raw-protocol client:

```bash
./target/release/server &                              # or BONSAI_CORES=8 …
BENCH_ADDR=127.0.0.1:5701 ./target/release/bench ladder 4 128   # → JSON, ~425k ops/s on 8 cores
```

### Tracking (Bencher)

`bench/track.sh` maps each `(backend, level)` to a Bencher benchmark and uploads with a
**regression gate on throughput** (`--err` fails on a drop) — wire it into CI. It runs
fully local (self-hosted, SQLite, no account) or against Bencher Cloud:

```bash
bencher up --detach --api-volume bencher_data:/var/lib/bencher/data   # API :6610, console :3000
BENCHER_HOST=http://localhost:6610 BENCHER_PROJECT=bonsaigrid-bench bench/track.sh
#   graphs → http://localhost:3000/perf/bonsaigrid-bench
```

Without `BENCHER_PROJECT`, `track.sh` just writes `bench/loadgen/bmf.json` and validates
with `bencher run --dry-run` — safe with no server at all.

### Dashboards

Each suite bakes a self-contained `bench/deploy/*.html` (charts, headline tiles, and
summary all **computed from the data**, nothing hand-written); `benchmark-all.sh` bakes
the combined `bench/deploy/index.html`. Served over http they also live-reload their
`*-combined.json`; opened as `file://` they use the baked snapshot. The **Correctness**
tile turns red with the mismatch count if any GET didn't match.

```bash
python3 -m http.server            # from the repo root
# then open http://localhost:8000/bench/deploy/index.html
python3 bench/gen_index.py        # re-bake the combined report from existing data
```

### Caveats

- **Apples-to-apples:** in `run-all-isolated.sh`, BonsaiGrid and Hazelcast share the
  heavyweight official Hazelcast client while Redis/Memcached use thin native clients,
  so *cross-backend* numbers there are directional (the controlled pair is BonsaiGrid
  vs Hazelcast). **memtier, open-loop, and YCSB** remove that asymmetry — they drive
  BonsaiGrid with the *same thin client* as the incumbent, so those are true
  server-vs-server results. `bench ladder` is the client-isolated server ceiling.
- **First run pulls images and builds** (needs network; caches to
  `~/.cache/bonsai-bench`). A single four-backend suite is ~5–8 min; `benchmark-all.sh`
  is ~20 min.
- **Ctrl-C is safe** — every runner traps and tears down its containers on exit.
