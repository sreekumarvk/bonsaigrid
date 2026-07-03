# Benchmark Harness — Design

**Date:** 2026-07-02
**Status:** Approved scope. Design record for implementation.
**Reference:** Anton Putra's "Redis vs Memcached" harness
(`github.com/antonputra/tutorials/lessons/225`) — same shape, adapted to four
backends over the Hazelcast client protocol.

## Goal

A fair, staged-load, Prometheus-instrumented Go load generator that benchmarks
**BonsaiGrid, Hazelcast, Redis, and Memcached** under one identical SET+GET
workload, producing client-side latency percentiles + throughput as a function of
concurrency. The headline comparison is **BonsaiGrid vs. Hazelcast** (same
`hazelcast-go-client`, swap the host — a true apples-to-apples A/B, since
BonsaiGrid speaks the Hazelcast wire protocol); Redis and Memcached provide
cross-family context.

## Non-Goals (v1)

- Multi-node / clustered topologies (baseline is single-node each, no backups, to
  match single-instance Redis/Memcached). Cluster runs are a follow-up.
- Kubernetes orchestration (Anton's lesson uses k8s; we target single-box
  docker-compose + local processes for reproducibility).
- Value-size / pipeline / mixed-ratio sweeps beyond one configured workload (the
  ramp varies concurrency, not payload) — parameterizable later.
- Server-side metric scraping for the comparison chart — latency is measured
  **client-side** (fair across implementations); server metrics are supplementary.

## Decisions (from scoping)

1. **Four backends**, one workload, backend chosen by `TARGET` env
   (`bonsaigrid` | `hazelcast` | `redis` | `memcached`).
2. **`hazelcast-go-client`** drives BOTH `bonsaigrid` and `hazelcast` (identical
   client code, different host). `go-redis` and `gomemcache` drive the other two
   (as in the reference harness).
3. **Reuse the reference design wholesale:** staged closed-loop ramp, the
   fine-grained `request_duration_seconds` histogram labeled `{op, target}`,
   Prometheus `/metrics`. Only the backend selector + the Hazelcast store are new.
4. A `Store` interface with four implementations (cleaner than the reference's
   inline `if client == ...`), so each op path is isolated and testable.
5. Harness lives in the BonsaiGrid repo under **`bench/loadgen/`** (Go) and
   **`bench/deploy/`** (compose + Prometheus + scripts), separate from the Rust
   `crates/bench`.

## Architecture

```
            ┌──────────────── bench/loadgen (Go) ────────────────┐
 TARGET ───►│ main.go: staged ramp (minClients→maxClients, +1/stage,           │
            │           closed-loop concurrency, think-time)                    │
            │   │ each request: store.Set(uuid, userJSON, ttl); store.Get(uuid) │
            │   ▼                                                               │
            │ store.go: Store{ Set, Get } ── hzStore (hazelcast-go-client) ─────┼──► BonsaiGrid  (TARGET=bonsaigrid)
            │                             ├─ hzStore (hazelcast-go-client) ─────┼──► Hazelcast   (TARGET=hazelcast)
            │                             ├─ redisStore (go-redis) ─────────────┼──► Redis
            │                             └─ mcStore (gomemcache) ──────────────┼──► Memcached
            │ metrics.go: Prometheus histogram request_duration_seconds{op,target}
            └──────────────────────────── /metrics ──────────────────────────► Prometheus ─► Grafana
```

### Component 1 — `store.go` (backend abstraction)

```go
type Store interface {
    Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
    Get(ctx context.Context, key string) ([]byte, error)
    Close() error
}
```

- **`hzStore`** wraps a `hazelcast-go-client` `*hazelcast.Map`. `Set` →
  `m.SetWithTTL(ctx, key, val, ttl)`; `Get` → `m.Get(ctx, key)` (value is the
  JSON `[]byte`). Constructed with the cluster host+port and a map name; used for
  both `bonsaigrid` and `hazelcast` (only the config host differs).
- **`redisStore`** wraps `*redis.Client` (`Set`/`Get` with the TTL).
- **`mcStore`** wraps `*memcache.Client` (`Set`/`Get`, TTL in seconds).
- `NewStore(target string, cfg Config) (Store, error)` selects the impl.

### Component 2 — `user.go` (workload payload)

The reference `User` (uuid, username, firstName, lastName, address) built from a
new UUID per request, `json.Marshal`ed to the value bytes. Key = the UUID string.
Value ≈ 60–100 bytes. Unchanged from the reference except that Set/Get go through
the `Store` interface, not per-backend methods.

### Component 3 — `metrics.go` (client-side latency)

Verbatim from the reference: a Prometheus registry, a `stage` gauge (current
concurrency), and `request_duration_seconds` — a `HistogramVec` labeled
`{op ∈ set|get, target}` with the reference's fine bucket list (10µs → 5s). A
`/metrics` server on `metricsPort`. Each op records `Observe(time.Since(start))`.

### Component 4 — `main.go` (staged ramp)

The reference loop: start at `minClients`, each stage bound in-flight goroutines
to the current count via a buffered channel (closed-loop), run the stage for
`stageIntervalS`, then `+1` client, until `maxClients`. Per request: optional
`requestDelayMs` think-time, then `Set` + `Get` (each timed). A warm-up stage
(configurable, its samples discarded via a metrics reset or a marked stage)
precedes measurement. Backend from `TARGET`; config from `config.yaml`.

### Component 5 — `bench/deploy/` (orchestration, single-box)

- **`docker-compose.yml`** — services: `hazelcast` (official image),
  `redis`, `memcached`, `prometheus`, optional `grafana`. BonsaiGrid runs as the
  locally-built `server` binary (documented command) or an optional
  `bench/deploy/Dockerfile.bonsaigrid` — kept out of compose so it uses the
  freshly-built binary.
- **`prometheus.yml`** — scrape the loadgen `/metrics`.
- **`grafana-dashboard.json`** — panels: p50/p90/p99 latency and RPS vs. `stage`,
  faceted by `target`.
- **`run.sh` / `Makefile`** — `make bench TARGET=bonsaigrid` starts the backend
  (if needed), runs the loadgen, and leaves Prometheus scraping. A `report.sh`
  queries Prometheus (`histogram_quantile`, `rate`) into a comparison table.

## Fairness & Methodology (documented; part of the deliverable)

- Same host/network for client and all backends (localhost or one machine); same
  value size, key space, ramp, and think-time across targets.
- **Baseline = single node, no backups, no persistence** — matches single-instance
  Redis/Memcached. BonsaiGrid/Hazelcast extra-capability lanes are separate runs.
- Warm-up before measured stages; each stage long enough to be steady-state.
- Report **client-side** latency percentiles (the fair, user-visible metric) +
  achieved RPS per stage. Capture the environment (CPU model + cores, kernel,
  io_uring availability, backend versions) alongside results.
- Note the architectural caveat in the writeup: Redis is single-threaded,
  Memcached has no partitioning/replication, so the chart is "what a client
  experiences at parity config," not an architecturally-identical comparison —
  BonsaiGrid/Hazelcast do strictly more (partitioning, backups, listeners).

## Risk — Go-client conformance (de-risk first)

We have verified **Python/Java** clients against BonsaiGrid, but **not**
`hazelcast-go-client`. Its connection/auth handshake or a specific codec it uses
may hit a BonsaiGrid gap. **Task 1 is a smoke test** (connect + one `Set`/`Get`
via the Go client against BonsaiGrid) *before* building the ramp. If it fails, the
harness has already done its job — it surfaced a Go-client parity gap; fixing that
gap (a handful of codecs / the client auth path) becomes a prerequisite sub-task,
tracked separately.

## Testing Strategy

- **Smoke (Task 1):** the Go client connects to BonsaiGrid and does one Set/Get
  round-trip (and separately to Hazelcast/Redis/Memcached). Gates the rest.
- **Unit:** each `Store` impl round-trips a value against its backend (build-time
  behind a `-short`/env guard so CI without the backend skips it); `NewStore`
  selects the right impl per `TARGET`; the ramp scheduler reaches `maxClients` and
  the `stage` gauge tracks it (drive the loop with a stub `Store` + tiny stage
  interval).
- **Integration (manual, documented):** `docker-compose up`, run each `TARGET`,
  confirm Prometheus shows `request_duration_seconds{target=...}` and the Grafana
  panels render — the acceptance check for a real benchmark run.

## Guardrail Compliance

Not applicable to the BonsaiGrid server (this is an external Go client). The
harness must not special-case any backend in the hot loop — identical timing and
op sequence per target — so the comparison stays fair.

## Phasing

- **1 — loadgen:** `Store` interface + 4 impls (with the Task-1 Go-client smoke),
  `user`, `metrics`, `main` ramp, `config`. Buildable; runs against one target.
- **2 — deploy:** compose + Prometheus + `run.sh`/`Makefile` + per-backend config.
- **3 — reporting:** Grafana dashboard + `report.sh` (Prometheus → comparison
  table); a `RESULTS.md` template capturing environment + methodology.
- **4 — BonsaiGrid dimensions:** re-run the two Hazelcast-protocol lanes with
  backups on/off, `BONSAI_PERSISTENCE=async` vs off, and a CP `AtomicLong` lane.

## Config

`bench/loadgen/config.yaml`:
- `metricsPort` (default 8081)
- `target` overridable by `TARGET` env
- per-backend `host`/`port` (`bonsaigrid`, `hazelcast`, `redis`, `memcached`)
- `mapName` (Hazelcast/BonsaiGrid IMap), `ttlSeconds`
- `test`: `minClients`, `maxClients`, `stageIntervalS`, `requestDelayMs`,
  `warmupStages`

## Open Questions / Risks

- **Go-client compat** — the headline risk (above); de-risked in Task 1.
- **BonsaiGrid packaging for compose** — run the built binary vs a Dockerfile;
  default to the binary + a documented command to avoid a slow image build in the
  benchmark loop.
- **Closed-loop vs open-loop** — the reference (and this spec) is closed-loop
  (bounded concurrency); an open-loop (fixed arrival rate) variant better exposes
  saturation latency and is a worthwhile follow-up, not v1.
- **hazelcast-go-client serialization** — send the value as `[]byte`/`string` to
  avoid Compact schema negotiation differences across the two servers; keep it
  identical for `bonsaigrid` and `hazelcast`.
