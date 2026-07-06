# BonsaiGrid — Requirements & Roadmap

**Single source of truth.** This file holds both the *enduring requirements* (what
BonsaiGrid must be — the guardrails that never change) and the *living status/roadmap*
(what's built, what's left, what's next). **Update it as each capability lands**, with
commit/crate evidence — see [§8, How this document stays in sync](#8-how-this-document-stays-in-sync).

**Last synced:** 2026-07-05.
Supersedes `docs/hazelcast-platform-gap-roadmap.md` (merged here). `docs/ROADMAP.md`
(15-epic history) and `docs/PARITY.md` (OSS/Enterprise scoping) remain as historical /
scoping references only.

---

## 1. Objective

A distributed, in-memory data grid designed as a highly memory-efficient, bare-metal
alternative to JVM-based architectures (e.g. Apache Hazelcast). It prioritizes
deterministic memory layout, a thread-per-core runtime, and kernel-bypass asynchronous
I/O for predictable latency and optimized memory utilization.

**North star:** a genuine **drop-in replacement** — unmodified Hazelcast clients connect
and work, operators keep their existing metrics/monitoring — with markedly better
latency and memory density underneath. Full functional parity, multi-node included; the
single-node MVP was only a stepping stone.

### Workspace topology

- **Java reference baseline:** `./hazelcast/` — the official Apache Hazelcast Git
  checkout. Read-only; exists only to extract the client wire protocol. Gitignored, not
  pushed. Do **not** imitate its JVM architecture.
- **Target codebase:** the Rust workspace under `crates/` (see [Layout](README.md#layout)).

---

## 2. Architectural Guardrails (Non-Negotiable)

These define the project's entire reason for existing. Violating them defeats the
purpose; do not relax them for convenience.

1. **Zero-allocation hot path.** After startup, the request / serialize / store path
   must do no heap allocation — no `malloc`/`free`, no `Box`, `Vec::new`, or `String`
   growth in the hot path. All working memory comes from pre-allocated contiguous pools
   (the slab allocator). Allocation is allowed only during initialization. *(Enforced by
   `crates/server/tests/zero_alloc.rs` — a counting allocator asserts 0 allocations over
   10k MapGets.)*
2. **Shared-nothing, thread-per-core.** Exactly one OS thread per CPU core, hard-pinned
   via `core_affinity`. **No `Mutex`/`RwLock` and no shared mutable memory between
   threads.** Cross-core coordination happens only through lock-free SPSC channels.
3. **Kernel-bypass I/O.** No blocking I/O or standard epoll in the hot path. Use
   `io_uring` (via `tokio-uring` or raw syscalls).

### Technology stack

Mandated primitives: `io_uring` (async socket engine), `core_affinity` (CPU pinning),
`crossbeam-channel`/`flume` (lock-free SPSC), `ahash`/`xxhash` (deterministic hashing).
kTLS keeps the hot path zero-alloc under encryption. See [README §Layout](README.md#layout)
for the full 15-crate map.

---

## 3. Current state — what's built ✅

**Foundation (the original v0.1 MVP — all four phases DONE):**

1. **Hazelcast client protocol** — frame headers (magic/flags/correlation/partition/
   type), `map.put`/`map.get` byte layouts, response payloads; validated byte-for-byte
   against Hazelcast's committed 2.10 conformance fixture.
2. **Deterministic slab allocator** — one `mmap`'d region at startup, fixed-size slabs,
   O(1) lock-free free-list, explicit OOM (never grows the heap).
3. **Thread-per-core io_uring reactor** — N pinned workers, independent poll loops,
   `SO_REUSEPORT`, per-core TPC ports.
4. **Sharded map & routing** — key-hash → owning core; cross-core delegation via SPSC.

**Solid platform capabilities:**

- **APIs & clients** — Hazelcast Open Client Protocol (2.10), smart-client partition
  routing (MurmurHash3, matches the client exactly — 1000/1000 keys), near-cache, JNI.
  Real Python + Java clients pass conformance. Also speaks **memcached** and **RESP
  (Redis)** on the same port, plus REST health endpoints.
- **Distributed architecture** — thread-per-core shared-nothing, io_uring, membership /
  heartbeat / master-election / migration, cross-core routing, zero-alloc hot path.
- **Fast data store** — sync K-backup replication, strict-majority quorum, owner-only
  reads, HLC time-ordered merge (deterministic-simulation verified in
  `crates/server/src/sim.rs`).
- **Data structures** — IMap, MultiMap, Queue, List, Set, Ringbuffer, PNCounter, Topic,
  Flake-ID, locks, CardinalityEstimator (HyperLogLog); entry listeners, transactions,
  entry processors.

---

## 4. Platform parity — gap roadmap

All five major platform-parity gaps are **shipped or substantially shipped**. Verified
box-by-box against the Hazelcast "Unified Real-Time Platform" diagram.

| # | Gap | Status | Size |
|---|-----|--------|------|
| 1 | **Security** (TLS/mTLS, authN, RBAC) | ✅ shipped (LDAP/JAAS backend remains) | M–L |
| 2 | **CP Subsystem** (Raft) | 🟢 substantially shipped (6 primitives + sessions + named groups) | XL |
| 3 | **Persistence / durable log** | ✅ shipped (sync-ack enhancement remains) | L |
| 4 | **Geo-replication / WAN** | ✅ shipped (active-active, IMap + structures) | L–XL |
| 5 | **Streaming / SQL depth** | 🟢 substantially shipped (distributed SQL + windowing) | L |

### Gap 1 — Security ✅
RBAC (resource+action, Hazelcast-parity), hashed-credential auth (PBKDF2-HMAC-SHA256,
constant-time) behind an `IdentityProvider` seam, client kTLS (userspace rustls
handshake + kernel per-record crypto; `disabled`/`permissive`/`required` modes), member
mTLS, **client-cert-as-principal** (mTLS CN → RBAC, wired into the reactor auth path).
**Remaining:** LDAP/JAAS backends (needs a directory server). `crates/security`.

### Gap 2 — CP Subsystem (Raft) 🟢
From-scratch **Raft core** (election with the restriction, log replication with
log-matching + conflict truncation, current-term majority commit; deterministic-sim
verified), **durable log** (crash/torn-tail safe, term/vote fsync'd), **compaction**,
forward-to-leader driver + state-machine registry. **Six client-reachable primitives**
(via `BONSAI_CP`): AtomicLong, AtomicReference, CountDownLatch, Semaphore, FencedLock,
**CPMap** (codec + dispatch, end-to-end). **CP sessions** (create/heartbeat/close/
generateThreadId + FencedLock session-expiry auto-release), **read-index (lease)
linearizable reads**, **named CP groups** (independent consensus domains routed by the
request's RaftGroupId; group-tagged member messages). **Remaining:** `InstallSnapshot`
(fast rejoin), named-group membership *subsets*, Semaphore per-session auto-release,
FencedLock `getLockOwnershipState`, live-cluster real-client conformance. `crates/raft`.
*(Enterprise-only in Hazelcast ≥ 5.5 — functional completeness beyond strict OSS parity.)*

### Gap 3 — Persistence / durable log ✅
Structure-agnostic **WAL** (`[len|crc32|type|payload]`, torn-tail safe, group-commit
fsync, off the hot path), **sectioned snapshots** (atomic tmp+fsync+rename) + `recover()`
(newest snapshot + WAL replay, idempotent via stamp-guarded merge), recovery **before
serving**, opt-in `BONSAI_PERSISTENCE=none|async|sync`. Covers IMap **and every
structure**. **Remaining:** `sync` deferred-ack (hold the reply until fsync; async
works); a WAL bytes-ring for a zero-alloc persist-*enabled* path. `crates/persistence`.

### Gap 4 — Geo-replication / WAN ✅
Asynchronous **active-active** cross-cluster replication. Capture (`wan_sink`) +
`apply_wan` loop-prevention (lock-free no-op when off — hot path preserved), durable
outbound **`WanQueue`** (framed/crc32/fsync'd, **per-target ack cursors**, **reclaim**
of records confirmed by every target — the unbounded-growth bug is fixed), HLC/
LatestUpdate convergence (deterministic two-cluster sim: one-way, active-active,
loop-free, outage-replay), live per-cluster WAN thread (blocking TCP off the hot path),
and capture for **every structure** via `WanOp::Aux`. **Remaining:** WAN-over-TLS,
initial full-state bootstrap, delta/compression, dynamic topology, more merge policies.
`crates/wan`.

### Gap 5 — Streaming / SQL depth 🟢
**Distributed SQL** — two-phase scatter/gather over the member transport (local partials
merged cluster-wide; `COUNT/SUM/AVG/MIN/MAX`, `GROUP BY`, windowed, projected rows).
**Event-time windowing** (tumbling/sliding/session, watermark-driven), **SQL windowing**
(`TUMBLE`/`HOP`), **stateful stream-stream keyed join** (watermark/TTL eviction).
**Connectors:** Kafka, MapStore/MapLoader, file, **JDBC/PostgreSQL** (loads a query into
an IMap; Docker-tested), **CDC** (Postgres logical-replication capture; Docker-tested),
**socket** source/sink. **Remaining:** SESSION-window SQL, `SELECT *` distribution,
continuous (vs batch) SQL, distributed joins (network shuffle). `crates/jet`,
`crates/query`.

---

## 5. Next up — prioritized

Ordered by value ÷ effort, and by whether it's buildable + testable here (no external
infra) vs. needs a live backend.

**A. CP finishers — self-contained, highest value (do next).** All deterministic-sim
testable. `InstallSnapshot` (rejoin after log compaction), Semaphore per-session
auto-release, FencedLock `getLockOwnershipState`, named-group membership subsets.

**B. Durability + SQL depth — self-contained.** Persistence `sync` deferred-ack (touches
the reactor deferred-reply path — the one item near the hot path); SESSION-window SQL;
`SELECT *` distribution; continuous SQL execution.

**C. Larger / architectural (scope first).** Distributed joins (network shuffle);
Management Center depth (remaining `MC*` ops); data-structure tail audit (ReplicatedMap /
JCache-ICache edge ops / ReliableTopic).

**D. Needs external infra (Docker-testable, like the JDBC/CDC connectors were).**
LDAP/JAAS auth backends; live-cluster real Hazelcast-client CP conformance; more
connectors (JMS, other DBs).

---

## 6. Won't do — genuinely infeasible

`IExecutorService` / distributed Runnable fan-out: BonsaiGrid is not a JVM and cannot
execute the serialized Java callables those APIs carry.

---

## 7. Already-closed hazards (context)

- **Split-brain default quorum** → strict majority (`membership::default_quorum`).
- **Merge lost-writes** → HLC time-ordered stamps with peer-stamp absorption
  (`store::next_stamp` / `observe_stamp`).
- **Distributed-correctness verification** → deterministic simulation harness
  (`crates/server/src/sim.rs`): durability-across-failover, split-brain, merge and
  migration convergence.
- **Benchmark "slower than Memcached"** → a single-connection Go-client artifact; with a
  per-worker connection pool the server does ~580k ops/s (8 cores). See
  [README §Benchmarks](README.md#benchmarks).

---

## 8. How this document stays in sync

This file is the **living contract**. When a capability lands (or a requirement changes):

1. Move the item from **§5 Next up** into the relevant **§4 gap** as ✅/🟢 with a
   one-line "what shipped" and the crate/commit evidence.
2. Update the **§4 status table** and the **Last synced** date at the top.
3. If a gap is now fully closed, mark it ✅ and trim its "Remaining."
4. Never let §5 list something already shipped, or a §4 "Remaining" contradict what's in
   the tree — reconcile in the same change that lands the code.

The supplementary docs (`docs/ROADMAP.md`, `docs/PARITY.md`) are historical and are
**not** kept in sync — this file is authoritative. Design specs live under
`docs/superpowers/specs/`.
