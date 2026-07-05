# Hazelcast Platform Parity — Gap Roadmap

**Created:** 2026-07-01 · **Last updated:** 2026-07-02
**Basis:** Hands-on assessment against the Hazelcast "Unified Real-Time Platform"
diagram, verified box-by-box against the actual BonsaiGrid codebase.
**Purpose:** Track platform gaps as an ordered roadmap. This revision records what
has **shipped** (with commits/crates as evidence) versus what **remains**.

> **Reconciliation note.** `docs/ROADMAP.md` marks all OSS-parity epics
> "complete," and `docs/PARITY.md` scopes the CP Subsystem *out* (Enterprise-only
> from Hazelcast 5.5). This document tracks **functional-depth and
> platform-capability**, not just the wire/API surface. Where docs disagree on
> "done," trust the box-by-box evidence and the commits cited below.

## Current state (what is genuinely solid)

- **APIs & Clients** ✅ — Hazelcast open binary protocol, smart-client partition
  routing, near-cache, JNI. Real Python/Java clients pass conformance.
- **Distributed Architecture** ✅ — thread-per-core shared-nothing, membership /
  heartbeat / master-election / migration, cross-core routing, zero-alloc hot
  path, io_uring.
- **Fast Data Store — Availability & Consistency (IMap-level CP)** ✅ — sync
  K-backup replication, strict-majority quorum, owner-only reads, HLC
  time-ordered merge. Verified by the deterministic simulation harness
  (`crates/server/src/sim.rs`).
- **Data structures** ✅ — IMap, MultiMap, Queue, List, Set, Ringbuffer,
  PNCounter, Topic, Flake-ID, locks; listeners, transactions, entry processors.

## The gaps, ordered — status snapshot

| # | Gap | Status | Size | Depends on |
|---|-----|--------|------|-----------|
| 1 | **Security** (TLS/mTLS, authN, RBAC) | ✅ **shipped** (minor backends remain) | M–L | — |
| 3 | **Persistence / durable log** (enabler) | ✅ **shipped** (sync-ack enhancement remains) | L | store internals |
| 2 | **CP Subsystem** (Raft) | 🟢 **substantially shipped** (5 primitives + sessions) | XL | member transport; persistence spine |
| 5 | **Streaming / SQL depth** | 🟢 **substantially shipped** (distributed SQL + windowing) | L | — (independent) |
| 4 | **Geo-replication / WAN** | ✅ **shipped** (active-active, IMap + structures) | L–XL | persistence spine (3) |

**Sequence achieved:** Security → Persistence spine → CP Subsystem → Streaming/SQL
depth → Geo/WAN. **All five major platform gaps are now shipped or substantially
shipped** — the last untouched box (Disaster Recovery & Geo-Replication) is closed.

---

## Gap 1 — Security ✅ SHIPPED

**Spec:** `docs/superpowers/specs/2026-07-01-security-tls-rbac-design.md`.
**Memory:** `bonsaigrid-security-impl`.

**Done:**
- Resource+action **RBAC** with Hazelcast-parity permissions (`crates/security`).
- **Hashed-credential auth** (PBKDF2-HMAC-SHA256, constant-time) behind an
  `IdentityProvider` seam.
- **Client kTLS** — userspace rustls handshake + kernel per-record crypto (keeps
  the zero-alloc hot path), with a three-state mode (`disabled`/`permissive`/
  `required`) for zero-downtime rollout.
- **Member mTLS** — mutual-TLS member mesh trust (`crates/member` transport).

**Remaining (minor):** LDAP/JAAS auth backends behind the `IdentityProvider` seam;
client-cert-as-principal mapping.

---

## Gap 3 — Persistence / durable log (the enabler) ✅ SHIPPED

**Spec:** `docs/superpowers/specs/2026-07-01-persistence-durable-log-design.md`.
**Memory:** `bonsaigrid-persistence`.

**Done (`crates/persistence`):**
- Structure-agnostic **WAL** (`[len|crc32|record_type|payload]`, torn-tail safe),
  segment writer + group-commit fsync, off the reactor hot path.
- **Sectioned snapshots** (atomic tmp+fsync+rename) + `recover()` (load newest
  snapshot, replay later WAL, idempotent via stamp-guarded merge).
- Recovery **before serving**; opt-in `BONSAI_PERSISTENCE=none|async|sync`.
- Covers **IMap and every data structure** (queue/list/set/multimap/ringbuffer/
  pncounter) via a `WalSink` seam emitting after the in-memory apply.

**Remaining:** `sync` durability deferred-ack (hold the client reply until fsync —
async is fully working); a WAL bytes-ring to make the persist-*enabled* path
zero-alloc (the default/disabled path already is).

---

## Gap 2 — CP Subsystem (Raft) 🟢 SUBSTANTIALLY SHIPPED

**Spec:** `docs/superpowers/specs/2026-07-02-cp-subsystem-raft-design.md`.
**Memory:** `bonsaigrid-cp-raft`. **Crate:** `crates/raft`.
Note: Enterprise-only in Hazelcast ≥ 5.5, so this is functional completeness for
the platform-diagram "Consistency" box, beyond strict OSS parity.

**Done:**
- **Raft core** — leader election (with the election restriction), log replication
  (log-matching + conflict truncation), current-term majority commit. Pure,
  message-driven, **deterministic-simulation verified** (single leader,
  replicate+commit, re-election, minority-cannot-commit, no divergence).
- **Durable Raft log** (crash-safe, torn-tail safe; term/vote fsync'd before a
  vote is granted → no double-vote on restart).
- **Log compaction / snapshots** — bounds the log (safe min-match-index
  compaction; no InstallSnapshot needed for the static group). Exercised by the
  whole safety suite under continuous compaction.
- **Forward-to-leader CP driver** + a generalized state-machine registry.
- **Five client-reachable primitives** (via `BONSAI_CP`): `IAtomicLong`,
  `IAtomicReference`, `ICountDownLatch`, `ISemaphore`, `FencedLock` (monotonic
  fencing token). Each = a registry entry + codec + one dispatch arm.
- **CP sessions** — create/heartbeat/close/generateThreadId with a
  deterministic replicated clock, and **session-expiry auto-release** for
  FencedLock (a dead client's locks free automatically).
- **Live wiring** — Raft/CP messages over the member io_uring transport;
  AtomicLong/etc. client codecs with deferred replies via the broker.

**Shipped since:** ✅ `CPMap` (`crates/raft/cpmap.rs`, sim-verified); ✅ read-index
(lease) linearizable reads (`RaftNode::has_read_lease`).
**Remaining:** multiple named CP groups; `InstallSnapshot` for a long-down/rejoining
member; Semaphore per-session auto-release; FencedLock `getLockOwnershipState`; the
live-cluster **real Hazelcast-client conformance test** (the algorithm is
deterministically verified; frame-level client compat needs a running cluster).

---

## Gap 5 — Streaming / SQL depth 🟢 SUBSTANTIALLY SHIPPED

**Memory:** `bonsaigrid-streaming-depth`. **Crates:** `crates/jet`, `crates/query`.
Was an MVP (mock processors, single-node SQL); now a real operator layer with
distributed execution.

**Done:**
- **Distributed SQL** (the headline distributed-execution capability) — two-phase
  scatter/gather over the member transport: each member computes a local partial
  (mergeable per-group `Acc`) or local rows; the coordinator gathers, merges, and
  applies `DISTINCT`/`ORDER BY`/`LIMIT` once cluster-wide. Covers `COUNT/SUM/AVG/
  MIN/MAX`, `GROUP BY`, windowed queries, and projected plain-row `SELECT`s.
  Verified in-process via the member sim (a scattered `COUNT(*)` merges across
  members).
- **Event-time windowing** — tumbling / sliding / session windows with
  Sum/Count/Min/Max/Avg, watermark-driven completion (`crates/jet`).
- **SQL windowing** — `TUMBLE`/`HOP` table functions (`window_start`/`window_end`
  as group columns), reusing the aggregation path.
- **Stateful stream-stream keyed join** with watermark/TTL state eviction.
- **File source connector** (beyond Kafka/MapStore).

**Remaining:** SESSION-window *SQL* (batch two-pass; distributed sessions span
members); `SELECT *` distribution (needs catalog star-columns on the member
thread); distributed joins (needs a network shuffle); continuous/streaming (vs
batch) SQL execution; JDBC/CDC connectors; **`IExecutorService` distributed
fan-out is infeasible** — BonsaiGrid is not a JVM and cannot execute the
serialized Java callables those APIs carry.

---

## Gap 4 — Geo-replication / WAN ✅ SHIPPED

**Spec:** `docs/superpowers/specs/2026-07-02-geo-wan-replication-design.md`.
**Plan:** `docs/superpowers/plans/2026-07-02-geo-wan-replication.md`.
**Memory:** `bonsaigrid-geo-wan`. **Crate:** `crates/wan`.

Asynchronous **active-active** cross-cluster replication — Hazelcast's WAN Replication
(the platform-diagram "Disaster Recovery and Geo-Replication" box). The last untouched
major box is now closed.

**Done (Phases A–D):**
- **Capture (`wan_sink`) + `apply_wan` loop prevention** — a second store sink mirrors
  local IMap mutations after the in-memory apply; inbound WAN records apply via
  `apply_wan` (HLC `put_merge`, persisted but never re-published), so active-active
  does not echo. Lock-free no-op when WAN is off (zero-alloc hot path preserved).
- **Durable outbound `WanQueue`** — framed, crc32/torn-tail-safe, fsync'd, with a
  durable committed cursor; recovers unacked records on reopen; byte-bound gate.
- **Convergence** — concurrent writes converge via the existing HLC/LatestUpdate
  merge; at-least-once delivery dedups for free under the stamp. Proven by a
  deterministic two-cluster sim (one-way, active-active, loop-free, outage-replay).
- **Live transport** — a per-cluster WAN thread (dedicated blocking TCP, off the hot
  path): inbound listener applies+acks batches; outbound loop drains the capture ring
  into the queue (throw / drop-oldest backpressure) and ships to each target, acking
  only what all targets confirm. Verified by a live two-cluster loopback-TCP test.
- **Structures (Phase D)** — extends capture to every persistence-covered structure
  (queue/list/set/multimap/ringbuffer/pncounter) via a `WanOp::Aux(kind)` record and
  the store's `emit_aux`; applied through `install_aux` (loop-free).
- **Config** — `BONSAI_WAN_TARGETS` / `_PORT` / `_BATCH` / `_QUEUE_MB` /
  `_BACKPRESSURE` / `_DIR`, wired via `setup_wan` at both server run paths.

**Remaining (follow-ups):** WAN over TLS (reuse the member mTLS bundle); initial
full-state bootstrap (v1 replicates from enable-forward); delta/compression + event
filtering; dynamic WAN topology/discovery; merge policies beyond HLC LatestUpdate;
per-target ack cursors (v1 uses one cursor = all-targets-confirmed).

---

## Minor / residual parity items (smaller, opportunistic)

**Shipped 2026-07-05** (see `docs/plans/opportunistic-tail.md`):
✅ **CardinalityEstimator** (HyperLogLog, full stack: algorithm + persist/snapshot/WAN
+ client codecs) · ✅ **Client-cert-as-principal** (CN → RBAC principal) · ✅ **WAN
per-target ack cursors** · ✅ **CP CPMap** · ✅ **CP read-index lease reads**.

**Remaining (external-infra or larger):**
- **Connectors:** Kafka (rskafka) + MapStore/MapLoader + file source shipped; JDBC,
  CDC, socket sources/sinks need a live backend to test.
- **Management Center depth:** metrics + Prometheus + MC codecs exist; close
  remaining `MC*` operations for full GUI parity.
- **Auth backends:** LDAP/JAAS behind the `IdentityProvider` seam (needs a directory).
- **Data-structure tail:** ReplicatedMap depth, JCache/ICache edge ops, ReliableTopic
  guarantees — audit against the client codecs.

## Already closed earlier (context)

- **Split-brain default quorum** → strict majority (`membership::default_quorum`).
- **Merge lost-writes** → HLC time-ordered stamps with peer-stamp absorption
  (`store::next_stamp` / `observe_stamp`).
- **Distributed-correctness verification** → deterministic simulation harness
  (`crates/server/src/sim.rs`): durability-across-failover, split-brain, merge
  convergence, migration convergence.

Memories: `bonsaigrid-security-impl`, `bonsaigrid-persistence`, `bonsaigrid-cp-raft`,
`bonsaigrid-streaming-depth`, `bonsaigrid-dst-harness`. See `docs/ROADMAP.md` /
`docs/PARITY.md` for the broader wire/API parity picture.
