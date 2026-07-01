# Hazelcast Platform Parity — Gap Roadmap

**Date:** 2026-07-01
**Basis:** Hands-on assessment against the Hazelcast "Unified Real-Time Platform"
diagram, verified box-by-box against the actual BonsaiGrid codebase this session
(not from the prior roadmap's self-reported status).
**Purpose:** Track the remaining platform gaps as an ordered roadmap so work can
resume here after the Security iteration ships.

> **Reconciliation note.** `docs/ROADMAP.md` marks all OSS-parity epics
> "complete," and `docs/PARITY.md` scopes the CP Subsystem *out* (Enterprise-only
> from Hazelcast 5.5). This document does not contradict the *wire/API* parity
> those docs track — the client-facing surface is broad and real. It records the
> **functional-depth and platform-capability gaps** a code-level audit surfaced:
> the pieces that exist as an MVP or a protocol surface but are not yet
> production-depth. Where this doc and the older ones disagree on "done," trust
> the box-by-box evidence cited below.

## Current state (what is genuinely solid)

From the box-by-box audit:

- **APIs & Clients** ✅ — Hazelcast open binary protocol, smart-client partition
  routing, near-cache, JNI. Real Python/Java clients pass conformance.
- **Distributed Architecture** ✅ — thread-per-core shared-nothing, membership /
  heartbeat / master-election / migration, cross-core routing, zero-alloc hot
  path, io_uring.
- **Fast Data Store — Availability & Consistency (IMap-level CP)** ✅ — sync
  K-backup replication, strict-majority quorum (fixed this session), owner-only
  reads, HLC time-ordered merge (fixed this session). Verified by the
  deterministic simulation harness (`crates/server/src/sim.rs`).
- **Data structures** ✅ — IMap, MultiMap, Queue, List, Set, Ringbuffer,
  PNCounter, Topic, Flake-ID, locks; listeners, transactions, entry processors.

## The gaps, ordered

Dependency and guardrail-tension summary (guardrails = zero-alloc hot path,
thread-per-core shared-nothing, io_uring kernel-bypass, no disk in hot path):

| # | Gap | Status | Guardrail tension | Size | Depends on |
|---|-----|--------|-------------------|------|-----------|
| 1 | **Security** (TLS/mTLS, authN, RBAC) | 🟡 auth only → **spec'd** | Medium (kTLS resolves it) | M–L | — |
| 2 | **CP Subsystem** (Raft) | ❌ | **High** (needs durable log) | XL | member protocol; **persistence spine** |
| 3 | **Persistence / durable log** (enabler) | ❌ | **High** (disk vs in-memory) | L | store internals |
| 4 | **Geo-replication / WAN** | ❌ | High (async, durable buffer) | L–XL | persistence spine (3) |
| 5 | **Streaming / SQL depth** | 🟡 MVP | Low | L | — (independent) |

**Recommended sequence:** **1 Security** (in progress) → **3 Persistence spine**
→ **2 CP Subsystem** (reuses the log) → **4 Geo/WAN** (reuses the log) →
**5 Streaming/SQL depth** slotted in independently whenever. The persistence
spine (3) is the pivot: both CP and WAN want a durable replicated log, so
building it once unblocks two gaps.

---

## Gap 1 — Security (IN PROGRESS)

**Spec:** `docs/superpowers/specs/2026-07-01-security-tls-rbac-design.md` (approved).
Covers kTLS on client + member transports (userspace rustls handshake, kernel
per-record crypto to preserve the zero-alloc hot path), a three-state TLS mode
(`disabled`/`permissive`/`required`) for zero-downtime rollout, mTLS member
trust, hashed-credential auth behind an `IdentityProvider` seam, and
Hazelcast-parity resource+action RBAC. Implementation plan is the next step.

---

## Gap 3 — Persistence / durable log (the enabler)

**Why first among the "hard" gaps:** it's the shared substrate for both CP (2)
and WAN (4), and it forces the project's biggest open architectural question.

**The core tension:** the guardrails say *no disk in the hot path*, but Raft and
Hot-Restart both require an fsync'd log. Resolution direction (to be designed):
keep the in-memory hot path untouched and treat persistence as an **out-of-band,
per-core append-only log** written by a dedicated path (io_uring supports async
file writes; fsync batching amortizes cost), never blocking the request loop.
Hazelcast's analog is **Hot Restart Store** (WAL + snapshots per partition).

**First deliverables:**
- Per-partition append-only WAL (record = op + key + value + stamp), io_uring
  file writes, group-commit fsync.
- Periodic snapshot + log truncation.
- Recovery on restart (replay snapshot + tail).
- Config: durability level (none / async / sync-on-commit).

**Design questions to resolve:** does a write ack wait for fsync (durable) or
just replication (fast)? Per-partition vs per-core log files? Snapshot format
(reuse `all_entries_stamped`).

---

## Gap 2 — CP Subsystem (Raft)

**Scope:** linearizable primitives Hazelcast exposes via CP — `IAtomicLong`,
`IAtomicReference`, `FencedLock`, `ISemaphore`, `ICountDownLatch`, `CPMap`, plus
CP sessions. Note: Enterprise-only in Hazelcast ≥ 5.5 (per `docs/PARITY.md`), so
this is *beyond* strict OSS parity — include it for functional completeness /
the platform-diagram "Consistency" box, not OSS-parity obligation.

**Why it's XL:** today's consistency is sync-backup + quorum (great for IMap, not
linearizable across arbitrary ops). A true CP subsystem needs **Raft** — leader
election, log replication, snapshots, membership changes — over a CP-member
group, with its own persistent log (reuses Gap 3).

**First deliverables:**
- Raft core (RequestVote / AppendEntries / commit index / snapshots) over the
  existing member io_uring transport.
- A CP group abstraction (odd-sized subset of members) + CP sessions.
- Build `AtomicLong` first (simplest state machine) end-to-end, then
  `FencedLock` (fencing token), then the rest.

**Guardrail note:** Raft's log is durable (Gap 3); the Raft RPC path rides the
existing member transport. Keep the Raft state machines off the IMap hot path
(separate CP-member role) so the AP data path is unaffected.

---

## Gap 4 — Geo-replication / WAN

**Scope:** asynchronous cross-cluster replication of map (and structure) updates
to a remote datacenter/region — Hazelcast's WAN Replication. Enterprise in
Hazelcast, but a real platform-diagram box ("Disaster Recovery and
Geo-Replication").

**Why after persistence:** WAN needs a **durable, replayable buffer** of outbound
mutations so a WAN-link outage doesn't lose updates — that's the same log built
in Gap 3 (or a dedicated WAN queue with the same durability machinery). It also
reuses the event-capture path already used for listeners.

**First deliverables:**
- WAN publisher: capture committed mutations → durable outbound queue → batch →
  ship to remote cluster over a WAN protocol (async, at-least-once).
- WAN consumer on the remote side: apply with the existing HLC/LatestUpdate merge
  (already skew-tolerant after this session's HLC work) for active-active.
- Config: target clusters, batching, backpressure, conflict policy.

**Guardrail note:** entirely off the hot path (async). The main design work is
durability + backpressure + conflict resolution (HLC merge already helps).

---

## Gap 5 — Streaming / SQL depth (independent)

**Current:** a real Jet-style DAG (`crates/jet`: processors, watermarks,
streaming joins, aggregation, `CREATE JOB`) and a SQL engine (`crates/query`:
`SELECT/WHERE/JOIN/GROUP BY/ORDER BY`, `INSERT`, `CREATE MAPPING/JOB`) — but
**single-node execution** and a limited operator/connector set.

**Depth to add (incremental, each shippable):**
- **Distributed/parallel query + job execution** across members (partition-aware
  scatter-gather; today it's single-node).
- **Windowing** beyond the current watermark primitive: tumbling / sliding /
  session windows with proper triggers.
- **More operators**: richer aggregations, stream-stream joins with state, sort,
  distinct-at-scale.
- **More connectors** (see minor list below): JDBC, file/CDC sources beyond
  Kafka.

**Why it can go anytime:** independent of the persistence spine; lives inside the
existing `jet`/`query` crates; low guardrail tension.

---

## Minor / residual parity items (smaller, opportunistic)

- **Connectors:** currently Kafka (rskafka) + MapStore/MapLoader. Add JDBC, file,
  CDC, socket sources/sinks (partly overlaps Gap 5).
- **Management Center depth:** metrics + Prometheus + MC protocol codecs exist;
  close remaining `MC*` operations for full GUI parity.
- **Auth backends:** LDAP/JAAS behind the `IdentityProvider` seam introduced in
  Gap 1.
- **Data-structure tail:** CardinalityEstimator, ReplicatedMap depth, JCache/ICache
  edge ops, ReliableTopic guarantees — audit against the 477 client codecs.
- **Client-cert-as-principal** mapping (extends Gap 1's `Principal`).

## Already closed this session (context)

- **Split-brain default quorum** → strict majority (`membership::default_quorum`).
- **Merge lost-writes** → HLC time-ordered stamps with peer-stamp absorption
  (`store::next_stamp` / `observe_stamp`).
- **Distributed-correctness verification** → deterministic simulation harness
  (`crates/server/src/sim.rs`) covering durability-across-failover, split-brain,
  merge convergence, migration convergence.

See the memory `bonsaigrid-dst-harness` and `docs/ROADMAP.md` / `docs/PARITY.md`
for the broader (wire/API) parity picture.
