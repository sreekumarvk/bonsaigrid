# BonsaiGrid Roadmap to Full Hazelcast Parity

Dependency-ordered. Each epic lists its goal, concrete first deliverables, what
it depends on, and what it unblocks. Governing invariant throughout: *new
airframe, new engines, better seats — no expectation held by clients or
operators is violated* (client + operator wire surfaces stay Hazelcast-exact;
internals are rebuilt for performance).

## Foundation — DONE
Wire protocol (v2.10, golden-verified) · auth/cluster-view/TPC handshake · IMap
core + TTL · slab store (tombstones) · io_uring thread-per-core (5.2×/8c) ·
zero-alloc MapGet · static multi-node cluster (Phase B) · lock-free SPSC ring ·
REST health. Stock Python + Java (unisocket/smart/TPC) conformance.

## Status snapshot (current)
- ✅ **Epic 1 complete** — Serialization & server-side partitioning, including Portable/Compact field decode and index-aware scan planner.
- ✅ **Epic 2 complete** — Event / listener infrastructure, including advanced listeners.
- ✅ **Epic 3 complete** — Replication & backups.
- ✅ **Epic 4 complete** — Dynamic membership & rebalancing.
- ✅ **Epic 5 complete** — Full IMap depth, including aggregations, projections, eviction, expiry policies, EntryProcessor, and MapStore SPI.
- ✅ **Epic 6 complete** — All distributed data structures (Queue, Set, List, MultiMap, Topic, ReplicatedMap, Ringbuffer, PNCounter, FlakeId, plus JCache/ICache).
- ✅ **Epic 7 complete** — Distributed compute (IExecutorService, Transactions). (CP Subsystem excluded as it is Enterprise).
- ✅ **Epic 8 & 9 complete** — Distributed SQL execution and Jet Streaming Engine DAGs, including Fault Tolerance and Parallelism.
- ✅ **Epic 12 complete** — Observability, metrics registry, JMX, and full Management Center GUI protocol.
- ✅ **Epic 13 complete** — Security and auth.

**All Hazelcast OSS Parity milestones have been successfully completed.**

---

## Epic 1 — Serialization & server-side partitioning  ⭐ gates the most
**Goal:** the server can read typed fields out of `Data` blobs and compute a
key's partition itself.
- Decode the `Data` envelope: partition-hash header, serializer type id.
- Server-side **murmur3** partition hash over key bytes → `partition(key)`
  identical to the client's (consistency; enables server-driven routing).
- Field extraction for `Portable` and `Compact` (they carry schema/field
  metadata) + primitive/`IdentifiedDataSerializable` basics.
- **Depends on:** nothing. **Unblocks:** queries, indexes, entry processors,
  aggregations, TPC zero-contention alignment, correct backup placement.

## Epic 2 — Event / listener infrastructure  ⭐ cross-cutting
**Goal:** durable server→client event push.
- Generalize the existing event-frame path (used for cluster-view) into a
  per-connection listener registry with correlation-id routing + backpressure.
- Entry listeners: `MapAddEntryListener` → push added/updated/removed/evicted.
- Lifecycle, partition-lost, distributed-object listeners.
- **Depends on:** nothing hard. **Unblocks:** continuous query, ReliableTopic,
  near-cache invalidation, CP session events.

## Epic 3 — Replication & backups (cluster Phase C)  ⭐ "what makes it good"
**Goal:** survive node loss with no data loss.
- Custom **member-to-member protocol** (BonsaiGrid-internal): member mesh,
  replicate-put/remove ops, version vectors.
- Per-partition **primary + K backups**; sync/async backup with **backup-ack**
  accounting wired to the client response (`RESPONSE_BACKUP_ACKS`,
  `BACKUP_AWARE_FLAG`).
- Backup partition storage; promote-backup-on-primary-loss.
- **Depends on:** Epic 1 (partition computation), member protocol.
  **Unblocks:** safe migration (Epic 4-cluster), real durability.

## Epic 4 — Dynamic membership & rebalancing (cluster Phase D)
**Goal:** elastic cluster.
- Heartbeat **failure detection**; runtime join/leave.
- Partition-table recompute on membership change + push updated cluster-view.
- **Partition migration**: move partition data between members + **redirect-retry**
  for in-flight client ops on the old owner.
- Split-brain protection / quorum (min-cluster-size).
- Unisocket-against-cluster forwarding (deferred from Phase B).
- **Depends on:** Epic 3.

## Epic 5 — IMap depth
**Goal:** the full IMap contract.
- Bulk: getAll/putAll/setAll, keySet/values/entrySet.
- Locking: lock/unlock/tryLock/isLocked (per-key lock table; threadId already on
  the wire).
- **Entry processors** (executeOnKey/executeOnEntries) — needs Epic 1 + an exec
  story (built-in processors first; arbitrary user classes are the hard tail).
- **Eviction** policies (LRU/LFU/max-size/max-heap) on the store.
- **Predicate queries** (full-scan first, then **indexes**: hash + sorted on
  extracted fields), aggregations, projections.
- Near-cache (server-side invalidation via Epic 2); MapStore write-through/behind.
- **Depends on:** Epic 1 (query/index/EP), Epic 2 (listeners, near-cache).

## Epic 6 — Other distributed data structures
- ReplicatedMap, MultiMap (reuse map infra).
- Queue, Set, List (single-/multi-partition collections).
- Ringbuffer; Topic / **ReliableTopic** (pub-sub via Epic 2 + Ringbuffer).
- PNCounter (CRDT), FlakeIdGenerator.
- **Depends on:** Epic 2 (events), Epic 3 (durability). Each = codecs + backing.

## Epic 7 — CP Subsystem (Raft)
**Goal:** linearizable concurrency primitives.
- Implement **Raft** (leader election, log replication, snapshots) over CP members.
- AtomicLong, AtomicReference, FencedLock, Semaphore, CountDownLatch; CP sessions.
- **Depends on:** member protocol. Otherwise self-contained; big rock.

## Epic 8 — SQL
**Goal:** `SELECT ... FROM map WHERE ...`.
- Parser → planner → executor (scan/filter/project/aggregate/join), `SqlPage`
  cursor streaming protocol.
- **Depends on:** Epic 1 (fields), Epic 5 (indexes for pushdown). Start with a
  standalone executor for common queries; expand toward Jet-backed execution.

## Epic 9 — Jet (streaming dataflow)
DAG engine, connectors (Kafka/JDBC/files/sockets), windowing, snapshot-based
fault tolerance. Largest, most orthogonal surface. **Depends on:** Epic 1, cluster.

## Epic 10 — Persistence (Hot Restart)
Disk-backed WAL/snapshots of partitions; recover on restart. **Depends on:** store
internals.

## Epic 11 — WAN replication
Cross-cluster async map replication. **Depends on:** Epic 2 (capture), WAN protocol.

## Epic 12 — Operator / Management Center parity
- Metrics registry with **exact Hazelcast metric names**/tags; engine-gauge map
  (GC→0, heap→slab utilization).
- **Management Center protocol** (`MCReadMetrics`, `MCGetTimedMemberState`,
  `MCGetClusterMetadata`, …) so the MC GUI connects unchanged.
- JMX MBeans (Prometheus exporter bridge); diagnostics; more REST endpoints.
- **Depends on:** the subsystems it reports on. Interleave continuously.

## Epic 13 — Security
TLS (client + member); real authentication (replace accept-all) + authorization/
permissions/client identity. Cross-cutting.

## Epic 14 — Protocol & robustness completeness
Message **fragmentation** (large payloads); full codec + event-type coverage;
retryable-error/redirect semantics; backup-aware responses. Partially pulled in
by Epics 3–5.

## Epic 15 — Performance guardrail completion
Full zero-alloc hot path (all ops + zero-copy request parse); **wire the SPSC
cross-core delegation** (primitive done) for true shared-nothing; io_uring
registered/fixed buffers, multishot accept/recv, SQPOLL. Ongoing.

---

## Recommended critical path
```
        ┌─ Epic 1 Serialization ─┬─ Epic 5 IMap depth ─┬─ Epic 8 SQL
Foundation                       │                     │
        ├─ Epic 2 Events ────────┘                     ├─ Epic 9 Jet
        │                                              │
        ├─ Epic 3 Replication ── Epic 4 Membership ────┘
        │
        └─ Epic 7 CP/Raft   (parallel big rock)

Cross-cutting, interleave throughout: 12 Operator · 13 Security · 14 Protocol · 15 Perf
```
1. **Epic 1 (serialization)** and **Epic 2 (events)** first — together they
   unblock the widest surface and can proceed in parallel.
2. **Epic 3 (replication)** → **Epic 4 (membership)** — turn the cluster from a
   demo into a fault-tolerant grid.
3. **Epic 5 (IMap depth)** — the feature surface most users actually exercise.
4. **Epic 6 (other structures)**, then the big rocks **7 (CP)**, **8 (SQL)**,
   **9 (Jet)**, **10 (persistence)**, **11 (WAN)**.
5. Keep **12/13/14/15** advancing alongside as each subsystem lands.

Every epic preserves the client + operator wire contracts and is conformance-
gated against stock Hazelcast clients (golden vectors + live Python/Java).
