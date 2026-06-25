# BonsaiGrid → Hazelcast Open Source Parity — Scoping Document

**Date:** 2026-06-25
**Purpose:** A complete, honest breakdown of where BonsaiGrid stands against
Hazelcast's **open-source (Community Edition)** feature set, and everything
required to reach full parity. Enterprise-only features are listed separately as
out-of-scope.

---

## 1. Method & scope

- **Target:** Hazelcast Community Edition (OSS), the unmodified-client invariant —
  stock Hazelcast clients must keep working; internals are reimplemented in Rust.
- **Status legend:** ✅ done · 🟡 partial / MVP · ❌ not started.
- **Effort legend (from-scratch Rust):** **S** ≈ days · **M** ≈ 1–2 wks ·
  **L** ≈ 3–6 wks · **XL** ≈ 2 mo+.
- Percentages are coverage of the *client-visible surface* for that area, not LOC.

---

## 2. Parity scorecard

| Area | Status | ~Coverage | Biggest remaining gap |
|---|---|---|---|
| Wire protocol / client handshake | ✅ | 95% | event/listener types beyond entry/topic/near-cache |
| Core runtime (thread-per-core, io_uring, slab) | ✅ | 100%* | *exceeds Hazelcast by design |
| IMap (CRUD/TTL) | ✅ | 85% | EntryProcessor, MapStore, eviction, indexes, locking edge ops |
| IMap querying (predicates) | 🟡 | 40% | most predicate types, indexes, aggregations, projections, paging |
| Other data structures | 🟡 | 70% | ICache/JCache, CardinalityEstimator, ReliableTopic, transactional variants |
| Serialization | 🟡 | 45% | Portable, IdentifiedDataSerializable, custom/global serializers |
| SQL engine | 🟡 | 20% | aggregations, GROUP BY/ORDER BY, UPDATE/DELETE, indexes, distributed exec |
| Streaming (Jet) | 🟡 | 10% | the whole pipeline/DAG engine, windowing, fault tolerance, connectors |
| CP subsystem (Raft) | ❌ | 0% | AtomicLong/Ref, CountDownLatch, Semaphore, FencedLock |
| Distributed compute (executors, EP) | ❌ | 5% | IExecutorService family, EntryProcessor |
| Transactions | ❌ | 0% | Transactional* structures, XA |
| MapStore / persistence integration | ❌ | 0% | MapStore/MapLoader, write-behind/through |
| Cluster mgmt (membership, migration, quorum) | ✅ | 80% | discovery plugins, lite members, cluster states |
| Listeners / events | 🟡 | 35% | item/message/lifecycle/migration/partition-lost/distributed-object listeners |
| Near cache | 🟡 | 60% | eviction, TTL, local-update policy, invalidation batching |
| Observability (metrics/JMX/MC) | 🟡 | 30% | full metrics surface, JMX, Management Center protocol |
| Eviction / expiration policies | ❌ | 5% | LRU/LFU/max-size, max-idle |
| Alternate protocols (REST/Memcache) | 🟡 | 15% | REST data API, Memcache |

---

## 3. What is DONE ✅

### 3.1 Runtime & wire
- **Hazelcast Open Client Protocol** (frames: 6-byte prefix, fragments, flags),
  ~89 message-type handlers, big-/little-endian codecs, auth (cluster name +
  user/pass), TPC channels, cluster-view subscription + **live** member/partition
  view events.
- **Thread-per-core, shared-nothing, io_uring reactor**; deterministic **slab
  allocator**; zero-allocation MapGet hot path; SPSC cross-thread rings;
  fragment reassembly. (This is BonsaiGrid's reason for existing and already
  *exceeds* the JVM architecture.)
- **REST health endpoints** + **Prometheus `/metrics`**.

### 3.2 Data structures (client-visible)
- **IMap:** put/get/remove/delete/containsKey/containsValue/size/isEmpty/clear/
  putAll/getAll/keySet/values/entrySet/replace/putIfAbsent, **TTL**, entry
  listeners, near-cache invalidation, partition routing (murmur3, matches client).
- **ReplicatedMap, MultiMap, IList, ISet, IQueue, Ringbuffer, PNCounter,
  FlakeIdGenerator, ITopic** (pub/sub), per-key **locks** (try/lock/unlock/
  isLocked/forceUnlock incl. blocking grant under contention).
- **Predicate query:** Equal, GreaterLess, And, Or over **Compact** values via a
  field extractor; `MapValues/KeySet/EntriesWithPredicate` full-scan.

### 3.3 Serialization
- **Compact** (schema service, RABIN fingerprint = client schemaId; record
  reader; FieldExtractor seam), **HazelcastJsonValue / json-flat**, string/integer
  `Data` partition hashing matching the client.

### 3.4 Multi-node / HA (full A→D arc + completeness)
- Static cluster + smart-client routing; **synchronous backup replication**;
  **automatic failover** + heartbeat detection + **master election**; **dynamic
  join + partition migration**; **quorum** write-gate + **per-entry merge**
  (LatestUpdate/PutIfAbsent); **restore-K** (re-replicate to a fresh backup after
  a death — survives double-failover); HA for IMap **and** the name-partitioned
  structures **and** key-partitioned MultiMap.

### 3.5 SQL + streaming (MVP)
- `SELECT cols/* FROM <map> [WHERE pred]` (Compact + json-flat), equi-**JOIN**,
  `CREATE MAPPING` (IMap/Kafka), `INSERT`, `CREATE JOB` streaming pipeline, a
  **Kafka/Redpanda connector** (rskafka) — the Redpanda pizza-recommender demo
  runs end to end (stream⋈table enrichment).

---

## 4. Gap analysis — what's left, by area

Each gap: **Hazelcast capability → BonsaiGrid status → work needed → effort.**

### 4.1 IMap completeness — 🟡
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| EntryProcessor (`executeOnKey/Keys/Entries`) | ❌ | server-side execution of a serialized processor against entries; needs a way to run user logic — for OSS parity, support the built-in processors + IDS-encoded ones; or a scripting seam | **L** |
| MapStore / MapLoader (write-through/behind, read-through) | ❌ | a `MapStore` SPI bridge to an external store; load-on-miss, store-on-put, write-behind queue, `loadAll`/`storeAll` | **L** |
| Eviction (LRU/LFU/RANDOM, max-size policies) | ❌ | per-map size accounting + eviction policy on the slab table | **M** |
| Expiration: max-idle, per-entry TTL semantics, expiry listeners | 🟡 | max-idle tracking; `EXPIRED` events; TTL already present | **S–M** |
| Entry-level locking ops (`tryPut/tget with lock`, `lock(key)` map ops) | 🟡 | a handful more locking message types reuse the existing lock store | **S** |
| `setTtl`, `evict`, `evictAll`, `flush`, `loadAll`, `getEntryView`, `localKeySet`, `removeAll(predicate)` | 🟡 | mechanical handler arms over the existing store | **M** |
| Interceptors | ❌ | per-map interceptor chain (rarely used) | **S** |

### 4.2 Querying & indexes — 🟡 (40%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Full predicate set (NotEqual, Between, In, Like, ILike, Regex, Not, InstanceOf, True/False, Paging, PartitionPredicate, SQL-string predicate) | 🟡 | decode + eval the remaining IDS predicate classes; the AST + evaluator seam already exists | **M** |
| **Indexes** (sorted / hash / bitmap, composite) | ❌ | an index store (B-tree/hash) per attribute, maintained on put/remove; the query planner consults it instead of full-scan (the `scan` candidate-set seam is ready) | **L** |
| Aggregations (count/sum/avg/min/max/distinct) + Projections | ❌ | aggregator codecs + a scan-and-fold executor | **M** |
| Continuous query / QueryCache (with listeners) | ❌ | a per-query materialized cache fed by entry events | **L** |
| Custom attributes / `ValueExtractor` | ❌ | server-side extractor registration (or attribute-path support over Compact/Portable) | **M** |

### 4.3 Serialization formats — 🟡 (45%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| **Portable** | ❌ | Portable reader/writer + field definitions + class-def service; needed by older clients & many query workloads | **L** |
| **IdentifiedDataSerializable** (general) | 🟡 | we decode IDS *predicates*; general IDS objects (factories/classes) need a registry to read fields for queries | **M** |
| Custom / global serializers, ByteArray, primitive Data types | 🟡 | mostly pass-through (values are opaque blobs); query/SQL need typed reads → covered by Compact/Portable/JSON | **S–M** |

### 4.4 SQL engine — 🟡 (20%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Aggregations, GROUP BY, ORDER BY, LIMIT/OFFSET, DISTINCT | ❌ | executor operators (hash-agg, sort, limit) | **L** |
| DML: UPDATE, DELETE, SINK INTO an IMap | 🟡 | INSERT done; add UPDATE/DELETE over the store + SINK | **M** |
| DDL: DROP MAPPING/JOB/INDEX, CREATE INDEX/VIEW, SHOW | 🟡 | catalog mutators + job lifecycle (DROP JOB to stop a thread) | **M** |
| Typed columns / full type system (not VARCHAR-only) | 🟡 | carry column types through the page encoder (CN-codecs) | **M** |
| Subqueries, multi-way joins, non-equi joins | ❌ | a real logical/physical planner | **XL** |
| **Distributed** SQL execution (scatter-gather across members) | ❌ | fragment the scan/agg/join across partition owners and merge | **L** |
| File / JDBC mappings | ❌ | additional connectors | **M** each |

### 4.5 Streaming (Jet) engine — 🟡 (10%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Pipeline/DAG API + `Job` submission (Java client `JetService`) | ❌ | the Jet job-submission protocol + a DAG executor (our `CREATE JOB` is SQL-only and single-stage) | **XL** |
| Windowing (tumbling/sliding/session), watermarks | ❌ | event-time machinery + windowed aggregation | **L** |
| Stateful transforms, fault tolerance (snapshots/exactly-once) | ❌ | state stores + distributed snapshotting | **XL** |
| Distributed job execution across members (parallelism) | ❌ | partition the source, shuffle edges, coordinate | **XL** |
| Connectors (Kafka ✅, files, JDBC, S3, sockets, message queues) | 🟡 | Kafka done; the rest are individual source/sink impls | **M** each |
| Job management (list/cancel/restart/resume, metrics) | ❌ | a job registry + control ops (we have spawn-only) | **M** |

### 4.6 CP Subsystem (Raft) — ❌ (0%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Raft consensus (log, leader election, snapshots, membership) | ❌ | a real Raft implementation as a separate consistency tier | **XL** |
| IAtomicLong, IAtomicReference | ❌ | linearizable register ops on Raft groups | **M** (on Raft) |
| ICountDownLatch, ISemaphore, FencedLock + CP sessions | ❌ | blocking primitives + session/heartbeat lifecycle | **L** (on Raft) |
| CP group management (default group, custom groups) | ❌ | group lifecycle, leadership APIs | **M** |

> Note: BonsaiGrid's AP membership (Phase D) is **not** Raft. The CP subsystem is
> an independent strongly-consistent tier and is the single largest greenfield area.

### 4.7 Distributed compute — ❌ (5%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| IExecutorService (submit/execute on key/member/all) | ❌ | task routing + a server-side execution model (the hard part: running user code — for OSS parity over a stock client this needs user-code deployment or a constrained task type) | **L** |
| DurableExecutorService, IScheduledExecutorService | ❌ | durable task ringbuffer + scheduler | **L** |
| EntryProcessor (see 4.1) | ❌ | — | **L** |

### 4.8 Transactions — ❌ (0%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| TransactionalMap/Queue/List/Set/MultiMap | ❌ | a transaction context: per-txn write-set, 1PC/2PC commit across partition owners, locking | **L** |
| XA transactions | ❌ | XAResource protocol on top of the above | **M** |

### 4.9 Persistence integration — ❌
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| MapStore/MapLoader, QueueStore, RingbufferStore | ❌ | the external-store SPI bridge (see 4.1) | **L** |
| (Disk persistence / Hot Restart = **Enterprise**, see §6) | — | — | — |

### 4.10 Cluster management — ✅ (80%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Backups (sync ✅ / **async**), partition table, migration, restore-K | ✅/🟡 | add async-backup-count semantics | **S** |
| Split-brain protection + merge policies | ✅ | add more merge policies (HigherHits, PassThrough, custom) | **S** |
| Discovery: TCP/IP ✅, **multicast**, cloud plugins (k8s/AWS/GCP/Azure/Eureka/Consul) | 🟡 | multicast join; pluggable discovery SPI | **M** |
| Lite members, member attributes, cluster states (ACTIVE/FROZEN/PASSIVE/NO_MIGRATION) | ❌ | membership metadata + state machine gating ops | **M** |
| Graceful shutdown / partition-safe leave | 🟡 | drain + migrate-out before exit | **M** |
| Partition groups / zone awareness | ❌ | group-aware backup placement | **M** |

### 4.11 Listeners & events — 🟡 (35%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Entry/Map listeners (added/updated/removed/evicted/expired, with predicate) | 🟡 | EVICTED/EXPIRED + predicate-filtered listeners | **S–M** |
| Item listeners (queue/list/set), Message listeners (topic) | 🟡 | item-added/removed events; topic message events ✅ | **S** |
| Membership, Lifecycle, Migration, PartitionLost, DistributedObject, Client listeners | ❌ | event encoders + registration for each family | **M** |

### 4.12 Other data structures — 🟡
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| **ICache (JCache / JSR-107)** | ❌ | the JCache protocol + semantics (largely an IMap variant with JCache events/expiry) | **L** |
| **ReliableTopic** (ringbuffer-backed) | 🟡 | reliable delivery semantics over the existing ringbuffer | **S–M** |
| **CardinalityEstimator** (HyperLogLog) | ❌ | HLL add/estimate | **S** |
| Transactional variants of structures | ❌ | see §4.8 | — |

### 4.13 Observability — 🟡 (30%)
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| Full member metrics surface (per-structure, per-partition, op latencies) | 🟡 | expand the metrics registry to Hazelcast's metric set | **M** |
| JMX MBeans | ❌ | a JMX exposer (or document Prometheus as the substitute) | **M** |
| **Management Center protocol** (so MC connects) | ❌ | the MC client/member protocol + the data feeds it expects | **L** |
| Diagnostics, slow-operation detector, health monitor | 🟡 | structured diagnostics logs + thresholds | **S–M** |

### 4.14 Alternate protocols — 🟡
| Capability | Status | Work needed | Effort |
|---|---|---|---|
| REST data API (maps/queues over HTTP) | 🟡 | health/metrics done; add the data endpoints | **M** |
| Memcache protocol | ❌ | a Memcache text/binary front-end mapped to IMap | **M** |

---

## 5. Recommended roadmap (sequencing)

Ordered for **highest parity-per-effort** and respecting dependencies. Each is a
self-contained epic (spec → plan → implement → test → commit), the way Phases C/D
and the demo were built.

1. **Query depth & indexes** (4.2) — full predicate set, indexes, aggregations,
   projections, paging. Unlocks real query parity and speeds SQL. *Deps: none.* **L**
2. **Serialization: Portable + general IDS** (4.3) — broadens query/SQL/clients.
   *Deps: none.* **L**
3. **SQL engine depth** (4.4) — aggregations, GROUP BY/ORDER BY/LIMIT, UPDATE/
   DELETE/SINK, DROP, typed columns; then **distributed execution**. *Deps: 1.* **L→XL**
4. **EntryProcessor + MapStore + eviction/expiration** (4.1, 4.9) — completes IMap,
   the most-used structure. *Deps: none.* **L**
5. **Listeners & events completeness** (4.11) + **observability/Management Center**
   (4.13). *Deps: none.* **M–L**
6. **CP Subsystem (Raft)** (4.6) — a Raft tier, then AtomicLong/Ref, CountDownLatch,
   Semaphore, FencedLock. Large and self-contained. *Deps: none.* **XL**
7. **Distributed compute + Transactions** (4.7, 4.8) — executors, EntryProcessor
   distribution, Transactional* + XA. *Deps: 4 (EP), CP for some locks.* **L–XL**
8. **Streaming (Jet) engine** (4.5) — the DAG/pipeline engine, windowing, fault
   tolerance, more connectors, job management. The largest area; our SQL `CREATE
   JOB` is a thin slice of it. *Deps: 3.* **XL**
9. **Remaining structures & protocols** — ICache/JCache, ReliableTopic,
   CardinalityEstimator, REST data API, Memcache, multicast/cloud discovery, lite
   members, cluster states. *Deps: none.* **M each**

A pragmatic "**80% of real-world usage**" milestone is roughly items **1–5**
(query/SQL/serialization/IMap-completeness/listeners): it makes the data-grid and
SQL surfaces broadly Hazelcast-equivalent without the two XL tiers (CP Raft, full
Jet). Items **6** and **8** are the heavyweight remainders.

---

## 6. Out of scope — Hazelcast **Enterprise** (not OSS)

Not required for open-source parity; listed so the boundary is explicit:
- **Persistence / Hot Restart Store** (disk-based recovery), **CP Persistence**.
- **Security suite:** TLS/SSL, mutual auth, RBAC/JAAS, auditing, socket
  interceptors (basic cluster-name + user/pass auth is OSS and done).
- **WAN Replication.**
- **Tiered Storage**, **High-Density (off-heap) Memory Store.**
- **Rolling upgrades** (member), **Blue/Green** client failover, **CPMap**,
  **User Code Namespaces**, advanced **Management Center**.

---

## 7. Summary

BonsaiGrid has a **complete, production-shaped distributed core** (runtime, wire
protocol, the main data structures, full multi-node HA with auto-failover /
dynamic membership / quorum / restore-K) plus **MVP slices** of the three large
"platform" tiers — querying, SQL, and streaming. The path to OSS parity is
dominated by **breadth in query/SQL/serialization/IMap** (items 1–5, achievable
incrementally) and two **greenfield heavyweight tiers**: the **CP/Raft**
consistency subsystem and the **full Jet** streaming engine. None of the
remaining work conflicts with the architectural guardrails — it extends the same
zero-allocation, thread-per-core, shared-nothing core behind the unmodified
Hazelcast client protocol.
