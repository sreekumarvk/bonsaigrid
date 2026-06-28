# BonsaiGrid → Hazelcast Open Source Parity — Scoping Document (validated)

**Date:** 2026-06-25
**Status:** Validated against the Hazelcast 5.x source tree (`./hazelcast`, 477
client protocol codecs) + the official OSS/Enterprise edition matrix.
**Companion docs:** [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) ·
[`TEST_STRATEGY.md`](TEST_STRATEGY.md)

> This supersedes the first-pass draft. The most important correction from
> validation: **the CP Subsystem is Enterprise-only from Hazelcast 5.5** (it was
> OSS ≤ 5.4). It is therefore **out of required OSS-parity scope** — removing the
> single largest greenfield tier (Raft). Conversely, **Jet streaming, SQL,
> transactions, and ContinuousQueryCache are confirmed OSS** and *in scope*.

---

## 1. Method, scope, and the OSS/Enterprise boundary

- **Target:** Hazelcast **Community Edition (open source)**, latest 5.x. The
  unmodified-client invariant holds: stock Hazelcast clients must keep working;
  internals are reimplemented in Rust on the zero-alloc / thread-per-core /
  shared-nothing / io_uring core.
- **Validation basis:** the 477 client codecs under
  `hazelcast/hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec/`
  (the authoritative list of client-reachable operations), the `com.hazelcast.*`
  feature packages, and the official editions/data-structures matrix
  (cross-checked: docs.hazelcast.com + 5.5 community release notes).
- **Legend:** ✅ done · 🟡 partial/MVP · ❌ not started. Effort: **S** ≈ days,
  **M** ≈ 1–2 wks, **L** ≈ 3–6 wks, **XL** ≈ 2 mo+ (from-scratch Rust).

### 1.1 The validated edition boundary (what OSS parity must / must not include)

**IN scope (confirmed OSS):**
- All AP data structures: IMap, ReplicatedMap, MultiMap, IQueue, IList, ISet,
  ITopic, ReliableTopic, Ringbuffer, **ICache/JCache (JSR-107)**, PNCounter,
  FlakeIdGenerator, **CardinalityEstimator**.
- Distributed compute: **IExecutorService, DurableExecutorService,
  IScheduledExecutorService, EntryProcessor**.
- **Jet streaming/batch engine** (merged into core in Platform 5.0).
- **SQL engine** (incl. streaming SQL, mappings, joins, aggregations, DML).
- All **serialization** formats (Compact, Portable, IDS, DataSerializable, JSON,
  custom).
- **Query**: predicates, indexes (sorted/hash/bitmap), aggregations, projections,
  paging predicate, attribute extractors, **ContinuousQueryCache**.
- **MapStore/MapLoader** (external store integration).
- **Transactions** (Transactional* + XA).
- Cluster: discovery (multicast, TCP/IP, AWS/GCP/Azure/k8s/Eureka/Consul plugins),
  backups (sync/async), partition groups, **split-brain protection + merge
  policies**, lite members, cluster states, graceful shutdown.
- Member-side **metrics, JMX, diagnostics, health, REST data API, Memcache**.
- **cluster-name authentication.**

**OUT of scope (Enterprise — do NOT build for OSS parity):**
- **CP Subsystem** and its data structures: IAtomicLong, IAtomicReference,
  ICountDownLatch, ISemaphore, FencedLock, **CPMap** (Enterprise from 5.5; CPMap
  always Enterprise). *Clients may still call CP codecs; an OSS member legitimately
  rejects them, so BonsaiGrid can stub them with a clear "Enterprise-only" error.*
- **Persistence / Hot Restart Store** (disk recovery), **CP Persistence**.
- **Security suite:** TLS/SSL, mutual TLS, JAAS/RBAC, auditing, socket/security
  interceptors. (cluster-name + user/pass auth is OSS and **done**.)
- **WAN Replication**, **Tiered Storage**, **High-Density off-heap store**,
  **User Code Namespaces**, **Blue/Green client failover**, **rolling member
  upgrades**, **Vector Collection/Search (Beta)**, the **Enterprise TPC engine**
  (note: BonsaiGrid is itself an open thread-per-core alternative), **clustered
  JMX/REST and >3-member Management Center**.
- Jet **lossless recovery / job placement / rolling job upgrade**; **SQL
  permissions** (security).

---

## 2. Parity scorecard (validated)

| Area | Hazelcast codecs* | BonsaiGrid | Status | ~Coverage |
|---|---|---|---|---|
| Client handshake / cluster / schema | 23 + MC | core ops | ✅ | 90% |
| **IMap** | **73** | ~24 ops | 🟡 | 55% |
| MultiMap | 23 | 5 ops | 🟡 | 45% |
| List / Set / Queue | 23/13/20 | 8/6/8 | 🟡 | 60% |
| ReplicatedMap | 21 | 11 ops | 🟡 | 60% |
| Ringbuffer | 9 | 6 | 🟡 | 65% |
| Topic / ReliableTopic | 4 / (ringbuffer) | 2 | 🟡 | 50% |
| **ICache (JCache)** | **33** | 0 | ❌ | 0% |
| PNCounter / FlakeId / CardinalityEstimator | 3/1/2 | 3/1/0 | 🟡 | 70% |
| Query predicates | 22 classes | 4 | 🟡 | 30% |
| Query indexes / aggregations / projections | (Map ops) | 0 | ❌ | 0% |
| ContinuousQueryCache | 6 | 0 | ❌ | 0% |
| **SQL** | 6 | MVP | 🟡 | 20% |
| **Jet streaming** | 20 | thin slice | 🟡 | 10% |
| Executors / DurableExec / ScheduledExec | 6/6/18 | 0 | ❌ | 0% |
| EntryProcessor | (Map ops) | 0 | ❌ | 0% |
| Transactions / XA | 38 / 7 | 0 | ❌ | 0% |
| MapStore / MapLoader | (config) | 0 | ❌ | 0% |
| Serialization formats | 5 | Compact+JSON | 🟡 | 45% |
| Multi-node / HA | (internal) | full A→D | ✅ | 80% |
| Listeners / events | many | 4 types | 🟡 | 35% |
| Observability / Mgmt Center | 40 (MC) | metrics+health | 🟡 | 25% |
| Eviction / expiration | (config) | TTL only | 🟡 | 25% |
| **CP subsystem** | 35 | 0 | — | **N/A (Enterprise)** |

\* Codec counts from the source tree; "ops" = distinct BonsaiGrid handler arms.

---

## 3. What is DONE ✅ (grounded in code)

- **Runtime/wire:** Open Client Protocol, ~89 handler arms, fragment reassembly,
  auth (cluster-name + user/pass), TPC channels, cluster-view live push.
  **Thread-per-core io_uring reactor with `core_affinity` pinning** (client *and*
  member planes are io_uring), **slab allocator**, zero-alloc MapGet, SPSC rings.
- **Structures:** IMap (CRUD/TTL/locking/bulk/keyset/values/entryset), MultiMap,
  IList, ISet, IQueue, ReplicatedMap (namespaced IMap), Ringbuffer, PNCounter,
  FlakeIdGenerator, ITopic, per-key blocking locks.
- **Serialization:** Compact (schema service = RABIN schemaId; field reader),
  HazelcastJsonValue/json-flat, Data partition hashing (murmur3, client-matching).
- **Query:** predicates Equal/GreaterLess/And/Or over Compact + JSON; full-scan
  `MapValues/KeySet/EntriesWithPredicate`.
- **SQL/streaming (MVP):** SELECT/WHERE/equi-JOIN, CREATE MAPPING (IMap/Kafka),
  INSERT, CREATE JOB; rskafka connector → the Redpanda demo runs end to end.
- **Multi-node HA:** static cluster + smart routing; synchronous replication;
  auto-failover + heartbeat detection + master election; dynamic join + partition
  migration; quorum gate + per-entry merge (LatestUpdate/PutIfAbsent); restore-K
  (survives double-failover); HA across IMap, the name-partitioned structures, and
  key-partitioned MultiMap.
- **Observability:** Prometheus `/metrics`, Hazelcast REST health endpoints.

---

## 4. Validated gap analysis (per area)

### 4.1 IMap — 🟡 (Hazelcast 73 codecs; we have ~24)
Missing, validated against the codec list:
| Capability | Codec(s) | Status | Effort |
|---|---|---|---|
| **EntryProcessor** | `MapExecuteOnKey/Keys/AllKeys/WithPredicate`, `SubmitToKey` | ❌ | **L** |
| **Indexes** | `MapAddIndex` (SORTED/HASH/BITMAP) | ❌ | **L** |
| **Aggregations** | `MapAggregate`, `MapAggregateWithPredicate` | ❌ | **M** |
| **Projections** | `MapProject`, `MapProjectWithPredicate` | ❌ | **M** |
| **Event journal** | `MapEventJournalSubscribe/Read` | ❌ | **M** |
| **MapStore** | `MapLoadAll/LoadGivenKeys`, `LoadAllKeys` | ❌ | **L** |
| TTL/idle variants | `PutWithMaxIdle`, `SetTtl`, `PutTransient`, `SetWithMaxIdle` | 🟡 | **S** |
| Evict / flush | `MapEvict/EvictAll/Flush` | ❌ | **S** |
| Predicate listeners | `AddEntryListenerWithPredicate(ToKey)` | 🟡 | **M** |
| Misc | `GetEntryView`, `RemoveAll(predicate)`, `TryPut/TryRemove`, `ReplaceIfSame`, `RemoveIfSame`, `localKeySet` | 🟡 | **M** |
| Interceptors | `MapAddInterceptor` | ❌ | **S** |

### 4.2 Query & indexes — 🟡 (4 of ~13 usable predicates; 0 indexes/aggs)
Validated predicate classes (`PredicateDataSerializerHook`): SQL(0), And(1)✅,
Between(2), Equal(3)✅, GreaterLess(4)✅, Like(5), ILike(6), In(7), InstanceOf(8),
NotEqual(9), Not(10), Or(11)✅, Regex(12), False(13), True(14), Paging(15),
Partition(16). **Missing:** Between, Like, ILike, In, NotEqual, Not, Regex,
True/False, Paging, Partition, SQL-string. Index types: **SORTED, HASH, BITMAP**.
| Capability | Status | Effort |
|---|---|---|
| Remaining predicate classes (decode + eval; AST seam exists) | 🟡 | **M** |
| Index store (sorted B-tree / hash / bitmap) + query planner using `scan` candidate seam | ❌ | **L** |
| Aggregations + Projections | ❌ | **M** |
| ContinuousQueryCache (`ContinuousQuery*` 6 codecs) + listeners | ❌ | **L** |
| Custom `ValueExtractor` / attribute paths | ❌ | **M** |

### 4.3 Serialization — 🟡 (Compact+JSON; need Portable + general IDS)
| Format | Status | Effort |
|---|---|---|
| **Portable** (reader/writer + class-def service) | ❌ | **L** |
| General **IdentifiedDataSerializable** (factory/class registry for query reads) | 🟡 (predicates only) | **M** |
| DataSerializable / custom / global serializers (values are opaque blobs; typed reads come via Compact/Portable/JSON) | 🟡 | **S–M** |

### 4.4 SQL — 🟡 (6 codecs; MVP)
| Capability | Status | Effort |
|---|---|---|
| Aggregations, GROUP BY, ORDER BY, LIMIT/OFFSET, DISTINCT | ❌ | **L** |
| DML: UPDATE, DELETE, SINK INTO IMap | 🟡 | **M** |
| DDL: DROP MAPPING/JOB/INDEX, CREATE INDEX/VIEW, SHOW MAPPINGS | 🟡 | **M** |
| Typed result columns (not VARCHAR-only) | 🟡 | **M** |
| Subqueries, multi-/non-equi joins, planner | ❌ | **XL** |
| **Distributed** execution (scatter-gather across owners) | ❌ | **L** |
| File / JDBC mappings | ❌ | **M** each |

### 4.5 Streaming (Jet) — 🟡 (OSS; 20 codecs; thin slice)
| Capability | Status | Effort |
|---|---|---|
| Jet **job submission protocol** + DAG executor (`JetSubmitJob`, `JetGetJobStatus`, …) | ❌ | **XL** |
| Windowing (tumbling/sliding/session) + watermarks | ❌ | **L** |
| Stateful transforms + fault tolerance (snapshots) | ❌ | **XL** |
| Distributed/parallel job execution | ❌ | **XL** |
| Connectors: Kafka ✅; files, JDBC, sockets, JMS | 🟡 | **M** each |
| Job mgmt: list/cancel/restart/resume/metrics | 🟡 (spawn-only) | **M** |

### 4.6 Distributed compute — ❌
| Capability | Codecs | Effort |
|---|---|---|
| IExecutorService (submit on key/member/all) | 6 | **L** |
| DurableExecutorService | 6 | **L** |
| IScheduledExecutorService | 18 | **L** |
| EntryProcessor (see 4.1) | (Map) | **L** |

> The hard part for all of these: **running user code** sent by a Java client.
> OSS parity needs either user-code-deployment (class loading) or constraining to
> IDS/Compact-encoded built-in tasks/processors. Decision deferred to the plan.

### 4.7 Transactions — ❌ (38 + 7 XA codecs; OSS)
Transactional Map/MultiMap/Queue/List/Set + XA. Needs a transaction context
(per-txn write-set, locks, 1PC/2PC across owners). Effort **L** (+ **M** XA).

### 4.8 MapStore / persistence integration — ❌
MapStore/MapLoader/QueueStore/RingbufferStore SPI bridge (read-through, write-
through, write-behind queue, loadAll). Effort **L**. *(Disk Persistence/Hot
Restart = Enterprise, excluded.)*

### 4.9 Cluster management — ✅ (80%)
| Capability | Status | Effort |
|---|---|---|
| Async-backup-count semantics | 🟡 | **S** |
| More merge policies (HigherHits, PassThrough, custom) | 🟡 | **S** |
| Multicast discovery + pluggable discovery SPI (k8s/cloud) | ❌ | **M** |
| Lite members, member attributes, cluster states (ACTIVE/FROZEN/PASSIVE/NO_MIGRATION) | ❌ | **M** |
| Graceful, partition-safe shutdown (drain + migrate-out) | 🟡 | **M** |
| Partition groups / zone-aware backup placement | ❌ | **M** |

### 4.10 Listeners & events — 🟡 (4 types)
Have: entry, near-cache invalidation, topic message, cluster-view. Missing:
membership, lifecycle, migration, partition-lost, distributed-object, item
(queue/list/set), client, predicate-filtered entry, EVICTED/EXPIRED. Effort **M**.

### 4.11 Other structures — 🟡
| Structure | Status | Effort |
|---|---|---|
| **ICache / JCache** (33 codecs) | ❌ | **L** |
| **ReliableTopic** (ringbuffer-backed) | 🟡 | **S–M** |
| **CardinalityEstimator** (HyperLogLog) | ❌ | **S** |

### 4.12 Observability — 🟡 (25%)
Full member metric set, JMX MBeans, **Management Center protocol** (40 MC codecs),
diagnostics/slow-op detector. Effort **M–L**.

### 4.13 Eviction / expiration — 🟡
Max-size policies + LRU/LFU/RANDOM eviction; max-idle. TTL exists. Effort **M**.

### 4.14 Alternate protocols — 🟡
REST data API (maps/queues over HTTP), Memcache front-end. Effort **M** each.

---

## 5. Validation log — assumptions poked & corrected

| Assumption (1st draft) | Verdict after validation | Source |
|---|---|---|
| CP Subsystem is OSS → build Raft tier | **WRONG.** Enterprise from 5.5 (CPMap always). **Removed from scope.** | 5.5 community release notes; data-structures table |
| Jet streaming is OSS | **Confirmed.** Merged into core 5.0; `com.hazelcast.jet` is in the OSS module. | source tree + 5.0 release blog |
| ContinuousQueryCache is Enterprise | **WRONG.** Enterprise only in old IMDG 3.x; **OSS in 5.x**. In scope. | docs 5.0 query/continuous-query-cache |
| MapStore OSS / Persistence Enterprise | Confirmed (both). | docs persistence |
| Transactions OSS | Confirmed (absent from Enterprise list; 38+7 codecs in OSS). | source + docs |
| Security suite Enterprise | Confirmed (chapter-level statement). cluster-name auth OSS (done). | docs security/overview |
| (agent) "client reactor uses epoll not io_uring" | **WRONG** — both client and member reactors are io_uring; pinning via `core_affinity` is implemented. | `crates/server/src/reactor.rs`, `main.rs` |
| (agent) "no automatic split-brain merge" | **Imprecise** — per-entry merge (LatestUpdate/PutIfAbsent) runs on migration/heal; quorum prevents minority writes. | `migration.rs`, `member_thread.rs` |
| Predicate set size | **Validated:** 22 classes, ~13 user-facing; we have 4. | `PredicateDataSerializerHook.java` |
| Index types | **Validated:** SORTED, HASH, BITMAP. | `IndexType.java` |
| Aggregate/Project/EntryProcessor/AddIndex are real client ops | **Validated** (codecs exist). | codec dir listing |

**Residual uncertainties (flagged, low-risk to the plan):** exact OSS/Enterprise
cutoffs for current-Debezium CDC and some Jet connectors; CP Sessions edition
label (CP is excluded anyway); TLS page lacks an explicit label (Enterprise via
chapter statement). None affect the in-scope roadmap.

---

## 6. Recommended sequencing (rationale)

Ordered by **parity-per-effort** and dependencies (full task breakdown in
`IMPLEMENTATION_PLAN.md`):

1. **Query depth + indexes + aggregations/projections** (4.1 advanced reads, 4.2) —
   highest leverage; also accelerates SQL. **L**
2. **Serialization: Portable + general IDS** (4.3) — unblocks many query/SQL/client
   workloads. **L**
3. **IMap completeness: EntryProcessor, MapStore, eviction/expiration, TTL/idle,
   misc ops, predicate+EVICTED/EXPIRED listeners** (4.1, 4.8, 4.13). **L**
4. **SQL engine depth** (4.4): aggregations, GROUP BY/ORDER BY/LIMIT, UPDATE/
   DELETE/SINK, DDL/typed columns; then **distributed execution**. **L→XL**
5. **Listeners/events completeness** (4.10) + **observability/Management Center**
   (4.12). **M–L**
6. **Remaining structures & protocols:** ICache/JCache, ReliableTopic,
   CardinalityEstimator, REST data API, Memcache, multicast/cloud discovery, lite
   members, cluster states (4.9, 4.11, 4.14). **M each**
7. **Distributed compute + Transactions** (4.6, 4.7): executors, EntryProcessor
   distribution, Transactional* + XA. **L–XL**
8. **Jet engine** (4.5): the full pipeline/DAG engine, windowing, fault tolerance,
   more connectors, job management. Largest remaining area. **XL**

**"80% of real-world OSS usage"** ≈ items **1–5** (query/SQL/serialization/IMap-
completeness/listeners/observability) — broadly Hazelcast-equivalent for the
data-grid + SQL surfaces **without** the two heavyweight remainders (full Jet,
distributed compute/transactions). With **CP removed from scope**, the only true
XL tier left for OSS parity is **Jet**.

---

## 7. Summary

BonsaiGrid already has a complete distributed core (runtime, wire protocol, the
AP data structures, full multi-node HA) plus MVP slices of query, SQL, and
streaming. Validation **shrinks** the remaining OSS scope by removing the CP/Raft
tier (Enterprise) and **confirms** Jet/SQL/transactions/CQC as in-scope. The work
is dominated by **breadth** (query/SQL/serialization/IMap/listeners — items 1–5,
incremental and low-risk) plus two heavyweight tiers (**distributed
compute+transactions**, and the **full Jet engine**). Everything extends the same
zero-allocation, thread-per-core, shared-nothing core behind the unmodified
Hazelcast client protocol.
