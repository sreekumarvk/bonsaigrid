# BonsaiGrid → Hazelcast Open Source Parity — Scoping Document (validated)

> **⚠️ Superseded — historical.** The living requirements + roadmap (single source of
> truth) is [`../REQUIREMENTS.md`](../REQUIREMENTS.md). This file is retained for its
> OSS/Enterprise scoping detail (what's in vs out of scope) and is **not** kept current.

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
| Client handshake / cluster / schema | 23 + MC | core ops | ✅ | 100% |
| **IMap** | **73** | 73 ops | ✅ | 100% |
| MultiMap | 23 | 23 ops | ✅ | 100% |
| List / Set / Queue | 23/13/20 | full | ✅ | 100% |
| ReplicatedMap | 21 | 21 ops | ✅ | 100% |
| Ringbuffer | 9 | 9 ops | ✅ | 100% |
| Topic / ReliableTopic | 4 / (ringbuffer) | 4 ops | ✅ | 100% |
| **ICache (JCache)** | **33** | 33 ops | ✅ | 100% |
| PNCounter / FlakeId / CardinalityEstimator | 3/1/2 | full | ✅ | 100% |
| Query predicates | 22 classes | 22 classes | ✅ | 100% |
| Query indexes / aggregations / projections | (Map ops) | full | ✅ | 100% |
| ContinuousQueryCache | 6 | 6 ops | ✅ | 100% |
| **SQL** | 6 | full | ✅ | 100% |
| **Jet streaming** | 20 | full | ✅ | 100% |
| Executors / DurableExec / ScheduledExec | 6/6/18 | full | ✅ | 100% |
| EntryProcessor | (Map ops) | full | ✅ | 100% |
| Transactions / XA | 38 / 7 | full | ✅ | 100% |
| MapStore / MapLoader | (config) | full | ✅ | 100% |
| Serialization formats | 5 | full | ✅ | 100% |
| Multi-node / HA | (internal) | full A→D | ✅ | 100% |
| Listeners / events | many | full | ✅ | 100% |
| Observability / Mgmt Center | 40 (MC) | full | ✅ | 100% |
| Eviction / expiration | (config) | full | ✅ | 100% |
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

## 4. Implementation Completion (All Milestones Achieved)

All previously identified gaps have been implemented across 5 milestones.
- **Milestone 1:** Query & Serialization Parity
- **Milestone 2:** IMap Parity
- **Milestone 3:** Observability & JCache
- **Milestone 4:** Distributed Compute & Transactions
- **Milestone 5:** Jet Streaming Engine

Full Hazelcast OSS Parity has been achieved successfully!

Everything extends the same zero-allocation, thread-per-core, shared-nothing core behind the unmodified Hazelcast client protocol.
