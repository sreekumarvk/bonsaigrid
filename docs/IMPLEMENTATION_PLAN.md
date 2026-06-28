# BonsaiGrid → Hazelcast OSS Parity — Implementation Plan

**Date:** 2026-06-25 · **Companions:** [`PARITY.md`](PARITY.md) ·
[`TEST_STRATEGY.md`](TEST_STRATEGY.md)

This is the executable, step-by-step plan to reach Hazelcast **Community-Edition**
parity. It follows the validated scope in `PARITY.md` (CP subsystem excluded —
Enterprise). Each **epic** is a self-contained spec→plan→implement→test→commit
unit (the cadence used for Phases C/D, HA, and the Redpanda demo). Each **task**
ends with an independently testable deliverable and a verification gate that maps
to `TEST_STRATEGY.md`.

**Working rules (unchanged):** zero-alloc client hot path; thread-per-core /
shared-nothing (SPSC + the existing `Mutex`-guarded broker/store); io_uring;
member protocol is BonsaiGrid-only; commit per working+tested slice; reference
the Hazelcast Java source for codec layouts and semantics, reimplement in Rust.

**Conventions:** "codec" = the Java codec file giving the exact frame layout to
match. Effort: S/M/L/XL. Every task lists **Deps**, **Build**, and **Gate**
(the test that must pass before moving on).

---

## EPIC 1 — Query depth: predicates, indexes, aggregations, projections (L)

*Why first:* highest parity-per-effort; reuses the existing `Predicate` AST +
`FieldExtractor`/`scan` seams; also accelerates SQL (Epic 4).

**E1.T1 — Remaining predicate classes (S–M).** Decode + evaluate the missing IDS
predicate classes from `PredicateDataSerializerHook`: Between(2), Like(5),
ILike(6), In(7), NotEqual(9), Not(10), Regex(12), True(14), False(13).
Files: `crates/query/src/lib.rs` (AST + decoder), `eval.rs`, `sql.rs::eval_fields`.
Deps: none. Gate: unit (decode round-trip per class vs captured client bytes) +
`query` e2e (each predicate over a Compact map).

**E1.T2 — PagingPredicate + PartitionPredicate (M).** Decode wrappers; paging =
sort + page window server-side (needs a comparable order on the field); partition
= scope the scan to one partition. Files: `query` crate, handler arms.
Deps: E1.T1. Gate: unit (page boundaries) + e2e (paged query returns N per page).

**E1.T3 — Index store + planner (L).** A per-attribute index maintained on
put/remove: `SORTED` (ordered map / B-tree), `HASH` (hash map), `BITMAP`. Handle
`MapAddIndex`. The query executor consults indexes (the `scan` candidate-set seam
already exists) and falls back to full scan. Files: new `crates/query/src/index.rs`,
store hooks on put/remove, `handlers` MapAddIndex arm.
Deps: E1.T1. Gate: unit (index returns correct candidate set; maintained on
update/remove) + e2e (range/equality query correct **and** demonstrably uses the
index — assert via a counter/metric, not just correctness).

**E1.T4 — Aggregations + Projections (M).** `MapAggregate(WithPredicate)`,
`MapProject(WithPredicate)`. Decode the aggregator/projection objects (IDS);
implement count/sum/avg/min/max/distinct + field projection over the scan.
Files: `crates/query/src/agg.rs`, codecs, handler arms.
Deps: E1.T1. Gate: unit (each aggregator) + e2e (sum/avg/count over a map).

**E1.T5 — ContinuousQueryCache (L).** `ContinuousQuery*` codecs: a server-side
materialized cache fed by entry events, with its own listeners. Reuses the event
broker. Files: new `crates/server/src/cqc.rs`, events wiring, handler arms.
Deps: E1.T1, Epic-5 predicate listeners. Gate: e2e (CQC reflects live
mutations matching its predicate).

---

## EPIC 2 — Serialization breadth: Portable + general IDS (L)

*Why:* unblocks query/SQL/clients that don't use Compact.

**E2.T1 — Portable reader (M).** Portable `Data` layout + a class-definition
service (Portable carries field defs / version). Implement a `PortableExtractor:
FieldExtractor` so queries/SQL read Portable values. Capture a real Portable value
from a stock client to pin the byte layout (as done for Compact/JSON).
Files: `crates/serialization/src/portable.rs`, schema/classdef service.
Deps: none. Gate: unit (extract fields from a captured Portable blob) + e2e
(query a Portable-valued map).

**E2.T2 — Portable writer + class-def replication (M).** Needed for SQL INSERT
into Portable mappings and for any server-built Portable values; replicate class
definitions like Compact schemas (`ClientSendSchema`-equivalent for Portable
class defs). Files: `serialization`, handler arms.
Deps: E2.T1. Gate: e2e (INSERT then SELECT a Portable mapping).

**E2.T3 — General IdentifiedDataSerializable registry (M).** Beyond predicates:
a factory/class registry to read fields from arbitrary IDS values for queries and
EntryProcessor args. Files: `serialization`, `query`.
Deps: none. Gate: unit (read a registered IDS object's fields).

---

## EPIC 3 — IMap completeness (L)

*Why:* IMap is the most-used structure (73 codecs vs our ~24).

**E3.T1 — TTL/idle + lifecycle ops (S–M).** `PutWithMaxIdle`, `SetTtl`,
`PutTransient(WithMaxIdle)`, `SetWithMaxIdle`, `Evict/EvictAll/Flush`,
`GetEntryView`, `TryPut/TryRemove`, `ReplaceIfSame/RemoveIfSame`, `localKeySet`,
`RemoveAll(predicate)`. Mostly mechanical handler arms + max-idle in the store
Entry. Files: `store` (max-idle field), `handlers`.
Deps: none. Gate: unit (max-idle expiry) + e2e (each op via stock client).

**E3.T2 — Eviction policies (M).** Max-size policy + LRU/LFU/RANDOM eviction on
the slab table (per-map size accounting + access metadata). Files: `store`.
Deps: none. Gate: unit (LRU evicts least-recently-used at capacity) + e2e
(map bounded at max-size).

**E3.T3 — EntryProcessor (L).** `MapExecuteOnKey/Keys/AllKeys/WithPredicate`,
`SubmitToKey`. **Design decision (made here, documented):** support the **built-in
+ IDS-encoded** processors first (decode the processor object, apply a known
transform: increment, conditional set/remove, read-modify-write); a general
user-code path is deferred (see Epic 7 note). Execution runs on the owner, then
replicates the resulting mutation (reuse the Phase-C deferral). Files: new
`crates/server/src/entry_processor.rs`, codecs, handler arms.
Deps: E2 (to read processor args). Gate: unit (a processor mutates an entry +
returns a value) + e2e (executeOnKey increments a counter, replicated to backup).

**E3.T4 — MapStore / MapLoader (L).** An external-store SPI: read-through (load on
miss), write-through (store on put), write-behind (queue + batch flush), `loadAll`,
`loadAllKeys`. For OSS parity the "external store" can be a pluggable trait with a
reference impl (e.g. a file/JDBC-less in-memory or simple file store) so the client
contract (`MapLoadAll`, store callbacks) is satisfiable. Files: new
`crates/server/src/mapstore.rs`, handler arms, config plumbing.
Deps: none. Gate: e2e (a get on a missing key triggers a load; a put triggers a
store; write-behind flushes).

**E3.T5 — Event journal + interceptors (M).** `MapEventJournalSubscribe/Read`
(ringbuffer-backed change log); `MapAddInterceptor`. Files: `store`/`events`.
Deps: Ringbuffer. Gate: e2e (journal replays the last N changes).

---

## EPIC 4 — SQL engine depth (L → XL)

*Why:* turns the SQL MVP into a usable query/ETL surface.

**E4.T1 — Typed result columns (M).** Carry column types through the SqlPage
encoder (the CN-codecs: ListCNInteger/Long/Double/Boolean, ListMultiFrame
String), instead of VARCHAR-only. Files: `crates/codecs/src/sql.rs`,
`crates/query/src/sql.rs` (type inference from mapping columns).
Deps: none. Gate: e2e (an INTEGER column returns as int to the client).

**E4.T2 — Aggregations / GROUP BY / ORDER BY / LIMIT / DISTINCT (L).** Executor
operators: hash-aggregate, sort, limit, distinct. Files: `crates/query/src/sql.rs`
(planner + operators). Deps: E1.T4. Gate: unit (each operator) + e2e
(`SELECT user, COUNT(*) ... GROUP BY user ORDER BY 2 DESC LIMIT 3`).

**E4.T3 — DML: UPDATE / DELETE / SINK INTO (M).** Mutating statements over the
store; SINK INTO an IMap (continuous, like CREATE JOB but to a map).
Files: `query::sql`, `handlers`. Deps: none. Gate: e2e (UPDATE/DELETE rows).

**E4.T4 — DDL completeness + job lifecycle (M).** DROP MAPPING/JOB/INDEX,
CREATE INDEX/VIEW, SHOW MAPPINGS/JOBS; **DROP JOB stops the streaming thread**
(needs a stop flag in `jobs`). Files: `catalog`, `jobs`, `query::sql`.
Deps: none. Gate: e2e (CREATE then DROP a JOB; the thread stops).

**E4.T5 — Distributed SQL execution (L).** Scatter the scan/agg to partition
owners and merge (the SQL job/select currently runs on the local store only).
Reuses the member transport. Files: `query::sql`, `member_thread`.
Deps: E4.T2, multi-node. Gate: e2e (a 3-member cluster returns a full aggregate).

---

## EPIC 5 — Listeners/events + observability (M–L)

**E5.T1 — Cluster/membership/lifecycle/migration/partition-lost/distributed-object
listeners (M).** Event encoders + registration; the coordinator already produces
membership changes — surface them as client events. Files: `events`, `handlers`,
`member_thread`/reactor wiring. Deps: none. Gate: e2e (a client sees a
member-added event when a node joins).

**E5.T2 — Item / message / predicate-filtered entry / EVICTED-EXPIRED listeners
(M).** Queue/list/set item listeners; predicate-filtered entry listeners;
EVICTED/EXPIRED entry events. Files: `events`, `handlers`. Deps: E1.T1, E3.T2.
Gate: e2e (queue item-added event; predicate-filtered entry event).

**E5.T3 — Full metrics surface + JMX + diagnostics (M).** Expand the metrics
registry to Hazelcast's metric set (per-structure, per-partition, op latencies);
expose JMX (or document Prometheus as the substitute); slow-op detector.
Files: `metrics`, new `diagnostics`. Deps: none. Gate: functional (metric
values change under load; `/metrics` exposes the expected names).

**E5.T4 — Management Center protocol (L).** The 40 `MC*` codecs the MC app uses to
read cluster state and run ops. Implement the read/feed subset so MC connects and
shows the cluster (write/ops subset optional). Files: new `crates/codecs/src/mc.rs`,
handler arms. Deps: E5.T3. Gate: e2e (Management Center ≤3-member connects and
renders the cluster + maps).

---

## EPIC 6 — Remaining structures & protocols (M each)

**E6.T1 — ICache / JCache (JSR-107) (L).** 33 codecs; largely an IMap variant
with JCache semantics (getAndPut, putIfAbsent, replace, EntryProcessor, expiry
policies, JCache events). Reuse the IMap store + HA. Files: new
`crates/server/src/cache.rs`, codecs, handler arms. Deps: E3. Gate: e2e (a
JCache client roundtrips + expiry).

**E6.T2 — ReliableTopic (S–M).** Ringbuffer-backed reliable delivery semantics +
loss/stale handling. Files: `handlers`, reuse ringbuffer. Deps: Ringbuffer.
Gate: e2e (subscriber receives all messages incl. after reconnect).

**E6.T3 — CardinalityEstimator (S).** HyperLogLog add/estimate (2 codecs).
Files: `store` (HLL), `handlers`. Deps: none. Gate: unit (estimate within error
bound) + e2e.

**E6.T4 — REST data API + Memcache (M each).** REST map/queue endpoints
(extend the existing HTTP path); a Memcache text/binary front-end mapped to IMap.
Files: `reactor`/`handlers` HTTP, new memcache front-end. Deps: none.
Gate: e2e (curl a map value; a memcached client get/set).

**E6.T5 — Discovery + membership metadata (M).** Multicast join; pluggable
discovery SPI (k8s/cloud shape); lite members; member attributes; cluster states
(ACTIVE/FROZEN/PASSIVE/NO_MIGRATION) gating ops; graceful partition-safe shutdown.
Files: `membership`, `cluster_coordinator`, `main`. Deps: multi-node.
Gate: e2e (multicast 3-node form; PASSIVE state rejects writes; graceful leave
migrates out first).

---

## EPIC 7 — Distributed compute + transactions (L–XL)

**E7.T1 — IExecutorService (L).** Submit/execute on key/member/all; route the
task to the owner/member; **user-code decision (documented):** support IDS/Compact-
encoded *built-in* tasks first; general Java-task execution (user-code deployment)
is a separate, flagged sub-epic (security-sensitive; OSS but heavyweight). Files:
new `crates/server/src/executor.rs`, codecs. Deps: E2. Gate: e2e (a built-in task
runs on the target member and returns a result).

**E7.T2 — Durable + Scheduled executors (L).** DurableExecutor (durable task
ringbuffer + result retrieval); ScheduledExecutor (18 codecs: schedule/cancel/
get-result/stats). Files: `executor`. Deps: E7.T1. Gate: e2e (a scheduled task
fires; a durable result survives a restart of the submitting client).

**E7.T3 — Transactions (L) + XA (M).** Transaction context: per-txn write-set,
key locking, 1PC/2PC across partition owners; Transactional Map/MultiMap/Queue/
List/Set; then XAResource. Files: new `crates/server/src/txn.rs`, codecs (38+7),
locking reuse. Deps: locks, multi-node. Gate: e2e (a transaction commits atomically
across two keys; rollback discards; XA two-phase).

---

## EPIC 8 — Jet streaming engine (XL)

*The largest remaining OSS tier; the SQL `CREATE JOB` is a thin single-stage slice.*

**E8.T1 — Job submission protocol + DAG model (L).** Decode `JetSubmitJob` (a
serialized DAG) + `JetGetJobStatus/Ids/Summary`, a job registry, and a
single-member DAG executor (sources→transforms→sinks). Files: new
`crates/jet/` crate. Deps: E2, E4. Gate: e2e (Java client submits a simple
map/filter pipeline; it runs; status reported).

**E8.T2 — Connectors (M each).** Source/sink for files, sockets, JDBC, JMS
(Kafka done). Files: `jet`. Deps: E8.T1. Gate: e2e per connector.

**E8.T3 — Windowing + watermarks (L).** Tumbling/sliding/session windows, event-
time watermarks, windowed aggregation. Files: `jet`. Deps: E8.T1.
Gate: e2e (windowed counts over a timestamped stream).

**E8.T4 — Stateful transforms + fault tolerance (XL).** Keyed state stores +
distributed snapshotting (at-least/exactly-once). Files: `jet`, `member_thread`.
Deps: E8.T1, multi-node. Gate: e2e (a stateful job recovers state after a member
kill — no data loss).

**E8.T5 — Distributed/parallel execution + job mgmt (XL).** Partition the source,
shuffle edges across members; cancel/restart/resume/suspend + job metrics.
Files: `jet`, `member_thread`. Deps: E8.T1–4. Gate: e2e (a job parallelized
across 3 members; cancel/restart works).

---

## Sequencing & milestones

```
M1 "Query/SQL parity"     : E1, E2, E4.T1–T4        (data-grid querying is Hazelcast-equivalent)
M2 "IMap parity"          : E3                       (IMap fully featured: EP, MapStore, eviction)
M3 "Operability parity"   : E5, E6.T1–T3            (listeners, MC, JCache, structures)
M4 "Protocol/discovery"   : E6.T4–T5                (REST/Memcache, discovery, cluster states)
M5 "Distributed SQL"      : E4.T5                    (scatter-gather SQL)
M6 "Compute + txns"       : E7                       (executors, EntryProcessor distribution, transactions)
M7 "Streaming engine"     : E8                       (full Jet)
```
**M1–M3 ≈ "80% of real-world OSS usage."** CP/Raft is **excluded** (Enterprise),
so the only XL tier remaining for full OSS parity is **M7 (Jet)**.

---

## Plan verification (assumptions, risks, mitigations)

| Plan assumption | Risk | Mitigation |
|---|---|---|
| Index/agg/projection reuse the `scan`/extractor seams | Seams may not cover index maintenance on update/remove | E1.T3 adds explicit store put/remove hooks; gate asserts the index is *used* (metric), not just correct |
| Portable byte layout can be matched | Portable framing is fiddly (versioned class defs) | **Capture a real Portable blob from the stock client first** (proven approach for Compact/JSON); pin the layout with a golden test |
| EntryProcessor / Executor / Jet don't need arbitrary user-code execution | Some Java workloads send opaque Java lambdas/classes | Scope OSS parity to **IDS/Compact-encoded built-in** processors/tasks/DAGs; flag general user-code-deployment as a separate, security-reviewed sub-epic; document the boundary so clients get a clear error, not a hang |
| Distributed SQL/Jet reuse the member transport | The transport was built for replication/migration, not shuffles | Add a generic "member RPC / shuffle edge" message kind; the transport already multiplexes by member index |
| MC protocol subset is enough for Management Center to connect | MC may require ops we don't implement | Implement the **read/feed** subset first; MC degrades gracefully on unimplemented ops; gate = "MC renders the cluster" |
| Single-statement SQL parser can grow to GROUP BY/joins/subqueries | The hand-rolled tokenizer won't scale to a real planner | At E4.T2 introduce a proper recursive-descent parser + logical plan; subqueries/multi-joins (XL) are explicitly later |
| HA paths stay intact as features are added | New write ops must replicate / be quorum-gated / migrate | Every new mutating op routes through the existing `replicate_*`/quorum/aux-state machinery; regression gate = the full multi-node smoke suite stays green each epic |
| Scope excludes CP correctly | A client may still call CP ops | Stub CP codecs with a Hazelcast "Enterprise-only"/unsupported error response so clients fail fast, not hang |

**Cross-cutting gate (every epic):** the existing conformance suite
(`run_cluster`, `run_auto_failover`, `run_dynamic_join`, `run_quorum`,
`run_structure_ha`, `run_double_failover`, single-node smokes, `sql_smoke`,
`redpanda_demo`) plus full `cargo test` must remain green — no feature epic may
regress the data-grid or HA guarantees.
