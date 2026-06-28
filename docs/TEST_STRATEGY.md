# BonsaiGrid → Hazelcast OSS Parity — Test Strategy

**Date:** 2026-06-25 · **Companions:** [`PARITY.md`](PARITY.md) ·
[`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md)

How every parity feature is proven correct, at three levels, on the test
infrastructure that already exists in this repo. The governing principle:
**a feature is not "done" until a stock Hazelcast client exercises it end-to-end
and the full regression suite stays green.**

---

## 1. The three layers (what each means here)

| Layer | Question it answers | Mechanism in this repo | Runs in |
|---|---|---|---|
| **Unit** | Is the logic/codec correct in isolation? | Rust `#[test]` per crate (parser, predicate eval, codec round-trip, index, merge, store ops). Wire layouts pinned by **golden tests** against bytes captured from a real client. | `cargo test` (fast, no broker) |
| **Functional** | Does one feature behave correctly against a running server via the **stock client**, single-node? | `conformance-python/*_smoke.py` driving `hazelcast-python-client` against a `BONSAI_CORES=1` server. | one server process |
| **E2E** | Does it hold across the real distributed/streaming topology (multi-node HA, Kafka)? | `conformance-python/run_*.sh` harnesses: multi-member clusters, kills/joins, and the Redpanda Docker broker. | cluster / Redpanda |

These map to the three things Hazelcast parity actually requires: **correct logic**,
**wire-compatible behavior with unmodified clients**, and **correct distributed
semantics**.

---

## 2. Cross-cutting practices (apply to every epic)

1. **Golden capture for every wire format.** Before implementing a codec, capture
   the real bytes from the stock client (the proven method: Compact schemaId,
   json-flat type −130, predicate IDS classes, SqlPage). Commit the hex as a unit
   golden so the encoder/decoder is pinned to the client, not to our guess.
2. **TDD on the pure layers.** Parser, predicate/aggregator eval, index, merge,
   serialization extractors — failing unit test first, then implement.
3. **Functional before e2e.** Prove each op with the stock client single-node
   before adding the distributed/streaming dimension.
4. **Regression gate per commit.** No epic may regress the data-grid or HA
   guarantees. The mandatory green set:
   - `cargo test` (all crates),
   - single-node: `smoke`, `structures`, `query`, `sql`, `listener`,
     `nearcache`, `lock`, `blocking_lock`, `topic`, `bulk`, `auth`,
   - multi-node: `run_cluster`, `run_auto_failover`, `run_dynamic_join`,
     `run_quorum`, `run_structure_ha`, `run_double_failover`, `run_failover`,
   - streaming: `run_redpanda_demo`.
5. **Negative + edge tests, not just happy path.** Below-quorum rejection, missing
   keys, predicate-no-match, malformed input → safe error (never a hang or panic);
   each new mutating op must be quorum-gated, replicated, and migratable.
6. **Property/fuzz where it pays.** Codec round-trip fuzz (encode∘decode == id) and
   merge-convergence (order-independence) — already used for wire + merge; extend
   to new codecs and the SQL parser.
7. **Determinism for distributed tests.** Kill/join by recorded PID; bounded waits
   tied to `BONSAI_HB_TIMEOUT_MS`; consume Kafka with `--num` + timeout. (Lessons
   already encoded in the existing harnesses.)

---

## 3. Per-epic test matrix

For each epic: **Unit** (pure logic/codecs) · **Functional** (stock client,
single-node) · **E2E** (distributed/streaming). New smoke files named
`<feature>_smoke.py`; new harnesses `run_<feature>.sh`.

### Epic 1 — Query depth
- **Unit:** decode round-trip for each predicate class (golden bytes per class);
  `eval` truth tables (Between/In/Like/Regex/NotEqual/Not); index returns the
  correct candidate set and is maintained on update/remove; each aggregator
  (count/sum/avg/min/max/distinct); paging window boundaries.
- **Functional:** `query2_smoke.py` — every predicate over a Compact + a JSON map;
  `index_smoke.py` — equality/range query returns correct rows **and** a metric
  proves the index was used (not full scan); `agg_smoke.py` — sum/avg/count/group.
- **E2E:** `cqc_smoke.py` — a ContinuousQueryCache reflects live mutations matching
  its predicate across a cluster.

### Epic 2 — Serialization
- **Unit:** golden extraction from a **captured Portable** blob (multiple field
  types + a versioned class def); IDS registry reads a registered object's fields;
  Portable writer encode∘decode == client-decodable.
- **Functional:** `portable_smoke.py` — put Portable objects, query/SELECT them;
  INSERT into a Portable mapping then SELECT.
- **E2E:** Portable class-def replication across a cluster (a member that didn't
  receive the def can still serve queries after gossip).

### Epic 3 — IMap completeness
- **Unit:** max-idle expiry; LRU/LFU eviction at capacity; EntryProcessor applies
  the decoded transform + returns the right value; MapStore write-behind batches.
- **Functional:** `imap_full_smoke.py` — SetTtl/Evict/TryPut/ReplaceIfSame/
  GetEntryView/RemoveAll(predicate); `eviction_smoke.py` — map bounded at max-size,
  LRU order; `entryproc_smoke.py` — executeOnKey/Keys/WithPredicate;
  `mapstore_smoke.py` — read-through load-on-miss, write-through, write-behind flush.
- **E2E:** an EntryProcessor mutation is replicated to the backup and survives a
  failover (extend `structure_ha`); MapStore survives across the cluster.

### Epic 4 — SQL depth
- **Unit:** typed column encoding (int/long/double/bool via the CN-codecs);
  aggregate/sort/limit/distinct operators; UPDATE/DELETE row effects; the new
  recursive-descent parser (golden ASTs for GROUP BY/ORDER BY/joins).
- **Functional:** `sql2_smoke.py` — typed columns; `GROUP BY ... ORDER BY ... LIMIT`;
  UPDATE/DELETE; DROP MAPPING/JOB (job thread stops); SHOW MAPPINGS.
- **E2E:** `sql_distributed_smoke.py` — a 3-member cluster returns a correct full
  aggregate (scatter-gather); `run_redpanda_demo` still green.

### Epic 5 — Listeners + observability
- **Unit:** event encoders for each listener type (golden frames); slow-op
  detector thresholds.
- **Functional:** `events_smoke.py` — membership/lifecycle/migration/distributed-
  object/item/predicate-filtered/EVICTED-EXPIRED listeners each fire with the right
  payload; `metrics_smoke.py` — `/metrics` exposes the expected names and values
  move under load.
- **E2E:** `mc_smoke.py` — Management Center (≤3 members) connects and renders the
  cluster + maps; a membership-added event reaches a connected client when a node
  joins (drive with `run_dynamic_join`).

### Epic 6 — Structures & protocols
- **Unit:** HLL estimate within error bound; ReliableTopic sequence handling.
- **Functional:** `jcache_smoke.py` (getAndPut/putIfAbsent/replace/expiry/events);
  `reliable_topic_smoke.py`; `cardinality_smoke.py`; `rest_smoke.py` (curl a map
  value); `memcache_smoke.py` (a memcached client get/set).
- **E2E:** JCache HA (survives failover, extend `structure_ha`); `run_multicast.sh`
  (multicast discovery forms a 3-node cluster); cluster-state PASSIVE rejects
  writes; graceful leave migrates out first.

### Epic 7 — Compute + transactions
- **Unit:** transaction write-set apply/rollback; 1PC/2PC state machine.
- **Functional:** `executor_smoke.py` (submit on key/member/all); `scheduled_
  smoke.py`; `txn_smoke.py` (commit atomic across 2 keys, rollback discards).
- **E2E:** `run_txn_failover.sh` — a transaction's effects survive a backup failover;
  XA two-phase across the cluster; a durable executor result survives client
  reconnect.

### Epic 8 — Jet streaming
- **Unit:** DAG decode (golden `JetSubmitJob` bytes); windowing math; snapshot
  serialize/restore.
- **Functional:** `jet_smoke.py` — submit a map/filter pipeline via the Java client,
  status reported; windowed counts over a timestamped source.
- **E2E:** `run_jet_kafka.sh` — a multi-stage streaming job (Kafka→transform→window
  →Kafka) across members; `run_jet_recovery.sh` — kill a member mid-job, assert
  exactly/at-least-once state recovery (no loss).

---

## 4. Tooling, CI, and gates

- **Runner:** a top-level `conformance-python/run_all.sh` that runs `cargo test`,
  then every single-node smoke against one server, then each multi-node `run_*.sh`,
  then `run_redpanda_demo`. Exit non-zero on any failure. This is the **release
  gate** for each epic.
- **Broker fixtures:** Redpanda via Docker (already scripted in
  `run_redpanda_demo.sh` — auto-start if absent). Java-client tests (Jet) need a
  JVM (present) + the Hazelcast Java client jar fetched once.
- **Coverage targets:** unit ≥ 80% line on the pure crates (`query`,
  `serialization`, `store`, codec modules) via `cargo llvm-cov`; every codec has a
  golden round-trip; every client-facing op has at least one functional smoke;
  every distributed guarantee (replication, failover, migration, quorum, restore-K)
  has an e2e harness.
- **Definition of done (per feature):** (1) unit + golden green; (2) a functional
  smoke driving the **stock client** green; (3) where the feature touches the
  distributed/streaming plane, an e2e harness green; (4) the full regression set
  (§2.4) still green; (5) committed with the feature, smoke, and any new harness.
- **Performance guardrail:** the zero-alloc MapGet test (`zero_alloc.rs`) and a
  throughput micro-bench (`bench` crate) must not regress — new features must not
  allocate on the IMap hot path.

---

## 5. Risk-targeted tests (where parity bugs hide)

| Risk | Targeted test |
|---|---|
| Wire layout drift from the real client | Golden hex captured from `hazelcast-python-client`/Java client per codec; CI re-decodes them |
| New write op skips HA (lost on failover) | Every mutating op added to `structure_ha`/`double_failover` style harness — kill the owner, assert survival |
| Predicate/SQL semantics differ from Hazelcast | Cross-check a sample query's result against a **real Hazelcast member** (run one in Docker) for the same data — differential test |
| Index returns wrong/stale candidates | Property test: index result == full-scan result for random ops; assert index is *used* via a counter |
| Merge/quorum regressions | Existing merge-convergence + `run_quorum`/`run_double_failover` stay in the gate |
| Streaming exactly-once claims | `run_jet_recovery.sh` injects a mid-job kill and asserts no loss/dup |
| Distributed SQL/Jet correctness | Differential vs a real Hazelcast cluster on the same dataset |

**Differential testing note:** for the semantically-subtle areas (predicates,
SQL results, aggregations, Jet windowing), the strongest correctness signal is to
run a **real Hazelcast Community member in Docker**, issue the identical
client calls to both it and BonsaiGrid, and assert equal results. This is the
recommended acceptance test for M1 (Query/SQL) and M7 (Jet).
