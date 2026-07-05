# Opportunistic Tail — Implementation Plan

**Date:** 2026-07-05. **Goal:** complete the small, self-contained parity items left
after the five major platform gaps shipped. Priority: **do not break existing
functionality**; every new item is TDD'd and verified the way its subsystem already
is (unit + deterministic sim).

## Status — all five "implement now" items SHIPPED (2026-07-05)

1. ✅ **CardinalityEstimator (HyperLogLog)** — `store/hll.rs` + full aux wiring
   (persist / snapshot / WAN) + client codecs (`976d167`).
2. ✅ **WAN per-target ack cursors** — `wan/queue.rs` + WAN thread (`9e7f4d2`).
3. ✅ **CP CPMap** — `raft/cpmap.rs`, sim-verified through Raft (`be9f3f4`).
4. ✅ **CP read-index lease reads** — `RaftNode::has_read_lease` (`b44c9bb`).
5. ✅ **Client-cert-as-principal** — `security` CN extractor + RBAC resolution
   (`da36611`).

Zero workspace-test failures; benchmark confirmed no hot-path regression. The
external-infra items below remain deferred (seams in place, honest notes).

## Scope decision (autonomous, best-practice)

The tail splits into items that are **fully implementable + testable in this
environment** and items that **require external infrastructure** and therefore cannot
be honestly "tested as working" here. I implement the former completely and leave a
clean seam + honest note for the latter (faking an untested backend would violate
"make sure new functionality works").

### Implement now (self-contained, testable)

1. **CardinalityEstimator (HyperLogLog)** — `crates/store`. A probabilistic distinct
   counter (Hazelcast `CardinalityEstimator`): `add(hash)` + `estimate()`. Pure
   algorithm (dense HLL, 2^14 registers, bias-corrected), a store structure with
   `aux_state` persistence + WAN capture (reuses the Gap-3/Gap-4 seams for free), and
   codecs. Fully unit-testable (error < 2% on 100k distinct).
2. **WAN per-target ack cursors** — `crates/wan`. Today one cursor = all-targets
   confirmed, so a lagging target pins the queue and a fast target re-ships acked
   records. Track an `acked` cursor **per target**; `unacked_for(target)` and
   `ack(target, seq)`; the outbound loop ships each target its own tail. Sim-testable.
3. **CP: CPMap** — `crates/raft`. The missing linearizable map primitive: a
   `CpMapSm` (put/get/remove/putIfAbsent/replace/cas/containsKey/size/clear) behind a
   new `OBJ_CP_MAP`, dispatched by `CpSm::apply`. Verified at the SM + `CpGroup` sim
   level, exactly as AtomicLong/Semaphore/FencedLock are.
4. **CP: read-index (lease) linearizable reads** — `crates/raft`. A read path on
   `CpGroup` that returns committed state without appending to the log, gated on the
   leader holding a valid lease (heartbeat-confirmed within the election timeout).
   Sim-tested: a leader serves reads; a deposed leader refuses.
5. **Client-cert-as-principal** — `crates/security`. Map a verified TLS client-cert
   subject CN to a principal name, so mTLS identity drives RBAC. Pure function over
   the cert subject; unit-tested. (kTLS wiring of the CN is the live follow-up.)

### Deferred — needs external infrastructure (seam noted, not faked)

- **JDBC / CDC connectors** — need a live JDBC database / CDC source to test. The Jet
  connector SPI is the seam; a real backend is a follow-up with the infra.
- **LDAP / JAAS auth backends** — need a directory server. The `IdentityProvider`
  trait is the seam; add the backend when an LDAP server is available.
- **Live Hazelcast-client CP conformance** — needs a running multi-member cluster +
  the stock Java/Python client. The algorithm is deterministically sim-verified; this
  is frame-level compat testing that needs the live cluster.
- **Distributed joins (network shuffle) / continuous streaming SQL** — architectural,
  **not "small"**; out of tail scope.
- **Persistence sync deferred-ack** — the `Durability::Sync` enum exists; wiring the
  client reply to defer until the persistence thread's fsync callback touches the
  reactor response path (deferred-reply broker). Real but riskier; do only if the
  above land cleanly with time to test, else leave the enum as async-equivalent (its
  current behavior) and note it.

## Testing

Each item: RED test → implement → GREEN, then `cargo test -p <crate>` for no
regression, then `cargo build --workspace` + `cargo test -p store -p server` before
commit. CP items add sim tests to `crates/raft`. Finish with the isolated benchmark
(`bench/run-all-isolated.sh`) to confirm no hot-path regression (these items are all
off the hot path or in cold structures, so throughput should be unchanged).

## Self-review

- **No hot-path risk:** HLL is a new structure (cold), CPMap/read-index are CP-only,
  WAN cursors and cert-principal are off the request loop. The IMap put/get/remove hot
  path is untouched. Benchmark confirms.
- **Reuse, don't reinvent:** HLL persistence + WAN replication come free via the
  existing `aux_state` seam. CPMap mirrors the AtomicLong SM shape exactly.
- **Honest scope:** external-infra items are seams + notes, not stubs pretending to work.
