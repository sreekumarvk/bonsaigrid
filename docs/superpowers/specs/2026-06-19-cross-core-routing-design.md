# BonsaiGrid Cross-Core Routing Design

**Date:** 2026-06-19
**Status:** Approved (design); pending implementation plan
**Scope:** How a request reaches the CPU core that owns its data in BonsaiGrid's thread-per-core runtime, while remaining wire-compatible with stock Hazelcast clients.

**North-star goal:** BonsaiGrid aims for **complete functional parity with Apache Hazelcast** (multi-node clustering, replication, and the full data-structure / feature surface) on a zero-allocation, thread-per-core, shared-nothing Rust runtime. Single-node v0.1 is a stepping stone, not the destination. Every decision in this document is constrained to stay forward-compatible with that goal.

**The governing invariant (one line):** *new airframe, new engines, better seats — but no expectation held by operators or users is violated.* Everything internal is rebuilt for performance; everything externally observable (the application contract **and** the operator/monitoring contract) stays identical; and the visible upside (lower latency, higher memory density) lands on axes that require no relearning.

**Design philosophy — parity at the boundary, not the internals.** The goal is *not* a faithful Rust clone of Hazelcast that inherits its design deficiencies. BonsaiGrid must match Hazelcast's **client-observable contract** (wire protocol, API semantics, consistency/durability guarantees); behind that boundary, everything is reimplemented from first principles for genuinely better latency, CPU, and memory efficiency. We reject Hazelcast's internal costs (JVM object headers, GC, thread-pool handoffs, lock contention, hot-path deserialization) and diverge freely in storage layout, data structures, and threading. **Caveat:** preserve observable *guarantees* — some apparent overhead (sync backup-acks, migration consistency, CP linearizability) is a contract clients depend on; optimize the implementation, not the guarantee. Relaxing a guarantee is an explicit, documented product decision.

**The one fixed boundary (non-negotiable):** *existing Hazelcast clients cannot be rewritten.* Real, unmodified Hazelcast client libraries (Java/Python/Go/C++/.NET/Node) in the field must connect and work as-is. The server adapts to the clients, never the reverse. This is the hard constraint that makes the client wire protocol — and the TPC routing alignment in this document — immutable; only the *member-to-member* protocol is free to be custom (BonsaiGrid-only).

---

## Problem

BonsaiGrid is a shared-nothing, thread-per-core, zero-allocation in-memory data grid that speaks the Hazelcast client wire protocol (see `bonsaigrid/REQUIREMENTS.md`). In a thread-per-core design each core owns a private slab allocator and a private shard of the keyspace, and cores coordinate only through lock-free SPSC rings — never shared memory or locks.

This creates a routing problem. A Hazelcast smart client maps `key → partition` (murmur3 over the serialized key `Data`) and then `partition → member` (the whole node), then picks *some* connection to that member. **Cores are invisible to the client**, so the core whose socket receives a request has no inherent relationship to the core that owns the request's key. Naively, with `N` cores, ~`(N−1)/N` of all requests land on the wrong core and must be delegated — and because a socket is owned by exactly one core under shared-nothing, the owning core cannot even write the response, forcing a *two-way* cross-core hop on nearly every request. That inverts the locality benefit the architecture exists for.

**Constraint:** Stock-client wire compatibility is non-negotiable. An unmodified Hazelcast client must connect and work. A custom shard-aware client is off the table.

## Key Insight: Hazelcast's TPC protocol already routes key→core

Hazelcast has a thread-per-core client mode ("TPC"). The mechanism, confirmed in the reference repo under `hazelcast/hazelcast/src/main/java/com/hazelcast/`:

- The authentication response (`client/impl/protocol/codec/ClientAuthenticationCodec.java`) carries `tpcPorts` (a list of ports, one per server core) and a `tpcToken`.
- The TPC client (`client/impl/connection/tcp/TpcChannelConnector.java`) opens one connection per advertised port.
- For each request, `client/impl/connection/tcp/TcpClientConnection.java` routes by:

  ```java
  int partitionId = clientMessage.getPartitionId();   // already in the frame header
  int channelIndex = partitionId % tpcChannels.length; // one channel per server core
  return tpcChannels[channelIndex].write(frame);        // straight to that core's socket
  ```

So a **stock** TPC client already performs key→core routing for us, provided the server advertises one TPC port per core and aligns partition ownership to the client's `partitionId % N` formula. The partition id is computed client-side and stamped into the frame header, so the server reads it directly and never re-hashes the key for routing.

## Chosen Approach: TPC-native alignment, with a delegation fallback

Approach **A** (TPC-native alignment) is the hot path; approach **B** (eat-the-hop delegation) is the mandatory fallback for non-TPC ("classic") clients and defensive misroutes. This is the only option that is simultaneously 100% stock-compatible and hop-free on the hot path. It also simplifies the hot path: routing comes from the header rather than a server-side key hash.

Honest framing: the headline differentiator is **not** the thread-per-core architecture (Hazelcast's TPC already does that). It is the no-GC / memory-density / tail-latency execution. Approach A simply lets BonsaiGrid match Hazelcast's routing model so a stock client works unmodified.

---

## Section 1 — Topology & ownership model

At startup, detect `N` = usable cores, launch `N` worker threads, each pinned via `core_affinity`. Each worker exclusively owns: one `io_uring`, its TPC listen socket, its private slab allocator, its private hash-map shard, and a fixed set of partitions.

**The alignment invariant (the linchpin of approach A):**
- We advertise exactly `N` TPC ports, in core order: `tpcPorts[i]` is accepted by core `i`.
- Core `i` owns partition `p` **iff `p % N == i`**.
- The stock client computes `channelIndex = partitionId % tpcChannels.length` and `tpcChannels.length == N`, so its channel `i` → our core `i`, and partition `p` → core `p % N`. **Ownership is aligned with the client's routing by construction** — no negotiation, no rebalancing.

A single "classic" main port (e.g. 5701) handles the initial connect + authentication for *all* clients, plus all traffic from non-TPC clients and non-partition-bound messages. Single node ⇒ partition ownership is static for the process lifetime; we never have to tell a client "wrong owner, retry."

## Section 2 — Hot path (TPC request lifecycle)

A request arrives on core `c`'s TPC socket, already routed correctly by the client:

1. `io_uring` recv into a pre-registered fixed buffer (no alloc).
2. Parse the frame header **in place**: length, flags, type, correlation id, **partition id**.
3. Assert `partition_id % N == c` (always true for a correct TPC client; a violation drops to the fallback path defensively).
4. Decode the op (`MapPut`/`MapGet`), taking key/value `Data` as **slices into the recv buffer** — zero copy.
5. Slab op on **private** structures: `put` copies the value blob into a slab and inserts `keyhash → slab_ptr` into the local hash shard; `get` looks up and borrows the slab slice. The only hashing here is `ahash`/`xxhash` over the key blob for the *bucket* — purely core-local; routing already came free from the header.
6. Encode the response into a pre-registered send buffer, stamp the same correlation id, `io_uring` send on the **same socket**.

Net: **zero cross-core hops, zero locks, zero heap allocation, zero server-side routing hash** on the hot path. This is the thread-per-core ideal, and it falls out of matching the client's own partition formula.

## Section 3 — Fallback path (non-TPC clients & defensive misroutes)

A classic client sends everything over the main connection, owned by some core `a`. For an op whose `partition_id % N == b`:
- **`b == a`:** handle locally, identical to the hot path.
- **`b != a`:** core `a` delegates. Since blobs live in `a`'s recv buffer (which will be reused, and can't be shared under shared-nothing), `a` **copies** key+value into a bounded transfer slot and pushes a descriptor onto the `a→b` ring. Core `b` does the slab op, then pushes the result back on the `b→a` ring; core `a` (the socket owner) writes the response.

This path **allows a copy** (it is explicitly *not* the zero-alloc hot path). Transfer slots are fixed-size, pre-allocated, carried inline in the ring. A fallback op whose payload exceeds the slot size is rejected with a retryable error that effectively says "use TPC" — we do **not** add a dynamic allocation escape hatch.

## Section 4 — SPSC ring topology

One ring per **ordered** core pair: for pair `{a,b}` there are two SPSC rings, `a→b` and `b→a` (single producer, single consumer each — strict SPSC preserved). Each ring carries **both** delegated work *and* returns, distinguished by a tag in the descriptor. Total: `N·(N−1)` rings, each a fixed-size pre-allocated slot array. Each core has `N−1` outbound and `N−1` inbound rings.

## Section 5 — Deadlock & backpressure policy

The rule: **a core never blocks on a full outbound ring.** Each core's reactor loop, every iteration:
1. Poll `io_uring` completions (network I/O).
2. Drain a **bounded batch** from each inbound ring, dispatching work and returns.
3. Flush pending outbound items; if a target ring is full, park the item in a small pre-allocated per-core **pending-deque** and move on — never spin.

Because every core *always* drains its inbound rings (step 2 runs unconditionally), there is no cyclic wait → no deadlock. The pending-deque holds descriptors only (correlation id + slot handle), so it's tiny. If it fills, the core **stops submitting recv** until it drains — backpressure then propagates to clients naturally via TCP flow control.

## Section 6 — Edge cases & testing

**Edge cases:**
- Connection state is pre-allocated per core (fixed max connections → reject `accept` at the cap).
- A per-connection reassembly buffer handles frames split across recvs or fragmented messages.
- The TPC handshake validates the `tpcToken` from auth to bind each per-core channel to its session.
- The partition-table response advertises one member owning all 271 partitions with static list versions (no membership events ever fire).

**Testing:**
- **The invariant** (property test): for all `p` in `0..271`, the core we assign `p` equals `p % N` — i.e. exactly where the client sends it.
- **Hot path**: a real stock Hazelcast TPC client (Java) does put/get; per-core counters assert **zero fallback delegations**.
- **Fallback**: a TPC-disabled client; assert correctness and that the rings carried the traffic.
- **Deadlock/backpressure**: stress with all-cross-core load (every core delegates to every other); assert no deadlock, bounded latency, backpressure engages under overload.
- **Ring**: model-check the SPSC ring (e.g. `loom`); the reactor is single-threaded per core, so the rings are the only concurrency.

---

## Out of scope (explicitly deferred)

- **Multi-node clustering, replication, backups.** This design is single-node; the partition table advertises one member. The protocol's backup-ack / backup-aware fields are answered as "no backups." Multi-node is the **explicit end goal** (it is what makes the system valuable), phased separately (A→B→C→D, see below) — deferred, not abandoned.
- **Eviction / TTL semantics.** Consistent with `REQUIREMENTS.md` Phase 2, a full slab pool returns an explicit OOM error rather than evicting. `map.put` TTL handling is out of scope for this routing design.
- **Slab size-classing, the hash-table data structure, and the full Phase 1 protocol surface** (auth, cluster-view, partition-table encoding, `Data` serialization specifics) are separate designs. This document covers only how a request reaches the owning core.

## Dependencies this design assumes (tracked elsewhere)

- A working `ClientAuthentication` + TPC-port handshake (`tpcToken`) so a client can bootstrap and obtain `tpcPorts`.
- A partition-table response advertising a single member owning all 271 partitions.
- Per-core `io_uring` reactors with pre-registered fixed buffers.
- A lock-free SPSC ring primitive and a per-core slab allocator.

These are prerequisites, not part of this routing design; they will be sequenced in the implementation plan.

## Forward-compatibility with the multi-node goal

This routing design is deliberately built so multi-node adds layers *above* it without reworking the hot path. Decisions locked here to protect that:

1. **Core-ownership invariant generalizes:** core `c` on node M owns `{ p : owner(p)==M and p % N_M == c }`. The stock TPC client computes `partitionId % tpcChannels.length` per-member, so this matches by construction with **no client-visible change** when partitions are spread across nodes.
2. **The partition table is authoritative and server-published** (v0.1 advertises one member; multi-node only changes its contents).
3. **Member-to-member protocol will be custom (BonsaiGrid-only).** Only the client protocol must be Hazelcast-compatible.
4. **Store data grouped by partition** (not purely by key hash) so a single partition's entries can be enumerated for replication/migration without a full scan. This is the one v0.1 *storage* decision that de-risks Phases C/D.

Phased roadmap: **A** v0.1 single node → **B** static multi-node cluster (no migration/replication) → **C** replication + backup-acks (the value milestone) → **D** dynamic membership, migration, split-brain protection.
