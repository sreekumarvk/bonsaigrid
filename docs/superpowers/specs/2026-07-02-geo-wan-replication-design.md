# Geo-replication / WAN — Design

**Date:** 2026-07-02
**Status:** Approved scope (active-active, IMap-first). Design record for implementation.
**Gap:** Platform-gap roadmap Gap 4 — the last untouched major gap.
**Memory:** `bonsaigrid-streaming-depth` (adjacent), `bonsaigrid-persistence` (reused spine).

## Goal

Active-active asynchronous cross-cluster replication of IMap updates. Both
clusters accept writes; each ships its committed mutations over a WAN link to the
other, and concurrent writes converge via the existing HLC/LatestUpdate merge. A
WAN-link outage loses nothing: outbound mutations sit in a durable buffer and
replay on reconnect. All of this runs **off the hot path** — the request loop
never touches disk or the WAN network.

This is Hazelcast's WAN Replication (the platform-diagram "Disaster Recovery and
Geo-Replication" box). Active-passive (one-way DR) is a config of active-active
(a cluster with no reverse target).

## Non-Goals (v1)

- Structures other than IMap (queue/list/set/multimap/ringbuffer/pncounter) — a
  fast follow (Phase D) reusing the same capture seam.
- Merge policies beyond HLC LatestUpdate (Hazelcast's PassThrough / custom merge
  policies).
- WAN over TLS (reuses the member mTLS bundle later; v1 is plaintext WAN or the
  existing transport TLS as-is).
- Delta/compression of the WAN stream; WAN event filtering by map/predicate.
- More than a simple static target list (no dynamic WAN topology / discovery).

## Decisions (from scoping)

1. **Active-active**, bidirectional, with loop prevention. HLC merge makes
   concurrent writes converge deterministically.
2. **IMap first**; other structures follow via the same `WalSink` capture.
3. **Reuse, don't reinvent:** capture = the store `WalSink` tap; durable buffer =
   the persistence `WalSegment` discipline; transport = the member io_uring
   `Transport`; remote apply + conflict resolution = `put_merge` + `observe_stamp`.
4. **At-least-once** delivery. `put_merge` is idempotent under the stamp, so
   re-delivery after a reconnect dedups for free.
5. New crate **`crates/wan`** for the pure logic (record, queue, publisher,
   consumer, wire codec); server wiring + the WAN thread live in the server crate.

## Architecture

Each cluster runs **both** roles (publisher + consumer):

```
                        ┌──────────────── cluster A ────────────────┐
 client write ─put──►  store  ─WanSink─► SPSC ─► WanQueue (durable) ─► WAN thread ─┐
                        ▲                                                          │ batch
             apply_wan  │ (HLC put_merge; NOT re-captured → no loop)              │ TCP
                        └──────────────── WanConsumer ◄────── remote batches ◄─────┼─────┐
                                                                                   │     │
                        (mirror image on cluster B) ───────────────────────────────┘     │
                        remote acks ◄──────────────────────────────────────────────────┘
```

### Component 1 — `WanRecord` (`crates/wan`)

The unit of replication: `{ op: Put|Remove, map: String, key: Vec<u8>, value:
Vec<u8>, stamp: u64 }` (`stamp` is the source HLC stamp). Byte codec
`encode`/`decode` (little-endian, length-prefixed), mirroring the persistence
record framing. A `WanBatch` is a sequence-numbered run of records.

### Component 2 — `WanQueue` (durable outbound buffer, `crates/wan`)

Per-remote-target durable queue over the persistence `WalSegment`
(append + group-commit fsync + torn-tail-safe replay):
- `append(record)` — frame + append (from the WAN thread, not the hot path).
- A monotonically increasing **sequence number** per appended record.
- A durable **committed cursor** (`acked_seq`): the highest sequence the remote
  has acknowledged, persisted in a small meta file (fsync'd on advance).
- `unacked() -> iterator` — records with `seq > acked_seq`, for (re)shipping.
- `ack(up_to_seq)` — advance + persist the cursor; segments fully below the
  cursor may be truncated/rolled (reuse the persistence snapshot/roll pattern).
- **Bound + backpressure:** a configured max size (bytes); when exceeded, the
  policy is `throw` (reject/park the producing write via a full SPSC ring) or
  `drop-oldest` (advance the tail, log the loss). Default `throw`.

### Component 3 — `WanPublisher` (capture, `crates/wan` + store hook)

A `WalSink` attached to the store's **new `wan_sink` slot**. Every local IMap
mutation (`map_put`/`map_remove`) is encoded to a `WanRecord` and pushed to the
WAN thread over an SPSC ring (zero hot-path disk/network). The WAN thread appends
it to the `WanQueue`.

### Component 4 — WAN thread (server)

Owns the disk + WAN socket for a target. Loop:
- Drain the capture ring → `queue.append`.
- Ship `queue.unacked()` to the remote in batches over the transport; on a
  `WanAck{up_to_seq}`, `queue.ack(up_to_seq)`.
- On link loss: stop shipping; the queue keeps growing (bounded). On reconnect:
  re-ship from `acked_seq` (at-least-once).
- Group-commit fsync cadence for appends (reuse the persistence-thread pattern).

### Component 5 — `WanConsumer` (remote apply, `crates/wan` + store hook)

Receives `WanBatch`es, applies each record via the **new `store.apply_wan(op,
map, key, value, stamp)`** = `put_merge(..., latest_update=true)` that emits to
`wal_sink` (persist the WAN-applied write) **but not `wan_sink`** (so it is never
re-published — loop prevention). Calls `observe_stamp(stamp)` so the local HLC
tracks the peer clock. Replies `WanAck{up_to_seq}` for the batch's highest seq.

### Component 6 — WAN wire codec (`crates/wan`)

`WanMsg::Batch{ up_to_seq, records: Vec<WanRecord> }` and `WanMsg::Ack{ up_to_seq
}`, encode/decode. Carried over the member io_uring `Transport` (bind on a WAN
port; dial the remote target's WAN endpoint). Reuses the transport's framing;
WAN is a distinct connection/role from intra-cluster member links.

### Store hooks (`crates/store`)

- `wan_sink: OnceLock<Arc<dyn WalSink>>` + `set_wan_sink(...)`. `put`/`put_ttl`/
  `put_merge`/`remove` emit to `wan_sink` **in addition to** `wal_sink` (after
  the in-memory apply, same as the persistence emit).
- `apply_wan(map, key, value, stamp)` — `put_merge` semantics that skip
  `wan_sink` (persist yes, republish no). The single mechanism that prevents WAN
  loops.

## Loop Prevention & Convergence (why active-active is safe)

- **No echo:** a record received via WAN is applied with `apply_wan`, which does
  not touch `wan_sink`, so it is never queued back to its origin. A locally
  originated write goes to both sinks (persist + publish).
- **Convergence:** both clusters stamp writes with HLC (`physical_ms:41 |
  counter:16 | member:7`). `put_merge` keeps the higher-stamped value; `observe_
  stamp` advances the local clock on inbound stamps. Concurrent writes to one key
  converge to the same value on both sides — the same property the deterministic
  sim already verifies for migration/replication.
- **At-least-once + idempotence:** re-shipping after a reconnect re-applies
  records; `put_merge` under an equal-or-lower stamp is a no-op, so duplicates are
  harmless.

## Testing Strategy

The verifiable core is a deterministic **two-cluster in-process sim** (mirrors the
member/CP sims): two `Store`s, each with a `WanPublisher` + `WanConsumer`, an
in-memory WAN link with fault injection.

- **Unit:** `WanRecord` codec roundtrip; `WanQueue` append → `unacked` → `ack`
  advances the durable cursor; queue survives reopen (durable).
- **Capture / loop prevention:** a local write is captured for WAN; a record
  applied via `apply_wan` is NOT captured (no echo).
- **One-way replication:** A writes k=v → ships → B applies → B reads v.
- **Active-active convergence:** A and B each write the same key concurrently
  (different HLC stamps) → after exchange, both hold the higher-stamped value.
- **Outage + replay:** sever the WAN link, A takes writes (queued, durable),
  reconnect → B receives all of them, converges; the queue cursor advances.
- **At-least-once dedup:** re-deliver a batch → no double-apply (stamp-guarded).
- **Durability:** reopen a `WanQueue` after "crash" → unacked records replay; a
  torn tail is dropped.

Live TCP over two real clusters is the integration layer (Phase C) — validated
by hand + the loopback transport tests; the logic is the sim's job.

## Guardrail Compliance

- The `WanSink` pushes to an SPSC ring; the WAN thread owns all disk + socket
  work — the reactor hot path never blocks on WAN (mirrors persistence/member
  threads). Zero-alloc hot path preserved when WAN is disabled (`wan_sink` unset
  = lock-free no-op, same as `wal_sink`).
- No `Mutex`/`RwLock` across cores for WAN; cross-thread hand-off is SPSC only.
- Conflict resolution is the existing HLC merge (no new shared mutable state).

## Phasing

- **A — capture + durable queue:** store `wan_sink`/`apply_wan`, `WanRecord`
  codec, `WanQueue` (durable + cursor + bound). Self-contained; unit + durability
  tested.
- **B — publisher↔consumer + convergence (acid test):** WAN codec + batching/ack;
  the two-cluster sim (one-way, active-active convergence, loop prevention,
  outage-replay, dedup).
- **C — live TCP + config + backpressure:** WAN thread over the real transport to
  a remote endpoint; `BONSAI_WAN_TARGETS` + batch/bound/backpressure config.
- **D — structures:** extend capture to `aux_state` for the non-map structures.

## Config

- `BONSAI_WAN_TARGETS` — remote cluster WAN endpoints (host:port list).
- `BONSAI_WAN_PORT` — this cluster's inbound WAN listen port.
- `BONSAI_WAN_BATCH` — max records per batch (default 256).
- `BONSAI_WAN_QUEUE_MB` — outbound queue bound (default 256).
- `BONSAI_WAN_BACKPRESSURE` — `throw` (default) | `drop-oldest`.
- Conflict policy: HLC LatestUpdate (fixed in v1).

## Open Questions / Risks

- **Queue truncation vs cursor:** segments below the acked cursor are reclaimable;
  reuse the persistence snapshot/roll to bound disk. A slow/dead remote pins the
  queue (bounded → backpressure fires) — same trade-off as Hazelcast.
- **Clock skew across datacenters:** HLC's physical component can diverge across
  regions; `observe_stamp` absorbs peer stamps so ordering stays monotonic, but a
  far-future remote stamp would advance the local clock. Acceptable (matches the
  intra-cluster HLC behavior); bounded-skew guarding is a follow-up.
- **WAN TLS:** deferred; the member mTLS bundle can be reused for the WAN
  connection in a later pass.
- **Initial sync of pre-existing data:** v1 replicates from the point WAN is
  enabled forward; a full-state bootstrap (ship a snapshot first) is a follow-up.
