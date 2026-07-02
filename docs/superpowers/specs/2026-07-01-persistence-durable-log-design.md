# Persistence / Durable Log (Hot Restart) — Design

**Date:** 2026-07-01
**Status:** Approved design; pending implementation plan.
**Scope:** Local durability for the in-memory store — a write-ahead log (WAL) +
periodic snapshot + recovery on restart (Hazelcast's **Hot Restart Store**).
Gap 3 of the platform-gap roadmap (`docs/hazelcast-platform-gap-roadmap.md`); its
primitives (append-only log, group-commit fsync, replay) are the shared substrate
the CP Subsystem (Raft log) and Geo/WAN (outbound queue) will reuse later.

## Goal

Let a BonsaiGrid node survive a restart with no loss of durably-acked data,
without violating the architectural guardrails (zero-allocation hot path,
thread-per-core shared-nothing, io_uring kernel-bypass, **no disk in the hot
path**). Persistence is opt-in and off by default.

## Non-Goals (v1 / this spec)

- A generic *replicated* log abstraction (Raft terms/indices). CP builds that on
  top of these primitives later.
- Cross-structure transactional atomicity in the WAL (each record is a single
  single-structure mutation).
- io_uring file I/O (the persistence thread uses plain blocking `write`/`fsync`,
  which is correct because it is off every reactor core; io_uring files are a
  later optimization).
- Cluster-coordinated Hot Restart recovery (v1 recovers each node locally;
  cluster-wide coordination is a follow-up).

## Decisions (locked in brainstorming)

1. **Durability is configurable**, `BONSAI_PERSISTENCE = none | async | sync`.
   The feature is **off by default** (`none`); `async` is the recommended mode
   when enabling; `sync` gives fsync-before-ack.
2. **A dedicated per-node persistence thread**, fed by the reactor cores over
   lock-free SPSC rings (mirroring the existing member/replication thread). All
   disk I/O is off the reactor cores.
3. **IMap first (Phase A)**, on a **structure-agnostic** WAL envelope + snapshot
   container, so the remaining structures (Phase B) are purely additive. The
   item is complete only after Phase B.

## Architecture

New crate `crates/persistence` owns the pure, reusable pieces (record codec,
snapshot codec, WAL segment writer, recovery/replay). A per-node persistence
thread consumes SPSC rings from the reactor cores and owns the on-disk state.

```
reactor core (mutation) --apply in-mem (unchanged)--> ack (async)
       |  push WAL record (pooled buffer, no alloc)
       v
  SPSC ring  -->  [persistence thread]
                    | append records to WAL segment
                    | group-commit fsync
                    | reverse ring: highest durable op_id  --> releases sync acks
                    | periodic: write sectioned snapshot + truncate WAL
on restart: load snapshot -> replay WAL tail -> open listeners
```

- On a mutation the core applies to the in-memory store exactly as today, then
  (if persistence is enabled) pushes a WAL record to its ring. On `none` the ring
  and record building are compiled out of the hot loop.
- The persistence thread is the *only* disk writer. Blocking `write`/`fsync` on
  that thread never blocks a reactor core.
- Local durability (fsync) and backup replication (backup-ack) are **orthogonal**
  guarantees. When both are `sync`, the client ack waits for both — the existing
  `Pending` deferred-response accounting is extended to track an fsync condition
  alongside the backup-ack count.

## Component 1 — WAL record envelope (structure-agnostic)

A length-prefixed, CRC-guarded frame:

```
[ len: u32 ][ crc32: u32 ][ record_type: u16 ][ payload: len bytes ]
```

- `crc32` covers `record_type` + `payload`. Recovery stops cleanly at a torn tail
  (a partial/corrupt final record from a crash mid-write) rather than misreading.
- `record_type` (v1): `MapPut`, `MapRemove`. Later structures add new values
  (`QueueOffer`, `QueuePoll`, `SetAdd`, `ListSet`, `MultiMapPut`, …) — **additive,
  no format migration.**
- `MapPut` payload: `stamp: u64 | ttl_ms: u64 | map | key | value` (each
  length-prefixed). `MapRemove`: `stamp: u64 | map | key`.
- Records are built into a **pooled per-core buffer** and copied into the SPSC
  ring's slot; no per-request heap allocation.

## Component 2 — Persistence thread & durability levels

- **Drain + group-commit**: the thread pulls all records available across the
  rings, `write()`s them to the current WAL segment, then issues one `fsync`
  covering the batch. On completion it publishes the highest durable `op_id` on a
  reverse SPSC ring.
- **`async`**: the reactor acks the client immediately after the in-memory apply;
  the thread fsyncs on its own cadence (every `flush_interval_ms`, default ~10 ms,
  or when a batch-size threshold is hit). A crash loses at most the last unflushed
  window.
- **`sync`**: the reactor **defers** the client ack (returns no immediate reply,
  exactly like a replicated write awaiting backup-acks) and tags it with the WAL
  `op_id`. When the reverse ring reports that `op_id` durable, the deferred reply
  is delivered. Reuses the sync-backup deferred-response path.
- **`none`**: no ring, no records, no thread — zero cost.
- **Backpressure**: if a ring is full (persistence can't keep up), an `async`
  write proceeds (best-effort, logged); a `sync` write's ack waits (natural
  backpressure). A full ring is surfaced as a metric, never silently dropped for
  `sync`.

## Component 3 — Snapshot (sectioned container) & truncation

- **Snapshot format**: a header + a sequence of typed **sections**:
  ```
  [ magic | version | stamp_watermark ]
  [ section: MapEntries  | count | (map,key,value,stamp)* ]   # v1
  [ section: AuxState    | ... ]                               # Phase B
  [ section: MultiMap    | ... ]                               # Phase B
  ```
  v1 writes only `MapEntries` from `store.all_entries_stamped()`. Phase B adds
  sections from the existing `aux_state_for_partition` / `mm_entries_for_partition`
  serializers.
- **Trigger**: by WAL size threshold (default) and/or interval, on the
  persistence thread.
- **Atomic install**: write `snapshot.tmp`, `fsync`, `rename` to `snapshot.N`
  (atomic) — a crash never leaves a half-written snapshot.
- **Truncation**: once `snapshot.N` is durable, records it supersedes are
  obsolete; the thread rolls to a new WAL segment and deletes the old segment(s)
  and older snapshots. Writes continue throughout (the snapshot reads a consistent
  stamped view; concurrent mutations land in the new segment).

## Component 4 — Recovery

On startup, if `BONSAI_PERSISTENCE_DIR` contains state, **before opening
listeners**:

1. Load the newest valid `snapshot.N` into the store (apply each section).
2. Replay the WAL segment(s) after the snapshot point, in append order,
   CRC-checking each record and **stopping cleanly at a torn final record**.
3. Each replayed record is applied via a `match record_type { … }` dispatch,
   re-hashing the key to its owning core/shard (so a changed core count still
   recovers correctly — replay is a full re-shard).

Recovery is idempotent under stamps: replaying a record whose stamp is ≤ the
stored stamp is a no-op (reuses `put_merge` semantics), so a snapshot+tail overlap
is harmless.

## Configuration

- `BONSAI_PERSISTENCE` = `none` (default) | `async` | `sync`.
- `BONSAI_PERSISTENCE_DIR` = directory for WAL segments + snapshots (required when
  enabled).
- `BONSAI_PERSISTENCE_FLUSH_MS` (default 10) — async fsync cadence.
- `BONSAI_PERSISTENCE_SNAPSHOT_MB` (default 64) — WAL size that triggers a
  snapshot + truncation.

## Testing Strategy

- **Unit**: record encode/decode + CRC (incl. a **torn-tail** record → replay
  stops cleanly); snapshot section round-trip; replay dispatch applies each record
  type; stamp-idempotent replay (lower stamp is a no-op).
- **Integration**:
  - **Crash-recovery acid test**: write N keys, drop the store, recover from
    `DIR`, assert every key/value/stamp is present.
  - **`sync` durability**: a `sync` write's ack is not delivered until the fsync
    signal arrives (drive the persistence thread deterministically).
  - **Snapshot boundary**: force a snapshot + truncation mid-workload, then crash
    + recover → correctness preserved across the boundary.
  - **`none` is a no-op**: existing behavior unchanged; no files created.
- **Guardrail**: extend the zero-alloc harness — a `MapPut` on the persistence
  hot path (record build + ring push) allocates zero times after warmup.

## Guardrail Compliance

- **Zero-alloc hot path**: records build into pooled per-core buffers and are
  copied into pre-sized SPSC ring slots; no per-request allocation. `none`
  compiles the path out.
- **Thread-per-core shared-nothing**: cores never touch disk and never share the
  WAL; each pushes to its own SPSC ring. The persistence thread owns all disk
  state. No `Mutex`/`RwLock` across cores for persistence.
- **No disk in the hot path**: all `write`/`fsync` are on the dedicated thread;
  `sync` durability is expressed as a deferred ack, never a blocking call on a
  reactor core.

## Phasing (completion gated on Phase B)

- **Phase A — IMap persistence**: `crates/persistence` (record + snapshot codecs,
  WAL segment writer, recovery), the persistence thread + SPSC wiring, durability
  levels (incl. `sync` deferred-ack integration with `Pending`), `MapEntries`
  snapshot + truncation, recovery, config. Fully testable and shippable.
- **Phase B — remaining structures**: record types + snapshot sections + replay
  arms + op-path wiring for queue / list / set / multimap / ringbuffer / pncounter
  (reusing the migration serializers). **The persistence item is complete only
  after Phase B.**

## Open Questions / Risks

- **Snapshot vs concurrent writes consistency**: `all_entries_stamped()` locks
  each shard in turn, so the snapshot is per-shard-consistent but not a single
  global instant. That is fine under stamp-idempotent replay (a write that races
  the snapshot is either in the snapshot or in the post-snapshot WAL, never lost),
  but must be verified by the snapshot-boundary test.
- **Persistence-thread throughput** as a single writer: acceptable for v1; per-core
  WAL segments (still one thread, multiple files) or io_uring file writes are the
  escape hatch if it bottlenecks. Surface fsync latency + ring-full as metrics.
- **fsync durability across the stack** (fs/drive write cache): documented as an
  operational requirement; out of scope to enforce.
