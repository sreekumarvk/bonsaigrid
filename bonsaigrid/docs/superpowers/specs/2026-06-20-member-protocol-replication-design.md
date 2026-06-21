# Member Protocol + Phase C Replication — Design Spec

**Date:** 2026-06-20
**Epic:** Multi-node Phase C (member-to-member transport + synchronous backup replication + explicit promotion)
**Status:** Approved (design), pending implementation plan

## Context

BonsaiGrid is a wire-compatible Rust reimplementation of Hazelcast. Multi-node
**Phase B is already complete**: each member process advertises the full
membership + a deterministic partition table (`primary(p) = p % N`), and a stock
smart client routes each key to its owner. `cluster_smoke.py` round-trips 1000
keys across a live 3-member cluster.

What does *not* exist yet: members are independent processes that never connect
to each other. There is no replication, so losing a member loses its data. This
epic adds the **member-to-member transport** and **synchronous backup
replication**, plus an **explicit promotion mechanism** so a backup can take over
a dead primary's partitions.

### Locked decisions (from brainstorming)

1. **Only the client protocol is Hazelcast-compatible.** The member-to-member
   protocol is custom / BonsaiGrid-only.
2. **Transport shape:** a dedicated member thread per process with its own
   `io_uring` loop (honors the kernel-bypass guardrail); full-mesh peer
   connections; the client reactor hands work to it over an SPSC ring and never
   blocks.
3. **Replica assignment:** ring-wise. `primary(p) = p % N`,
   `backups(p) = { (p + j) % N : j in 1..=K }`. `K = BONSAI_BACKUPS` env, default
   1, capped at `N - 1`.
4. **Backup durability:** **synchronous**. The primary applies locally, ships the
   mutation to its K backups, waits for their acks, then responds OK to the
   client. Read-your-writes survives failover.
5. **Failover scope:** replication + an **explicit promotion mechanism** (a
   trigger that says "member X is dead"). Automatic failure *detection*
   (heartbeats) is deferred to Phase D and plugs into this same trigger.

### Guardrails (non-negotiable, from CLAUDE.md)

- Zero-allocation hot path after startup; pre-allocated pools.
- Shared-nothing, thread-per-core; **no `Mutex`/`RwLock` across threads** for new
  cross-thread coordination — use SPSC rings. (The existing `Arc<Store>` keeps its
  current internal sharding; this epic adds no new shared-mutable structures
  between the reactor and member threads beyond the store it already shares.)
- Kernel-bypass `io_uring`; no blocking I/O or epoll in the hot path.

## Goals / Non-goals

**Goals**
- A custom framed member transport over `io_uring`, full-mesh.
- Synchronous backup replication of IMap `put` / `put_ttl` / `remove` / `set` /
  `clear`, with the client response deferred until backups ack.
- An explicit promotion mechanism that reassigns a dead member's partitions to
  their backups and republishes the partition table to clients.
- Deterministic tests, including an e2e cluster-failover smoke proving a promoted
  backup serves the dead primary's data.

**Non-goals (deferred)**
- Automatic failure detection / heartbeats (Phase D).
- Partition migration on dynamic join/leave (Phase D).
- Replication of non-IMap structures (lists, queues, multimap, ringbuffer,
  PNCounter). They reuse the same machinery later with no rework.
- Anti-entropy / read-repair / merkle sync. A backup that was down during a write
  (and whose ack was synthesized on disconnect) may be stale; reconciling that is
  Phase D's migration concern.

## Architecture

Each member process runs **two I/O planes**:

```
            client port 5701+i                 member port 7701+i
        ┌───────────────────────┐          ┌────────────────────────┐
        │   Client reactor      │  SPSC    │   Member thread        │
 client │ (io_uring, hot path)  │ ──ring──▶│ (io_uring, own loop)   │◀── peers
   ───▶ │  apply local write    │ jobs     │  outbound: ship backups │ ──▶ peers
        │  defer client reply   │          │  inbound: apply + ack   │
        └───────────┬───────────┘          └────────────┬───────────┘
                    │  broker.enqueue(conn,resp)         │  store.put_ttl(...)
                    ▼                                    ▼
              Arc<Store>  ◀──────────── shared ──────────────▶ Arc<Store>
```

### Components (units with clear boundaries)

1. **`member::transport`** — `io_uring` peer connections + framing. Owns the
   member-port listener and the outbound connections to peers. Encodes/decodes the
   length-prefixed member messages. Exposes: "send `Msg` to member index m" and a
   callback for "received `Msg` from member index m".

2. **`member::wire`** — the message enum + encode/decode (pure, fully unit
   tested, no I/O).

3. **`member::replication`** — the replication state machine on the member thread:
   - Outbound: consume `MemberJob`s from the SPSC ring. `MemberJob::Replicate`
     fans a `BackupPut` / `BackupRemove` to `backups(p)` and registers
     `op_id → PendingOp { remaining_acks, response_bytes, conn_id }`;
     `MemberJob::Membership` swaps the member thread's `ReplicaMap` copy.
   - Inbound acks: decrement `remaining_acks`; at 0, `broker.enqueue(conn_id,
     response_bytes)` and drop the pending op.
   - Inbound mutations (this member is a backup): apply to `Arc<Store>`, reply
     `Ack{op_id}` to the sender. Never re-replicate.
   - Ack-timeout sweep: pending ops older than `ACK_TIMEOUT` get force-completed
     (response sent anyway) so a dead backup can't wedge a write.

4. **`membership::ReplicaMap`** — pure assignment: `primary(p)`, `backups(p)` from
   (member list, K). Plus `promote(dead_index)` → a new member list + partition
   table with the dead member removed and its partitions reassigned to the
   surviving backups (ring-wise). No I/O. **The reactor owns the authoritative
   copy** (it serves both client routing/cluster-view *and* the promote endpoint,
   both on the reactor thread). The member thread keeps its **own copy**, refreshed
   only via an SPSC `MemberJob::Membership` control message — no shared mutable
   state between threads (guardrail #2).

5. **Reactor integration (`handlers.rs`)** — the IMap write arms gain a
   "replicate or respond" decision: if `K>0` and peers exist, push a job and
   return `Deferred` instead of an immediate response.

6. **Promotion trigger (`server::admin`)** — an HTTP endpoint on the existing
   client-port HTTP router (e.g. `POST /cluster/promote?dead=<index>`) that calls
   `membership` to update the partition table and push cluster-view events to
   connected clients. This is the Phase-D seam.

## Member wire protocol (custom)

Length-prefixed frames: `[u32 len big-endian][u8 kind][body]`. Strings:
`[u32 len][utf8]`. Byte blobs (`key`/`value`): `[u32 len][bytes]`.

| kind | message | body |
|------|---------|------|
| 0 | `Hello` | `member_index: u32` (sent first on each new connection) |
| 1 | `BackupPut` | `op_id: u64`, `name: str`, `key: blob`, `value: blob`, `ttl_ms: u64` |
| 2 | `BackupRemove` | `op_id: u64`, `name: str`, `key: blob` |
| 3 | `Ack` | `op_id: u64` |

`op_id` is a per-primary monotonic `u64` (the primary owns the namespace; acks
are matched on the primary that issued the op). Endianness is big-endian for
header/length fields to match the rest of the codebase's network conventions;
the body is BonsaiGrid-internal so any consistent choice is fine — we use
big-endian throughout for uniformity.

Promotion sends **no** member message — it is a local membership recompute that
republishes the partition table to *clients* via the existing cluster-view event.

## Data flow

### Synchronous write (`MapPut` shown; `put_ttl`/`remove`/`set`/`clear` analogous)

1. Client → owner member's reactor (smart client already routed correctly).
2. Handler applies to the local store and builds the **client response bytes**
   (including the old value for `put`).
3. If `K == 0` or no live peers back this partition → send the response now
   (today's behavior; single-node unchanged).
4. Else → allocate `op_id`, push `MemberJob::Replicate { mutation, op_id,
   response_bytes, conn_id }` onto the SPSC ring, and return **`Deferred`** (no
   immediate reply) — identical in spirit to the existing lock-wait deferral.
5. Member thread: send the mutation to each `backups(p)`; record `PendingOp`.
6. Each backup applies and returns `Ack{op_id}`.
7. On the last ack, `broker.enqueue(conn_id, response_bytes)`. The reactor flushes
   it on its next event drain. The client sees OK only after backups are durable.

The response's `backupAcks` header field (offset 12) stays **0** — durability is
already guaranteed by the primary blocking, so the client waits for no extra
backup-ack messages. Correct regardless of the client's
`backup_ack_to_client_enabled` setting.

### Backup apply

Inbound `BackupPut{name,key,value,ttl}` → `store.put_ttl(name, key, value, ttl)`;
`BackupRemove` → `store.remove`. Reply `Ack{op_id}`. Backups do not re-replicate
and do not publish entry-listener events (the primary already did).

### Promotion (explicit)

`POST /cluster/promote?dead=<index>` on a surviving member:
1. `membership.promote(index)` removes the dead member from the member list and
   reassigns its partitions to their ring-wise backups.
2. Bump member-list-version and partition-list-version.
3. Push `members_view_event` + `partitions_view_event` to this member's connected
   clients (reusing `cluster_view`).
4. Push a `MemberJob::Membership` onto the SPSC ring so the member thread's
   `ReplicaMap` copy matches (keeps post-promotion writes correct).
5. The promoted backup already holds the data (sync replication), so the client
   re-routes and reads succeed.

Idempotent: promoting an already-removed index is a no-op.

## Configuration

| env | meaning | default |
|-----|---------|---------|
| `BONSAI_MEMBERS` | cluster size (existing) | 1 |
| `BONSAI_MEMBER_INDEX` | this member's index (existing) | 0 |
| `BONSAI_BACKUPS` | sync backup count K | 1 (capped at N−1) |

Member port = `7701 + BONSAI_MEMBER_INDEX`. Single-node mode (`BONSAI_MEMBERS=1`)
starts no member thread and behaves exactly as today.

## Error handling

- **Mesh not yet formed at startup:** the member thread retries outbound connects
  with backoff. A write needing an absent backup blocks only until `ACK_TIMEOUT`.
- **Ack timeout (`ACK_TIMEOUT`, default 5 s):** a pending op past the deadline is
  force-completed — the client response is sent anyway (availability over
  indefinite block), and a counter is bumped. A dead/slow backup never wedges the
  primary forever.
- **Peer disconnect:** mark that backup unavailable; outstanding acks it owed are
  treated as satisfied so in-flight writes complete. New writes to a partition it
  backs proceed with the remaining live backups (or immediately if none remain).
- **Promotion idempotency:** re-promoting a removed member is a no-op; promoting
  with no surviving backup for a partition logs and leaves that partition
  unowned (cannot recover data that never replicated).

## Testing

**Unit**
- `ReplicaMap`: `primary`/`backups` for representative (N,K); `promote` reassigns
  the dead member's partitions to the correct survivors; idempotency.
- `member::wire`: encode→decode round-trip for every message kind.
- `replication`: pending-ack accounting — K acks drive `remaining_acks` to 0 and
  enqueue exactly once; ack timeout force-completes.

**Integration (Rust, in-process)**
- Two members over loopback member ports: `put` on the primary; assert the
  backup's store contains the key; assert the client response is withheld until
  the ack arrives, then delivered.

**e2e (`conformance-python/cluster_failover_smoke.py`)**
- 3 members, `BONSAI_BACKUPS=1`. Smart client puts N keys. `POST
  /cluster/promote?dead=0` on members 1 and 2, then kill member 0's process.
  Assert the smart client still `get`s **all** of member 0's former keys from the
  promoted backups. This is the proof Phase C delivers value (survives node loss).
- A `run_failover.sh` harness launches the cluster, runs the smoke, tears down
  (mirrors `run_cluster.sh`).

## File structure

- `crates/member/` (new crate): `wire.rs` (messages + codec), `transport.rs`
  (io_uring peer mesh), `replication.rs` (state machine), `lib.rs`.
- `crates/server/src/membership.rs` (new): `ReplicaMap` + `promote`.
- `crates/server/src/admin.rs` (new) or extend `http_route`: the promote endpoint.
- `crates/server/src/handlers.rs`: write arms emit `Deferred` + push jobs; a
  `Deferred` sentinel in the dispatch return.
- `crates/server/src/main.rs`: spawn the member thread in `run_multi_node`, build
  the SPSC ring, pass the sender into the dispatch closure.
- `crates/spsc/`: reuse for the reactor→member ring (extend if needed).
- `conformance-python/cluster_failover_smoke.py`, `conformance-python/run_failover.sh`.

## Forward-compatibility with Phase D

- The promotion **trigger** is the only thing Phase D replaces — it swaps the
  manual HTTP call for a heartbeat-driven detector calling the same
  `membership.promote`.
- The member transport already carries framed control messages; Phase D adds a
  `Heartbeat` kind and a `MigrateChunk` kind without changing the transport.
- `ReplicaMap` already centralizes assignment; migration mutates it.
