# Phase D: Dynamic Membership — Design Spec

**Date:** 2026-06-21
**Epic:** Multi-node Phase D — heartbeat failure detection + automatic failover, dynamic join, partition migration, quorum + per-entry split-brain merge.
**Status:** Approved (design), pending implementation plan.

## Context

Phases A–C are complete: a static multi-node cluster with a stock smart client
(B), synchronous backup replication, and an **explicit** promotion mechanism (C)
left as the seam for this phase. Today membership is a fixed list passed at
startup; a dead member is only promoted by a manual `POST /cluster/promote`.

Phase D makes membership **dynamic and self-managing**: members detect each
other's death and fail over automatically, new members join at runtime and
receive their partitions via data migration, a quorum gate prevents a minority
from diverging, and on split-brain heal entries are reconciled by a merge policy.

### Locked decisions (from brainstorming)

1. **Coordinator model:** the **oldest live member is the master**. It owns the
   authoritative member list + a monotonic **generation**, finalizes every
   membership change, and publishes updates. Master death promotes the
   next-oldest. (Hazelcast's model.)
2. **Partition table is derived, not shipped.** Ownership is the existing
   ring assignment over the *current member list* (`owner(p)`, `backups_of(p)`).
   The master publishes only the **member list + generation**; every member
   computes the identical table locally.
3. **Split-brain depth:** quorum minimum *plus* per-entry merge policy
   (`LatestUpdate` / `PutIfAbsent`) on heal.
4. **Failure detector:** simple deadline (miss heartbeats past a timeout → dead),
   configurable interval/timeout. Not phi-accrual.
5. **Join discovery:** static **seed list** (`BONSAI_SEEDS`), like Hazelcast
   TCP-IP join.
6. **Migration trigger:** any membership change → master recomputes the table →
   migrates every partition whose owner or backup set changed. One mechanism
   covers join, death-rebalance, and graceful leave.

### Guardrails (unchanged, from CLAUDE.md)

- Zero-allocation client hot path; member/coordination traffic is off it.
- Shared-nothing across threads except SPSC rings and the existing
  `Mutex`-guarded `Arc<EventBroker>` / sharded `Arc<Store>`.
- Kernel-bypass io_uring; no blocking I/O in the client hot path.
- Only the client protocol is Hazelcast-compatible; the member protocol is ours.

## Goals / Non-goals

**Goals**
- Heartbeat failure detection with **automatic failover** (no manual trigger),
  including master re-election.
- Live cluster-view push to **connected** clients on every membership change
  (the piece Phase C deferred).
- Runtime **join** via seed list + **partition migration** (bulk data transfer).
- **Quorum** write-gate and **per-entry merge** on split-brain heal.

**Non-goals (documented limitations)**
- Phi-accrual detection, WAN replication, rack/zone awareness.
- Minimizing partition movement (we use modulo ring assignment, not Hazelcast's
  movement-optimal algorithm — correctness over optimality).
- Persistent storage / recovery from disk.
- Fully automated network-partition e2e in CI: the *merge logic* is unit-tested
  and a *scripted heal scenario* exercises it, but simulating a real kernel-level
  network partition between processes is out of automated scope.

## Architecture

```
        client port 5701+i                         member mesh (io_uring, 7701+i)
   ┌─────────────────────────┐   reverse SPSC   ┌──────────────────────────────┐
   │  Client reactor          │◀── ClusterEvent ─│  Member thread               │
   │  - auth/cluster-view from │                 │  - heartbeats (send + detect) │
   │    authoritative Cluster  │   fwd SPSC      │  - coordinator (master logic) │
   │  - push view events to    │── MemberJob ───▶│  - migration source/sink      │
   │    LIVE clients on change  │                 │  - replication (Phase C)      │
   └─────────────┬─────────────┘                 └──────────────┬───────────────┘
                 │ Rc<RefCell<Cluster>> (authoritative)          │ own Cluster copy
                 ▼                                               ▼
            Arc<Store> ◀───────────────── shared ──────────────▶ Arc<Store>
```

Two SPSC rings now connect the planes: the existing **forward** ring
(reactor → member, `MemberJob`) and a new **reverse** ring (member → reactor,
`ClusterEvent`) so coordination decisions made on the member thread update the
authoritative `Cluster` and notify clients. The reactor drains the reverse ring
on its existing 20 ms event tick.

### Components

1. **`member::wire`** — new message kinds (all big-endian, length-prefixed):
   `Heartbeat{from,generation}`, `JoinRequest{uuid,client_port,member_port}`,
   `MemberView{generation,master_join_id,members:[(uuid,host,client_port,member_port,join_id)]}`,
   `MigrateStart{generation,partition}`, `MigrateChunk{generation,partition,entries:[(map,key,value,stamp)]}`,
   `MigrateEnd{generation,partition}`.

2. **`server::membership::Cluster`** — extended:
   - Each member carries `uuid`, addresses, and `join_id` (monotonic join order;
     lower = older). `master()` = alive member with the smallest `join_id`.
     `is_master(self)`.
   - `generation: u64`. `apply_view(MemberView)` replaces the member list if the
     incoming generation is **newer** (stale views ignored).
   - `add_member` / `remove_member` (master-only mutators) bump generation.
   - `quorum: usize`; `has_quorum()` = `live_count() >= quorum`.
   - Ring `owner`/`backups_of` unchanged (operate over alive members ordered by
     join_id).

3. **`server::cluster_coordinator`** — the brain, run on the member thread inside
   the transport `Handler`:
   - **Heartbeats:** on each tick send `Heartbeat` to all known peers at
     `HB_INTERVAL`; track `last_seen[peer]`. A peer past `HB_TIMEOUT` (and past
     the post-join grace period) is *suspect*.
   - **Master role:** if `is_master(self)`, on a suspect death remove the member,
     bump generation, broadcast `MemberView`, and schedule migrations. On a
     `JoinRequest`, add the member, bump generation, broadcast `MemberView`,
     schedule migrations.
   - **Non-master:** apply received `MemberView`s; if the current master is
     suspect, recompute master locally — if *self* is the new master, take over
     (broadcast the next `MemberView`).
   - **Reverse signal:** whenever the local `Cluster` changes, push a
     `ClusterEvent::View(cluster_snapshot)` up the reverse ring so the reactor
     updates the authoritative copy + notifies clients.

4. **`server::migration`** — for a scheduled `(partition, dest)` the holder
   streams `MigrateStart`/`MigrateChunk`(batched)/`MigrateEnd`; the destination
   applies entries to its store (respecting per-entry stamps via the merge
   policy). Generation-tagged and idempotent; a stale-generation migration is
   dropped.

5. **Store change — per-entry stamp.** Each IMap value gains a monotonic update
   `stamp: u64` (a per-member logical counter; ties broken by member uuid). Used
   by `LatestUpdate` merge. `put_ttl` records the stamp; a new
   `put_merge(map,key,value,stamp,policy)` applies merge semantics. Reads/normal
   puts are unchanged on the wire (stamp is server-internal metadata).

6. **Reactor integration.** `reactor::run` gains an optional `on_tick` hook (or
   reuses the 20 ms timer) to drain the reverse ring; on a `ClusterEvent::View`
   it updates the authoritative `Cluster` and appends `members_view` +
   `partitions_view` events (bumped versions) to **every** live binary client
   connection. Writes consult `Cluster::has_quorum()`; below quorum they return a
   Hazelcast error response instead of applying.

## Data flow

**Heartbeat / death (D1).** Member thread ticks → sends `Heartbeat` to peers →
updates `last_seen`. Master sees peer X stale → `Cluster::remove_member(X)` (gen++)
→ broadcast `MemberView` → schedule migrations to restore K backups → push
`ClusterEvent::View` to reactor → reactor updates authoritative `Cluster` +
pushes view events to live clients. Clients re-route to the surviving owner
(which holds the data from Phase C replication). No manual step.

**Master election.** If the master is the stale member, each survivor recomputes
`master()` from its member list minus the suspect. The new master (next-oldest)
removes the dead master, bumps generation, and broadcasts — its higher generation
wins, so the cluster converges on one master.

**Join + migration (D2).** New member boots with `BONSAI_SEEDS`, connects, sends
`JoinRequest` to the master. Master adds it (new highest `join_id`), gen++,
broadcasts `MemberView`. Every member recomputes the table; the master diffs old
vs new and schedules migrations for changed partitions. Holders stream entries to
new holders; on `MigrateEnd` the partition is live on the destination. Clients
get the new `partitions_view` and route to the newcomer.

**Quorum + merge (D3).** A write checks `has_quorum()`; below it → error response
(reads unaffected). On heal, two sub-clusters reconnect with different
generations; the lower-generation members rejoin the higher master, which runs a
**migration with merge**: for each incoming `(key,value,stamp)`, apply
`merge_policy` — `LatestUpdate` keeps the higher stamp, `PutIfAbsent` keeps the
existing. Configurable via `BONSAI_MERGE` (default `LatestUpdate`).

## Configuration

| env | meaning | default |
|-----|---------|---------|
| `BONSAI_MEMBERS` | bootstrap cluster size (existing; static seed count) | 1 |
| `BONSAI_MEMBER_INDEX` | this member's bootstrap index / join order | 0 |
| `BONSAI_BACKUPS` | sync backup count K (existing) | 1 |
| `BONSAI_SEEDS` | comma-separated `host:member_port` seeds for join | derived from MEMBERS |
| `BONSAI_QUORUM` | minimum live members to accept writes | 1 |
| `BONSAI_MERGE` | `LatestUpdate` \| `PutIfAbsent` | LatestUpdate |
| `BONSAI_HB_INTERVAL_MS` / `BONSAI_HB_TIMEOUT_MS` | heartbeat cadence / death deadline | 500 / 3000 |

Single-node mode (`BONSAI_MEMBERS=1`) starts no member thread; behavior unchanged.

## Error handling

- **Generation guard:** every `MemberView`/migration carries a generation; lower
  than local is ignored. Prevents flapping, reordered messages, and dueling
  masters (highest generation wins).
- **Join grace:** a newly added member isn't eligible to be declared dead for
  `HB_TIMEOUT * 2` after join (avoids killing a slow starter).
- **Migration mid-failure:** if a holder dies mid-migration, the master reschedules
  from the surviving holder at the new generation; partial chunks on the
  destination are overwritten by the retry (idempotent by key+stamp).
- **Quorum loss:** below quorum, writes error out but the member stays up and
  serving reads; it recovers automatically when quorum is restored.
- **Self-suspect:** a member that finds itself partitioned from the master (no
  view, no quorum) stops accepting writes rather than forming a rival cluster.

## File structure

- `crates/member/src/wire.rs` — new message kinds + codec (extend existing).
- `crates/server/src/membership.rs` — `Cluster` join-order/master/generation/quorum.
- `crates/server/src/cluster_coordinator.rs` (new) — heartbeats, election, join,
  migration scheduling (member-thread logic).
- `crates/server/src/migration.rs` (new) — partition transfer source/sink + merge.
- `crates/server/src/member_thread.rs` — host the coordinator in the `Handler`;
  add the reverse ring producer.
- `crates/server/src/reactor.rs` — reverse-ring drain hook + live cluster-view
  broadcast.
- `crates/server/src/handlers.rs` — quorum write-gate; stamp on writes.
- `crates/store/src/lib.rs` — per-entry stamp + `put_merge`.
- `crates/server/src/main.rs` — wire seeds/quorum/merge/HB config, both rings.
- `conformance-python/auto_failover_smoke.py`, `dynamic_join_smoke.py`,
  `quorum_smoke.py` + harness scripts.

## Implementation phasing (the plan will follow this; each lands working)

- **D1 — heartbeats + auto-failover + live cluster-view push.** Deliverable:
  kill a member with no manual promote; a connected client keeps working.
- **D2 — dynamic join + partition migration.** Deliverable: start an extra member
  at runtime; it serves its partitions; clients read all keys.
- **D3 — quorum gate + per-entry merge.** Deliverable: writes rejected below
  quorum; merge unit tests + scripted heal scenario.

## Testing

**Unit:** master election from join_ids; `apply_view` generation guard; ring
reassignment on add/remove; quorum gate; merge policy (`LatestUpdate` keeps higher
stamp, `PutIfAbsent` keeps existing); migration chunk encode/decode.

**Integration (Rust, in-process):** two/three coordinators over loopback member
ports — a simulated heartbeat gap promotes the right successor; a join triggers
the expected migrations and the destination store ends with the right entries.

**E2E (stock client):**
- `auto_failover_smoke.py`: 3 members (K=1), put N keys, SIGKILL one **without**
  promoting; after the heartbeat timeout a client reads all keys.
- `dynamic_join_smoke.py`: start 2 members, put N keys, launch a 3rd; after it
  joins, a client reads all keys (some now served by the newcomer) and the
  member list reports 3.
- `quorum_smoke.py`: quorum=2; drop to 1 live member; assert writes error and
  reads still work.
- Regression: existing `cluster`, `failover`, single-node, and query smokes stay
  green.
