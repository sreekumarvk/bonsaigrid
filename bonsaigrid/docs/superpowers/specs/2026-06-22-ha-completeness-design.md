# HA Completeness — Design Spec

**Date:** 2026-06-22
**Epic:** Make the whole grid survive node loss — replicate + migrate ALL data
structures (not just IMap), and restore the backup count (K) after a death.
**Status:** Approved (design), pending implementation plan.

## Context

Phases C/D made IMap highly available: synchronous backups, automatic failover,
dynamic join + migration, quorum. But two HA gaps remain:

1. **Only IMap is replicated/migrated.** The auxiliary structures (IList, ISet,
   IQueue, MultiMap, Ringbuffer, PNCounter, FlakeIdGenerator) live in the owner's
   memory only. A node loss silently drops their data.
2. **No restore-K.** After a death, a partition's surviving backup becomes the
   owner but the cluster is down a replica; `migration::outgoing` only fires on
   *owner* changes, so the backup count is never restored. A second death can
   then lose data that should have been safe.

This epic closes both, behind the unmodified Hazelcast client protocol.

### Locked decisions (from brainstorming)

1. **State-based HA for the auxiliary structures.** They are single-partition and
   low-throughput, so on a mutation the owner serializes that partition's
   auxiliary state and ships it to the backups. One "install partition state"
   path serves replication, migration, and restore-K. **IMap is unchanged**
   (op-based synchronous backups + entry-stream migration).
2. **Synchronous** auxiliary backups — the mutating op defers the client reply
   until the backup has installed the new state, reusing the existing
   `Replicator`/`Pending` deferral. An acked auxiliary write survives node loss.
3. **Restore-K via a generalized migration plan** — a partition's holders =
   `{owner} ∪ backups`; after any membership change the new owner streams state
   to every *new* holder that wasn't an old holder. This single rule covers join,
   death-rebalance, and restore-K.

### Guardrails (unchanged)

- Zero-allocation IMap hot path; auxiliary/coordination traffic is off it.
- Shared-nothing across threads except SPSC rings + the `Mutex`-guarded
  `EventBroker` / sharded `Store`.
- Kernel-bypass io_uring; member protocol is BonsaiGrid-only.

## Goals / Non-goals

**Goals**
- Synchronous backup of every auxiliary-structure mutation.
- Migration of auxiliary state on join/leave/rebalance.
- Restore-K: after a death (or join), re-replicate so every partition again has
  its configured K backups.
- Quorum gate extends to auxiliary writes.

**Non-goals (documented)**
- **MultiMap HA.** Discovered during implementation: MultiMap is **key-partitioned**
  (the client routes `mm.put(name,key,val)` to the *key's* partition, like IMap),
  not name-partitioned like IList/ISet/IQueue. So the name-based state mechanism
  is wrong for it. MultiMap HA needs IMap-style op-based replication (or storing
  its entries in the partitioned slab table) — a follow-up. It is excluded here.
- Per-op auxiliary backups (chose state-based; fine for these small structures).
- ReplicatedMap "replicate to all members" CRDT semantics (it is stored as a
  namespaced IMap and is HA as an IMap; full all-member replication is separate).
- PNCounter true CRDT merge across a split (its state is replicated for HA, but
  split-brain reconciliation of counters is out of scope).
- Compaction of migrated auxiliary state (full snapshot each time).

## Architecture

```
   client write (e.g. queue.offer)
        │
        ▼  apply locally
   handlers ── serialize partition aux state ──▶ Replicator (SPSC) ──▶ member thread
        │  defer client reply                                   │ send BackupState to backups
        ▼                                                        │ on all acks: broker.enqueue(reply)
   (no immediate reply)                                          ▼
                                              backup member: install_aux_state + Ack

   membership change ─▶ coordinator holder-diff plan ─▶ new owner streams
                        (owner ∪ backups)                MigrateChunk (IMap) + MigrateAux (aux)
                                                          to each NEW holder
```

### Components

1. **`store` (aux snapshot/install + name partitioning).**
   - `aux_state_for_partition(p, count) -> Vec<u8>`: serialize every auxiliary
     structure whose `partition_for_name(name) == p` into one self-describing
     blob (typed sections: list/set/queue/multimap/ringbuffer/pncounter/flake).
   - `install_aux_state(&[u8])`: deserialize and replace those structures.
   - `partition_for_name(name, count) -> i32`: the partition id Hazelcast assigns
     a distributed object by hashing its name as String `Data` (so the server's
     owner matches where the client routes the structure's operations).

2. **`member::wire`.**
   - `BackupState { op_id, partition, payload }` — synchronous aux replication.
   - `MigrateAux { generation, partition, payload }` — aux state during migration.

3. **`server::migration`.** Generalize `outgoing` → `plan`: for each partition,
   compare old vs new `holders` (`{owner} ∪ backups`); if `self` is the new owner
   and there is a new holder absent from the old holder set, schedule a send of
   that partition to each such new holder. (Owner-change is the special case where
   the new owner differs; restore-K is the case where only a backup is new.)

4. **`server::member_thread`.**
   - Backup side: `BackupState` → `store.install_aux_state` + `Ack`;
     `MigrateAux` → `store.install_aux_state`.
   - Sending side: on a planned migration, stream IMap entries (existing) **and**
     one `MigrateAux` (the partition's aux blob) to each new holder.

5. **`server::handlers`.** Each auxiliary **mutation** op (list_add/remove/clear,
   set_add/remove/clear, queue_offer/poll/remove/clear, mm_put/remove,
   rb_add, pn_add) applies locally, then replicates the partition's aux state
   synchronously via a new `replicate_state(...)` helper (mirrors
   `replicate_write`): defer the client reply until the backup acks. The quorum
   gate is extended to these op types.

## Data flow

**Auxiliary write (`queue.offer` shown).**
1. Handler applies `store.queue_offer(...)`, builds the client response, sets corr.
2. `p = partition_for_name(name)`. If `repl` present and `backups_of(p)` non-empty:
   `payload = store.aux_state_for_partition(p)`, push a `MemberJob::Replicate`
   whose message is `BackupState{op_id, p, payload}`, and return **deferred**.
3. Member thread sends `BackupState` to `backups_of(p)`, registers the pending
   ack; each backup installs the state and acks; on the last ack the deferred
   reply is delivered. (Identical machinery to IMap.)

**Membership change.** Coordinator computes the holder-diff plan; the new owner of
each changed partition streams `MigrateChunk*` (IMap) + `MigrateAux` (aux) to each
new holder. Covers join, death-rebalance, and restore-K uniformly.

## Error handling

- Generation guards on migration; ack-timeout backstop on deferred aux writes
  (reused from Phase C/D).
- Below quorum, auxiliary writes are rejected like IMap writes.
- Migration install **replaces** a partition's aux state (idempotent); a retried
  or duplicated `MigrateAux` is harmless.
- A death mid-migration reschedules from the surviving holder at the new generation.

## File structure

- `crates/store/src/lib.rs`: `aux_state_for_partition`, `install_aux_state`,
  `partition_for_name`, and a small typed (de)serializer for the aux blob.
- `crates/member/src/wire.rs`: `BackupState`, `MigrateAux` (kinds 10/11).
- `crates/server/src/migration.rs`: generalized `plan` (holder diff).
- `crates/server/src/member_thread.rs`: install handlers + aux send in migration.
- `crates/server/src/handlers.rs`: `replicate_state` helper + gate auxiliary
  mutations on it and on quorum.
- `crates/server/src/cluster_coordinator.rs`: use the generalized plan (also on
  death, for restore-K).
- `conformance-python/structure_ha_smoke.py` + `run_structure_ha.sh`,
  `double_failover_smoke.py` + `run_double_failover.sh`.

## Testing

**Unit**
- `aux_state_for_partition` → `install_aux_state` round-trips every structure type
  (into a fresh store) with identical contents.
- `partition_for_name` is deterministic and in range.
- generalized `plan`: a join schedules sends to the new member; a death schedules
  a send from the new owner to a fresh backup (restore-K); no change → empty.

**Integration (Rust, in-process)** — two member threads: a `BackupState` install
lands the aux structure on the backup; the deferred reply is delivered only after
the ack.

**E2E (stock client)**
- `structure_ha_smoke.py`: put data into an IQueue, IList, ISet, MultiMap (3
  members, K=1); SIGKILL the owner of those structures; after auto-failover a
  fresh client reads the same contents.
- `double_failover_smoke.py`: IMap + a queue with N entries; kill one member,
  wait for restore-K, kill a **second** member, assert all data still present —
  proving K was actually restored (single-failover survival isn't enough).
- Regression: all existing smokes (cluster, failover, auto-failover, dynamic
  join, quorum, single-node, query) stay green.
