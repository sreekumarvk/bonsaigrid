# MultiMap Key-Partitioned HA — Design Spec

**Date:** 2026-06-22
**Epic:** Make MultiMap survive node loss (the follow-up the HA-completeness epic deferred).
**Status:** Approved (autonomous), pending implementation.

## Context

MultiMap is **key-partitioned** like IMap: the client routes `mm.put(name,key,val)`
to the *key's* partition. The HA-completeness epic's name-based state mechanism
replicated it to the wrong backup, so it was excluded. This epic gives MultiMap
correct per-key replication and migration.

## Decision

Treat each `(name, key)` as a key-partitioned entry holding a **value-set**.
Replicate/migrate that entry to `backups_of(partition_id(key))`, **state-per-key**
(ship the whole current value-set; install replaces). Synchronous, reusing the
IMap `Replicator`/`Pending` deferral — consistent with everything else.

## Components

- **`store`** (multimap is its own `Mutex<HashMap<String, HashMap<Vec<u8>, Vec<Vec<u8>>>>>`):
  - `mm_install(name, key, values)`: set `multimaps[name][key]=values`; if `values`
    is empty, remove the key (and the map if it becomes empty).
  - `mm_entries_for_partition(p, count) -> Vec<(String, Vec<u8>, Vec<Vec<u8>>)>`:
    every `(name,key,values)` whose `partition_id(key)==p`. (Uses
    `serialization::partition_id` on the key Data.)
- **`member::wire`**: `BackupMm { op_id, name, key, values }` (kind 12) and
  `MigrateMm { generation, partition, entries }` (kind 13).
- **`member_thread`**: backup side installs `BackupMm`/`MigrateMm` via `mm_install`
  and acks `BackupMm`; the migration stream sends `MigrateMm` (multimap entries for
  the partition) alongside IMap + aux.
- **`handlers`**: `mm_put`/`mm_remove` apply locally, then synchronously replicate
  the `(name,key)` value-set to `backups_of(partition_id(key))` via a
  `replicate_mm` helper (defer the client reply until ack). Re-add `131328`/`131840`
  to the quorum gate.

## Data flow

`mm.put(name,key,val)` → owner applies → `partition = partition_id(key)` →
`values = mm_get(name,key)` → defer; backups install via `BackupMm`; deliver on
ack. Migration: for each migrating partition the new owner streams `MigrateMm`
(its multimap entries for that partition) → backup installs.

## Testing

- Unit: `mm_entries_for_partition` round-trips through `mm_install` (fresh store);
  empty value-set removes the key.
- E2E: extend `structure_ha_smoke` to include MultiMap (must now survive); a
  `multimap_ha_smoke` that fills a MultiMap with many keys, kills the owner, and
  reads them back. Double-failover still green.

## Non-goals

- MultiMap aggregate ops across partitions (`mm_size` scatter-gather) — pre-existing
  per-member gap, separate.
