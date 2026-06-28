# HA Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replicate + migrate ALL data structures (not just IMap) and restore the backup count K after a death, so the whole grid survives node loss.

**Architecture:** Auxiliary structures (list/set/queue/multimap/ringbuffer/pncounter/flake) are single-partition; HA for them is state-based — on a mutation the owner serializes the partition's aux state and synchronously replicates it to the backups (reusing the IMap deferral machinery). A generalized holder-diff migration plan (holders = owner ∪ backups) covers join, death-rebalance, and restore-K. IMap is unchanged.

**Tech Stack:** Rust, io_uring member transport, in-repo spsc, stock hazelcast-python-client.

## Global Constraints

- IMap op-based sync backups + entry-stream migration stay as-is.
- Auxiliary HA is state-based + synchronous (defer client reply until backup acks).
- `partition_for_name(name)` must match the client's string-Data partitioning so failover routes correctly.
- Generation guards, ack-timeout, quorum gate reused.
- Member protocol BonsaiGrid-only; member port 7701+i.

## File Structure

- `crates/store/src/lib.rs`: `partition_for_name`, `aux_state_for_partition`, `install_aux_state` + typed aux blob codec.
- `crates/member/src/wire.rs`: `BackupState` (kind 10), `MigrateAux` (kind 11).
- `crates/server/src/migration.rs`: generalized `plan` (holder diff).
- `crates/server/src/member_thread.rs`: install `BackupState`/`MigrateAux`; send `MigrateAux` during migration.
- `crates/server/src/handlers.rs`: `replicate_state` helper; gate aux mutations on it + quorum.
- `crates/server/src/cluster_coordinator.rs`: use generalized plan on death too (restore-K).
- `conformance-python/{structure_ha,double_failover}_smoke.py` + `run_*.sh`.

---

## Task 1: Store aux snapshot/install + name partitioning

**Files:** Modify `crates/store/src/lib.rs`.

**Produces:**
- `Store::partition_for_name(&self, name: &str, count: i32) -> i32` — wrap the name as the client's String `Data` and reuse `serialization`-equivalent murmur partitioning. (Capture the client's string Data format empirically; the simplest match is `partition_id` over the murmur of the name's UTF-8 with the String type header — verify against a captured value.)
- `Store::aux_state_for_partition(&self, partition: i32, count: i32) -> Vec<u8>` — typed blob of every aux structure whose `partition_for_name(name)==partition`. Sections: u8 kind tag + name + body. Kinds: list, set, queue, multimap, ringbuffer, pncounter, flake.
- `Store::install_aux_state(&self, bytes: &[u8])` — clear+install those structures (replace).

- [ ] Capture the client's partition for a known structure name (serialize `client._serialization_service.to_data("q")` and compute its partition) to pin `partition_for_name`.
- [ ] Implement the typed codec (little-endian lengths) over all aux collections.
- [ ] Unit tests: round-trip each structure type through `aux_state_for_partition` → `install_aux_state` into a fresh store; `partition_for_name` deterministic + in range.
- [ ] `cargo test -p store` green. Commit: `feat(store): aux state snapshot/install + partition_for_name`.

## Task 2: Wire BackupState + MigrateAux

**Files:** Modify `crates/member/src/wire.rs`.

**Produces:** `Msg::BackupState{op_id:u64, partition:i32, payload:Vec<u8>}` (kind 10), `Msg::MigrateAux{generation:u64, partition:i32, payload:Vec<u8>}` (kind 11).

- [ ] Add variants + encode/decode (reuse blob helpers). Extend the round-trip test.
- [ ] `cargo test -p member` green. Commit: `feat(member): BackupState + MigrateAux messages`.

## Task 3: Synchronous aux replication in handlers

**Files:** Modify `crates/server/src/member_thread.rs` (install + ack BackupState), `crates/server/src/handlers.rs` (`replicate_state` + gate aux mutations).

**Produces:**
- member_thread on_msg: `BackupState{op_id,partition,payload}` → `store.install_aux_state(&payload)` + `Ack{op_id}`.
- `handlers::replicate_state(repl, cluster, conn_id, resp, partition, payload) -> bool` — mirror of `replicate_write` but the message is `BackupState`; partition passed directly (computed via `store.partition_for_name`).
- Each aux mutation arm (list_add 328704, list_remove, list_clear; set_add 394240, set_remove, set_clear; queue_offer 196864, queue_poll, queue_remove, queue_clear; mm_put 131328, mm_remove; rb_add 1508864; pn_add 1901056) applies locally, builds+corr the response, then `replicate_state`; defer if it returns true. Add these msg types to `is_quorum_gated_write`.

- [ ] Implement; build.
- [ ] Reuse the existing 2-member `replication.rs` integration test pattern OR add a focused test: a `BackupState` job lands the structure on the backup store and the deferred reply is delivered after the ack.
- [ ] `cargo test -p server` + `bash conformance-python/run_cluster.sh` green. Commit: `feat(server): synchronous state-based replication for auxiliary structures`.

## Task 4: Generalized migration plan + restore-K + aux migration

**Files:** Modify `crates/server/src/migration.rs`, `crates/server/src/cluster_coordinator.rs`, `crates/server/src/member_thread.rs`.

**Produces:**
- `migration::plan(old, new, count, self_uuid) -> Vec<(i32, usize)>`: holders(p) = `owner ∪ backups_of`; if `self` is the new owner and a new holder wasn't an old holder, emit `(p, new_holder_index)` for each such new holder. (Replaces `outgoing`; keep the name `outgoing` or rename — update callers + tests.)
- coordinator `on_tick` death path returns these migrations (restore-K), not empty.
- member_thread `apply_change`: for each migrated partition, also send `MigrateAux{generation, partition, store.aux_state_for_partition(partition)}` to the dest; backup side installs it.

- [ ] Implement the holder-diff plan; update `migration.rs` tests (join schedules to newcomer; a death schedules owner→fresh-backup; no-change empty).
- [ ] Wire restore-K into the coordinator death path and aux into the migration stream.
- [ ] `cargo test` + `bash conformance-python/run_dynamic_join.sh` + `run_auto_failover.sh` green. Commit: `feat(server): generalized holder-diff migration (restore-K) + aux migration`.

## Task 5: E2E — structure HA + double failover

**Files:** `conformance-python/structure_ha_smoke.py`, `double_failover_smoke.py`, `run_structure_ha.sh`, `run_double_failover.sh`.

- [ ] `structure_ha_smoke.py`: 3 members K=1; fill an IQueue, IList, ISet, MultiMap; SIGKILL the owner; after auto-failover a fresh client reads identical contents. Harness records PIDs; the smoke kills the right owner (kill by partition owner — simplest: kill member 0 and choose structure names that land on member 0, OR kill all-but-one and read from survivors). Pragmatic: put data, kill one member, assert a fresh client still reads all structures' contents.
- [ ] `double_failover_smoke.py`: IMap + IQueue with N entries; kill one member; `sleep` for restore-K; kill a second member; assert all data still present (proves K restored).
- [ ] Both harnesses green; full regression (cluster, failover, auto-failover, dynamic join, quorum, single-node, query) green; clippy clean on changed crates. Commit + push: `test(cluster): structure-HA + double-failover (restore-K) e2e`.

## Self-Review

- **Spec coverage:** aux snapshot/install (T1), wire (T2), sync aux replication + quorum (T3), generalized plan + restore-K + aux migration (T4), e2e incl. double-failover proving restore-K (T5). Covered.
- **Deferred (documented in spec):** per-op aux backups, PNCounter CRDT merge, ReplicatedMap all-member semantics, snapshot compaction.
- **Type consistency:** `BackupState`/`MigrateAux`, `aux_state_for_partition`/`install_aux_state`/`partition_for_name`, `plan` named consistently across tasks.
