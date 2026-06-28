# Phase D: Dynamic Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a BonsaiGrid cluster self-managing: heartbeat failure detection with automatic failover + master re-election (D1), runtime join with partition migration (D2), and a quorum write-gate with per-entry merge on heal (D3).

**Architecture:** The oldest live member is the master; it owns the member list + a monotonic generation and finalizes every change. The partition table is derived deterministically (existing ring assignment) from the member list, so only the member list + generation is published. Coordination runs on the member thread (extending the Phase C transport `Handler`); a new reverse SPSC ring (member→reactor) carries membership changes so the reactor updates the authoritative `Cluster` and pushes cluster-view events to live clients.

**Tech Stack:** Rust, `io-uring`, in-repo `spsc`, `member` crate, stock `hazelcast-python-client` for e2e.

## Global Constraints

- Zero-allocation client hot path; coordination/migration is off it.
- Shared-nothing across threads except SPSC rings + the `Mutex`-guarded `EventBroker` / sharded `Store`.
- Kernel-bypass io_uring; no blocking I/O in the client hot path.
- Member protocol is BonsaiGrid-only; only the client protocol is Hazelcast-compatible.
- Partition assignment: ring over **alive members ordered by join_id**; `owner(p)`, `backups_of(p)`.
- Generation guards every change: a message/view with a generation lower than local is ignored.
- Single-node (`BONSAI_MEMBERS=1`) starts no member thread; behavior unchanged.
- Config: `BONSAI_SEEDS`, `BONSAI_QUORUM` (default 1), `BONSAI_MERGE` (LatestUpdate|PutIfAbsent, default LatestUpdate), `BONSAI_HB_INTERVAL_MS` (500), `BONSAI_HB_TIMEOUT_MS` (3000). Member port = 7701 + index.

## File Structure

- `crates/member/src/wire.rs` — extend `Msg` with Heartbeat / JoinRequest / MemberView / MigrateStart / MigrateChunk / MigrateEnd.
- `crates/server/src/membership.rs` — `Cluster`: `MemberInfo{uuid,host,client_port,member_port,join_id}`, `generation`, `quorum`, `master()`, `is_master`, `apply_view`, `add_member`, `remove_member`, `has_quorum`, `view()`.
- `crates/server/src/cluster_coordinator.rs` (new) — heartbeat send/detect, election, join handling, migration scheduling; the member-thread brain.
- `crates/server/src/migration.rs` (new) — partition transfer source/sink + merge policy.
- `crates/server/src/member_thread.rs` — host coordinator in `MemberHandler`; reverse-ring producer; `ClusterEvent`.
- `crates/server/src/reactor.rs` — reverse-ring drain + live cluster-view broadcast (new `on_cluster` hook).
- `crates/server/src/handlers.rs` — quorum write-gate; stamp on writes.
- `crates/store/src/lib.rs` — per-entry `stamp` + `put_merge` + `entries_for_partition` + `all_entries_stamped`.
- `crates/server/src/main.rs` — wire config + both rings + coordinator.
- `conformance-python/{auto_failover,dynamic_join,quorum}_smoke.py` + `run_*.sh`.

---

# PHASE D1 — Heartbeats, auto-failover, live cluster-view push

### Task 1: Cluster join-order + generation + master + quorum

**Files:** Modify `crates/server/src/membership.rs`.

**Interfaces — Produces:**
- `MemberInfo { uuid:(i64,i64), host:String, client_port:i32, member_port:i32, join_id:u64 }`
- `Cluster` fields: `members: Vec<MemberInfo>`, `alive: Vec<bool>`, `backups`, `generation:u64`, `quorum:usize`.
- `Cluster::self_join_id` (this member's join_id).
- `master(&self) -> Option<usize>` — alive member with smallest `join_id`.
- `is_master(&self, my_join_id:u64) -> bool`.
- `has_quorum(&self) -> bool` = `live_count() >= quorum.max(1)`.
- `add_member(&mut self, MemberInfo)` / `remove_member_by_uuid(&mut self,(i64,i64))` — bump `generation`.
- `apply_view(&mut self, generation:u64, members:Vec<MemberInfo>) -> bool` — replace if `generation > self.generation`; returns whether applied.
- Keep ring `owner`/`backups_of` but order candidates by `join_id` (sort indices by join_id once per call or keep members sorted by join_id).

- [ ] Replace the `Member`-based `Cluster` internals with `MemberInfo` (carry join_id). `Cluster::new(members: Vec<MemberInfo>, backups, quorum)`. Keep `member_tuples`/`partition_table` reading alive members ordered by join_id.
- [ ] Unit tests: `master()` returns smallest-join_id alive member; after `remove_member_by_uuid(master)` the next-oldest is master; `apply_view` ignores older generation, applies newer; `has_quorum` true/false around the threshold; ring `owner`/`backups_of` stable under join_id ordering.
- [ ] `cargo test -p server membership` green. Commit: `feat(server): Cluster join-order identity, generation, master election, quorum`.

> Note: `handlers.rs` / `main.rs` / `member_thread.rs` construct `Cluster` and read `members[i].uuid/host/port`; update those call sites to `MemberInfo` (mechanical) so the crate compiles. Keep the manual `promote` endpoint working (`remove_member_by_uuid`).

### Task 2: Heartbeat + control messages in the wire

**Files:** Modify `crates/member/src/wire.rs`.

**Interfaces — Produces (new `Msg` variants):**
- `Heartbeat { from_join_id: u64, generation: u64 }`
- `JoinRequest { uuid:(i64,i64), host:String, client_port:i32, member_port:i32 }`
- `MemberView { generation:u64, members: Vec<MemberRec> }` where `MemberRec=(uuid:(i64,i64),host:String,client_port:i32,member_port:i32,join_id:u64)`
- (D2 adds MigrateStart/Chunk/End — leave kinds 7/8/9 reserved now.)

- [ ] Add kinds 4=Heartbeat, 5=JoinRequest, 6=MemberView; extend encode/decode with a `put_i64`/`get_i64` for uuids and a member-list codec. Round-trip unit test for each new variant (including a 3-member `MemberView`).
- [ ] `cargo test -p member` green. Commit: `feat(member): heartbeat + join + member-view control messages`.

### Task 3: Reverse ring + reactor live cluster-view broadcast

**Files:** Modify `crates/server/src/reactor.rs`, `crates/server/src/member_thread.rs`, `crates/server/src/main.rs`.

**Interfaces — Produces:**
- `member_thread::ClusterEvent { generation:u64, member_tuples:Vec<MemberTuple>, partition_table:Vec<((i64,i64),Vec<i32>)>, member_list_version:i32, partition_list_version:i32 }` (a ready-to-broadcast snapshot).
- `reactor::run(..., mut on_cluster: impl FnMut() -> Option<ClusterBroadcast>, ...)` — a new hook polled each 20 ms tick; when it returns `Some`, the reactor appends `members_view`+`partitions_view` event frames to **every** live binary connection (in addition to per-conn `drain_events`). `ClusterBroadcast = Vec<Frame-message-bytes>` (pre-encoded events) OR the reactor builds them from the snapshot via a closure — implement as: `on_cluster` returns `Option<Vec<u8>>` already-encoded event bytes to append to every binary conn's `out`.

- [ ] Add the reverse ring `spsc::channel::<ClusterEvent>` in `main.rs run_multi_node`; the consumer is owned by a small adapter the reactor polls via `on_cluster`. The member thread holds the **producer** (in `MemberHandler`) and pushes a `ClusterEvent` whenever its `Cluster` changes.
- [ ] In `reactor::run`, on each `flush_events` tick, call `on_cluster()`; if it yields encoded event bytes, append them to every live `Mode::Binary` conn and arm sends. Update the authoritative `Cluster` (the reactor-side `Rc<RefCell<Cluster>>`) inside the `on_cluster` adapter from the drained `ClusterEvent`.
- [ ] Manual check: a 3-member cluster, `curl /cluster/promote?dead=0` on member 1; a client connected to member 1 receives a partitions_view event (observable: it keeps working when member 0 is later killed). Commit: `feat(server): reverse ring + live cluster-view push to connected clients`.

### Task 4: Coordinator — heartbeats + failure detection + election

**Files:** Create `crates/server/src/cluster_coordinator.rs`; modify `member_thread.rs` (host it in `MemberHandler::on_tick`/`on_msg`), `main.rs`.

**Interfaces — Produces:**
- `Coordinator { cluster: Cluster, self_join_id:u64, last_seen: HashMap<u64,u64> /*join_id->tick*/, tick:u64, hb_interval_ticks:u32, hb_timeout_ticks:u32, join_tick: HashMap<u64,u64> }`
- `Coordinator::on_tick(&mut self, outbox:&mut Vec<(usize,Msg)>) -> Option<ClusterEvent>` — send heartbeats at interval to all alive peers; detect deaths; if master, finalize (remove + gen++ + broadcast MemberView) ; return a `ClusterEvent` when the local view changed.
- `Coordinator::on_view(&mut self, generation, members) -> Option<ClusterEvent>` — apply_view; returns event if changed.
- `Coordinator::on_heartbeat(&mut self, from_join_id, generation)` — record last_seen; if generation newer, request/accept view (heartbeat carries only generation, so on a newer generation the member waits for the MemberView; record and move on).
- Peer addressing: the transport routes by **member index** (position). Map join_id↔index via the current member list. Heartbeats/views go to all alive peers' indices.

- [ ] Implement detection: a peer with `tick - last_seen[jid] > hb_timeout_ticks` and past join grace is dead. Master removes it, bumps generation, sets `outbox` MemberView to all peers, returns ClusterEvent. Non-master that finds the master dead recomputes master; if self is new master, takes over (removes old master, gen++, broadcast).
- [ ] Wire into `MemberHandler`: `on_tick` calls coordinator (alongside the Phase C replication drain) and pushes any returned `ClusterEvent` up the reverse ring; `on_msg` routes Heartbeat/MemberView to the coordinator (and pushes resulting events).
- [ ] Unit test (in `cluster_coordinator.rs`): construct a 3-member coordinator as member 1; advance ticks without heartbeats from member 0 (the master); assert member 1 (next-oldest) elects itself master, removes 0, and emits a ClusterEvent with 2 members and a higher generation.
- [ ] `cargo test -p server` green. Commit: `feat(server): heartbeat failure detection + master election`.

### Task 5: D1 e2e — automatic failover (no manual promote)

**Files:** Create `conformance-python/auto_failover_smoke.py`, `conformance-python/run_auto_failover.sh`.

- [ ] Harness: launch 3 members (`BONSAI_MEMBERS=3 BONSAI_BACKUPS=1`, default HB), record PIDs; run the smoke; kill all by PID.
- [ ] Smoke: smart client A puts N=300 keys; SIGKILL member 0 by PID (recorded by harness in `/tmp/bonsai_m0.pid`); **do not** call promote; `time.sleep(5)` (> HB_TIMEOUT); a fresh client B (seeds 5702,5703) reads all 300 keys → all present. Print `AUTO FAILOVER SMOKE OK`.
- [ ] `bash conformance-python/run_auto_failover.sh` → OK. Regression: `run_cluster.sh`, `run_failover.sh`, single-node smokes still green. Commit: `test(cluster): D1 e2e — automatic failover via heartbeat detection`.

---

# PHASE D2 — Dynamic join + partition migration

### Task 6: Store per-entry stamp + migration enumeration + merge apply

**Files:** Modify `crates/store/src/lib.rs`.

**Interfaces — Produces:**
- `Entry` gains `stamp: u64`.
- `Store::put_stamped(map,key,value,ttl_ms,stamp) -> Option<Vec<u8>>` and keep `put_ttl` (assigns a fresh local stamp via an internal `AtomicU64`).
- `Store::next_stamp(&self) -> u64` (monotonic local counter; the high bits seeded by member uuid lo to break cross-member ties — set at construction via `Store::with_stamp_seed(seed)`).
- `Store::all_entries_stamped(&self) -> Vec<(String,Vec<u8>,Vec<u8>,u64)>` — (map,key,value,stamp) across all maps.
- `Store::entries_for_partition(&self, partition:i32, partition_count:i32) -> Vec<(String,Vec<u8>,Vec<u8>,u64)>` — filter by `serialization::partition_id(key)==partition` (store depends on `serialization`? No — pass a `hash_fn`/compute partition in the caller). Simplify: return `all_entries_stamped`; the caller filters by partition. So only `all_entries_stamped` is needed in the store.
- `Store::put_merge(map,key,value,ttl,stamp, latest_update:bool)` — if key absent → insert; else if `latest_update && stamp > existing.stamp` → overwrite; if `!latest_update` (PutIfAbsent) → keep existing. 

- [ ] Add `stamp` to `Entry` (and to the slab record layout if stamp isn't inline — it's a fixed field on `Entry`, so inline is fine). `put_ttl` sets `stamp = next_stamp()`. Add `all_entries_stamped`, `put_merge`, stamp seed.
- [ ] Unit tests: `put_merge` LatestUpdate keeps higher stamp, drops lower; PutIfAbsent keeps existing; `all_entries_stamped` returns everything with monotonic stamps. `cargo test -p store` green.
- [ ] Commit: `feat(store): per-entry stamp + all_entries_stamped + put_merge`.

### Task 7: Migration source/sink + merge policy

**Files:** Create `crates/server/src/migration.rs`; modify `member/src/wire.rs` (kinds 7/8/9), `member_thread.rs`.

**Interfaces — Produces:**
- wire: `MigrateStart{generation,partition}`, `MigrateChunk{generation,partition,entries:Vec<(String,Vec<u8>,Vec<u8>,u64)>}`, `MigrateEnd{generation,partition}`.
- `migration::plan(old:&Cluster, new:&Cluster, count:i32) -> Vec<(i32 /*partition*/, usize /*dest index*/)>` — partitions whose owner changed old→new, mapped to the new owner. (Backups handled by re-replication; for MVP migrate to new **owner** only.)
- `MemberHandler` applies inbound `MigrateChunk` via `store.put_merge(...latest_update...)`; `MigrateEnd` marks the partition live (no-op for MVP — data already applied).
- `MergePolicy { LatestUpdate, PutIfAbsent }` (from `BONSAI_MERGE`).

- [ ] Implement `plan` (diff owners) + unit test: for a 2→3 member growth, exactly the partitions reassigned to the new member are planned, dest = new member.
- [ ] Sender: when the coordinator schedules `(partition,dest)`, the current owner streams its `all_entries_stamped` filtered to that partition as `MigrateStart`/`MigrateChunk`(≤256 entries each)/`MigrateEnd` to `dest`. Receiver applies via `put_merge`.
- [ ] Unit/integration: in-process 2-member migration of a partition's entries; assert the dest store has them. Commit: `feat(server): partition migration (plan + stream + merge apply)`.

### Task 8: Coordinator join handling + migration scheduling

**Files:** Modify `cluster_coordinator.rs`, `main.rs` (seeds), `member_thread.rs`.

**Interfaces:**
- `Coordinator::on_join_request(&mut self, MemberInfo) -> Option<ClusterEvent>` (master only): assign new `join_id = max+1`, add_member, gen++, broadcast MemberView, compute `migration::plan(old,new)` and emit migration sends (the new owner pulls, or the old owner pushes — push from old owner).
- On `apply_view` that adds members, every member runs `plan(old,new)`; the member that is the **current owner** of a now-reassigned partition pushes it to the new owner.
- `main.rs`: a joining member starts with `BONSAI_SEEDS`; on boot it connects to seeds and sends `JoinRequest` to the believed master (lowest join_id among seeds, refined once it gets a `MemberView`).

- [ ] Implement join: a member started with `BONSAI_JOIN=1` (or seeds present and index >= bootstrap N) sends JoinRequest. Master integrates + schedules migration. 
- [ ] Integration (in-process or scripted): start a 2-member cluster, then a coordinator for member 2 joins; assert it ends up in the member list and the planned partitions migrate. Commit: `feat(server): dynamic join + migration scheduling`.

### Task 9: D2 e2e — dynamic join

**Files:** `conformance-python/dynamic_join_smoke.py`, `run_dynamic_join.sh`.

- [ ] Harness: start 2 members (`BONSAI_MEMBERS=2`), put keys, then launch a 3rd member configured to join (seeds = members 0,1), wait for convergence.
- [ ] Smoke: client A (2 seeds) puts N=300 keys; launch member 2; `sleep` for join+migration; fresh client B (seeds incl. member 2) reads all 300 keys AND the member list size is 3 (assert via a get that routes to member 2's now-owned partitions). Print `DYNAMIC JOIN SMOKE OK`.
- [ ] `bash run_dynamic_join.sh` → OK; regressions green. Commit: `test(cluster): D2 e2e — dynamic join + migration`.

---

# PHASE D3 — Quorum + per-entry merge

### Task 10: Quorum write-gate

**Files:** Modify `crates/server/src/handlers.rs`, `main.rs` (`BONSAI_QUORUM`), `membership.rs` (already has `has_quorum`).

**Interfaces:** writes (put/remove/delete/set/putAll) check `cluster.has_quorum()`; below quorum return a Hazelcast error response (`encode_error`) instead of applying. Reads unaffected.

- [ ] Add an `error_response(msg_type, corr, class, message)` codec (Hazelcast exception frame: error-code + class-name + message), or reuse the simplest exception encoding the client accepts (HazelcastException). Gate writes on `has_quorum()`.
- [ ] Unit test: a `Cluster` with quorum=2 and 1 live member → `has_quorum()` false; dispatch of a put returns the error message type. Commit: `feat(server): quorum write-gate (reject writes below minimum cluster size)`.

### Task 11: D3 e2e — quorum + merge unit coverage

**Files:** `conformance-python/quorum_smoke.py`, `run_quorum.sh`; merge already unit-tested in Task 6/7.

- [ ] Harness: 3 members with `BONSAI_QUORUM=2`.
- [ ] Smoke: client puts succeed with 3 live; SIGKILL two members (down to 1 < quorum); after HB_TIMEOUT a put raises an error (client sees an exception) while a get still returns prior data. Print `QUORUM SMOKE OK`.
- [ ] Scripted heal scenario (documented): a Rust integration test drives `put_merge` with interleaved stamps from two "members" and asserts LatestUpdate convergence (stands in for a true network-partition heal, which is out of automated scope per the spec).
- [ ] `bash run_quorum.sh` → OK; full regression suite green; clippy clean on new crates. Commit + push: `test(cluster): D3 e2e — quorum gate + merge convergence`.

---

## Self-Review

- **Spec coverage:** heartbeats+detection (T4), auto-failover e2e (T5), election (T1/T4), live cluster-view push (T3), join (T8) + migration (T6/T7) + e2e (T9), quorum gate (T10) + e2e (T11), per-entry merge (T6 store + T7 apply + T11 convergence test), generation guard (T1 apply_view, used in T4/T7/T8), config (T8/T10/main). All covered.
- **Deferred (documented in spec):** phi-accrual, movement-optimal assignment, true network-partition CI automation, persistence.
- **Type consistency:** `MemberInfo`/`MemberRec`, `generation:u64`, `join_id:u64`, `ClusterEvent`, `stamp:u64`, `MergePolicy`, `Coordinator` methods are named identically across tasks.
- **Phasing:** D1 (T1–5), D2 (T6–9), D3 (T10–11) each end in a green e2e smoke + passing regressions.
