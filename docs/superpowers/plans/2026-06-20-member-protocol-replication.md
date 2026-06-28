# Member Protocol + Phase C Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a custom member-to-member io_uring transport with synchronous backup replication of IMap writes and an explicit promotion mechanism, so a BonsaiGrid cluster survives node loss.

**Architecture:** Each member process runs a second I/O plane — a dedicated member thread with its own io_uring loop on port `7701+i`, full-mesh with peers. The client reactor applies a write locally, hands a `MemberJob` to the member thread over an SPSC ring, and defers the client response; the member thread ships the mutation to backups, collects acks, and delivers the deferred response via `EventBroker::enqueue`. Promotion is an explicit HTTP trigger that rewrites the shared `Cluster` (member list + partition table), so reconnecting clients route to the backup that already holds the data.

**Tech Stack:** Rust, `io-uring` crate, the in-repo `spsc` crate, `core_affinity`. New crate `member`; new modules `server::membership`, `server::member_thread`.

## Global Constraints

- Zero-allocation client hot path after startup; member replication is off that path.
- Shared-nothing between threads except via SPSC rings and the already-`Mutex`-guarded `Arc<EventBroker>` / internally-sharded `Arc<Store>` (existing pattern).
- Kernel-bypass io_uring; no blocking I/O in the client hot path.
- Member-to-member protocol is custom/BonsaiGrid-only; only the client protocol is Hazelcast-compatible.
- Replica assignment ring-wise: `primary(p)=p%N`, `backups(p)={(p+j)%N : j in 1..=K}`, `K=BONSAI_BACKUPS` (default 1, cap N-1).
- Backups synchronous: client OK only after backups ack; response `backupAcks` field stays 0.
- Member port = `7701 + BONSAI_MEMBER_INDEX`. Single-node (`BONSAI_MEMBERS=1`) starts no member thread.

---

## File Structure

- `crates/member/` (new): `Cargo.toml`, `src/lib.rs`, `src/wire.rs` (messages + codec), `src/transport.rs` (io_uring peer mesh), `src/replication.rs` (pending-ack state machine + apply).
- `crates/server/src/membership.rs` (new): `Cluster` (dynamic member list + versions + ReplicaMap + `promote`).
- `crates/server/src/member_thread.rs` (new): spawns the member thread, owns the SPSC consumer, wires transport↔replication↔store↔broker.
- `crates/server/src/handlers.rs` (modify): `dispatch`/`dispatch_bytes` take `&Cluster` and an `Option<&Replicator>`; IMap write arms defer + push jobs; add `Promote` handling via http route.
- `crates/server/src/main.rs` (modify): build `Cluster`, in multi-node spawn member thread + SPSC ring + Replicator; add `/cluster/promote` route.
- `crates/server/src/lib.rs` (modify): `pub mod membership; pub mod member_thread;`.
- `crates/server/tests/zero_alloc.rs` (modify): updated dispatch signature.
- `conformance-python/cluster_failover_smoke.py` (new), `conformance-python/run_failover.sh` (new).

---

## Task 1: Member wire protocol (`member::wire`)

**Files:**
- Create: `crates/member/Cargo.toml`, `crates/member/src/lib.rs`, `crates/member/src/wire.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces — Produces:**
- `enum Msg { Hello{index:u32}, BackupPut{op_id:u64,name:String,key:Vec<u8>,value:Vec<u8>,ttl_ms:u64}, BackupRemove{op_id:u64,name:String,key:Vec<u8>}, Ack{op_id:u64} }`
- `fn encode(msg:&Msg) -> Vec<u8>` — full frame incl. `[u32 len][u8 kind][body]`, big-endian.
- `fn decode(buf:&[u8]) -> Option<(Msg, usize)>` — returns the message and bytes consumed, or None if a full frame isn't buffered yet.

- [ ] **Step 1:** Add `crates/member` to workspace `members` list in `bonsaigrid/Cargo.toml`.
- [ ] **Step 2:** Write `crates/member/Cargo.toml` (`name="member"`, edition 2021, no deps for wire).
- [ ] **Step 3:** Write `crates/member/src/lib.rs` with `pub mod wire;`.
- [ ] **Step 4 (failing test):** in `wire.rs`, a `#[cfg(test)]` round-trip for each variant:
  ```rust
  #[test] fn roundtrip() {
      for m in [Msg::Hello{index:2}, Msg::Ack{op_id:7},
                Msg::BackupPut{op_id:9,name:"m".into(),key:b"k".to_vec(),value:b"v".to_vec(),ttl_ms:0},
                Msg::BackupRemove{op_id:3,name:"m".into(),key:b"k".to_vec()}] {
          let b = encode(&m); let (d,n) = decode(&b).unwrap();
          assert_eq!(d, m); assert_eq!(n, b.len());
      }
      assert!(decode(&[0,0,0,9]).is_none()); // incomplete frame
  }
  ```
- [ ] **Step 5:** Run `cargo test -p member` → FAIL (encode/decode missing).
- [ ] **Step 6:** Implement `Msg` (derive `Clone,Debug,PartialEq`), `encode`, `decode`. Helpers: `put_u32`/`put_u64`/`put_blob`/`put_str` (big-endian) and bounds-checked readers; `decode` returns None when `buf.len() < 4+len`.
- [ ] **Step 7:** Run `cargo test -p member` → PASS.
- [ ] **Step 8:** Commit: `feat(member): custom member wire protocol (Hello/BackupPut/BackupRemove/Ack)`.

---

## Task 2: Cluster membership + replica assignment (`server::membership`)

**Files:**
- Create: `crates/server/src/membership.rs`
- Modify: `crates/server/src/lib.rs` (add `pub mod membership;`)

**Interfaces:**
- Consumes: `handlers::Member`.
- Produces:
  - `struct Cluster { pub members: Vec<Member>, pub alive: Vec<bool>, pub backups: usize, pub member_list_version: i32, pub partition_list_version: i32 }`
  - `Cluster::new(members: Vec<Member>, backups: usize) -> Cluster` (all alive, versions=1).
  - `Cluster::live_count(&self) -> usize`
  - `Cluster::primary(&self, partition: i32) -> usize` — `partition as usize % members.len()`.
  - `Cluster::backups_of(&self, partition: i32) -> Vec<usize>` — ring-wise next `min(backups, live-1)` *alive* members after the primary, skipping dead.
  - `Cluster::promote(&mut self, dead: usize)` — set `alive[dead]=false`; bump both versions. (Reassignment is implicit: `primary`/`backups_of` already skip dead members — see below.)
  - `Cluster::owner(&self, partition: i32) -> usize` — first **alive** member in the ring starting at `partition % len` (this is what clients are told owns the partition).
  - `Cluster::member_tuples(&self) -> Vec<MemberTuple>` — only alive members.
  - `Cluster::partition_table(&self) -> Vec<((i64,i64), Vec<i32>)>` — for each alive member, the partitions it `owner`s.

Note: keep `primary(p)=p%N` as the *home* index; `owner(p)` walks the ring to the first alive member so a dead home falls through to its backup. `backups_of` returns the alive members after the owner.

- [ ] **Step 1 (failing test):**
  ```rust
  #[test] fn assignment_and_promote() {
      let m = |i| Member{uuid:(i as i64,i as i64),host:"127.0.0.1".into(),port:5701+i};
      let mut c = Cluster::new(vec![m(0),m(1),m(2)], 1);
      assert_eq!(c.owner(0), 0); assert_eq!(c.backups_of(0), vec![1]);
      assert_eq!(c.owner(1), 1); assert_eq!(c.backups_of(1), vec![2]);
      // partition 0: home=0. promote(0) -> owner falls to 1, which had the backup.
      c.promote(0);
      assert!(!c.alive[0]);
      assert_eq!(c.owner(0), 1);
      assert_eq!(c.member_tuples().len(), 2);
      assert!(c.partition_table().iter().all(|(u,_)| *u != (0,0)));
      assert!(c.member_list_version >= 2 && c.partition_list_version >= 2);
  }
  ```
- [ ] **Step 2:** Run `cargo test -p server membership` → FAIL.
- [ ] **Step 3:** Implement `Cluster`. `owner`: `for j in 0..len { let i=(p%len + j)%len; if alive[i] {return i} }`. `backups_of`: collect the next alive members after `owner`, up to `min(backups, live_count-1)`. `promote`: `alive[dead]=false; member_list_version+=1; partition_list_version+=1`. `partition_table`: iterate partitions 0..271, group by `owner`. `member_tuples`: alive only.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit: `feat(server): Cluster membership + ring-wise replica assignment + promote`.

---

## Task 3: Wire Cluster into auth/cluster-view (replace static Cfg tables)

**Files:**
- Modify: `crates/server/src/handlers.rs` (`dispatch`, `dispatch_bytes`, `auth_response`, handler 768, 4864 schema member uuid, the partition-verify log), `crates/server/src/main.rs`, `crates/server/tests/zero_alloc.rs`

**Interfaces:**
- `dispatch(req, conn_id, store, cfg, broker, schemas, cluster: &Cluster, repl: Option<&member_thread::Replicator>) -> Vec<Vec<Frame>>`
- `dispatch_bytes(msg, conn_id, store, cfg, broker, schemas, cluster, repl, out)`
- `auth_response(cfg, cluster, status)`.

Rationale: member list, versions, and partition table become dynamic (post-promote), so reads must come from `Cluster`, not `Cfg`. `Cfg` keeps only static config (self_index, cluster_name, auth, tpc_ports).

- [ ] **Step 1:** Add `backups_of`/`owner` usage; change `auth_response` to take `cluster` and read `cluster.member_tuples()`, `cluster.partition_table()`, `cluster.member_list_version`, `cluster.partition_list_version`, and self member = `cluster.members[cfg.self_index]`.
- [ ] **Step 2:** Update handler `768` (cluster view) to use `cluster.member_tuples()` / `cluster.partition_table()` and the cluster versions.
- [ ] **Step 3:** Update handler `4864` self-uuid and the `65792` partition-verify log to read `cluster.members[cfg.self_index].uuid` and `cluster.owner(...)`.
- [ ] **Step 4:** Thread `cluster: &Cluster` + `repl: Option<&Replicator>` through `dispatch`/`dispatch_bytes`. For now pass `repl=None`, write arms unchanged.
- [ ] **Step 5:** In `main.rs` build `let cluster = membership::Cluster::new(cluster_members(members), backups)` for multi-node and a single-member `Cluster` for single-node; for single-node wrap in `Arc<Cluster>` shared to cores, pass `&cluster`, `None`. For multi-node wrap in `Rc<RefCell<Cluster>>` (built in Task 6); for this task pass `&cluster.borrow()` and `None`.
- [ ] **Step 6:** Fix `crates/server/tests/zero_alloc.rs`: build a `Cluster::new(...)` and pass it + `None` to `dispatch_bytes`.
- [ ] **Step 7:** Run `cargo test -p server` + `bash conformance-python/run_cluster.sh` → both green (no behavior change yet).
- [ ] **Step 8:** Commit: `refactor(server): serve member list + partition table from dynamic Cluster`.

---

## Task 4: Replication state machine (`member::replication`)

**Files:**
- Create: `crates/member/src/replication.rs`; Modify: `crates/member/src/lib.rs` (`pub mod replication;`)

**Interfaces — Produces:**
- `struct PendingOp { pub remaining: u32, pub conn_id: u64, pub response: Vec<u8> }`
- `struct Pending { ... }` with:
  - `Pending::new() -> Pending`
  - `register(&mut self, op_id:u64, remaining:u32, conn_id:u64, response:Vec<u8>) -> Option<(u64,Vec<u8>)>` — if `remaining==0`, returns `Some((conn_id-as... ))`; actually returns `Some((conn_id, response))` immediately for the 0-backup case; else stores and returns None.
  - `ack(&mut self, op_id:u64) -> Option<(u64, Vec<u8>)>` — decrement; when it hits 0, remove and return `Some((conn_id, response))` to deliver; else None.
  - `apply(store:&store::Store, msg:&member::wire::Msg)` — free fn: for `BackupPut`/`BackupRemove` call `store.put_ttl`/`store.remove`; ignore others.

- [ ] **Step 1 (failing test):**
  ```rust
  #[test] fn acks_complete_once() {
      let mut p = Pending::new();
      assert!(p.register(1, 2, 42, vec![9]).is_none());     // needs 2 acks
      assert!(p.ack(1).is_none());                          // 1 of 2
      assert_eq!(p.ack(1), Some((42, vec![9])));            // 2 of 2 -> deliver
      assert!(p.ack(1).is_none());                          // already delivered
      assert_eq!(p.register(2, 0, 7, vec![1]), Some((7, vec![1]))); // 0 backups -> immediate
  }
  ```
- [ ] **Step 2:** Run `cargo test -p member replication` → FAIL.
- [ ] **Step 3:** Implement with a `HashMap<u64, PendingOp>`. Add `member` dep on `store`? No — `apply` needs `store::Store`. Add `store = { path = "../store" }` to `crates/member/Cargo.toml`. `apply` matches the msg and calls store.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit: `feat(member): replication pending-ack accounting + backup apply`.

---

## Task 5: Member transport (`member::transport`, io_uring mesh)

**Files:**
- Create: `crates/member/src/transport.rs`; Modify: `crates/member/src/lib.rs`, `crates/member/Cargo.toml` (add `io-uring`, `libc`).

**Interfaces — Produces:**
- `struct Transport { ... }`
- `Transport::start(self_index: usize, member_ports: Vec<i32>) -> Transport` — binds `member_ports[self_index]` for inbound; lazily connects outbound to peers on first send.
- `Transport::run(self, mut on_msg: impl FnMut(usize, member::wire::Msg, &mut dyn FnMut(usize, &Msg)), poll: impl FnMut(&mut dyn FnMut(usize,&Msg)) -> bool)`:
  driven loop — on each inbound `Msg` from peer `src`, call `on_msg(src, msg, &mut send)`; every ~1ms tick call `poll(&mut send)` to let the owner drain the SPSC ring (returns false to stop). `send(dest_index, &Msg)` writes to peer `dest` (establishing the outbound connection if needed).

Design notes (model on `crates/server/src/reactor.rs`):
- One `IoUring`. Inbound: `AcceptMulti` on the bound listener; per-conn `Recv` into a 64 KiB buffer; accumulate bytes, `wire::decode` in a loop. Track which member index a connection is by the `Hello` it sends first (inbound) or by construction (outbound).
- Outbound: on first `send(dest,...)`, `socket()`+`Connect` (io_uring `Connect` opcode) to `127.0.0.1:member_ports[dest]`, send `Hello{self_index}` then the message; queue sends until connected.
- A `Timeout` SQE (1 ms) drives `poll`.
- Use `user_data` tagging like the reactor (`ACCEPT_BASE`, `TIMEOUT_UD`, conn-indexed recv/send) to demux completions.

- [ ] **Step 1 (integration test, failing):** `crates/member/tests/loopback.rs`:
  ```rust
  // Two transports on ephemeral-ish ports exchange a BackupPut -> Ack.
  // member 0 sends BackupPut{op_id:5,...} to member 1; member 1 replies Ack{5};
  // assert member 0 sees the Ack within 2s. (Run each transport on its own thread.)
  ```
  (Concrete: ports `[17801, 17802]`; thread A = index 0, thread B = index 1; B's `on_msg` replies `Ack` via the provided `send`; A's `poll` sends one `BackupPut` then waits; assert via an `AtomicBool`/channel that A's `on_msg` got `Ack{op_id:5}`.)
- [ ] **Step 2:** Run `cargo test -p member --test loopback` → FAIL (transport missing).
- [ ] **Step 3:** Implement `Transport`. Keep buffers per connection; reuse the reactor's submit/flush pattern. Outbound connect state machine: `Disconnected -> Connecting -> Ready` with a pending-send queue per peer.
- [ ] **Step 4:** Run → PASS (allow up to a few seconds).
- [ ] **Step 5:** Commit: `feat(member): io_uring full-mesh transport (loopback put/ack verified)`.

---

## Task 6: Member thread + Replicator + reactor integration

**Files:**
- Create: `crates/server/src/member_thread.rs`
- Modify: `crates/server/src/lib.rs`, `crates/server/src/handlers.rs` (write arms), `crates/server/src/main.rs`, `crates/server/Cargo.toml` (add `member`, `spsc`).

**Interfaces — Produces:**
- `enum MemberJob { Replicate{ partition:i32, op_id:u64, msg:member::wire::Msg, conn_id:u64, response:Vec<u8> }, Membership(membership::Cluster) }`
- `struct Replicator { tx: spsc::Producer<MemberJob>, next_op: std::cell::Cell<u64>, backups: usize }`
  - `Replicator::replicate(&self, partition:i32, msg_no_opid:..., conn_id:u64, response:Vec<u8>, n_backups_for_partition:usize) -> bool` — if `n_backups==0` returns false (caller responds normally); else assigns `op_id`, pushes `MemberJob::Replicate`, returns true (caller defers).
- `member_thread::spawn(self_index, member_ports, backups, store: Arc<Store>, broker: Arc<EventBroker>, rx: spsc::Consumer<MemberJob>) -> std::thread::JoinHandle<()>`

Member thread loop (using `Transport::run`):
- Holds `Pending`, a local `Cluster` copy (for `backups_of`), `store`, `broker`.
- `poll(send)`: drain `rx`; for each `Replicate` job, `send(dest,&msg)` to each `cluster.backups_of(partition)` and `pending.register(op_id, n, conn_id, response)`; if register returns `Some((conn,resp))` (0 backups race) enqueue immediately. For `Membership(c)` swap the local cluster.
- `on_msg(src, msg, send)`: `BackupPut|BackupRemove` -> `replication::apply(&store,&msg)` then `send(src, &Ack{op_id})`. `Ack{op_id}` -> `pending.ack(op_id)`; if `Some((conn,resp))` -> `broker.enqueue(conn, resp)`.
- Ack-timeout sweep inside `poll`: track op insert tick; force-complete after ~5 s (deliver response, bump a counter). (MVP: a simple per-op counter incremented each poll; expire at N polls ≈ 5 s.)

Handler change (IMap writes — `65792` put, `66304` putIfAbsent if present, `66048` is get, `66560` remove, set/clear):
- After applying locally and building `response_frames`, if `repl` is `Some` and `cluster.backups_of(partition) non-empty`: compute `partition = serialization::partition_id(&key, PARTITION_COUNT)`, build `Msg::BackupPut{op_id:0,...}` (op_id filled by Replicator), `let response = write_message(&response_frames)`, call `repl.replicate(...)`. If it returns true, **return `vec![]`** (deferred). Else return the normal `vec![response_frames]`.

- [ ] **Step 1:** Add deps; create `MemberJob`, `Replicator` (in `member_thread.rs`).
- [ ] **Step 2 (test):** unit-test `Replicator::replicate` returns false for 0 backups and true (pushing a job) for >0 using a fresh spsc channel and asserting `rx.pop()` yields a `Replicate` with a non-zero `op_id`.
- [ ] **Step 3:** Implement `member_thread::spawn` (build `Transport`, `Pending`, local `Cluster`, run the loop).
- [ ] **Step 4:** Modify the `65792` put and `66560` remove arms to defer when backups exist. Keep other writes non-replicated for MVP (documented).
- [ ] **Step 5:** In `main.rs run_multi_node`: read `BONSAI_BACKUPS`; build `Rc<RefCell<Cluster>>`; if `members>1 && backups>0` create `spsc::channel::<MemberJob>(4096)`, `member_thread::spawn(...)` with `store.clone()`, `broker.clone()`, `rx`; build `Replicator{tx, next_op:Cell::new(1), backups}`; pass `Some(&repl)` into the dispatch closure. Member ports = `(0..members).map(|i| 7701+i as i32)`.
- [ ] **Step 6:** Run `cargo test -p server` and `bash conformance-python/run_cluster.sh` → green. Add a 2-member Rust integration test (`crates/server/tests/replication.rs`): start two member threads (real `Transport`) sharing nothing; drive a `Replicate` job through a `Replicator`; assert (a) the backup `store.get` returns the value and (b) `broker.drain(conn)` yields the deferred response only after the ack.
- [ ] **Step 7:** Commit: `feat(server): member thread + synchronous IMap put/remove replication`.

---

## Task 7: Promotion endpoint + Membership job

**Files:**
- Modify: `crates/server/src/main.rs` (http route + shared `Rc<RefCell<Cluster>>`), `crates/server/src/handlers.rs` (`http_health` or a new `admin` arm).

**Interfaces:**
- HTTP `POST`/`GET` `/cluster/promote?dead=<index>` on the client port → `cluster.borrow_mut().promote(index)`, then push `MemberJob::Membership(cluster.borrow().clone())` to the member thread, return `200 {"promoted":<index>,"plist":<version>}`.

- [ ] **Step 1:** In `run_multi_node`, capture a clone of `Rc<RefCell<Cluster>>` and the `Replicator` (for the membership push) in the `http` closure. Parse `/cluster/promote?dead=N`.
- [ ] **Step 2:** On promote: `cluster.borrow_mut().promote(n)`; `repl.send_membership(cluster.borrow().clone())` (add `Replicator::send_membership` pushing `MemberJob::Membership`). Respond JSON.
- [ ] **Step 3:** Manual check: start 3 members, `curl -XPOST 'localhost:5701/cluster/promote?dead=0'` → 200; a fresh client's auth from member 1 now lists 2 members and no partition owned by member 0.
- [ ] **Step 4:** Commit: `feat(server): explicit promotion endpoint (Phase D detector seam)`.

---

## Task 8: End-to-end cluster failover smoke

**Files:**
- Create: `conformance-python/cluster_failover_smoke.py`, `conformance-python/run_failover.sh`

- [ ] **Step 1:** Write `run_failover.sh` (mirror `run_cluster.sh`): build server; launch 3 members with `BONSAI_MEMBERS=3 BONSAI_BACKUPS=1 BONSAI_MEMBER_INDEX=i`; `sleep 2`; run `cluster_failover_smoke.py`; capture RC; kill all; `exit RC`. The python script itself kills member 0 mid-test (it knows the pids via an env var the script exports, or via `pkill -f BONSAI_MEMBER_INDEX=0`).
- [ ] **Step 2:** Write `cluster_failover_smoke.py`:
  ```python
  # 1) smart client A puts N=300 keys across the cluster (replicated to backups).
  # 2) POST http://127.0.0.1:5702/cluster/promote?dead=0 and :5703 too.
  # 3) kill member 0's process (os.system("pkill -f 'BONSAI_MEMBER_INDEX=0'")).
  # 4) NEW smart client B (members 5702,5703) gets all N keys -> all present.
  print("CLUSTER FAILOVER SMOKE OK")
  ```
  Use `urllib.request` for the promote POSTs. Allow a short sleep after promote for the partition table to settle. Assert every key read by B equals its value.
- [ ] **Step 3:** Run `bash conformance-python/run_failover.sh` → `CLUSTER FAILOVER SMOKE OK`.
- [ ] **Step 4:** Run the full suite: `cargo test` (all green), `bash conformance-python/run_cluster.sh` (still green).
- [ ] **Step 5:** Commit: `test(cluster): e2e failover smoke — promoted backup serves dead primary's keys`.

---

## Self-Review notes

- **Spec coverage:** transport (T5), wire (T1), sync replication + deferred response (T4,T6), ReplicaMap + promote (T2,T7), Membership SPSC control (T6,T7), config `BONSAI_BACKUPS`/port (T6), error handling: ack-timeout (T6), 0-backup immediate (T4/T6), single-node unchanged (T3/T6). e2e failover (T8). All covered.
- **Deferred (documented):** non-IMap structure replication; live cluster-view push to *existing* clients on promote (test uses a reconnecting client B); auto-detection (Phase D).
- **Type consistency:** `Msg`, `MemberJob`, `Cluster`, `Pending`, `Replicator` names/signatures used identically across tasks.
