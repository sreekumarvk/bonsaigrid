# Persistence / Durable Log (Hot Restart) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Local durability — a WAL + periodic snapshot + recovery on restart — so a node survives a restart with no loss of durably-acked IMap data (Phase A), then all data structures (Phase B), without disk on the hot path.

**Architecture:** A pure-logic `crates/persistence` crate (type-tagged WAL record codec, sectioned snapshot codec, WAL segment file writer, recovery/replay into the `Store`). A dedicated per-node persistence thread consumes WAL records from reactor cores over SPSC rings (mirroring the member thread), group-commit fsyncs, snapshots + truncates, and — for `sync` durability — signals fsync completion so a deferred client ack is released.

**Tech Stack:** Rust; `crc32fast` (torn-tail detection), `crates/spsc` (rings), `crates/store` (apply/replay + snapshot source). Blocking `write`/`fsync` on the persistence thread only.

**Spec:** `docs/superpowers/specs/2026-07-01-persistence-durable-log-design.md`

## Global Constraints

- **Guardrails:** reactor cores do zero disk I/O; the persistence thread is the only disk writer; record building on the hot path allocates zero times after warmup (pooled buffers + pre-sized ring); `none` mode compiles the path out; no `Mutex`/`RwLock` across cores for persistence.
- **Structure-agnostic formats:** WAL envelope `[len:u32][crc32:u32][record_type:u16][payload]`; snapshot is a header + typed sections. v1 populates only `MapPut`/`MapRemove` records and the `MapEntries` section — new structures are additive (Phase B), no format migration.
- **Durability config:** `BONSAI_PERSISTENCE = none (default) | async | sync`; `BONSAI_PERSISTENCE_DIR`; `BONSAI_PERSISTENCE_FLUSH_MS` (default 10); `BONSAI_PERSISTENCE_SNAPSHOT_MB` (default 64).
- **Idempotent replay:** apply replayed records via stamp-guarded merge (`put_merge`) so a snapshot/tail overlap or a re-replay is a no-op when the stored stamp is ≥ the record's.
- **Recovery before listeners open.**

---

## File Structure

- `crates/persistence/Cargo.toml` — new crate (`crc32fast`, `store`).
- `crates/persistence/src/lib.rs` — re-exports; `Durability` enum; `recover(dir, &Store)`.
- `crates/persistence/src/record.rs` — `RecordType`, encode (`encode_map_put`, `encode_map_remove` into a buffer), `decode_record` (CRC-checked, returns consumed length or torn/invalid).
- `crates/persistence/src/snapshot.rs` — sectioned snapshot write (`write_snapshot(path, &Store)`) + read (`load_snapshot(path, &Store)`).
- `crates/persistence/src/wal.rs` — `WalSegment` (append bytes, fsync, roll) + `read_segment` (iterate records, stop at torn tail).
- `crates/server/src/persist_thread.rs` — `Persister` handle (SPSC producer + op-id), `PersistJob`, the persistence thread loop (drain, group-commit, snapshot, truncate, durable-signal), `spawn_persistence`.
- `crates/server/src/handlers.rs` / `reactor.rs` / `main.rs` — enqueue WAL records on mutations; build the `Persister`; call `recover` at startup.

---

## Phase A — IMap persistence

### Task A1: Crate scaffold + WAL record codec

**Files:** Create `crates/persistence/Cargo.toml`, `src/lib.rs`, `src/record.rs`; Modify root `Cargo.toml` (member).

**Produces:** `enum RecordType { MapPut=1, MapRemove=2 }`; `fn encode_map_put(buf: &mut Vec<u8>, stamp: u64, ttl_ms: u64, map: &str, key: &[u8], val: &[u8])` (appends a full framed record); `fn encode_map_remove(buf, stamp, map, key)`; `enum Decoded { Record { rtype: RecordType, payload: Vec<u8>, consumed: usize }, NeedMore, Torn }`; `fn decode_record(bytes: &[u8]) -> Decoded` (verifies `crc32fast` over type+payload; `Torn` on a length that overruns or a CRC mismatch at the tail).

- [ ] Unit tests: encode a MapPut then `decode_record` round-trips (rtype, stamp, ttl, map, key, val, consumed == frame len); a truncated buffer → `NeedMore`; a buffer with a flipped payload byte → `Torn`; two concatenated records decode sequentially by `consumed`.
- [ ] Implement; `cargo test -p persistence`; commit.

### Task A2: WAL segment writer/reader

**Files:** Create `crates/persistence/src/wal.rs`.

**Consumes:** `decode_record`.
**Produces:** `struct WalSegment { file, path, bytes_written }` with `open(path) -> io::Result<WalSegment>` (append mode), `append(&mut self, framed: &[u8]) -> io::Result<()>`, `fsync(&self) -> io::Result<()>`, `len(&self) -> u64`; `fn read_segment(path, mut apply: impl FnMut(RecordType, &[u8])) -> io::Result<()>` (reads the whole file, decodes records in order, **stops cleanly at `Torn`/`NeedMore`** — a crash-truncated tail is not an error).

- [ ] Unit test: append two encoded records + fsync; `read_segment` yields both in order; manually truncate the file mid-second-record → `read_segment` yields only the first (torn tail ignored).
- [ ] Implement; test; commit.

### Task A3: Sectioned snapshot codec

**Files:** Create `crates/persistence/src/snapshot.rs`.

**Consumes:** `store::Store` (`all_entries_stamped`, `put_merge`).
**Produces:** `fn write_snapshot(path: &Path, store: &Store) -> io::Result<()>` (header `magic|version`, then a `MapEntries` section: count + each `(map,key,value,stamp)` length-prefixed; write to `path.tmp`, fsync, rename to `path`); `fn load_snapshot(path: &Path, store: &Store) -> io::Result<()>` (parse sections; for `MapEntries` call `store.put_merge(map,key,val,0,stamp,true)`); unknown section types are skipped (forward-compat for Phase B).

- [ ] Unit test: populate a Store with 3 maps/keys, `write_snapshot`, load into a fresh Store, assert every entry+stamp matches; assert `.tmp` no longer exists (atomic rename).
- [ ] Implement; test; commit.

### Task A4: Recovery (snapshot + WAL tail)

**Files:** Modify `crates/persistence/src/lib.rs`.

**Consumes:** `load_snapshot`, `read_segment`, `Store::put_merge`/`remove`.
**Produces:** `fn recover(dir: &Path, store: &Store) -> io::Result<()>` — find the newest `snapshot.N`, `load_snapshot` it, then `read_segment` each WAL segment after it in order, applying `MapPut`→`put_merge`, `MapRemove`→a stamp-guarded remove. Missing dir → Ok (nothing to recover). `enum Durability { None, Async, Sync }` + `parse(&str)`.

- [ ] Integration test (`crates/persistence/tests/recovery.rs`): write a snapshot + a WAL segment (via the codecs) representing keys k0..k9; `recover` into a fresh Store; assert all present. Add a MapRemove for k5 after the snapshot; recover → k5 absent. Torn final WAL record → recovery still succeeds with the intact prefix.
- [ ] Implement; test; commit.

**Phase A core (A1–A4) is a shippable, fully-tested durable log + recovery with no threads/server wiring.**

### Task A5: Persistence thread + `Persister` (async durability)

**Files:** Create `crates/server/src/persist_thread.rs`; Modify `crates/server/src/lib.rs`, `Cargo.toml` (dep on `persistence`).

**Consumes:** `spsc::channel`, `wal::WalSegment`, `snapshot::write_snapshot`, `record` encoders.
**Produces:** `enum PersistJob { Record(Vec<u8>), }` (framed record bytes); `struct Persister { tx: spsc::Producer<PersistJob> }` with `persist_map_put(...)`/`persist_map_remove(...)` (encode into a reused buffer, push framed bytes); `fn spawn_persistence(dir, store: Arc<Store>, rx: spsc::Consumer<PersistJob>, flush_ms, snapshot_bytes) -> JoinHandle` — loop: drain all `Record`s → `append`; every `flush_ms` → `fsync`; when segment `len` ≥ `snapshot_bytes` → `write_snapshot` + roll to a new segment + delete the old one.

- [ ] Integration test (`crates/server/tests/persist.rs`): spawn the thread with a temp dir + a Store; push 100 MapPut records via `Persister`; wait until a fsync cadence passes; `persistence::recover` a fresh Store from the dir; assert all 100 present. Force `snapshot_bytes` small to trigger a snapshot+truncate mid-run; recover → still correct.
- [ ] Implement; test; commit.

### Task A6: Wire mutations → Persister in the reactor path (async)

**Files:** Modify `crates/server/src/handlers.rs` (thread an `Option<&Persister>` into `dispatch_bytes`/`dispatch`; on MapPut/MapRemove/etc. that hit `store.put*`, also `persister.persist_map_put/remove`), `crates/server/src/main.rs` (build the ring + `Persister` + spawn the thread when `Durability != None`; call `persistence::recover` before opening listeners), reactor call sites.

- [ ] Integration test (`persist.rs` extended): drive a MapPut through `dispatch_bytes` with a `Persister`; recover a fresh Store; the key is present. With `Durability::None` / no persister, no files are created (regression: existing dispatch tests unchanged).
- [ ] Zero-alloc guard: extend `zero_alloc.rs` — a MapPut with a `Persister` present builds+pushes the record allocating zero times over 10k calls (reused encode buffer + pre-sized ring).
- [ ] Implement; test; commit.

### Task A7: `sync` durability (deferred ack on fsync)

**Files:** Modify `crates/server/src/persist_thread.rs` (per-op-id tracking + a reverse "durable op_id" signal), `handlers.rs` (a `sync` MapPut defers the reply, delivered on fsync via the `EventBroker`, mirroring the sync-backup deferred response).

- [ ] Integration test: a `sync` MapPut returns no immediate reply; after the persistence thread fsyncs and signals, the deferred reply is delivered to the broker for that conn. A wrong/again signal doesn't double-deliver.
- [ ] Implement; test; commit.

---

## Phase B — remaining data structures (item completion gate)

For each of queue, list, set, multimap, ringbuffer, pncounter: add its record type(s) + a snapshot section + a replay arm + wire its mutating ops to `persister`. Reuse the store's existing `aux_state_for_partition` / `mm_entries_for_partition` serializers for the snapshot sections.

### Task B1: Aux structures record types + snapshot section

**Files:** `crates/persistence/src/record.rs` (add `AuxOp` record carrying a serialized aux mutation, or per-structure record types), `snapshot.rs` (add `AuxState` + `MultiMap` sections written from `store.aux_state_for_partition`/`mm_entries_for_partition`, loaded via `store.install_aux_state`/`mm_install`), `lib.rs` recovery (apply the new sections + records).

- [ ] Unit tests: aux snapshot section round-trips a queue+list+set+ringbuffer+pncounter through a Store; recovery restores them.
- [ ] Implement; test; commit.

### Task B2: Wire aux mutation ops → Persister

**Files:** `handlers.rs` — every aux mutation (queue offer/poll, list add/set/remove, set add/remove, multimap put/remove, ringbuffer add, pncounter add) enqueues its record.

- [ ] Integration test: exercise each structure through `dispatch_bytes` with a `Persister`, crash+recover, assert each structure's state is restored.
- [ ] Implement; test; commit.

**The persistence item is complete only after Task B2 (all structures persist + recover).**

---

## Per-Phase Test Matrix

| Task | Unit | Functional | Integration |
|------|------|-----------|-------------|
| A1 | record encode/decode + CRC torn | — | — |
| A2 | segment append/read + torn tail | — | — |
| A3 | snapshot round-trip + atomic rename | — | — |
| A4 | — | recover dispatch | crash-recovery (snapshot+tail, remove, torn) |
| A5 | — | thread drain/fsync/snapshot | 100-write recover + snapshot/truncate |
| A6 | zero-alloc persist | dispatch→persist | dispatch MapPut → recover; None = no files |
| A7 | — | sync defers ack | fsync signal delivers deferred reply |
| B1 | aux section round-trip | — | aux recovery |
| B2 | — | each structure dispatch | crash+recover all structures |

## Self-Review

- **Spec coverage:** WAL envelope ✅ (A1), segment+torn ✅ (A2), sectioned snapshot ✅ (A3/B1), recovery ✅ (A4), persistence thread+async ✅ (A5–A6), sync deferred-ack ✅ (A7), config ✅ (A4 Durability + A6 env), guardrails/zero-alloc ✅ (A6), all structures ✅ (B1–B2). Idempotent replay ✅ (put_merge in A3/A4).
- **Placeholder scan:** none — each task names files, interfaces, concrete tests.
- **Type consistency:** `RecordType`, `decode_record`/`Decoded`, `WalSegment`/`read_segment`, `write_snapshot`/`load_snapshot`, `recover`, `Durability`, `Persister`/`PersistJob`, `spawn_persistence` are used consistently across tasks.

## Execution Note

Phase A core (A1–A4) is self-contained (codecs + recovery, no threads) and fully verifiable; implemented first. A5–A7 add the thread + wiring. Phase B completes all structures (the item's completion gate).


## Status (2026-07-01)

- ✅ **A1–A4 shipped** (`108bcef`) — crates/persistence: WAL record codec (CRC torn-tail), WAL segment writer/reader, sectioned snapshot (atomic rename), recover(); 9 unit + crash-recovery acid test.
- ✅ **A5–A6 shipped** (`8b2e683`) — store `WalSink` seam (emits after apply; lock-free OnceLock; zero-alloc when unset), `Persister` + `spawn_persistence` (group-commit fsync, roll-before-snapshot truncation), main.rs recover+attach+spawn. End-to-end test: 500 writes+removes → thread → snapshot+truncate → recover.
- ⏳ **A7 pending** — `sync` durability deferred-ack (defer the client reply until the persistence thread signals fsync-done). `async` (default recommended) is fully working.
- ✅ **Phase B shipped** (`8b9445c`) — all non-map structures (queue/list/set/multimap/ringbuffer/pncounter) persist + recover via an `AuxState` record (full post-mutation state, last-state-wins) and a snapshot `Aux` section. Fixed a self-deadlock in `all_aux` (array-as-for-iterator held lock guards across the loop). Tests: all six recover from WAL and from snapshot.

**Completion gate met: all data structures persist and recover.** Only `sync` durability deferred-ack (A7) remains as a durability-mode enhancement (`async`, the default/recommended, is fully working).

IMap async persistence works end-to-end on `main`; whole workspace green, zero-alloc hot path intact (sink unset = no cost).
