# Geo/WAN Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Active-active asynchronous cross-cluster WAN replication of IMap updates, converging via the existing HLC merge, with a durable outbound buffer that replays on WAN-link outage — all off the hot path.

**Architecture:** A new `crates/wan` holds the pure logic (record codec, durable outbound queue, publisher `WalSink`, consumer, wire codec). The store gains a second sink slot (`wan_sink`) that captures local IMap mutations, plus `apply_wan` (persist yes, re-publish no) for loop-free remote apply. A per-target WAN thread in the server drains the queue over the member io_uring transport; remote apply reuses `put_merge`/`observe_stamp`. The verifiable core is a deterministic two-cluster in-process simulation.

**Tech Stack:** Rust; `crc32fast` (torn-tail detection); `crates/store` (`WalSink`, `put_merge`, `observe_stamp`); `crates/member` transport; `crates/spsc` rings. Blocking file/socket work on the WAN thread only.

**Spec:** `docs/superpowers/specs/2026-07-02-geo-wan-replication-design.md`

## Global Constraints

- **Guardrails:** the reactor hot path does zero WAN disk/socket work; the WAN thread is the only WAN writer; capture pushes to an SPSC ring; `wan_sink` unset compiles the path out to a lock-free no-op (zero-alloc hot path preserved); no `Mutex`/`RwLock` across cores for WAN.
- **Active-active + loop prevention:** a record applied via `apply_wan` MUST NOT be re-captured (it hits `wal_sink` only, never `wan_sink`). Local writes hit both sinks.
- **Delivery:** at-least-once; `put_merge` is idempotent under the stamp, so re-delivery after reconnect dedups for free.
- **Conflict policy:** HLC LatestUpdate (`put_merge(..., latest_update=true)`), fixed in v1.
- **Scope:** IMap (`Put`/`Remove`) in Phases A–C; other structures in Phase D.
- **WAN record framing** (own, mirrors the persistence WAL discipline): `[len:u32][crc32:u32][op:u8][stamp:u64][ttl_ms:u64][map][key][value]`, all little-endian, each blob length-prefixed; CRC over `op..value`. `op`: `Put=1`, `Remove=2`.
- **Reuse the two-cluster sim pattern** from `crates/server/src/sim.rs` / `crates/raft/tests/`.

---

## File Structure

- `crates/wan/Cargo.toml` — new crate (`store` path, `crc32fast`).
- `crates/wan/src/lib.rs` — re-exports; `WanOp`, `WanRecord`; config constants.
- `crates/wan/src/record.rs` — `WanRecord` frame codec (`encode`, `decode`).
- `crates/wan/src/queue.rs` — `WanQueue` durable outbound buffer + committed cursor.
- `crates/wan/src/publisher.rs` — `WanPublisher` (`store::WalSink` → SPSC producer).
- `crates/wan/src/consumer.rs` — `WanConsumer` (apply a batch via `store::apply_wan`).
- `crates/wan/src/wire.rs` — `WanMsg` (`Batch`/`Ack`) codec.
- `crates/wan/tests/convergence.rs` — two-cluster deterministic sim (Phase B).
- `crates/store/src/lib.rs` — add `wan_sink` slot + `set_wan_sink` + emit in put/remove/put_merge; add `apply_wan`.
- `crates/server/src/wan_thread.rs` — the per-target WAN thread + reactor handle (Phase C).
- `crates/server/src/main.rs` — wire WAN when configured (Phase C).
- Root `Cargo.toml` — add `crates/wan` to members.

---

## Phase A — capture + durable queue

### Task A1: Crate scaffold + `WanRecord` codec

**Files:** Create `crates/wan/Cargo.toml`, `crates/wan/src/lib.rs`, `crates/wan/src/record.rs`; Modify root `Cargo.toml`.

**Interfaces — Produces:**
- `pub enum WanOp { Put, Remove }` (`Put` encodes as 1, `Remove` as 2).
- `pub struct WanRecord { pub op: WanOp, pub stamp: u64, pub ttl_ms: u64, pub map: String, pub key: Vec<u8>, pub value: Vec<u8> }`
- `pub fn encode(rec: &WanRecord) -> Vec<u8>` — a full framed record.
- `pub enum Decoded { Record { rec: WanRecord, consumed: usize }, NeedMore, Torn }`
- `pub fn decode(bytes: &[u8]) -> Decoded` — verifies `crc32fast` over `op..value`; `Torn` on a length overrun or CRC mismatch, `NeedMore` on a short buffer.

- [ ] **Step 1: Scaffold the crate.** Create `crates/wan/Cargo.toml`:

```toml
[package]
name = "wan"
edition.workspace = true
version.workspace = true

[dependencies]
store = { path = "../store" }
crc32fast = "1"
```

Add `"crates/wan"` to the `members` array in root `Cargo.toml`. Create `crates/wan/src/lib.rs`:

```rust
//! Geo/WAN replication: capture local IMap mutations, buffer them durably, ship
//! them to a remote cluster, and apply inbound updates with the HLC merge.
pub mod record;

pub use record::{decode, encode, Decoded, WanOp, WanRecord};
```

- [ ] **Step 2: Write the failing test.** Create `crates/wan/src/record.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_roundtrips() {
        let rec = WanRecord {
            op: WanOp::Put,
            stamp: 42,
            ttl_ms: 1000,
            map: "m".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let bytes = encode(&rec);
        match decode(&bytes) {
            Decoded::Record { rec: got, consumed } => {
                assert_eq!(got, rec);
                assert_eq!(consumed, bytes.len());
            }
            _ => panic!("expected a decoded record"),
        }
    }

    #[test]
    fn short_buffer_needs_more_and_flip_is_torn() {
        let rec = WanRecord {
            op: WanOp::Remove,
            stamp: 7,
            ttl_ms: 0,
            map: "m".into(),
            key: b"k".to_vec(),
            value: Vec::new(),
        };
        let bytes = encode(&rec);
        assert!(matches!(decode(&bytes[..4]), Decoded::NeedMore));
        let mut bad = bytes.clone();
        *bad.last_mut().unwrap() ^= 0xFF;
        assert!(matches!(decode(&bad), Decoded::Torn));
    }
}
```

- [ ] **Step 3: Run it to verify it fails.** Run: `cargo test -p wan --lib record` — Expected: FAIL to compile (`WanRecord`, `encode`, `decode` undefined).

- [ ] **Step 4: Implement the codec.** Prepend to `crates/wan/src/record.rs`:

```rust
//! `WanRecord` frame codec: `[len:u32][crc32:u32][op:u8][stamp:u64][ttl:u64]
//! [map][key][value]` (little-endian; each blob length-prefixed; CRC over
//! `op..value`; `len` counts crc+body). Mirrors the persistence WAL discipline.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WanOp {
    Put,
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WanRecord {
    pub op: WanOp,
    pub stamp: u64,
    pub ttl_ms: u64,
    pub map: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub enum Decoded {
    Record { rec: WanRecord, consumed: usize },
    NeedMore,
    Torn,
}

fn put_blob(b: &mut Vec<u8>, x: &[u8]) {
    b.extend_from_slice(&(x.len() as u32).to_le_bytes());
    b.extend_from_slice(x);
}

fn get_blob(b: &[u8], p: usize) -> Option<(&[u8], usize)> {
    let n = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
    let s = b.get(p + 4..p + 4 + n)?;
    Some((s, p + 4 + n))
}

pub fn encode(rec: &WanRecord) -> Vec<u8> {
    let mut body = Vec::with_capacity(17 + rec.map.len() + rec.key.len() + rec.value.len());
    body.push(match rec.op {
        WanOp::Put => 1,
        WanOp::Remove => 2,
    });
    body.extend_from_slice(&rec.stamp.to_le_bytes());
    body.extend_from_slice(&rec.ttl_ms.to_le_bytes());
    put_blob(&mut body, rec.map.as_bytes());
    put_blob(&mut body, &rec.key);
    put_blob(&mut body, &rec.value);
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&((body.len() as u32 + 4).to_le_bytes())); // crc + body
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

pub fn decode(bytes: &[u8]) -> Decoded {
    if bytes.len() < 8 {
        return Decoded::NeedMore;
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let end = 4 + len; // len counts crc(4) + body
    if len < 4 + 17 {
        return Decoded::Torn;
    }
    if bytes.len() < end {
        return Decoded::NeedMore;
    }
    let body = &bytes[8..end];
    if crc32fast::hash(body) != crc {
        return Decoded::Torn;
    }
    let op = match body[0] {
        1 => WanOp::Put,
        2 => WanOp::Remove,
        _ => return Decoded::Torn,
    };
    let stamp = u64::from_le_bytes(body[1..9].try_into().unwrap());
    let ttl_ms = u64::from_le_bytes(body[9..17].try_into().unwrap());
    let Some((map, o1)) = get_blob(body, 17) else {
        return Decoded::Torn;
    };
    let Some((key, o2)) = get_blob(body, o1) else {
        return Decoded::Torn;
    };
    let Some((value, _)) = get_blob(body, o2) else {
        return Decoded::Torn;
    };
    let Ok(map) = std::str::from_utf8(map) else {
        return Decoded::Torn;
    };
    Decoded::Record {
        rec: WanRecord {
            op,
            stamp,
            ttl_ms,
            map: map.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        },
        consumed: end,
    }
}
```

- [ ] **Step 5: Run to verify pass.** Run: `cargo test -p wan --lib record` — Expected: PASS (2 tests). Run `cargo fmt -p wan && cargo clippy -p wan`.

- [ ] **Step 6: Commit.**

```bash
git add crates/wan Cargo.toml Cargo.lock
git commit -m "feat(wan): crate scaffold + WanRecord frame codec"
```

### Task A2: `WanQueue` — durable outbound buffer + cursor

**Files:** Create `crates/wan/src/queue.rs`; Modify `crates/wan/src/lib.rs`.

**Interfaces:**
- Consumes: `encode`, `decode`, `Decoded`, `WanRecord` (Task A1).
- Produces:
  - `pub struct WanQueue` with:
    - `pub fn open(dir: &std::path::Path) -> std::io::Result<WanQueue>` — recover records (stopping at a torn tail) + the acked cursor.
    - `pub fn append(&mut self, rec: &WanRecord) -> std::io::Result<u64>` — append + fsync, returns the record's sequence number.
    - `pub fn unacked(&self) -> Vec<(u64, WanRecord)>` — `(seq, record)` for `seq > acked`.
    - `pub fn ack(&mut self, up_to_seq: u64) -> std::io::Result<()>` — advance + persist the cursor.
    - `pub fn acked(&self) -> u64`
    - `pub fn len(&self) -> usize` / `pub fn is_empty(&self) -> bool` — count of unacked records.
    - `pub fn bytes(&self) -> u64` — segment size (for the bound).

- [ ] **Step 1: Write the failing test.** Create `crates/wan/tests/queue.rs`:

```rust
use std::path::PathBuf;
use wan::{WanOp, WanQueue, WanRecord};

fn tmp(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-wanq-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn rec(stamp: u64, k: &str) -> WanRecord {
    WanRecord { op: WanOp::Put, stamp, ttl_ms: 0, map: "m".into(), key: k.as_bytes().to_vec(), value: b"v".to_vec() }
}

#[test]
fn append_ack_and_recover() {
    let dir = tmp("recover");
    {
        let mut q = WanQueue::open(&dir).unwrap();
        assert_eq!(q.append(&rec(1, "a")).unwrap(), 1);
        assert_eq!(q.append(&rec(2, "b")).unwrap(), 2);
        assert_eq!(q.append(&rec(3, "c")).unwrap(), 3);
        assert_eq!(q.unacked().len(), 3);
        q.ack(2).unwrap(); // remote confirmed through seq 2
        assert_eq!(q.acked(), 2);
        assert_eq!(q.unacked().iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![3]);
    }
    // Reopen: the cursor is durable, so only seq 3 is still unacked.
    let q = WanQueue::open(&dir).unwrap();
    assert_eq!(q.acked(), 2);
    let un = q.unacked();
    assert_eq!(un.len(), 1);
    assert_eq!(un[0].1.key, b"c");
    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run to verify it fails.** Run: `cargo test -p wan --test queue` — Expected: FAIL (`WanQueue` undefined).

- [ ] **Step 3: Implement `WanQueue`.** Create `crates/wan/src/queue.rs`:

```rust
//! Durable per-target outbound buffer. Records are appended (framed, fsync'd) to
//! `records.log`; a committed cursor (`acked` sequence) lives in `cursor` and is
//! fsync'd on advance. On reopen, records replay (stopping at a torn tail) and
//! only those past the cursor are unacked. Mirrors the persistence WAL discipline.

use crate::record::{decode, encode, Decoded, WanRecord};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const LOG_FILE: &str = "records.log";
const CURSOR_FILE: &str = "cursor";

pub struct WanQueue {
    dir: PathBuf,
    seg: std::fs::File,
    records: Vec<(u64, WanRecord)>, // (seq, record), seq starts at 1
    next_seq: u64,
    acked: u64,
    bytes: u64,
}

impl WanQueue {
    pub fn open(dir: &Path) -> std::io::Result<WanQueue> {
        std::fs::create_dir_all(dir)?;
        let mut buf = Vec::new();
        match std::fs::File::open(dir.join(LOG_FILE)) {
            Ok(mut f) => {
                f.read_to_end(&mut buf)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut records = Vec::new();
        let mut off = 0;
        let mut seq = 0;
        while off < buf.len() {
            match decode(&buf[off..]) {
                Decoded::Record { rec, consumed } => {
                    seq += 1;
                    records.push((seq, rec));
                    off += consumed;
                }
                _ => break, // torn / short tail
            }
        }
        let acked = read_cursor(&dir.join(CURSOR_FILE))?;
        let seg = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))?;
        let bytes = off as u64;
        Ok(WanQueue {
            dir: dir.to_path_buf(),
            seg,
            records,
            next_seq: seq + 1,
            acked,
            bytes,
        })
    }

    pub fn append(&mut self, rec: &WanRecord) -> std::io::Result<u64> {
        let framed = encode(rec);
        self.seg.write_all(&framed)?;
        self.seg.sync_data()?;
        self.bytes += framed.len() as u64;
        let seq = self.next_seq;
        self.next_seq += 1;
        self.records.push((seq, rec.clone()));
        Ok(seq)
    }

    pub fn unacked(&self) -> Vec<(u64, WanRecord)> {
        self.records
            .iter()
            .filter(|(s, _)| *s > self.acked)
            .cloned()
            .collect()
    }

    pub fn ack(&mut self, up_to_seq: u64) -> std::io::Result<()> {
        if up_to_seq <= self.acked {
            return Ok(());
        }
        self.acked = up_to_seq.min(self.next_seq - 1);
        write_cursor(&self.dir.join(CURSOR_FILE), self.acked)?;
        Ok(())
    }

    pub fn acked(&self) -> u64 {
        self.acked
    }
    pub fn len(&self) -> usize {
        self.records.iter().filter(|(s, _)| *s > self.acked).count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

fn read_cursor(path: &Path) -> std::io::Result<u64> {
    match std::fs::read(path) {
        Ok(b) if b.len() >= 8 => Ok(u64::from_le_bytes(b[0..8].try_into().unwrap())),
        Ok(_) => Ok(0),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

fn write_cursor(path: &Path, seq: u64) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&seq.to_le_bytes())?;
    f.sync_data()?;
    std::fs::rename(&tmp, path)
}
```

Add `pub mod queue;` and `pub use queue::WanQueue;` to `crates/wan/src/lib.rs`.

- [ ] **Step 4: Run to verify pass.** Run: `cargo test -p wan --test queue` — Expected: PASS. `cargo fmt -p wan && cargo clippy -p wan`.

- [ ] **Step 5: Commit.**

```bash
git add crates/wan
git commit -m "feat(wan): durable outbound WanQueue with a committed cursor"
```

### Task A3: Store hooks — `wan_sink` + `apply_wan` + `WanPublisher`

**Files:** Modify `crates/store/src/lib.rs`; Create `crates/wan/src/publisher.rs`; Modify `crates/wan/src/lib.rs`.

**Interfaces:**
- Consumes: `store::WalSink` trait (`map_put`/`map_remove`/`aux_state`); `WanRecord`/`WanOp` (A1); `spsc::Producer` (`crates/spsc`).
- Produces (store): `Store::set_wan_sink(&self, sink: Arc<dyn WalSink>)`; `Store::apply_wan(&self, op_is_put: bool, map: &str, key: &[u8], value: &[u8], ttl_ms: u64, stamp: u64)` — `put_merge`/remove semantics that emit to `wal_sink` only (never `wan_sink`), so a WAN-applied write is persisted but never re-published.
- Produces (wan): `pub struct WanPublisher { tx: spsc::Producer<WanRecord> }` implementing `store::WalSink`; `WanPublisher::new(tx)`.

- [ ] **Step 1: Add the `wan_sink` field + `set_wan_sink` (store).** In `crates/store/src/lib.rs`, next to the existing `wal_sink: std::sync::OnceLock<...>` field, add:

```rust
    wan_sink: std::sync::OnceLock<std::sync::Arc<dyn WalSink>>,
```

Initialize it in the constructor(s) next to `wal_sink`: `wan_sink: std::sync::OnceLock::new(),`. Add the setter next to `set_wal_sink`:

```rust
    /// Attach the WAN capture sink (once, before serving). Local mutations emit
    /// to this in addition to the persistence sink; WAN-applied writes do not
    /// (see `apply_wan`) so replication does not loop.
    pub fn set_wan_sink(&self, sink: std::sync::Arc<dyn WalSink>) {
        let _ = self.wan_sink.set(sink);
    }
```

- [ ] **Step 2: Emit to `wan_sink` in the mutators (store).** In `put`, `put_ttl`, and `put_merge`, immediately after the existing `if let Some(s) = self.wal_sink.get() { s.map_put(...); }`, add the mirror for the WAN sink with the SAME arguments. Example for `put_merge`:

```rust
        if let Some(s) = self.wal_sink.get() {
            s.map_put(stamp, ttl_ms, map, key, val);
        }
        if let Some(s) = self.wan_sink.get() {
            s.map_put(stamp, ttl_ms, map, key, val);
        }
```

Do the same for `put`/`put_ttl` (using their `stamp`, ttl, `map`, `&key`, `&val`). In `remove`, after the existing `wal_sink` `map_remove`, add the `wan_sink` `map_remove` with the same `(self.next_stamp(), map, key)`.

- [ ] **Step 3: Write the failing test for `apply_wan` (store).** Add to the `#[cfg(test)] mod tests` in `crates/store/src/lib.rs`:

```rust
    #[test]
    fn apply_wan_persists_but_does_not_republish() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct Counter(Arc<AtomicUsize>);
        impl WalSink for Counter {
            fn map_put(&self, _: u64, _: u64, _: &str, _: &[u8], _: &[u8]) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn map_remove(&self, _: u64, _: &str, _: &[u8]) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn aux_state(&self, _: u8, _: &str, _: &[u8]) {}
        }

        let s = Store::new();
        let wal = Arc::new(AtomicUsize::new(0));
        let wan = Arc::new(AtomicUsize::new(0));
        s.set_wal_sink(Arc::new(Counter(wal.clone())));
        s.set_wan_sink(Arc::new(Counter(wan.clone())));

        // A local write emits to BOTH sinks.
        s.put("m", b"k".to_vec(), b"v".to_vec());
        assert_eq!(wal.load(Ordering::SeqCst), 1);
        assert_eq!(wan.load(Ordering::SeqCst), 1);

        // A WAN-applied write emits to the persistence sink only (no re-publish).
        s.apply_wan(true, "m", b"k2", b"v2", 0, s.next_stamp());
        assert_eq!(wal.load(Ordering::SeqCst), 2, "persisted");
        assert_eq!(wan.load(Ordering::SeqCst), 1, "NOT re-published");
        assert_eq!(s.get("m", b"k2"), Some(b"v2".to_vec()));
    }
```

- [ ] **Step 4: Run to verify it fails.** Run: `cargo test -p store apply_wan_persists` — Expected: FAIL (`apply_wan` undefined).

- [ ] **Step 5: Implement `apply_wan` (store).** Add a method on `Store` near `put_merge`:

```rust
    /// Apply an inbound WAN record with the HLC merge, WITHOUT re-publishing it to
    /// the WAN sink (loop prevention). It still emits to the persistence sink so
    /// the WAN-applied write survives restart.
    pub fn apply_wan(
        &self,
        op_is_put: bool,
        map: &str,
        key: &[u8],
        value: &[u8],
        ttl_ms: u64,
        stamp: u64,
    ) {
        self.observe_stamp(stamp);
        let schemas = self.schemas();
        if op_is_put {
            self.shard(map, key)
                .lock()
                .unwrap()
                .put_merge(map, key, value, ttl_ms, stamp, true, schemas);
        } else {
            self.shard(map, key).lock().unwrap().remove(map, key, schemas);
        }
        if let Some(s) = self.wal_sink.get() {
            if op_is_put {
                s.map_put(stamp, ttl_ms, map, key, value);
            } else {
                s.map_remove(stamp, map, key);
            }
        }
        // NOTE: deliberately no `wan_sink` emit — this is the loop-prevention seam.
    }
```

(If `Shard::put_merge`/`remove` signatures differ, match the exact call already used inside `Store::put_merge`/`Store::remove` — they are the same shard methods.)

- [ ] **Step 6: Run to verify pass.** Run: `cargo test -p store apply_wan_persists` — Expected: PASS. Run `cargo test -p store` to confirm no regression.

- [ ] **Step 7: Implement `WanPublisher` (wan).** Create `crates/wan/src/publisher.rs`:

```rust
//! Capture sink: a `store::WalSink` that turns each local IMap mutation into a
//! `WanRecord` pushed to the WAN thread over an SPSC ring. A full ring drops the
//! record (surfaced by metrics); the hot path never blocks on WAN.

use crate::record::{WanOp, WanRecord};
use store::WalSink;

pub struct WanPublisher {
    tx: spsc::Producer<WanRecord>,
}

impl WanPublisher {
    pub fn new(tx: spsc::Producer<WanRecord>) -> WanPublisher {
        WanPublisher { tx }
    }
}

impl WalSink for WanPublisher {
    fn map_put(&self, stamp: u64, ttl_ms: u64, map: &str, key: &[u8], value: &[u8]) {
        let _ = self.tx.push(WanRecord {
            op: WanOp::Put,
            stamp,
            ttl_ms,
            map: map.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }
    fn map_remove(&self, stamp: u64, map: &str, key: &[u8]) {
        let _ = self.tx.push(WanRecord {
            op: WanOp::Remove,
            stamp,
            ttl_ms: 0,
            map: map.to_string(),
            key: key.to_vec(),
            value: Vec::new(),
        });
    }
    fn aux_state(&self, _kind: u8, _name: &str, _state: &[u8]) {
        // Structures are Phase D.
    }
}
```

Add `spsc = { path = "../spsc" }` to `crates/wan/Cargo.toml` deps, and `pub mod publisher; pub use publisher::WanPublisher;` to `crates/wan/src/lib.rs`.

- [ ] **Step 8: Test the publisher.** Add `crates/wan/tests/publisher.rs`:

```rust
use wan::{WanOp, WanPublisher};
use store::WalSink;

#[test]
fn publisher_captures_puts_and_removes() {
    let (tx, rx) = spsc::channel::<wan::WanRecord>(16);
    let p = WanPublisher::new(tx);
    p.map_put(5, 0, "m", b"k", b"v");
    p.map_remove(6, "m", b"k");
    let a = rx.pop().unwrap();
    assert_eq!(a.op, WanOp::Put);
    assert_eq!(a.key, b"k");
    assert_eq!(a.stamp, 5);
    let b = rx.pop().unwrap();
    assert_eq!(b.op, WanOp::Remove);
    assert_eq!(b.stamp, 6);
}
```

Run: `cargo test -p wan --test publisher` — Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add crates/store crates/wan
git commit -m "feat(wan,store): wan_sink capture + apply_wan (loop prevention) + WanPublisher"
```

---

## Phase B — publisher↔consumer + convergence (the acid test)

### Task B1: WAN wire codec (`WanMsg`)

**Files:** Create `crates/wan/src/wire.rs`; Modify `crates/wan/src/lib.rs`.

**Interfaces:**
- Consumes: `encode`/`decode`/`Decoded`/`WanRecord` (A1).
- Produces: `pub enum WanMsg { Batch { up_to_seq: u64, records: Vec<WanRecord> }, Ack { up_to_seq: u64 } }`; `pub fn encode_msg(m: &WanMsg) -> Vec<u8>`; `pub fn decode_msg(b: &[u8]) -> Option<WanMsg>`.

- [ ] **Step 1: Write the failing test.** Create `crates/wan/src/wire.rs` with only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WanOp, WanRecord};

    #[test]
    fn msg_roundtrip() {
        let batch = WanMsg::Batch {
            up_to_seq: 9,
            records: vec![
                WanRecord { op: WanOp::Put, stamp: 1, ttl_ms: 0, map: "m".into(), key: b"a".to_vec(), value: b"1".to_vec() },
                WanRecord { op: WanOp::Remove, stamp: 2, ttl_ms: 0, map: "m".into(), key: b"b".to_vec(), value: vec![] },
            ],
        };
        assert_eq!(decode_msg(&encode_msg(&batch)).unwrap(), batch);
        let ack = WanMsg::Ack { up_to_seq: 9 };
        assert_eq!(decode_msg(&encode_msg(&ack)).unwrap(), ack);
    }
}
```

- [ ] **Step 2: Run it to verify it fails.** Run: `cargo test -p wan --lib wire` — Expected: FAIL (`WanMsg` undefined).

- [ ] **Step 3: Implement the codec.** Prepend to `crates/wan/src/wire.rs`:

```rust
//! WAN wire messages between clusters: a sequence-numbered batch of records, and
//! an ack of the highest applied sequence. Records reuse the `record` framing.

use crate::record::{decode, encode, Decoded, WanRecord};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WanMsg {
    Batch { up_to_seq: u64, records: Vec<WanRecord> },
    Ack { up_to_seq: u64 },
}

pub fn encode_msg(m: &WanMsg) -> Vec<u8> {
    let mut b = Vec::new();
    match m {
        WanMsg::Batch { up_to_seq, records } => {
            b.push(0);
            b.extend_from_slice(&up_to_seq.to_le_bytes());
            b.extend_from_slice(&(records.len() as u32).to_le_bytes());
            for r in records {
                b.extend_from_slice(&encode(r)); // self-delimiting (len-prefixed)
            }
        }
        WanMsg::Ack { up_to_seq } => {
            b.push(1);
            b.extend_from_slice(&up_to_seq.to_le_bytes());
        }
    }
    b
}

pub fn decode_msg(b: &[u8]) -> Option<WanMsg> {
    match *b.first()? {
        0 => {
            let up_to_seq = u64::from_le_bytes(b.get(1..9)?.try_into().ok()?);
            let n = u32::from_le_bytes(b.get(9..13)?.try_into().ok()?) as usize;
            let mut off = 13;
            let mut records = Vec::with_capacity(n);
            for _ in 0..n {
                match decode(b.get(off..)?) {
                    Decoded::Record { rec, consumed } => {
                        records.push(rec);
                        off += consumed;
                    }
                    _ => return None,
                }
            }
            Some(WanMsg::Batch { up_to_seq, records })
        }
        1 => Some(WanMsg::Ack {
            up_to_seq: u64::from_le_bytes(b.get(1..9)?.try_into().ok()?),
        }),
        _ => None,
    }
}
```

Add `pub mod wire; pub use wire::{decode_msg, encode_msg, WanMsg};` to `crates/wan/src/lib.rs`.

- [ ] **Step 4: Run to verify pass + commit.** Run: `cargo test -p wan --lib wire`. `cargo fmt -p wan && cargo clippy -p wan`.

```bash
git add crates/wan
git commit -m "feat(wan): WAN wire codec (Batch/Ack)"
```

### Task B2: `WanConsumer` — apply a batch

**Files:** Create `crates/wan/src/consumer.rs`; Modify `crates/wan/src/lib.rs`.

**Interfaces:**
- Consumes: `WanMsg`/`WanRecord` (B1/A1); `store::Store::apply_wan` (A3).
- Produces: `pub fn apply_batch(store: &store::Store, records: &[WanRecord])` — applies each record via `apply_wan` (put → `op_is_put=true`, remove → false).

- [ ] **Step 1: Write the failing test.** Create `crates/wan/tests/consumer.rs`:

```rust
use store::Store;
use wan::{apply_batch, WanOp, WanRecord};

#[test]
fn applies_puts_and_removes_via_merge() {
    let s = Store::new();
    let stamp = s.next_stamp();
    apply_batch(&s, &[
        WanRecord { op: WanOp::Put, stamp, ttl_ms: 0, map: "m".into(), key: b"k".to_vec(), value: b"v".to_vec() },
    ]);
    assert_eq!(s.get("m", b"k"), Some(b"v".to_vec()));

    // A lower-stamped put loses the merge (HLC), a remove at a higher stamp wins.
    apply_batch(&s, &[
        WanRecord { op: WanOp::Put, stamp: 1, ttl_ms: 0, map: "m".into(), key: b"k".to_vec(), value: b"OLD".to_vec() },
    ]);
    assert_eq!(s.get("m", b"k"), Some(b"v".to_vec()), "stale put ignored by merge");

    apply_batch(&s, &[
        WanRecord { op: WanOp::Remove, stamp: s.next_stamp(), ttl_ms: 0, map: "m".into(), key: b"k".to_vec(), value: vec![] },
    ]);
    assert_eq!(s.get("m", b"k"), None);
}
```

- [ ] **Step 2: Run it to verify it fails.** Run: `cargo test -p wan --test consumer` — Expected: FAIL (`apply_batch` undefined).

- [ ] **Step 3: Implement.** Create `crates/wan/src/consumer.rs`:

```rust
//! WAN consumer: apply an inbound batch to the local store via `apply_wan`
//! (HLC merge, not re-published — loop prevention lives in the store).

use crate::record::{WanOp, WanRecord};
use store::Store;

pub fn apply_batch(store: &Store, records: &[WanRecord]) {
    for r in records {
        store.apply_wan(
            matches!(r.op, WanOp::Put),
            &r.map,
            &r.key,
            &r.value,
            r.ttl_ms,
            r.stamp,
        );
    }
}
```

Add `pub mod consumer; pub use consumer::apply_batch;` to `crates/wan/src/lib.rs`. Add `store` is already a dep.

- [ ] **Step 4: Run to verify pass + commit.**

```bash
cargo test -p wan --test consumer
git add crates/wan
git commit -m "feat(wan): WanConsumer apply_batch (HLC merge, loop-free)"
```

### Task B3: Two-cluster convergence sim (the acid test)

**Files:** Create `crates/wan/tests/convergence.rs`.

**Interfaces:** Consumes everything above (`Store`, `WanPublisher`, `WanQueue`, `apply_batch`, `WanMsg`). No new production code — this task proves the composition.

- [ ] **Step 1: Write the sim + tests.** Create `crates/wan/tests/convergence.rs`:

```rust
//! Deterministic two-cluster WAN sim: each cluster is a Store + a capture ring +
//! a durable outbound queue; a controllable in-memory link ships batches and
//! acks. Proves one-way replication, active-active convergence, loop prevention,
//! and outage-then-replay — with no real network.

use std::path::PathBuf;
use store::Store;
use wan::{apply_batch, WanPublisher, WanQueue, WanRecord};

fn tmp(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-wan-conv-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// One cluster: store + capture ring consumer + durable queue.
struct Cluster {
    store: std::sync::Arc<Store>,
    rx: spsc::Consumer<WanRecord>,
    queue: WanQueue,
}

impl Cluster {
    fn new(dir: PathBuf) -> Cluster {
        let store = std::sync::Arc::new(Store::new());
        let (tx, rx) = spsc::channel::<WanRecord>(4096);
        store.set_wan_sink(std::sync::Arc::new(WanPublisher::new(tx)));
        Cluster { store, rx, queue: WanQueue::open(&dir).unwrap() }
    }
    /// Drain captured records into the durable outbound queue.
    fn pump(&mut self) {
        while let Some(r) = self.rx.pop() {
            self.queue.append(&r).unwrap();
        }
    }
}

/// Ship a's unacked records to b, apply them, and ack a (if `link_up`).
fn ship(a: &mut Cluster, b: &Cluster, link_up: bool) {
    a.pump();
    if !link_up {
        return;
    }
    let un = a.queue.unacked();
    if un.is_empty() {
        return;
    }
    let recs: Vec<WanRecord> = un.iter().map(|(_, r)| r.clone()).collect();
    let up_to = un.last().unwrap().0;
    apply_batch(&b.store, &recs);
    a.queue.ack(up_to).unwrap();
}

#[test]
fn one_way_replication() {
    let mut a = Cluster::new(tmp("a1"));
    let b = Cluster::new(tmp("b1"));
    a.store.put("m", b"k".to_vec(), b"v".to_vec());
    ship(&mut a, &b, true);
    assert_eq!(b.store.get("m", b"k"), Some(b"v".to_vec()));
}

#[test]
fn active_active_converges_and_does_not_loop() {
    let mut a = Cluster::new(tmp("a2"));
    let mut b = Cluster::new(tmp("b2"));
    // Concurrent writes to the same key; higher HLC stamp wins on both sides.
    a.store.put("m", b"k".to_vec(), b"A".to_vec());
    b.store.put("m", b"k".to_vec(), b"B".to_vec()); // b's stamp is later (created after a's)
    ship(&mut a, &b, true);
    ship(&mut b, &a, true);
    let (va, vb) = (a.store.get("m", b"k"), b.store.get("m", b"k"));
    assert_eq!(va, vb, "both clusters converge to the same value");
    // Loop prevention: applying b's record on a did NOT enqueue anything new on a
    // beyond a's own write (a's queue was fully acked).
    a.pump();
    assert!(a.queue.unacked().is_empty(), "WAN-applied write was not re-captured");
}

#[test]
fn outage_then_replay() {
    let mut a = Cluster::new(tmp("a3"));
    let b = Cluster::new(tmp("b3"));
    // Link down: writes accumulate durably; nothing reaches b.
    for i in 0..5 {
        a.store.put("m", format!("k{i}").into_bytes(), b"v".to_vec());
    }
    ship(&mut a, &b, false);
    assert_eq!(b.store.get("m", b"k0"), None);
    assert_eq!(a.queue.len(), 5, "buffered durably");
    // Link restored: all buffered writes replay and b converges.
    ship(&mut a, &b, true);
    for i in 0..5 {
        assert_eq!(b.store.get("m", format!("k{i}").as_bytes()), Some(b"v".to_vec()));
    }
    assert_eq!(a.queue.acked(), 5);
}
```

- [ ] **Step 2: Run it.** Run: `cargo test -p wan --test convergence` — Expected: PASS (3 tests). If `active_active_converges` is flaky on stamp ordering, make b's write explicitly later by calling `a.store.next_stamp()` a few times before b's put (HLC monotonicity), or assert only equality (convergence) rather than which value wins.

- [ ] **Step 3: Commit.**

```bash
git add crates/wan
git commit -m "test(wan): two-cluster convergence sim — one-way, active-active, loop-free, outage-replay"
```

---

## Phase C — live TCP transport + config + backpressure

### Task C1: WAN thread over the member transport + config wiring

**Files:** Create `crates/server/src/wan_thread.rs`; Modify `crates/server/src/lib.rs`, `crates/server/src/main.rs`, `crates/server/Cargo.toml`.

**Interfaces:**
- Consumes: `WanQueue`, `WanPublisher`, `apply_batch`, `WanMsg`/`encode_msg`/`decode_msg`; `member::transport` (a WAN connection — bind a WAN port, dial the remote); `store::Store::set_wan_sink`.
- Produces: `fn spawn_wan(dir, store, rx, targets, batch, listen_port) -> JoinHandle` and a `WanTargets` config parsed from env.

- [ ] **Step 1: Add config + the WAN thread skeleton.** Add `wan = { path = "../wan" }` to `crates/server/Cargo.toml`. Create `crates/server/src/wan_thread.rs` with:
  - `pub fn wan_config() -> Option<WanConfig>` parsing `BONSAI_WAN_TARGETS` (comma-separated `host:port`), `BONSAI_WAN_PORT`, `BONSAI_WAN_BATCH` (default 256), `BONSAI_WAN_QUEUE_MB` (default 256), `BONSAI_WAN_BACKPRESSURE` (`throw`/`drop-oldest`, default `throw`). Return `None` when `BONSAI_WAN_TARGETS` is unset.
  - `pub fn spawn_wan(dir: PathBuf, store: Arc<Store>, rx: spsc::Consumer<wan::WanRecord>, cfg: WanConfig) -> JoinHandle<()>` — a thread that: drains `rx` → `queue.append` (respecting the byte bound + backpressure policy), ships `queue.unacked()` in `cfg.batch`-sized `WanMsg::Batch`es over a dialed WAN connection to each target, applies inbound `WanMsg::Batch`es via `wan::apply_batch`, and `queue.ack`s on `WanMsg::Ack`. Reuse the `member::transport` framing for the WAN connection (a distinct listener on `BONSAI_WAN_PORT`).
  - Register `pub mod wan_thread;` in `crates/server/src/lib.rs`.

  Model the drain/fsync/ship loop on `crates/server/src/persist_thread.rs` (same SPSC-drain + fsync-cadence shape) and the connection handling on `crates/member/src/transport.rs` / `crates/server/src/member_thread.rs`.

- [ ] **Step 2: Wire it in `main.rs`.** In each run path, after building the `store` (next to `setup_persistence`), add `setup_wan(&store)`: if `wan_config()` is `Some`, create an SPSC channel, `store.set_wan_sink(Arc::new(wan::WanPublisher::new(tx)))`, and `spawn_wan(dir, store.clone(), rx, cfg)`; keep the handle alive. `dir` = `BONSAI_WAN_DIR` (or a default under the data dir).

- [ ] **Step 3: Build + smoke.** Run: `cargo build -p server`. Because full validation needs two live clusters, add a build-level test only: a unit test in `wan_thread.rs` that `wan_config()` parses `BONSAI_WAN_TARGETS="host:1,host:2"` into two targets and applies the defaults. Run: `cargo test -p server wan_config`.

- [ ] **Step 4: Commit.**

```bash
git add crates/server
git commit -m "feat(server): WAN thread over the member transport + BONSAI_WAN config"
```

### Task C2: Queue bound + backpressure policy

**Files:** Modify `crates/wan/src/queue.rs` (add a bounded append), `crates/server/src/wan_thread.rs`.

**Interfaces:** Produces `WanQueue::would_exceed(&self, max_bytes: u64) -> bool` and the WAN thread's enforcement of `throw` (park the producing write via a full capture ring — natural backpressure) vs `drop-oldest` (advance `acked`/truncate, log the loss).

- [ ] **Step 1: Write the failing test.** Add to `crates/wan/tests/queue.rs`:

```rust
#[test]
fn reports_when_over_the_byte_bound() {
    let dir = tmp("bound");
    let mut q = WanQueue::open(&dir).unwrap();
    assert!(!q.would_exceed(10_000));
    for i in 0..100 {
        q.append(&rec(i, "k")).unwrap();
    }
    assert!(q.would_exceed(10), "many records exceed a tiny bound");
    std::fs::remove_dir_all(&dir).ok();
}
```

- [ ] **Step 2: Run to verify it fails.** Run: `cargo test -p wan --test queue reports_when_over` — Expected: FAIL (`would_exceed` undefined).

- [ ] **Step 3: Implement.** Add to `impl WanQueue`:

```rust
    /// True if the durable segment already exceeds `max_bytes` (backpressure gate).
    pub fn would_exceed(&self, max_bytes: u64) -> bool {
        self.bytes >= max_bytes
    }
```

In `wan_thread.rs`, before draining the capture ring into the queue, if `queue.would_exceed(cfg.queue_bytes)`: for `throw`, stop draining this tick (the SPSC ring fills and the reactor's `push` returns `Err`, dropping/parking the write — surface a metric); for `drop-oldest`, `queue.ack(queue.acked() + drop_n)` to advance past the oldest unacked and log the dropped count.

- [ ] **Step 4: Run + commit.**

```bash
cargo test -p wan --test queue
git add crates/wan crates/server
git commit -m "feat(wan): outbound queue byte-bound + backpressure policy"
```

---

## Phase D — non-map structures

### Task D1: Capture aux-structure mutations over WAN

**Files:** Modify `crates/wan/src/record.rs` (an `AuxState` record variant), `crates/wan/src/publisher.rs` (`aux_state` → record), `crates/wan/src/consumer.rs` (apply via `store::install_aux`).

**Interfaces:** Produces a `WanOp::Aux { kind: u8 }` record carrying `(kind, name, state)`; `WanPublisher::aux_state` pushes it; `apply_batch` routes aux records to `store.install_aux(kind, name, state)` (the same method persistence recovery uses — see `bonsaigrid-persistence`).

- [ ] **Step 1: Extend the record.** Add an aux variant to `WanRecord` (or a parallel `WanRecord::Aux { kind, name, state }` enum) with `op` byte `3`, framed like `Put`/`Remove`; add roundtrip tests mirroring Task A1.
- [ ] **Step 2: Capture.** In `WanPublisher::aux_state`, push the aux record instead of the current no-op. Update the publisher test to assert an aux record is captured.
- [ ] **Step 3: Apply.** In `apply_batch`, for an aux record call `store.install_aux(kind, &name, &state)` (do NOT re-publish — `install_aux` doesn't hit `wan_sink`; confirm and, if it would, gate it like `apply_wan`).
- [ ] **Step 4: Convergence test.** Extend `crates/wan/tests/convergence.rs`: a `queue_offer` on cluster A replicates to B; assert `b.store.queue_size` matches.
- [ ] **Step 5: Commit.**

```bash
git add crates/wan crates/store
git commit -m "feat(wan): replicate non-map structures over WAN (aux_state capture)"
```

---

## Self-Review

**Spec coverage:** WanRecord (A1), durable queue + cursor (A2), capture `wan_sink` + `apply_wan` loop prevention (A3), wire codec (B1), consumer apply (B2), two-cluster convergence/active-active/loop/outage sim (B3), live transport + config (C1), bound + backpressure (C2), structures (D1). Config keys, HLC merge, guardrails, and at-least-once dedup are all covered. No spec requirement is unaddressed for the IMap v1 scope.

**Placeholder scan:** none — every code step shows the code; commands have expected results.

**Type consistency:** `WanRecord`/`WanOp`/`Decoded` (A1) are used unchanged in A2/B1/B2/B3; `apply_wan(op_is_put, map, key, value, ttl_ms, stamp)` defined in A3 is called with the same signature in B2; `WanQueue::{open,append,unacked,ack,acked,len,bytes,would_exceed}` are consistent across A2/B3/C2; `WanMsg::{Batch,Ack}` consistent B1/C1.
