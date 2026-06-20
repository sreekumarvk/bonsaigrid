# Increment 0: Single-Core Walking Skeleton — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the smallest BonsaiGrid that an unmodified Hazelcast client can connect to and perform `IMap.put` / `IMap.get` against, proving wire compatibility end-to-end.

**Architecture:** Single OS thread, blocking `std::net` TCP, one connection handled at a time (or thread-per-connection). Server speaks the Hazelcast Open Client Protocol (generation 2, fixtures version 2.10): consumes the `CP2` preamble, decodes/encodes length-prefixed little-endian frames, answers the auth + cluster-view handshake advertising a single member that owns all 271 partitions, and serves map ops from an in-memory table that stores opaque serialized `Data` blobs (never deserialized). TPC is disabled in this increment.

**Tech Stack:** Rust (stable), `std::net`, `std::collections::HashMap`. No external runtime crates yet. Conformance: Hazelcast's committed golden binary fixture `2.10.protocol.compatibility.binary`; a Java 17 harness using the real Hazelcast client; a Python `hazelcast` client smoke test in a venv.

## Global Constraints

- **Wire compatibility is immutable.** The server adapts to stock clients; clients are never modified. All multi-byte integers are **little-endian**. Reference: `./hazelcast/hazelcast/src/main/java/com/hazelcast/client/impl/protocol/`.
- **This increment deliberately does NOT meet the project performance guardrails.** Zero-allocation hot path, `io_uring`, thread-per-core, and the slab allocator are **out of scope here** — they are increments 1–3, each a measurable perf win. Do not add them now; do not treat their absence as a defect.
- **Opaque `Data`.** Map keys and values are Hazelcast-serialized `Data` blobs. Store and return their exact bytes; never parse or deserialize them.
- **Single core, N=1.** The server advertises one member owning partitions `0..=270`; `partitionCount = 271`. TPC is disabled (`tpcPorts = null`).
- **Determinism.** `Date.now()`/random are unavailable in this environment for fixtures; member UUID and cluster UUID are fixed constants defined in Task 12.
- **Protocol constants** (from the reference codecs): frame prefix size = 6 bytes (`u32 LE length` + `u16 LE flags`); flags `UNFRAGMENTED_MESSAGE = 0xC000`, `IS_FINAL_FLAG = 1<<13 = 0x2000`, `BEGIN_DATA_STRUCTURE_FLAG = 1<<12`, `END_DATA_STRUCTURE_FLAG = 1<<11`, `IS_NULL_FLAG = 1<<10`, `IS_EVENT_FLAG = 1<<9`. Header offsets in the initial frame content: `type` @0 (i32), `correlationId` @4 (i64), `partitionId` @12 (i32, requests) / `backupAcks` @12 (u8, responses). Message type ids: Auth 256/257, AddClusterViewListener 768/769 (+events 770 members, 771 partitions), MapPut 65792/65793, MapGet 66048/66049.

---

## File Structure

```
bonsaigrid/
├── Cargo.toml                      # workspace
├── crates/
│   ├── protocol/                   # frame envelope + primitive codecs (no networking)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── frame.rs            # Frame, flags, wire read/write
│   │       ├── fixed.rs            # LE int/long/short/bool/byte/uuid
│   │       ├── primitives.rs       # string + data frame codecs, null frame
│   │       └── message.rs          # ClientMessage: type/correlationId/partitionId accessors
│   ├── codecs/                     # message + custom-type codecs
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── address.rs          # Address (BEGIN/initial[port]/host/END)
│   │       ├── member_info.rs      # MemberInfo
│   │       ├── partition_table.rs  # EntryList<UUID, List<Integer>>
│   │       ├── auth.rs             # ClientAuthentication 256/257
│   │       ├── cluster_view.rs     # AddClusterViewListener 768/769 + events 770/771
│   │       └── map.rs              # MapPut 65792, MapGet 66048
│   ├── store/                      # single-node opaque-blob map
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── server/                     # TCP loop + handshake wiring + binary main
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── connection.rs       # CP2 preamble, frame read/write, dispatch
│           ├── handlers.rs         # per-message-type response logic
│           └── main.rs             # bind, accept loop
├── tests/
│   └── golden/                     # vendored 2.10.protocol.compatibility.binary + index
├── conformance-java/               # Maven project: real Hazelcast client harness
└── conformance-python/             # venv + smoke script
```

---

## Phase 0 — Project bootstrap

### Task 1: Cargo workspace

**Files:**
- Create: `bonsaigrid/Cargo.toml`
- Create: `bonsaigrid/crates/protocol/Cargo.toml`
- Create: `bonsaigrid/crates/protocol/src/lib.rs`

**Interfaces:**
- Produces: a buildable workspace; `protocol` crate compiles.

- [ ] **Step 1: Create the workspace manifest**

`bonsaigrid/Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = ["crates/protocol", "crates/codecs", "crates/store", "crates/server"]

[workspace.package]
edition = "2021"
version = "0.0.0"
```

- [ ] **Step 2: Create the protocol crate manifest**

`bonsaigrid/crates/protocol/Cargo.toml`:
```toml
[package]
name = "protocol"
edition.workspace = true
version.workspace = true
```

- [ ] **Step 3: Create a placeholder lib so the workspace builds**

`bonsaigrid/crates/protocol/src/lib.rs`:
```rust
//! Hazelcast client wire-protocol frame envelope and primitive codecs.
```

- [ ] **Step 4: Verify it builds**

Run: `cd bonsaigrid && cargo build`
Expected: compiles (the other members don't exist yet — temporarily comment them out of `members` if needed, or create empty stubs in this task). To keep the workspace valid, also create minimal `Cargo.toml` + `src/lib.rs` (or `src/main.rs` for `server`) stubs for `codecs`, `store`, `server` now, each with the same package skeleton.

- [ ] **Step 5: Commit**

```bash
cd bonsaigrid && git init -q && git add -A && git commit -q -m "chore: bootstrap cargo workspace"
```
*(If the repo root should be the git root instead, init there; confirm with the user. The workspace must be under version control before Task 2.)*

---

## Phase 1 — Frame envelope + primitive codecs

### Task 2: Little-endian fixed-size codecs

**Files:**
- Create: `bonsaigrid/crates/protocol/src/fixed.rs`
- Modify: `bonsaigrid/crates/protocol/src/lib.rs` (add `pub mod fixed;`)

**Interfaces:**
- Produces:
  - `pub fn write_i32_le(buf: &mut [u8], pos: usize, v: i32)`
  - `pub fn read_i32_le(buf: &[u8], pos: usize) -> i32`
  - `pub fn write_i64_le(buf: &mut [u8], pos: usize, v: i64)`
  - `pub fn read_i64_le(buf: &[u8], pos: usize) -> i64`
  - `pub fn write_u16_le(buf: &mut [u8], pos: usize, v: u16)`
  - `pub fn read_u16_le(buf: &[u8], pos: usize) -> u16`
  - `pub const UUID_SIZE: usize = 17;`
  - `pub fn write_uuid(buf: &mut [u8], pos: usize, uuid: Option<(i64, i64)>)` — null flag byte + msb + lsb (matches `FixedSizeTypesCodec.encodeUUID`)
  - `pub fn read_uuid(buf: &[u8], pos: usize) -> Option<(i64, i64)>`

- [ ] **Step 1: Write the failing test**

`bonsaigrid/crates/protocol/src/fixed.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i32_roundtrip_is_little_endian() {
        let mut b = [0u8; 4];
        write_i32_le(&mut b, 0, 66048); // MapGet request type 0x010200
        assert_eq!(b, [0x00, 0x02, 0x01, 0x00]);
        assert_eq!(read_i32_le(&b, 0), 66048);
    }

    #[test]
    fn uuid_null_is_single_flag_byte() {
        let mut b = [0xFFu8; UUID_SIZE];
        write_uuid(&mut b, 0, None);
        assert_eq!(b[0], 1); // isNull = true
    }

    #[test]
    fn uuid_present_roundtrips() {
        let mut b = [0u8; UUID_SIZE];
        write_uuid(&mut b, 0, Some((0x1122334455667788, 0x99AABBCCDDEEFF00u64 as i64)));
        assert_eq!(b[0], 0); // isNull = false
        assert_eq!(read_uuid(&b, 0), Some((0x1122334455667788, 0x99AABBCCDDEEFF00u64 as i64)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bonsaigrid && cargo test -p protocol fixed`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement**

Prepend to `fixed.rs` (above the test module):
```rust
//! Little-endian fixed-size encoders, mirroring Hazelcast `FixedSizeTypesCodec`.

pub const UUID_SIZE: usize = 17; // 1 null-flag byte + 2 * i64

pub fn write_i32_le(buf: &mut [u8], pos: usize, v: i32) {
    buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes());
}
pub fn read_i32_le(buf: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap())
}
pub fn write_i64_le(buf: &mut [u8], pos: usize, v: i64) {
    buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
}
pub fn read_i64_le(buf: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap())
}
pub fn write_u16_le(buf: &mut [u8], pos: usize, v: u16) {
    buf[pos..pos + 2].copy_from_slice(&v.to_le_bytes());
}
pub fn read_u16_le(buf: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap())
}

pub fn write_uuid(buf: &mut [u8], pos: usize, uuid: Option<(i64, i64)>) {
    match uuid {
        None => buf[pos] = 1,
        Some((msb, lsb)) => {
            buf[pos] = 0;
            write_i64_le(buf, pos + 1, msb);
            write_i64_le(buf, pos + 9, lsb);
        }
    }
}
pub fn read_uuid(buf: &[u8], pos: usize) -> Option<(i64, i64)> {
    if buf[pos] == 1 {
        None
    } else {
        Some((read_i64_le(buf, pos + 1), read_i64_le(buf, pos + 9)))
    }
}
```
Add `pub mod fixed;` to `lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bonsaigrid && cargo test -p protocol fixed`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -q -m "feat(protocol): little-endian fixed-size codecs"
```

### Task 3: Frame envelope (wire read/write)

**Files:**
- Create: `bonsaigrid/crates/protocol/src/frame.rs`
- Modify: `bonsaigrid/crates/protocol/src/lib.rs` (`pub mod frame;`)

**Interfaces:**
- Consumes: `fixed::{write_i32_le, read_i32_le, write_u16_le, read_u16_le}`
- Produces:
  - `pub struct Frame { pub flags: u16, pub content: Vec<u8> }`
  - flag consts: `pub const UNFRAGMENTED: u16 = 0xC000; pub const IS_FINAL: u16 = 0x2000; pub const BEGIN_DS: u16 = 0x1000; pub const END_DS: u16 = 0x0800; pub const IS_NULL: u16 = 0x0400; pub const IS_EVENT: u16 = 0x0200;`
  - `pub const PREFIX_LEN: usize = 6;`
  - `impl Frame { pub fn is_null(&self) -> bool; pub fn is_end(&self) -> bool; pub fn is_begin(&self) -> bool; }`
  - `pub fn write_message(frames: &[Frame]) -> Vec<u8>` — serializes a full message; sets `IS_FINAL` on the last frame.
  - `pub fn read_message(bytes: &[u8]) -> Option<(Vec<Frame>, usize)>` — parses one complete message; returns frames + bytes consumed, or `None` if more bytes are needed.

- [ ] **Step 1: Write the failing test**

`bonsaigrid/crates/protocol/src/frame.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_frame_message_roundtrips_with_final_flag() {
        let f = Frame { flags: UNFRAGMENTED, content: vec![1, 2, 3] };
        let wire = write_message(&[f]);
        // length = 6 + 3 = 9 (LE), flags = UNFRAGMENTED | IS_FINAL = 0xE000 (LE)
        assert_eq!(&wire[0..4], &[9, 0, 0, 0]);
        assert_eq!(&wire[4..6], &(UNFRAGMENTED | IS_FINAL).to_le_bytes());
        assert_eq!(&wire[6..9], &[1, 2, 3]);

        let (frames, used) = read_message(&wire).unwrap();
        assert_eq!(used, 9);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].content, vec![1, 2, 3]);
    }

    #[test]
    fn read_message_returns_none_when_incomplete() {
        let f = Frame { flags: UNFRAGMENTED, content: vec![1, 2, 3] };
        let wire = write_message(&[f]);
        assert!(read_message(&wire[..5]).is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bonsaigrid && cargo test -p protocol frame`
Expected: FAIL — undefined items.

- [ ] **Step 3: Implement**

Prepend to `frame.rs`:
```rust
//! Hazelcast client-protocol frame envelope.
//! Wire frame = [u32 LE length = 6 + content.len][u16 LE flags][content].
//! A message is a sequence of frames; the last frame has IS_FINAL set.

use crate::fixed::{read_i32_le, read_u16_le, write_i32_le, write_u16_le};

pub const PREFIX_LEN: usize = 6;
pub const UNFRAGMENTED: u16 = 0xC000;
pub const IS_FINAL: u16 = 0x2000;
pub const BEGIN_DS: u16 = 0x1000;
pub const END_DS: u16 = 0x0800;
pub const IS_NULL: u16 = 0x0400;
pub const IS_EVENT: u16 = 0x0200;

#[derive(Clone, Debug)]
pub struct Frame {
    pub flags: u16,
    pub content: Vec<u8>,
}

impl Frame {
    pub fn is_null(&self) -> bool { self.flags & IS_NULL != 0 }
    pub fn is_begin(&self) -> bool { self.flags & BEGIN_DS != 0 }
    pub fn is_end(&self) -> bool { self.flags & END_DS != 0 }
}

pub fn write_message(frames: &[Frame]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, f) in frames.iter().enumerate() {
        let len = (PREFIX_LEN + f.content.len()) as i32;
        let mut prefix = [0u8; PREFIX_LEN];
        write_i32_le(&mut prefix, 0, len);
        let flags = if i + 1 == frames.len() { f.flags | IS_FINAL } else { f.flags };
        write_u16_le(&mut prefix, 4, flags);
        out.extend_from_slice(&prefix);
        out.extend_from_slice(&f.content);
    }
    out
}

pub fn read_message(bytes: &[u8]) -> Option<(Vec<Frame>, usize)> {
    let mut frames = Vec::new();
    let mut off = 0;
    loop {
        if bytes.len() < off + PREFIX_LEN {
            return None;
        }
        let len = read_i32_le(bytes, off) as usize;
        let flags = read_u16_le(bytes, off + 4);
        if bytes.len() < off + len {
            return None;
        }
        let content = bytes[off + PREFIX_LEN..off + len].to_vec();
        frames.push(Frame { flags, content });
        off += len;
        if flags & IS_FINAL != 0 {
            return Some((frames, off));
        }
    }
}
```
Add `pub mod frame;` to `lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bonsaigrid && cargo test -p protocol frame`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -q -m "feat(protocol): frame envelope read/write"
```

### Task 4: String/Data/null primitive frame codecs + ClientMessage accessors

**Files:**
- Create: `bonsaigrid/crates/protocol/src/primitives.rs`
- Create: `bonsaigrid/crates/protocol/src/message.rs`
- Modify: `bonsaigrid/crates/protocol/src/lib.rs`

**Interfaces:**
- Consumes: `frame::{Frame, IS_NULL, ...}`, `fixed::*`
- Produces:
  - `primitives::string_frame(s: &str) -> Frame` and `decode_string(f: &Frame) -> String`
  - `primitives::data_frame(blob: &[u8]) -> Frame` (content = blob verbatim; opaque)
  - `primitives::null_frame() -> Frame` (flags `IS_NULL`, empty content)
  - `message::msg_type(frames: &[Frame]) -> i32` (read i32 @0 of frame 0)
  - `message::correlation_id(frames: &[Frame]) -> i64` (read i64 @4 of frame 0)
  - `message::set_correlation_id(frames: &mut [Frame], id: i64)`
  - `message::partition_id(frames: &[Frame]) -> i32` (read i32 @12 of frame 0)

- [ ] **Step 1: Write the failing test**

`bonsaigrid/crates/protocol/src/primitives.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn string_frame_is_utf8_content() {
        let f = string_frame("dev");
        assert_eq!(f.content, b"dev");
        assert_eq!(decode_string(&f), "dev");
    }
    #[test]
    fn data_frame_is_verbatim_blob() {
        let blob = [0x00u8, 0x00, 0x00, 0x01, 0xAB];
        assert_eq!(data_frame(&blob).content, blob);
    }
    #[test]
    fn null_frame_sets_null_flag() {
        assert!(null_frame().is_null());
    }
}
```

`bonsaigrid/crates/protocol/src/message.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Frame, UNFRAGMENTED};
    #[test]
    fn reads_type_correlation_partition_from_initial_frame() {
        let mut content = vec![0u8; 16];
        crate::fixed::write_i32_le(&mut content, 0, 66048); // type
        crate::fixed::write_i64_le(&mut content, 4, 42);    // correlation
        crate::fixed::write_i32_le(&mut content, 12, 7);    // partition
        let frames = vec![Frame { flags: UNFRAGMENTED, content }];
        assert_eq!(msg_type(&frames), 66048);
        assert_eq!(correlation_id(&frames), 42);
        assert_eq!(partition_id(&frames), 7);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bonsaigrid && cargo test -p protocol`
Expected: FAIL — undefined items.

- [ ] **Step 3: Implement**

`primitives.rs` (above tests):
```rust
//! String/Data/null primitive frame codecs.
use crate::frame::{Frame, IS_NULL, UNFRAGMENTED};

pub fn string_frame(s: &str) -> Frame {
    Frame { flags: 0, content: s.as_bytes().to_vec() }
}
pub fn decode_string(f: &Frame) -> String {
    String::from_utf8(f.content.clone()).expect("utf8")
}
pub fn data_frame(blob: &[u8]) -> Frame {
    Frame { flags: 0, content: blob.to_vec() }
}
pub fn null_frame() -> Frame {
    Frame { flags: IS_NULL, content: Vec::new() }
}
// Re-export for handlers that build initial frames.
pub fn initial_frame(content: Vec<u8>) -> Frame {
    Frame { flags: UNFRAGMENTED, content }
}
```

`message.rs` (above tests):
```rust
//! Accessors over a message's initial frame header.
use crate::fixed::{read_i32_le, read_i64_le, write_i64_le};
use crate::frame::Frame;

pub fn msg_type(frames: &[Frame]) -> i32 { read_i32_le(&frames[0].content, 0) }
pub fn correlation_id(frames: &[Frame]) -> i64 { read_i64_le(&frames[0].content, 4) }
pub fn set_correlation_id(frames: &mut [Frame], id: i64) {
    write_i64_le(&mut frames[0].content, 4, id);
}
pub fn partition_id(frames: &[Frame]) -> i32 { read_i32_le(&frames[0].content, 12) }
```
Add `pub mod primitives;` and `pub mod message;` to `lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bonsaigrid && cargo test -p protocol`
Expected: PASS (all protocol tests).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -q -m "feat(protocol): string/data/null codecs and message header accessors"
```

### Task 5: Vendor the golden conformance fixture + index

**Files:**
- Create: `bonsaigrid/tests/golden/2.10.protocol.compatibility.binary` (copied from reference)
- Create: `bonsaigrid/tests/golden/INDEX.md` (human-readable map of message index → codec)
- Create: `bonsaigrid/crates/protocol/tests/golden_frame.rs`

**Interfaces:**
- Consumes: `protocol::frame::read_message`
- Produces: a reusable `fn load_golden() -> Vec<u8>` test helper and proof the fixture parses into the expected number of messages.

- [ ] **Step 1: Copy the fixture and document its layout**

```bash
cp ../hazelcast/hazelcast/src/test/resources/2.10.protocol.compatibility.binary bonsaigrid/tests/golden/
```
Write `INDEX.md` noting: the file is a concatenation of complete client messages in the exact order of the `test_*` methods in
`../hazelcast/hazelcast/src/test/java/com/hazelcast/client/protocol/compatibility/ClientCompatibilityTest_2_10.java`,
and the reference field values are in the sibling `ReferenceObjects.java`. Record the index of `ClientAuthenticationCodec_encodeRequest`, `_decodeResponse`, `MapPutCodec_encodeRequest`, `_decodeResponse`, `MapGetCodec_encodeRequest`, `_decodeResponse` by counting `test_` methods in order (each `encode*`/`decode*` consumes one message). This index list is the input to Tasks 8–10's golden assertions.

- [ ] **Step 2: Write the failing test**

`bonsaigrid/crates/protocol/tests/golden_frame.rs`:
```rust
use std::fs;
use protocol::frame::read_message;

fn load_golden() -> Vec<u8> {
    fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/golden/2.10.protocol.compatibility.binary"))
        .expect("golden fixture present")
}

#[test]
fn golden_parses_into_many_complete_messages() {
    let bytes = load_golden();
    let mut off = 0;
    let mut count = 0;
    while off < bytes.len() {
        let (_frames, used) = read_message(&bytes[off..]).expect("each message parses");
        off += used;
        count += 1;
    }
    assert_eq!(off, bytes.len(), "consumed the whole fixture with no trailing bytes");
    assert!(count > 100, "fixture contains every codec's messages");
}
```

- [ ] **Step 3: Run test to verify it fails, then passes**

Run: `cd bonsaigrid && cargo test -p protocol --test golden_frame`
Expected: PASS once the fixture is present and `read_message` is correct. If it fails on a non-final multi-frame message, that is a real bug in `read_message`'s `IS_FINAL` loop — fix it here.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -q -m "test(protocol): vendor 2.10 golden fixture and verify frame parsing"
```

---

## Phase 2 — Message and custom-type codecs

### Task 6: Custom-type framing helpers (BEGIN/END) + AddressCodec

**Files:**
- Create: `bonsaigrid/crates/codecs/src/lib.rs`, `address.rs`
- Modify: `bonsaigrid/crates/codecs/Cargo.toml` (depend on `protocol`)

**Interfaces:**
- Consumes: `protocol::frame::{Frame, BEGIN_DS, END_DS}`, `protocol::primitives::*`, `protocol::fixed::*`
- Produces:
  - `pub fn begin_frame() -> Frame` / `pub fn end_frame() -> Frame`
  - `address::encode(out: &mut Vec<Frame>, host: &str, port: i32)` — emits `BEGIN`, initial frame `[port i32 @0]`, host string frame, `END` (mirrors `AddressCodec`).

- [ ] **Step 1: Write the failing test**

`address.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use protocol::frame::{BEGIN_DS, END_DS};
    use protocol::fixed::read_i32_le;
    use protocol::primitives::decode_string;

    #[test]
    fn address_encodes_begin_port_host_end() {
        let mut frames = Vec::new();
        encode(&mut frames, "127.0.0.1", 5701);
        assert!(frames[0].flags & BEGIN_DS != 0);
        assert_eq!(read_i32_le(&frames[1].content, 0), 5701);
        assert_eq!(decode_string(&frames[2]), "127.0.0.1");
        assert!(frames[3].flags & END_DS != 0);
    }
}
```

- [ ] **Step 2: Run to fail.** `cargo test -p codecs address` → FAIL.

- [ ] **Step 3: Implement**

`codecs/src/lib.rs`:
```rust
pub mod address;
use protocol::frame::{Frame, BEGIN_DS, END_DS};
pub fn begin_frame() -> Frame { Frame { flags: BEGIN_DS, content: Vec::new() } }
pub fn end_frame() -> Frame { Frame { flags: END_DS, content: Vec::new() } }
```
`address.rs`:
```rust
use protocol::frame::Frame;
use protocol::fixed::write_i32_le;
use protocol::primitives::string_frame;
use crate::{begin_frame, end_frame};

pub fn encode(out: &mut Vec<Frame>, host: &str, port: i32) {
    out.push(begin_frame());
    let mut initial = vec![0u8; 4];
    write_i32_le(&mut initial, 0, port);
    out.push(Frame { flags: 0, content: initial });
    out.push(string_frame(host));
    out.push(end_frame());
}
```
`codecs/Cargo.toml` adds `protocol = { path = "../protocol" }`.

- [ ] **Step 4: Run to pass.** `cargo test -p codecs address` → PASS.

- [ ] **Step 5: Commit.** `git add -A && git commit -q -m "feat(codecs): BEGIN/END helpers and AddressCodec"`

### Task 7: MemberInfoCodec + partition-table (EntryList<UUID,List<Integer>>) codec

**Files:**
- Create: `bonsaigrid/crates/codecs/src/member_info.rs`, `partition_table.rs`
- Modify: `codecs/src/lib.rs`

**Interfaces:**
- Consumes: `address::encode`, `protocol::fixed::*`, BEGIN/END helpers, `ListIntegerCodec` pattern.
- Produces:
  - `member_info::encode(out: &mut Vec<Frame>, uuid: (i64,i64), host: &str, port: i32, lite: bool)` — port the exact field layout from
    `../hazelcast/.../codec/custom/MemberInfoCodec.java`: `BEGIN`; initial frame containing `uuid` (17B) then `liteMember` bool then the fixed numeric fields in that file's declared offset order; nested `Address`; attributes map; `MemberVersion`; `END`. Use BEGIN/END for nested structures exactly as the Java emits them.
  - `partition_table::encode(out: &mut Vec<Frame>, entries: &[((i64,i64), Vec<i32>)])` — `EntryListUUIDListIntegerCodec`: a fixed-size frame of UUIDs (key list) followed by a `ListMultiFrame` of `ListInteger` frames (value lists), per `../hazelcast/.../codec/builtin/EntryListUUIDListIntegerCodec.java`.

**Note:** This task's exact byte layout is **pinned by the golden fixture**, not by transcription. Implement against the reference file, then assert with Step 1's golden test. If the assertion fails, the reference file is the authority — adjust until bytes match.

- [ ] **Step 1: Write the failing golden test**

`member_info.rs` test: decode the `ClientAuthenticationCodec_decodeResponse` message from the golden fixture (index from Task 5), extract the `memberInfos` sublist frames, and assert your `member_info::encode` of the `ReferenceObjects` member values reproduces those exact frames (compare `write_message` bytes of the relevant frame span). Mirror the same approach for `partition_table` using the `partitions` field of the same message.

```rust
// Pseudocode shape — fill indices from tests/golden/INDEX.md:
// let msg = nth_message(&load_golden(), AUTH_DECODE_RESPONSE_INDEX);
// let expected_span = frames_for_member_infos(&msg);
// let mut got = Vec::new();
// member_info::encode(&mut got, REF_MEMBER_UUID, REF_HOST, REF_PORT, false);
// assert_eq!(write_message(&got_as_message), write_message(&expected_as_message));
```

- [ ] **Step 2: Run to fail.** `cargo test -p codecs member_info` → FAIL.

- [ ] **Step 3: Implement** per the two reference files cited above.

- [ ] **Step 4: Run to pass.** Iterate until golden bytes match exactly.

- [ ] **Step 5: Commit.** `git commit -q -m "feat(codecs): MemberInfo and partition-table codecs (golden-verified)"`

### Task 8: ClientAuthentication codec (decode request 256, encode response 257)

**Files:**
- Create: `bonsaigrid/crates/codecs/src/auth.rs`
- Modify: `codecs/src/lib.rs`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `auth::AuthRequest { cluster_name: String, client_type: String, .. }` and `auth::decode_request(frames: &[Frame]) -> AuthRequest` (reads initial-frame fixed fields per `ClientAuthenticationCodec`: uuid @ offset after partitionId, serializationVersion, routingMode, cpDirectToLeaderRouting; then the var-frames `clusterName`, nullable username/password, clientType, clientHazelcastVersion, clientName, labels).
  - `auth::AuthResponse { .. }` and `auth::encode_response(resp: &AuthResponse) -> Vec<Frame>` per the response field order: initial frame [type@0, backupAcks@12, status@13, memberUuid@14, serializationVersion, partitionCount, clusterId, failoverSupported, memberListVersion, partitionListVersion], then nullable `address`, `serverHazelcastVersion` string, nullable `tpcPorts` (null here), nullable `tpcToken` (null), `memberInfos` list, `partitions` entry-list, `keyValuePairs` map (empty).

- [ ] **Step 1: Write golden tests** — assert `decode_request` recovers `ReferenceObjects` values from the fixture's auth-request message, and `encode_response` of the `ReferenceObjects` response values reproduces the fixture's auth-response bytes. Indices from Task 5.

- [ ] **Step 2: Run to fail.** `cargo test -p codecs auth` → FAIL.

- [ ] **Step 3: Implement** per `ClientAuthenticationCodec.java` offsets (already extracted in Global Constraints + the reference file).

- [ ] **Step 4: Run to pass.** Iterate against golden bytes.

- [ ] **Step 5: Commit.** `git commit -q -m "feat(codecs): ClientAuthentication codec (golden-verified)"`

### Task 9: AddClusterViewListener codec + members/partitions view events

**Files:**
- Create: `bonsaigrid/crates/codecs/src/cluster_view.rs`
- Modify: `codecs/src/lib.rs`

**Interfaces:**
- Produces:
  - `cluster_view::encode_response() -> Vec<Frame>` (type 769, empty).
  - `cluster_view::members_view_event(version: i32, members: &[((i64,i64), &str, i32, bool)]) -> Vec<Frame>` (type 770; initial frame sets `IS_EVENT` flag, `version` @ offset after partitionId; then `memberInfos` list).
  - `cluster_view::partitions_view_event(version: i32, partitions: &[((i64,i64), Vec<i32>)]) -> Vec<Frame>` (type 771; `IS_EVENT`; `version`; then partition entry-list).

- [ ] **Step 1: Write golden tests** for the two event encoders against the fixture's `ClientAddClusterViewListenerCodec` event messages (the compatibility test encodes events too). Assert `IS_EVENT` flag is set on frame 0.

- [ ] **Step 2–4:** fail → implement per `ClientAddClusterViewListenerCodec.java` → pass.

- [ ] **Step 5: Commit.** `git commit -q -m "feat(codecs): cluster-view listener response and view events"`

### Task 10: MapPut + MapGet codecs

**Files:**
- Create: `bonsaigrid/crates/codecs/src/map.rs`
- Modify: `codecs/src/lib.rs`

**Interfaces:**
- Produces:
  - `map::PutRequest { name: String, key: Vec<u8>, value: Vec<u8>, thread_id: i64, ttl: i64 }`, `map::decode_put(frames) -> PutRequest` (initial frame: threadId @ offset after partitionId, ttl @ +8; then `name` string, `key` data frame, `value` data frame).
  - `map::encode_put_response(old: Option<&[u8]>) -> Vec<Frame>` (type 65793; nullable data via `null_frame`/`data_frame`).
  - `map::GetRequest { name: String, key: Vec<u8>, thread_id: i64 }`, `map::decode_get(frames) -> GetRequest`.
  - `map::encode_get_response(val: Option<&[u8]>) -> Vec<Frame>` (type 66049).

- [ ] **Step 1: Write golden tests** — `decode_put`/`decode_get` recover `ReferenceObjects` values (`aData` key/value) from the fixture; `encode_*_response(Some(aData))` reproduces fixture response bytes; `encode_*_response(None)` emits a single null frame.

- [ ] **Step 2–4:** fail → implement per `MapPutCodec.java`/`MapGetCodec.java` → pass.

- [ ] **Step 5: Commit.** `git commit -q -m "feat(codecs): MapPut and MapGet codecs (golden-verified)"`

---

## Phase 3 — TCP server skeleton

### Task 11: Connection: CP2 preamble + frame read/write loop

**Files:**
- Create: `bonsaigrid/crates/server/src/lib.rs`, `connection.rs`
- Modify: `server/Cargo.toml` (depend on `protocol`, `codecs`, `store`)

**Interfaces:**
- Consumes: `protocol::frame::{read_message, write_message}`.
- Produces:
  - `connection::handle(stream: TcpStream, dispatch: impl FnMut(Vec<Frame>) -> Vec<Vec<Frame>>)` — reads the 3-byte `CP2` preamble (rejecting and closing otherwise), then loops: accumulate bytes, `read_message`, call `dispatch` (which may return zero or more reply messages — responses *and* pushed events), `write_message` each. Echoes correlation id (handlers set it).

- [ ] **Step 1: Write the failing test** (uses a real loopback socket, no Hazelcast client):
```rust
// tests/cp2_echo.rs — connect, send b"CP2" + a hand-built auth request message,
// run a dispatch stub that returns a single empty-typed reply with the same
// correlation id, and assert the bytes read back parse as one message with that id.
```

- [ ] **Step 2: Run to fail.** `cargo test -p server cp2` → FAIL.

- [ ] **Step 3: Implement** the preamble check (`let mut p = [0u8;3]; stream.read_exact(&mut p)?; if &p != b"CP2" { return; }`) and the accumulate/parse/dispatch/write loop using a growable read buffer and `read_message`'s "needs more bytes" `None`.

- [ ] **Step 4: Run to pass.**

- [ ] **Step 5: Commit.** `git commit -q -m "feat(server): CP2 preamble and frame read/write loop"`

---

## Phase 4 — Bootstrap handshake wiring

### Task 12: Handlers: identity constants + dispatch table

**Files:**
- Create: `bonsaigrid/crates/server/src/handlers.rs`
- Modify: `server/src/lib.rs`

**Interfaces:**
- Consumes: all codecs; `protocol::message::{msg_type, correlation_id, set_correlation_id}`.
- Produces:
  - constants: `MEMBER_UUID: (i64,i64) = (0x0000_0000_0000_0001, 0x0000_0000_0000_0001)`, `CLUSTER_ID: (i64,i64) = (0x0000_0000_0000_0002, 0x0000_0000_0000_0002)`, `PARTITION_COUNT: i32 = 271`, `HOST = "127.0.0.1"`, `PORT = 5701`, `SERVER_VERSION = "5.8.0"`.
  - `handlers::dispatch(req: Vec<Frame>, store: &Store) -> Vec<Vec<Frame>>` — match on `msg_type`:
    - 256 (auth): reply 257 with status 0, this member, `PARTITION_COUNT`, member list `[MEMBER_UUID@HOST:PORT]`, partition table `[(MEMBER_UUID, 0..=270)]`, tpc null. Set reply correlation id from request.
    - 768 (cluster-view): reply 769, **plus** push `members_view_event(1, [member])` and `partitions_view_event(1, [(MEMBER_UUID, 0..=270)])`, all stamped with the request's correlation id.
    - 65792 (put) / 66048 (get): delegate to Task 13.
    - else: reply with an empty response of `type+1` echoing correlation id (keeps unknown ops from hanging the client).

- [ ] **Step 1: Write the failing test** — feed a hand-built auth request (correlation id 99) to `dispatch`; assert one reply of type 257 with correlation id 99, `partitionCount` 271, and a member list of length 1. Feed a cluster-view request; assert three messages back (769 + 770 + 771), all correlation id matched.

- [ ] **Step 2: Run to fail.**

- [ ] **Step 3: Implement** the match and the partition-range builder `(0..=270).collect()`.

- [ ] **Step 4: Run to pass.**

- [ ] **Step 5: Commit.** `git commit -q -m "feat(server): bootstrap handshake handlers"`

### Task 13: Wire `main` + bind + accept loop against a real client

**Files:**
- Create: `bonsaigrid/crates/server/src/main.rs`
- Modify: `server/src/handlers.rs` (add put/get arms, using Task 14 store)

**Interfaces:**
- Consumes: `connection::handle`, `handlers::dispatch`, `store::Store`.
- Produces: a runnable binary `bonsaigrid` that binds `127.0.0.1:5701` and serves connections (thread-per-connection via `std::thread::spawn`).

- [ ] **Step 1: Implement** `main`:
```rust
fn main() -> std::io::Result<()> {
    let store = std::sync::Arc::new(store::Store::new());
    let listener = std::net::TcpListener::bind("127.0.0.1:5701")?;
    for stream in listener.incoming() {
        let store = store.clone();
        std::thread::spawn(move || {
            let _ = server::connection::handle(stream.unwrap(), |req| {
                server::handlers::dispatch(req, &store)
            });
        });
    }
    Ok(())
}
```

- [ ] **Step 2: Manual smoke** — `cargo run -p server &` then in another shell confirm it accepts a TCP connection on 5701 (`nc -z 127.0.0.1 5701`). The authoritative check is Task 16.

- [ ] **Step 3: Commit.** `git commit -q -m "feat(server): runnable binary with accept loop"`

---

## Phase 5 — Map storage + put/get

### Task 14: In-memory opaque-blob store

**Files:**
- Create: `bonsaigrid/crates/store/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct Store(Mutex<HashMap<(String, Vec<u8>), Vec<u8>>>)` keyed by `(map_name, key_blob)`.
  - `pub fn new() -> Store`
  - `pub fn put(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>>` (returns prior)
  - `pub fn get(&self, map: &str, key: &[u8]) -> Option<Vec<u8>>`

  *(A `Mutex` is used here only because Task 13 uses thread-per-connection. This is the non-hot-path skeleton; the shared-nothing per-core store replaces it in increment 3 — see the routing spec. Do not optimize now.)*

- [ ] **Step 1: Write the failing test** — `put` returns `None` first time then the old value; `get` returns the stored blob verbatim; different map names are isolated.

- [ ] **Step 2–4:** fail → implement → pass (`cargo test -p store`).

- [ ] **Step 5: Commit.** `git commit -q -m "feat(store): single-node opaque-blob map"`

### Task 15: Put/Get handler arms

**Files:**
- Modify: `bonsaigrid/crates/server/src/handlers.rs`

**Interfaces:**
- Consumes: `map::{decode_put, decode_get, encode_put_response, encode_get_response}`, `store::Store`.

- [ ] **Step 1: Write the failing test** — build a MapPut request (`name="m"`, key `[1,2]`, value `[9]`), dispatch, assert reply type 65793 with a null old-value; then a MapGet for the same key returns 66049 carrying `[9]`.

- [ ] **Step 2: Run to fail.**

- [ ] **Step 3: Implement** the 65792/66048 arms: decode, `store.put/get`, encode response, set correlation id.

- [ ] **Step 4: Run to pass.**

- [ ] **Step 5: Commit.** `git commit -q -m "feat(server): MapPut/MapGet handlers"`

---

## Phase 6 — Conformance harnesses

### Task 16: Java conformance harness (real Hazelcast client) + parity score

**Files:**
- Create: `bonsaigrid/conformance-java/pom.xml`
- Create: `bonsaigrid/conformance-java/src/test/java/bonsai/ImapConformanceTest.java`
- Create: `bonsaigrid/conformance-java/README.md`

**Interfaces:**
- Consumes: the running `bonsaigrid` binary on `127.0.0.1:5701`.
- Produces: a JUnit suite using the **real** `com.hazelcast:hazelcast:5.8.x` client (`HazelcastClient.newHazelcastClient` with `clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701")` and cluster name `dev`), running ported `IMap` scenarios; the count of passing scenarios is the parity score.

- [ ] **Step 1: Prerequisite check**

Run: `java -version`
Expected: 17+. If not, document in README: install JDK 17 (`apt-get install openjdk-17-jdk`) — required to run any Hazelcast 5.x client. This task is blocked until JDK 17 is available; the golden tests (Tasks 5–10) and Rust integration tests still fully gate correctness without it.

- [ ] **Step 2: Write `pom.xml`** depending on `com.hazelcast:hazelcast:5.8.0` and JUnit 5.

- [ ] **Step 3: Write the conformance test**

```java
@Test
void put_then_get_returns_value() {
    ClientConfig cfg = new ClientConfig();
    cfg.setClusterName("dev");
    cfg.getNetworkConfig().addAddress("127.0.0.1:5701");
    HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
    IMap<String, String> map = client.getMap("m");
    assertNull(map.put("k", "v"));
    assertEquals("v", map.get("k"));
    client.shutdown();
}
```

- [ ] **Step 4: Run end-to-end**

```bash
cargo run -p server &       # start BonsaiGrid
cd conformance-java && mvn -q test
```
Expected: `put_then_get_returns_value` PASSES — a real stock client connected to BonsaiGrid, completed the handshake, and round-tripped a map entry. **This is the increment's success criterion.** If the client hangs during bootstrap, the missing/late piece is a handshake message (Phase 4) — add the message the client is waiting for and re-run.

- [ ] **Step 5: Commit.** `git commit -q -m "test(conformance): real Hazelcast Java client IMap put/get against BonsaiGrid"`

### Task 17: Python smoke test

**Files:**
- Create: `bonsaigrid/conformance-python/smoke.py`, `requirements.txt`, `README.md`

- [ ] **Step 1: Implement**

`requirements.txt`: `hazelcast-python-client==5.5.*`
`smoke.py`:
```python
import hazelcast
client = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=["127.0.0.1:5701"])
m = client.get_map("m").blocking()
assert m.put("k", "v") is None
assert m.get("k") == "v"
print("PYTHON SMOKE OK")
client.shutdown()
```

- [ ] **Step 2: Run** (PEP-668 safe):
```bash
python3 -m venv conformance-python/.venv
conformance-python/.venv/bin/pip install -r conformance-python/requirements.txt
cargo run -p server &
conformance-python/.venv/bin/python conformance-python/smoke.py
```
Expected: prints `PYTHON SMOKE OK`.

- [ ] **Step 3: Commit.** `git commit -q -m "test(conformance): python client smoke test"`

---

## Self-Review

**Spec coverage** (against the cross-core routing spec + walking-skeleton scope):
- Single-core degenerate case of the routing invariant (core 0 owns all partitions): Tasks 12 (partition table `(MEMBER_UUID, 0..=270)`). ✓ No SPSC/delegation, as intended for N=1.
- Stock-client compatibility (immutable contract): Tasks 6–16, validated by golden vectors + real client. ✓
- Opaque `Data`: Task 14 stores blobs verbatim; never deserialized. ✓
- Forward-compat note (storage replaced per-core later): called out in Task 14. ✓
- Out-of-scope guardrails (io_uring/zero-alloc/slab) explicitly deferred: Global Constraints. ✓

**Placeholder scan:** Tasks 7–10 intentionally lean on golden-vector assertions instead of transcribing every byte — this is a *verification strategy*, not a placeholder: each cites the exact reference file and a concrete failing test that pins the bytes. All code-bearing steps show real code. No "TBD"/"handle edge cases"/"similar to Task N".

**Type consistency:** `Frame`, `Vec<Frame>` message representation, and `(i64,i64)` UUID tuples are used uniformly across protocol/codecs/server. `dispatch` returns `Vec<Vec<Frame>>` (multiple reply messages) consistently in Tasks 11–15. Store key is `(String, Vec<u8>)` in Tasks 14–15.

**Known empirical risk (documented, not hidden):** the exact `MemberInfoCodec` field layout (Task 7) and whether a smart client needs both the auth-response member list *and* the cluster-view events (Task 12) are pinned by golden bytes + the live client in Task 16. If Task 16 reveals an additional bootstrap message the client awaits (e.g. `ClientAddClusterViewListener` ordering, or a `getDistributedObjects` call), add it as a follow-up task — this is expected in walking-skeleton handshake work.
