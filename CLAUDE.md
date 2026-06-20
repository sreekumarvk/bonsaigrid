# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace Topology

This is a two-directory workspace, not a single project. Commands and context are anchored at the repo root.

- **`./bonsaigrid/`** — The target deliverable: a greenfield, zero-allocation Rust data grid (BonsaiGrid). **Currently contains only `REQUIREMENTS.md`** — no Rust code, no `Cargo.toml` yet. This is what gets built.
- **`./hazelcast/`** — A read-only reference baseline: a full checkout of the Apache Hazelcast Java repo. It exists **only** to extract the client wire protocol (Phase 1). Do not modify it; do not treat its JVM architecture as a model to imitate — BonsaiGrid is explicitly a bare-metal alternative to it.

`bonsaigrid/REQUIREMENTS.md` is the authoritative spec. Read it before any implementation work.

## Architectural Guardrails (Non-Negotiable)

These constraints define the project's entire reason for existing. Violating them defeats the purpose; do not relax them for convenience.

1. **Zero-allocation hot path.** After startup, the request/serialize/store path must do no heap allocation — no `malloc`/`free`, no `Box`, `Vec::new`, or `String` growth in the hot path. All working memory comes from pre-allocated contiguous pools (the slab allocator). Allocation is allowed only during initialization.
2. **Shared-nothing, thread-per-core.** Exactly one OS thread per CPU core, hard-pinned via `core_affinity`. **No `Mutex`/`RwLock` and no shared mutable memory between threads.** Cross-core coordination happens only through lock-free SPSC channels (`crossbeam-channel`/`flume`).
3. **Kernel-bypass I/O.** No blocking I/O or standard epoll in the hot path. Use `io_uring` via `tokio-uring` (or raw syscalls).

Mandated crates for `bonsaigrid/Cargo.toml`: `tokio-uring`, `core_affinity`, `crossbeam-channel`/`flume`, `ahash`/`xxhash`.

## Implementation Roadmap

Build in this order (from `REQUIREMENTS.md`):

1. **Protocol extraction** — Map Hazelcast's client/server binary frame format from `./hazelcast/`; document `map.put`/`map.get` request and response byte layouts.
2. **Slab allocator (`allocator.rs`)** — One large `mmap`'d region at startup, divided into fixed-size slabs, with an O(1) lock-free free-list. At 100% utilization, return an explicit OOM error; never grow the heap.
3. **Thread-per-core reactor** — Detect core count, launch N pinned workers, each with its own independent `io_uring` poll loop, parsing TCP frames without allocation.
4. **Sharded map & routing** — Key hash determines the owning core. A worker receiving a packet for another core's key delegates via that core's SPSC ring; the owning core performs the memory op in its private slab space.

## Phase 1 Reference: Hazelcast Client Protocol

The wire protocol lives under `hazelcast/hazelcast/src/main/java/com/hazelcast/client/impl/protocol/` (note the nested `hazelcast/hazelcast/` module path).

- **Frame/message layout:** `.../protocol/ClientMessage.java`. Key facts: encoding is **little-endian** (`Bits.readIntL`/`readLongL`). Each frame is prefixed by `SIZE_OF_FRAME_LENGTH_AND_FLAGS` (4-byte length + 2-byte flags). The initial frame's content carries: `TYPE` (int @ offset 0), `CORRELATION_ID` (long @ offset 4), `PARTITION_ID` (int @ offset 12). Flag bits (`BEGIN_FRAGMENT_FLAG = 1<<15`, `END_FRAGMENT_FLAG = 1<<14`, `UNFRAGMENTED_MESSAGE`, `IS_NULL_FLAG`, etc.) are defined as constants in this file.
- **Operation codecs:** `.../protocol/codec/MapPutCodec.java` and `MapGetCodec.java` define `REQUEST_MESSAGE_TYPE` / `RESPONSE_MESSAGE_TYPE` and the encode/decode logic. Message type IDs: MapPut request `65792` / response `65793`; MapGet request `66048` / response `66049`. Ignore the `CPMap*`, `MultiMap*`, `ReplicatedMap*`, and `Transactional*` variants for the MVP.

## Reference Build (Hazelcast, Java)

Only needed to compile/run the reference protocol or its test codecs — the BonsaiGrid deliverable is Rust.

```bash
cd hazelcast
./mvnw -pl hazelcast -am install -DskipTests   # build the core module
./mvnw -pl hazelcast test -Dtest=ClassName     # run a single test class
```
