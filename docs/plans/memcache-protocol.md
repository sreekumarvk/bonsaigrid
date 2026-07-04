# Plan: Memcached ASCII protocol — implementation + test plan

Spec: [`docs/specs/memcache-protocol.md`](../specs/memcache-protocol.md). TDD throughout.

## Implementation steps

1. **Codec module `crates/server/src/memcache.rs`** (pure, fully unit-tested)
   - `Command` enum: `Get{keys}`, `Gets{keys}`, `Store{op,key,flags,exptime,cas,data,noreply}`
     with `op ∈ {Set,Add,Replace,Append,Prepend,Cas}`, `Delete{key,noreply}`,
     `Incr{key,delta,noreply}`, `Decr{…}`, `Touch{key,exptime,noreply}`,
     `FlushAll{delay,noreply}`, `Version`, `Verbosity{noreply}`, `Quit`, `Error(msg)`.
   - `frame(acc: &[u8]) -> Frame::{Need, Have(usize)}` — the framing in §4 of the spec.
   - `parse(cmd: &[u8]) -> Command` — parse one complete command (incl. data block).
   - Response encoders → `&mut Vec<u8>`: value lines, `STORED`, `END`, `ERROR`, etc.
   - Header helpers: `pack(flags,cas,data)`, `unpack(blob) -> (flags,cas,&data)`.
   - `exptime_to_ttl_ms(exptime) -> TtlKind::{Never, Ms(u64), ExpireNow}`.

2. **Store `Store::mc_update`** (`crates/store/src/lib.rs`)
   - `mc_update(map,key, f: FnOnce(Option<&[u8]>) -> McAction) -> McOutcome` — lock
     the shard once, read live value, apply `Store|Remove|Keep`. Add `McAction`,
     `McOutcome`. Unit-tested for atomicity semantics (add/replace/cas outcomes).

3. **Execution `memcache::execute(cmd, store) -> (Vec<u8> reply, bool close)`**
   - Maps each `Command` to store ops using `mc_update` / `get` / `put_ttl` /
     `remove` / `clear`, packing/unpacking the header and enforcing the value cap.

4. **Reactor** (`crates/server/src/reactor.rs`)
   - `Mode::Memcache`; lowercase-verb detection; the `Memcache` framing loop; add the
     `memcache` closure param to `run` + `on_recv`.

5. **Wiring** (`crates/server/src/main.rs` both call sites)
   - Build the `memcache` closure over the shared `Store` and pass it to `reactor::run`.

## Test plan (modeled on memcached's `t/*.t`)

memcached's Perl suite drives a live server over a socket and asserts exact replies.
We mirror the same cases at three levels:

**A. Codec unit tests** (`memcache.rs #[cfg(test)]`) — pure parse/encode/frame:
- frame: incomplete line → `Need`; storage command needs data+`\r\n` → `Need` then `Have`.
- parse: every command + `noreply`; bad key (>250, spaces, control) → `Error`;
  malformed numerics → `Error`.
- header pack/unpack round-trip; exptime mapping (0 / relative / absolute / negative).

**B. Execution/component tests** (drive `execute` against a real `Store`) — mirror
memcached t-files:
- `getset`: set→get returns value+flags; get miss → `END`; overwrite.
- `add`/`replace`: add new→`STORED`, add existing→`NOT_STORED`; replace missing→`NOT_STORED`.
- `cas`: gets→cas; cas match→`STORED`, stale→`EXISTS`, missing→`NOT_FOUND`.
- `incrdecr`: incr/decr numeric; decr floors at 0; non-numeric→`CLIENT_ERROR`.
- `append`/`prepend`: concat, flags preserved, miss→`NOT_STORED`.
- `flags`: arbitrary 32-bit flag round-trips.
- `expiry`: exptime in the past → immediate miss; touch updates ttl.
- `delete`: hit→`DELETED`, miss→`NOT_FOUND`.
- `flush_all`: clears everything.
- `noreply`: mutations with `noreply` emit no bytes.
- `error`: unknown verb→`ERROR`; too-large value→`SERVER_ERROR`.
- multi-`get`: `get k1 k2 k3` returns present keys then `END`.

**C. Integration over the wire** (`crates/server/tests/` or `bench/verify-*`):
- Rust: raw socket → real server, `set`/`get`/`delete` round-trip (detection works).
- Go: a `gomemcache`-driven test (in `bench/loadgen`) — Set/Get/Add/Delete/Increment
  against BonsaiGrid's port; asserts values match. This is the real-client conformance.

**Green bar = all of A/B/C pass + `cargo test -p server -p store` + gomemcache test.**

## Benchmark + plot (after tests pass)

- Add a `bonsaigrid-mc` loadgen store (`store_bgmc.go`) using `gomemcache` pointed at
  BonsaiGrid's port; a `bonsaigrid-mc` TARGET.
- `run-all-isolated.sh`: after the BonsaiGrid step, also bench it via the memcached
  client (reuse the container) → `results-bonsaigrid-mc.json` → merged as a 5th series.
- Dashboard: add a 5th backend colour/series (validated palette); the data-driven
  headline already generalises to N backends.
- Run, bake, plot. Expect `bonsaigrid-mc` to land near the thin-client ceiling
  (thin client, no Hazelcast tax) — the honest server-vs-Memcached number.

## Commit strategy

1. spec + plan docs. 2. codec + store `mc_update` + tests. 3. reactor + wiring +
integration tests. 4. loadgen memcached client + dashboard 5th series + fresh run.
Push to `main`.
