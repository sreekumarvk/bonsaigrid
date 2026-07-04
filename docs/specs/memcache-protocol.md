# Spec: Memcached ASCII protocol for BonsaiGrid

**Status:** design → implementing. **Date:** 2026-07-04.
**Companion:** [`docs/plans/memcache-protocol.md`](../plans/memcache-protocol.md) (implementation + test plan).

## 1. Motivation

BonsaiGrid already multiplexes protocols on one TCP port: the reactor sniffs the
first bytes — `CP2` → Hazelcast binary client, otherwise → HTTP/REST
(`reactor.rs:on_recv`). Adding the **memcached ASCII (text) protocol** as a third
protocol delivers two things:

1. **A fair, same-client benchmark.** Today BonsaiGrid is driven through the
   heavyweight official Hazelcast client while Memcached uses the thin `gomemcache`.
   If BonsaiGrid speaks memcached, `gomemcache` can drive *both* — an apples-to-apples
   server-vs-server comparison with no client tax.
2. **A real drop-in feature.** BonsaiGrid can serve existing memcached clients
   alongside Hazelcast and REST.

## 2. Goals / non-goals

**Goals**
- Implement the classic memcached **ASCII text protocol** for the common commands,
  correct enough to be driven by `gomemcache`, `libmemcached`, and the reference
  client behaviours.
- Zero impact on the Hazelcast/REST paths and on memory density of non-memcache maps.
- Thread-per-core safe: single-key mutations are atomic under the store's per-shard lock.

**Non-goals (this change)**
- The **binary protocol** and the newer **meta protocol** (`mg/ms/md/ma/mn/me`).
- UDP transport, SASL auth, `stats` (and sub-stats), `gat`/`gats`.
- Delayed `flush_all <delay>` precision (we honour immediate flush; a delay is
  accepted but treated as immediate — documented).

## 3. Command surface & completeness vs memcached

memcached's ASCII protocol (`doc/protocol.txt`) command set, and our coverage:

| Group | Command | This change | Notes |
|-------|---------|:-----------:|-------|
| Storage | `set` | ✅ | flags + exptime + noreply |
| Storage | `add` | ✅ | store iff absent → `STORED` / `NOT_STORED` |
| Storage | `replace` | ✅ | store iff present |
| Storage | `append` | ❌ phase 2 | RMW that must **preserve existing TTL** (see below) |
| Storage | `prepend` | ❌ phase 2 | ditto |
| Storage | `cas` | ✅ | store iff cas matches → `STORED`/`EXISTS`/`NOT_FOUND` |
| Retrieval | `get` | ✅ | multi-key; `VALUE <k> <flags> <bytes>\r\n<data>\r\n … END` |
| Retrieval | `gets` | ✅ | as `get` + cas unique |
| Retrieval | `gat` / `gats` | ❌ phase 2 | get-and-touch |
| Deletion | `delete` | ✅ | `DELETED` / `NOT_FOUND` |
| Arithmetic | `incr` / `decr` | ❌ phase 2 | RMW that must **preserve existing TTL** (see below) |
| Touch | `touch` | ✅ | `TOUCHED` / `NOT_FOUND` (sets a new TTL, so no preservation needed) |
| Admin | `flush_all` | ✅ | immediate (delay accepted, treated as immediate) |
| Admin | `version` | ✅ | `VERSION bonsaigrid-<v>` |
| Admin | `verbosity` | ✅ | accepted, `OK` |
| Admin | `quit` | ✅ | close connection |
| Admin | `stats [sub]` | ❌ phase 2 | large surface |
| Meta | `mg/ms/md/ma/mn/me` | ❌ phase 2 | meta protocol |

## 4. Wire format & framing

- **Text lines** terminated by `\r\n`. A command is one line, except **storage
  commands** which are `<line>\r\n<data block>\r\n` where the line's `<bytes>` field
  gives the data length.
- **Framing** (`memcache::frame(acc) -> Frame`): find the first `\r\n`. If the verb
  is a storage command (`set/add/replace/append/prepend/cas`), parse `<bytes>` and
  require `line_len + bytes + 2` bytes present (data + trailing `\r\n`); otherwise the
  command ends at the first `\r\n`. Returns `Need` (buffer more) or `Have(n)`.
- The reactor accumulates partial reads in `Conn.acc` already; `Mode::Memcache`
  loops `frame` → dispatch → `drain(0..n)` like the binary path.

## 5. Semantics

- **key:** ≤ 250 bytes, no control chars or whitespace → else `CLIENT_ERROR bad
  command line format`.
- **flags:** unsigned 32-bit, stored verbatim, returned on `get`.
- **exptime:** `0` = never; `1..=2592000` (30 days) = relative seconds; `>2592000`
  = absolute unix time; `<0` = expire immediately. Converted to the store's
  `ttl_ms` (absolute→relative via wall clock; negative→already-expired = delete).
- **cas:** a per-store monotonically-increasing 64-bit unique, assigned on every
  successful mutation, returned by `gets`, compared by `cas`.
- **value size:** capped at `MC_MAX_VALUE` (default 1 MiB, matching memcached) →
  `SERVER_ERROR object too large for cache`.
- **noreply:** trailing `noreply` token on a mutation suppresses the reply.

## 6. Storage mapping

- memcached's flat keyspace maps to a **dedicated store map** `"memcache"` (constant),
  so it never collides with Hazelcast maps and its value layout stays private.
- **Value blob layout:** `[flags: u32 LE][cas: u64 LE][data …]` — a 12-byte header
  prepended to the user data. `get`/`gets` strip it; mutations re-emit it. This keeps
  `Entry` unchanged (no per-entry memory cost for non-memcache maps).
- **Atomicity:** a new `Store::mc_update(map, key, f)` locks the key's shard once,
  reads the current live value (respecting TTL), calls `f(Option<&[u8]>) -> McAction`,
  and applies `Store(val,ttl) | Remove | Keep` under the same lock. Every conditional
  / RMW command (`add`, `replace`, `cas`, `incr`, `decr`, `append`, `prepend`,
  `touch`) is a small closure over `mc_update`. `get`/`set`/`delete`/`flush_all`
  reuse existing `get`/`put_ttl`/`remove`/`clear`.

## 7. Reactor integration

- New `Mode::Memcache`. Detection: after `CP2` (binary), a **lowercase** memcached
  verb (`get `, `gets `, `set `, `add `, …) selects `Memcache`; uppercase HTTP methods
  still select `Http`. (memcached verbs are lowercase; HTTP methods uppercase — no
  collision.)
- `reactor::run` gains a `memcache: FnMut(&[u8], &mut Vec<u8>) -> bool` closure
  (returns `true` to close the connection, for `quit`). Both call sites (main.rs
  single-core + per-core) build it over the shared `Store`, exactly like `dispatch`.

## 8. Error handling

Reply strings match memcached: `ERROR` (unknown command), `CLIENT_ERROR <msg>`
(malformed / bad key / non-numeric incr), `SERVER_ERROR <msg>` (too large). Storage
outcomes: `STORED`, `NOT_STORED`, `EXISTS`, `NOT_FOUND`, `DELETED`, `TOUCHED`, `OK`.

## 9. Critique (so we don't lose the spec)

- **Flags/cas in the value blob vs `Entry` fields.** Blob keeps `Entry` and memory
  density untouched and isolates memcache concerns, at the cost of a 12-byte
  prefix and header parsing on the hot path. Accepted: memcache is a separate
  namespace and the parse is a couple of `u32/u64` reads. Alternative (Entry fields)
  rejected to avoid taxing every Hazelcast entry.
- **cas scope.** A per-store atomic counter (not per-key) is monotonic and unique,
  which satisfies the protocol. It is *not* persisted across restart — acceptable for
  a cache (memcached's cas also resets on restart).
- **TTL-preserving RMW (`incr`/`decr`/`append`/`prepend`) deferred to phase 2.**
  These four must keep the item's *existing* expiration, but `mc_update` currently
  exposes only the value, not the remaining TTL. Rather than silently reset TTLs
  (a subtle correctness bug), they are out of scope for this change; phase 2 extends
  `mc_update` to pass `Option<(&[u8], remaining_ttl_ms)>`. `add`/`replace`/`cas`/
  `touch` are unaffected because they set a fresh TTL from the command.
- **`flush_all <delay>`.** Precise delayed flush needs a flush epoch compared per
  entry. Out of scope; we clear immediately and document it. Risk: a client relying
  on delayed flush sees immediate. Low; rare in practice.
- **Multi-get partial cross-core.** A `get k1 k2 k3` spanning keys on different shards
  reads each under its own shard lock (not one global snapshot). memcached has no
  cross-key atomicity either, so this matches its semantics.
- **Value size cap.** Enforced to avoid unbounded slab pressure; matches memcached's
  default 1 MiB item limit.
- **Detection ambiguity.** A pathological client sending an uppercase verb would be
  misrouted to HTTP; real memcached clients send lowercase. Documented constraint.

## 10. Success criteria

- `gomemcache` set/get/delete/add/replace/incr/decr/cas/touch/flush round-trips
  correctly against BonsaiGrid.
- Unit + integration + gomemcache tests green (see plan's test plan).
- The benchmark harness can drive BonsaiGrid via `gomemcache` and plot it as a fifth
  series next to real Memcached.
