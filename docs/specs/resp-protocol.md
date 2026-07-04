# Spec: RESP2 (Redis protocol) for BonsaiGrid

**Status:** implemented. **Date:** 2026-07-04.
Companion to [`memcache-protocol.md`](memcache-protocol.md) — same idea, Redis wire.

## Motivation

A fourth protocol on the reactor's shared port (after Hazelcast binary, HTTP/REST,
and memcached ASCII), so standard **Redis clients** and **`memtier_benchmark`
`--protocol redis`** can drive BonsaiGrid. This gives a same-thin-client,
apples-to-apples comparison against real Redis, and is a real drop-in feature.

## Detection & framing

- The reactor sniffs the first byte: `CP2`→Hazelcast, `*`→**RESP**, uppercase→HTTP,
  lowercase→memcached. RESP requests are arrays of bulk strings
  (`*N\r\n$len\r\n<data>\r\n…`), so `*` disambiguates cleanly.
- `resp::frame(acc)` returns the length of the next complete command array (or
  `Need`); `resp::parse` extracts the bulk-string args (binary-safe).

## Command surface

| Command | Status | Notes |
|---|:--:|---|
| `PING` / `ECHO` | ✅ | |
| `GET` | ✅ | bulk reply or `$-1` (nil) |
| `SET key val [EX s|PX ms|NX|XX|KEEPTTL]` | ✅ | conditional (NX/XX) via atomic `mc_update` |
| `DEL` / `EXISTS` (variadic) | ✅ | integer reply |
| `DBSIZE` / `FLUSHDB` / `FLUSHALL` | ✅ | scoped to the `redis` map |
| `SELECT` / `AUTH` / `CONFIG` / `COMMAND` / `INFO` / `RESET` | ✅ | handshake no-ops so clients connect |
| `QUIT` | ✅ | `+OK` then close |
| `INCR` / `DECR` / `EXPIRE` / `TTL` / `MSET` / `MGET` | ❌ phase 2 | RMW / TTL-preserving / multi-key atomicity |
| `EXAT` / `PXAT` (absolute) | ❌ phase 2 | memtier uses relative EX/PX |

## Storage

Keys live in a dedicated `redis` store map (isolated from Hazelcast and memcache
maps). Values are stored raw (RESP has no per-item flags/cas). `SET EX/PX` maps to
the store's relative `ttl_ms`. `NX`/`XX` use `Store::mc_update` for atomic
check-and-set under the shard lock.

## Errors

RESP simple errors: unknown command → `-ERR unknown command '<x>'`; malformed
framing → `-ERR protocol error`; wrong arity → `-ERR wrong number of arguments…`.

## Tests

Unit: framing (need/have), parse (binary-safe args), SET/GET/DEL/EXISTS, NX/XX,
PING/ECHO/FLUSHALL/QUIT/unknown, handshake no-ops. Over-the-wire: a raw RESP socket
round-trip, and the full `memtier_benchmark --protocol redis` run against the
`bonsaigrid-redis` target (a real Redis client end-to-end).
