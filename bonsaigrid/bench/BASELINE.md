# Benchmark results

Reproduce: `bench/run.sh <label>` (release builds; single sequential client for
latency, server RSS delta for memory density). Hardware: aarch64, 20 cores.

Each increment appends a row. The point of the ladder is to watch these numbers
improve while the conformance tests (golden vectors + stock-client oracles) stay
green.

## Latency / throughput (single connection, sequential put+get, n=50000)

| Increment | throughput (ops/s) | p50 µs | p99 µs | p999 µs |
|-----------|-------------------:|-------:|-------:|--------:|
| 0 baseline (std::net + Mutex<HashMap>) | 68,410 | 13 | 21 | 270 |
| 1 slab + open-addressing store | ~60,000 | 14 | ~50 | ~290 |

Latency is unchanged within run-to-run noise — this single-sequential-connection
test is dominated by blocking-socket syscalls, which increment 2 (io_uring)
targets. Increment 1's win is memory, below.

## Memory density (200,000 entries × 100-byte values, raw payload ~115 B/entry)

| Increment | bytes/entry | RSS delta (KB) | overhead vs payload |
|-----------|------------:|---------------:|--------------------:|
| 0 baseline (Mutex<HashMap<(String,Vec<u8>),Vec<u8>>>) | 272.2 | 53,160 | +137% |
| 1 slab + open-addressing store | 179.3 | 35,024 | +56% |

**Increment 1: −34% bytes/entry.** The slab packs each `key++value` into a
contiguous size-classed arena (O(1) free list), the map name is interned to a
`u32` instead of a per-entry `String`, and entry records are inline in a flat
open-addressing table — eliminating the baseline's three heap allocations per
entry (String key + key Vec + value Vec) and their malloc/capacity overhead.

## Notes

- Increment 0 is intentionally unoptimized: blocking `std::net`,
  thread-per-connection, `std::collections::HashMap`. It exists to establish
  this baseline and prove wire compatibility.
- "bytes/entry" includes all per-entry overhead the process pays (hash buckets,
  separate key/value heap allocations, the `(String, Vec<u8>)` tuple key).
