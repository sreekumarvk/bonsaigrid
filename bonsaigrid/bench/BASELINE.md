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

## Memory density (200,000 entries × 100-byte values, raw payload ~115 B/entry)

| Increment | bytes/entry | RSS delta (KB) | overhead vs payload |
|-----------|------------:|---------------:|--------------------:|
| 0 baseline (Mutex<HashMap<(String,Vec<u8>),Vec<u8>>>) | 272.2 | 53,160 | +137% |

## Notes

- Increment 0 is intentionally unoptimized: blocking `std::net`,
  thread-per-connection, `std::collections::HashMap`. It exists to establish
  this baseline and prove wire compatibility.
- "bytes/entry" includes all per-entry overhead the process pays (hash buckets,
  separate key/value heap allocations, the `(String, Vec<u8>)` tuple key).
