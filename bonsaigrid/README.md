# BonsaiGrid

A high-performance, resource-efficient reimplementation of Apache Hazelcast on a
zero-allocation, thread-per-core, shared-nothing Rust runtime. **Goal:** a
genuine drop-in replacement — unmodified Hazelcast clients connect and work, and
operators keep their existing metrics/monitoring — with markedly better latency
and memory density underneath.

Design philosophy and the cross-core routing architecture:
`../docs/superpowers/specs/2026-06-19-cross-core-routing-design.md`.
Build plan: `../docs/superpowers/plans/2026-06-19-increment-0-walking-skeleton.md`.

## Status: Increment 0 — walking skeleton ✅

Single-core, single-node server speaking the Hazelcast Open Client Protocol
(fixtures version 2.10). An **unmodified Hazelcast client connects and performs
`IMap.put` / `IMap.get`**, in both unisocket and smart-routing modes.

This increment deliberately does **not** yet meet the performance guardrails
(zero-alloc hot path, `io_uring`, thread-per-core, slab allocator) — those are
the measurable optimization increments that follow:

| # | Increment | Measurable win |
|---|-----------|----------------|
| 0 | Walking skeleton (this) | compat proven + benchmark harness |
| 1 | slab allocator + open-addressing store | memory density |
| 2 | raw io_uring + zero hot-path alloc | p99 latency |
| 3 | multi-core + TPC + `p%N` routing | throughput scaling |
| 4 | cross-core delegation fallback (SPSC) | classic-client correctness at N cores |

## Layout

- `crates/protocol` — frame envelope + little-endian primitive codecs.
- `crates/codecs` — auth, cluster-view, map, and nested member/partition codecs.
- `crates/store` — single-node opaque-blob map (`Data` never deserialized).
- `crates/server` — CP2 preamble, frame loop, handshake + map dispatch, binary.
- `tests/golden` — Hazelcast's committed 2.10 conformance fixture.
- `conformance-python` / `conformance-java` — stock-client end-to-end oracles.

## Build & test

```bash
cargo test                      # unit + golden-vector conformance (byte-exact)
cargo run -p server             # bind 127.0.0.1:5701

# stock-client end-to-end (Python; JVM-free):
python3 -m venv conformance-python/.venv
conformance-python/.venv/bin/pip install -r conformance-python/requirements.txt
conformance-python/.venv/bin/python conformance-python/smoke.py   # PYTHON SMOKE OK
```

## Testing strategy

1. **Golden-vector codec conformance** (`cargo test`) — encode/decode validated
   byte-for-byte against Hazelcast's own committed `2.10.protocol.compatibility.binary`.
2. **Behavioural conformance** — a real Hazelcast client runs ported `IMap`
   scenarios against the server (Python now; Java parity harness needs JDK 17+).

Both layers ensure we match the immutable client contract, not just our own idea
of it.
