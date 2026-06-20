# BonsaiGrid

A high-performance, resource-efficient reimplementation of Apache Hazelcast on a
zero-allocation, thread-per-core, shared-nothing Rust runtime. **Goal:** a
genuine drop-in replacement — unmodified Hazelcast clients connect and work, and
operators keep their existing metrics/monitoring — with markedly better latency
and memory density underneath.

Design philosophy and the cross-core routing architecture:
`../docs/superpowers/specs/2026-06-19-cross-core-routing-design.md`.
Build plan: `../docs/superpowers/plans/2026-06-19-increment-0-walking-skeleton.md`.

## Status: multi-node static cluster ✅

Server speaking the Hazelcast Open Client Protocol (fixtures version 2.10), in
two modes:
- **Single node** (default): thread-per-core, io_uring, per-core TPC ports.
- **Multi-node** (`BONSAI_MEMBERS=K`): K processes form a static cluster; each is
  one member owning partitions `{p : p % K == index}`. A stock **smart** client
  routes each key to its owner — verified by 1000 keys round-tripping across a
  3-member cluster (`conformance-python/run_cluster.sh`).

Unmodified Hazelcast clients (Python + Java) connect and perform `IMap.put` /
`IMap.get` — unisocket, smart-routing, TPC-enabled, and cross-cluster. See
`bench/BASELINE.md` for measured single-node performance.

| # | Increment | Target | Result |
|---|-----------|--------|--------|
| 0 | Walking skeleton | compat proven | ✅ stock Python + Java clients pass |
| 1 | slab + open-addressing store | memory density | ✅ 272 → 179 B/entry (−34%) |
| 2 | single-core io_uring reactor | per-core efficiency | ✅ 173k ops/s/core (64 conns) |
| 3 | thread-per-core + TPC + `SO_REUSEPORT` | throughput scaling | ✅ 5.2× on 8 cores (690k ops/s); TPC client validated |

Remaining on the roadmap: cross-core SPSC delegation (zero-lock shared-nothing),
true zero-allocation response encoding, then multi-node (clustering, replication
— see the routing spec's A→B→C→D phases), and operator-surface parity (metrics /
Management Center).

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
