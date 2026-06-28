# Java conformance harness (parity oracle)

The canonical "are we building the right thing" check: the **real, unmodified
Hazelcast Java client** driving ported `IMap` scenarios against a running
BonsaiGrid. The number of passing scenarios is the **parity score**, which grows
as each increment lands more functionality.

## Prerequisite

JDK 17+ (any Hazelcast 5.x client requires it). This environment ships only
Java 8, so this harness is **not runnable here yet** — install JDK 17 to enable
it:

```bash
sudo apt-get install -y openjdk-17-jdk
```

The Rust golden-vector tests (`cargo test`) and the Python smoke test
(`../conformance-python`) fully gate correctness in the meantime; the Python
client already proves end-to-end wire compatibility.

## Run

```bash
# repo root:
cargo run -p server &
cd conformance-java && mvn test
```
