# BonsaiGrid Quick Start

Two quick starts in one place:

- **[Part 1 — Use BonsaiGrid](#part-1--use-bonsaigrid-from-a-java-client)** — start a
  server and talk to it from an unmodified Hazelcast Java client.
- **[Part 2 — Benchmark BonsaiGrid](#part-2--benchmark-bonsaigrid)** — run the suites
  and get one report page.

For the full reference, see [README.md](README.md).

---

## Part 1 — Use BonsaiGrid from a Java client

New to In-Memory Data Grids (IMDGs) or Hazelcast? This is for you. BonsaiGrid is a
blazingly fast, zero-allocation data grid written in Rust that speaks the standard
Hazelcast binary protocol — so official Hazelcast clients store and retrieve data
across a network at extreme speed. We will start a server, create a Java Maven project
from scratch, and interact with a distributed `IMap`.

### 1. Prerequisites

- **JDK 11 or higher** (`javac -version`)
- **Maven** (`mvn -version`)
- **Rust and Cargo** — to compile the server
  (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

### 2. Start the BonsaiGrid server

BonsaiGrid's engine uses `io_uring`, deeply integrated into the Linux kernel for
maximum performance.

```bash
# 1. Clone the repository
git clone https://github.com/sreekumarvk/bonsaigrid.git
cd bonsaigrid

# 2. Compile and run the server in release mode for maximum performance
cargo run --release --bin server
```

You should see output indicating the server is listening on `127.0.0.1:5701`. Leave
this terminal open.

### 3. Create the Java application

Open a **new terminal** and scaffold the project:

```bash
mkdir -p my-bonsai-app/src/main/java/com/example
cd my-bonsai-app
```

Create `pom.xml` in `my-bonsai-app/` — the only dependency is the official Hazelcast
Java client:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>bonsai-demo</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <dependencies>
        <!-- The standard Hazelcast Java Client -->
        <dependency>
            <groupId>com.hazelcast</groupId>
            <artifactId>hazelcast</artifactId>
            <version>5.3.6</version>
        </dependency>
    </dependencies>
</project>
```

Create `src/main/java/com/example/App.java` — it connects to your local server,
creates a distributed `IMap`, and reads/writes data:

```java
package com.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class App {
    public static void main(String[] args) {
        System.out.println("Starting Java Client...");

        // 1. Configure the Client
        ClientConfig config = new ClientConfig();
        // Point the client to the BonsaiGrid server we started on port 5701
        config.getNetworkConfig().addAddress("127.0.0.1:5701");
        config.setClusterName("dev"); // "dev" is the default cluster name

        // 2. Connect to the server
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Successfully connected to BonsaiGrid!");

        // 3. Get a distributed map (like a HashMap, but stored over the network)
        IMap<String, String> capitalCities = client.getMap("capitals");

        // 4. Put some data into the grid
        capitalCities.put("France", "Paris");
        capitalCities.put("Japan", "Tokyo");
        capitalCities.put("Canada", "Ottawa");

        // 5. Read the data back
        String capitalOfJapan = capitalCities.get("Japan");
        System.out.println("The capital of Japan is: " + capitalOfJapan);
        System.out.println("Total items in the map: " + capitalCities.size());

        // 6. Shut down gracefully
        client.shutdown();
    }
}
```

### 4. Build and run

From `my-bonsai-app/`:

```bash
mvn clean compile exec:java -Dexec.mainClass="com.example.App"
```

Expected output:

```text
Starting Java Client...
... [Hazelcast Client Logs] ...
Successfully connected to BonsaiGrid!
The capital of Japan is: Tokyo
Total items in the map: 3
```

Congratulations — you've built a Java application talking to a blazingly fast
BonsaiGrid server over the network.

### 5. Next steps

- Restart the Java app: the map persists in the server's memory across client runs.
- Explore Distributed Queues, SQL querying, and Event Listeners — BonsaiGrid supports
  them all.

---

## Part 2 — Benchmark BonsaiGrid

The two-minute version. For the full reference see the
[Benchmarks](README.md#benchmarks) section of the README.

### 1. You need two things

- **Docker** — daemon running (`docker info` works). Everything else (Redis, Memcached,
  Hazelcast, the BonsaiGrid container, the Go loadgen, go-ycsb, memtier) runs in
  containers.
- **Rust / `cargo`** — builds the BonsaiGrid server.

`go` and `node` are **not** required on the host — the Go tools are built inside
containers. `python3` merges results and bakes the dashboards.

### 2. Check your CPU count first ⚠️

The harness pins the server and the load client to **disjoint** CPU sets so the client
can't steal cycles from the server. The defaults assume a **~20-core** machine: server
on cores `0-7`, client on `8-19`. On a smaller box you **must** shrink them, or the two
overlap and the numbers are meaningless:

```bash
# 8-core laptop, for example: 4 cores server, 3 cores client
SERVER_CPUS=0-3 CLIENT_CPUS=4-7 bench/benchmark-all.sh
```

Pick any two non-overlapping ranges that fit your machine; give the client at least as
many cores as the server.

### 3. Run everything, get one page

```bash
bench/benchmark-all.sh
```

This runs all four benchmark suites in sequence and bakes a single self-contained
report at **`bench/deploy/index.html`** — an executive summary plus every benchmark's
charts. It's a long run (~20 min at defaults). To view it:

```bash
python3 -m http.server            # from the repo root
# then open http://localhost:8000/bench/deploy/index.html
```

(Opening the file directly also works — it falls back to the baked-in snapshot.)

Useful knobs:

```bash
SKIP="ycsb openloop" bench/benchmark-all.sh        # run a subset of suites
STAGE_SECS=3 RATES="50000,200000,500000" \
  bench/benchmark-all.sh                            # shorter stages / fewer points
```

### 4. …or run just one suite

Each writes its own `bench/loadgen/*-combined.json` and bakes its own dashboard under
`bench/deploy/`. They all honor `SERVER_CPUS` / `CLIENT_CPUS` / `SERVER_MEM`.

| I want to measure… | Command | Dashboard |
|---|---|---|
| Fair four-backend throughput/latency | `bench/run-all-isolated.sh` | `bench/deploy/dashboard.html` |
| Standard-tool numbers (p99.9, hit/miss, network) | `bench/run-memtier.sh` | `bench/deploy/memtier.html` |
| **Honest capacity** (tail latency under load) | `bench/run-openloop.sh` | `bench/deploy/openloop.html` |
| YCSB workloads A–F | `bench/run-ycsb.sh` | `bench/deploy/ycsb.html` |
| The Rust hot path (no network) | `cargo bench -p store --bench hotpath` | `target/criterion/` |

Just want **one** number? Run `bench/run-openloop.sh` — it reports *usable throughput*
(the highest load that still holds p99 under a 10 ms SLO), which is the capacity that
actually holds in production. The "peak ops/sec" from the other runs sits past that
elbow.

### 5. Where results land

- `bench/loadgen/*-combined.json` — the merged data (checked in).
- `bench/deploy/*.html` — self-contained dashboards; `index.html` is the combined report.
- Re-bake the report from existing data without re-running: `python3 bench/gen_index.py`.

### Troubleshooting

- **"cpu governor = powersave" warning** — for stable latency, set it to `performance`:
  `sudo cpupower frequency-set -g performance` (preflight tries this if it can).
- **"port … is in use"** — a stray server/container from a previous run.
  `bench/preflight.sh` clears `bench_*` containers; kill any leftover
  `target/release/server` process.
- **First run is slow** — it pulls Docker images and builds the server + Go tools once
  (cached afterward under `~/.cache/bonsai-bench`). Needs network.
- **Ctrl-C** — safe; every runner tears down its containers on exit.
