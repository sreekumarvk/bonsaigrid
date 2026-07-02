# REQUIREMENTS.md

## Project Name: BonsaiGrid (v0.1 MVP)
**Objective:** A distributed, in-memory data grid designed as a highly memory-efficient, bare-metal alternative to JVM-based architectures (e.g., Apache Hazelcast). It prioritizes deterministic memory layout, a thread-per-core runtime, and kernel-bypass asynchronous I/O to provide predictable latencies and optimized memory utilization.

---

## 📂 Workspace Topology
All execution commands and AI context evaluations are anchored at the root directory:
* **Java Reference Baseline:** `./hazelcast/` (The official Apache Hazelcast Git repository)
* **Target Output Directory:** `./bonsaigrid/` (A clean, zero-allocation Rust codebase)

---

## 🛠 Architectural Guardrails (Non-Negotiable)

1. **Zero-Allocation Hot Path**
   * Post-initialization, the data processing, network serialization, and memory storage path must completely avoid dynamic heap allocations (`malloc`, `free`, standard `Box`, `Vec`, or `String` generation).
   * All data structures must rely on pre-allocated contiguous memory pools.

2. **Shared-Nothing Concurrency (Thread-Per-Core)**
   * The runtime must map exactly one operating system thread to one physical/logical CPU core using strict hardware pinning (`core_affinity`).
   * Threads must never share memory via locks (`std::sync::Mutex`, `RwLock`). Cross-thread data coordination must happen exclusively via lock-free Single-Producer Single-Consumer (SPSC) channels.

3. **Kernel Bypass Network I/O**
   * The application must bypass synchronous blocking I/O and standard epoll runtimes in the hot path. 
   * It must interact with the Linux kernel directly using `io_uring` capabilities via `tokio-uring` or raw system calls.

---

## 📦 Core Technology Stack (Rust)
The target `./bonsaigrid/Cargo.toml` must enforce the following dependency parameters:
* `tokio-uring` (Asynchronous `io_uring` file/socket engine)
* `core_affinity` (Hardware CPU thread pinning)
* `crossbeam-channel` or `flume` (Lock-free synchronization)
* `ahash` or `xxhash` (Ultra-low latency, deterministic hashing primitives)

---

## 📋 Implementation Roadmap & Iterations

### Phase 1: Hazelcast Open Binary Client Protocol Extraction
Before constructing the storage engine, the system must interface flawlessly with existing Hazelcast client drivers. 
* **Target Analysis Space:** `./hazelcast/` protocol codec files and wire frame specifications.
* **Requirements:**
  * Map the exact binary footprint of Hazelcast's client-server frame headers (including Magic Bytes, Message Flags, Correlation ID, Partition ID, and Operation Type ID).
  * Document the serialization mechanics for basic operation vectors: `map.put` and `map.get`.
  * Define the exact byte-level response payloads expected by upstream Hazelcast client libraries.

### Phase 2: The Deterministic Slab Allocator (`allocator.rs`)
* Allocate a single, massive virtual memory chunk from the OS at startup (using `mmap` or similar primitives).
* Divide this memory space into uniform, fixed-size chunks (slabs).
* Implement a lock-free free-list to fetch and reclaim slabs in strict $O(1)$ constant time.
* If memory utilization reaches 100%, throw an explicit `Out Of Memory` constraint error rather than dynamically scaling the heap buffer.

### Phase 3: The Thread-Per-Core Network Reactor
* Scan host architecture to determine physical core constraints and launch exactly $N$ workers.
* Establish dedicated, independent `io_uring` polling loops per worker thread.
* Listen for incoming TCP frames, parsing raw binary sequences into stateless context frames on the fly without allocation.

### Phase 4: Sharded In-Memory Map & Routing Engine
* Build a specialized hash table where the hash value of the protocol Key dictates the specific CPU core that owns that data point.
* **Routing Policy:** If Worker Core $A$ processes a socket packet whose key hash assigns it to Worker Core $B$, Core $A$ must delegate the transaction to Core $B$'s dedicated SPSC ring buffer. Core $B$ then executes the final memory operation inside its private, unshared Slab Allocator space.