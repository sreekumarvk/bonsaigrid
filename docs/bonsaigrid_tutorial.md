# Let's Build a Distributed In-Memory Data Grid (BonsaiGrid)

Welcome! If you're new to distributed systems, you are in the right place. We are going to break down the **BonsaiGrid** repository piece by piece. By the end of this tutorial, you'll understand what an in-memory data grid is, the extreme engineering required to make it incredibly fast, and how to build one from scratch.

This tutorial takes inspiration from Andrej Karpathy's teaching style: we won't throw buzzwords at you without explaining *why* they exist. We'll start from the basic intuition, encounter problems, and then introduce the technical solutions.

---

## 1. The Problem: Fast Storage and the Limits of the Traditional Hashmap

Let's start from absolute first principles. Why are we here?

In the world of real-time processing—whether you're building a massive multiplayer game, a high-frequency trading platform, or a live recommendation engine—you need to store and retrieve data *fast*. Not in milliseconds, but in microseconds. 

For this, software engineers rely on the most fundamental data structure: the **Hashmap** (or `dict` in Python, `HashMap` in Java, `std::collections::HashMap` in Rust). Hashmaps live entirely in RAM (Random Access Memory). You give it a key (like `"user_123"`), and it instantly gives you the value. It's $O(1)$ constant time lookup. It is the engine of the internet.

So, problem solved, right? Just use a Hashmap?

Unfortunately, a traditional Hashmap completely breaks down when you try to use it at a massive scale. Here is why:

### The Memory Limit (Running out of RAM)
A traditional Hashmap is bound to the physical memory of a single machine. If your application becomes hugely popular and you need to store 500 Gigabytes of real-time user sessions, but your server only has 64GB of RAM, your application will crash with an "Out of Memory" error. You can buy bigger servers (vertical scaling), but eventually, you hit the absolute physical limits of hardware.

### The Concurrency Limit (The Lock Contention Problem)
Modern servers have lots of CPU cores (e.g., 32 or 64 cores). If you want to handle a million requests per second, you need all 64 cores working at the same time. But a traditional Hashmap is not thread-safe. If two CPU cores try to write to the same Hashmap at the exact same microsecond, the data gets corrupted. 
To fix this, programmers use **Locks** (like a `Mutex`). But locks ruin performance. If Core 1 locks the Hashmap to update a value, Cores 2 through 64 must literally fall asleep and wait in line. Instead of running 64 times faster, your multi-core server ends up running at the speed of a single core.

### The Garbage Collection Limit (Memory Bloat)
In managed languages like Java (which traditionally dominate this space), every item you put in a Hashmap is an "Object". Creating millions of objects creates massive memory bloat (metadata overhead). Worse, the Java Virtual Machine (JVM) periodically has to clean up old objects. This is called "Garbage Collection." When a massive GC cycle hits, your entire application completely freezes for hundreds of milliseconds. In real-time systems, a 500ms freeze is an eternity.

---

## 2. The Solution: How Do We Make It Scale?

To solve these three critical flaws of the traditional Hashmap, we must fundamentally redesign how we store data. This brings us to **BonsaiGrid**.

To make our Hashmap scale, we need to do three things:

1. **Distribute it across a cluster of machines.** If one machine only has 64GB of RAM, we link 10 machines together over the network to create a 640GB "virtual" Hashmap. This is what we call an **In-Memory Data Grid**. When you ask the grid for `"user_123"`, the grid knows exactly which machine in the cluster holds that data and fetches it instantly.
2. **Remove all locks (Shared-Nothing Architecture).** Instead of making 64 cores fight over one giant Hashmap, we slice the data up. We give each CPU core its own tiny, private Hashmap. Core 1 owns keys A-M, Core 2 owns N-Z. They never share memory, and therefore, they never need to lock. This is called a **Thread-Per-Core** model.
3. **Eliminate Garbage Collection (Zero-Allocation).** We bypass the JVM entirely and write our system in bare-metal **Rust**. We allocate a massive chunk of memory on startup, and then manage it manually using a **Slab Allocator**. We never trigger a system memory allocation again.

---

## 3. The High-Level Overview

Now that we understand the problem and the solution, what are we actually building? To build BonsaiGrid, we need four major components. Think of this as the "Walking Skeleton" of our system:

1. **The Network Listener (Reactor):** A loop that listens for incoming TCP network connections from clients.
2. **The Protocol Decoder:** Clients speak a specific binary language (Hazelcast's Open Binary Client Protocol). We need to intercept the 1s and 0s coming over the network and translate them into commands like "Put X=Y" or "Get X".
3. **The Storage Engine:** A super-fast hash map in memory where we actually save the data.
4. **The Routing / Clustering Engine:** The brain that decides *which* CPU core or *which* server machine actually holds a specific piece of data. 

---

## 4. The "Extreme" Constraints (Things we must take care of)

Putting a standard Rust `HashMap` behind a TCP server is a valid solution that might be sufficient for smaller scale problems. However, the problem we are trying to solve is much larger—requiring massive scale and extreme throughput. To achieve this, we need to extend the solution further using three strict architectural "guardrails":

> [!CAUTION]
> **Constraint 1: Zero-Allocation in the Hot Path**
> **The Problem:** In managed languages like Java, allocating memory dynamically on the "heap" for every request triggers Garbage Collection pauses. Even in languages without garbage collection like standard Rust, constantly calling `malloc` (or creating new `String`s or `Vec`s) millions of times a second introduces significant overhead and allocator contention, which degrades system performance.
> **The Solution:** We will allocate one massive chunk of memory when the server boots up. After that, we are *forbidden* from allocating new memory. We will recycle memory from this pre-allocated chunk using a **Slab Allocator**.

> [!WARNING]
> **Constraint 2: Shared-Nothing, Thread-Per-Core**
> **The Problem:** Usually, if multiple CPU threads want to write to the same `HashMap`, you use a Lock (like a `Mutex`). But Locks are disastrous for performance. If 16 CPU cores all want the lock, 15 of them fall asleep waiting in line.
> **The Solution:** We use a **Thread-Per-Core (TPC)** model. We pin exactly one OS thread to one physical CPU core (`core_affinity`). Every core gets its *own private, isolated HashMap*. They never share memory. If Core 1 receives a network packet meant for Core 2, Core 1 drops it into a lock-free mailbox (an SPSC channel) for Core 2. **No locks!**

> [!TIP]
> **Constraint 3: Kernel Bypass I/O (`io_uring`)**
> **The Problem:** Standard networking requires asking the Linux kernel to read data via system calls, which causes expensive "context switches" between your app and the OS.
> **The Solution:** We use `io_uring` (via `tokio-uring`). This sets up a shared memory ring between our app and the Linux kernel. The kernel quietly drops network packets into this ring, and our app reads them without ever making a system call.

---

## 5. How the System Actually Works (Step-by-Step)

Let's look at how these concepts translate into actual implementation phases in the repository.

### Phase 1: Speaking the Language (Protocol Extraction)
Before we can store data, we have to understand the client. Hazelcast clients send binary "Frames". 
We look at the bytes:
- The first 4 bytes tell us the **Length** of the frame.
- The next 2 bytes are **Flags** (e.g., is this the end of a message?).
- Then we read the **Message Type ID** (e.g., `65792` means "Map.Put", `66048` means "Map.Get").

Our `crates/protocol` and `crates/codecs` folders handle this. They take raw bytes and zero-copy parse them into Rust structs.

### Phase 2: The Slab Allocator (`crates/store`)
Instead of dynamically allocating memory for every new key-value pair, we build an "Egg Carton". 
At startup, we ask the OS for a massive 10GB block of memory via `mmap`. We divide this block into millions of equal-sized "slots" (slabs).
When a client says `Put("user_1", "data")`, we look at our lock-free "free-list" to instantly find an empty slot in $O(1)$ constant time, and put the data there. If we fill up the 10GB, we don't ask the OS for more memory—we explicitly throw an "Out of Memory" error. This ensures absolute predictability.

### Phase 3: The Thread-Per-Core Reactor (`crates/server`)
When you boot the server, it scans your hardware. Let's say you have an 8-core CPU.
The server launches exactly 8 threads. It pins Thread 0 strictly to CPU Core 0, Thread 1 to Core 1, etc.
Each thread spins up its own infinite loop using `io_uring` to constantly check for new network packets. There is no central "manager" thread slowing things down. They all work completely independently.

### Phase 4: Cross-Core and Cross-Machine Routing
This is the magic of distributed systems. To scale beyond one machine, the entire keyspace is divided into a fixed number of "Partitions" (e.g., 271). Each partition is exclusively owned by a specific machine, and further assigned to exactly one CPU core on that machine.

While there is a much smarter, zero-hop way to do this (which we'll cover in a moment), it helps to first understand the suboptimal approach. Let's trace a request from a basic ("dumb") client routed through a standard network load balancer:
1. User sends `Put(Key="apple", Value="red")`. 
2. A standard network load balancer operates at TCP Layer 4. It only sees raw bytes and doesn't understand the BonsaiGrid binary protocol, so it cannot extract the key `"apple"` to compute its hash. Therefore, it blindly routes the TCP connection to a random node—let's say **Machine A, Core 0**.
3. Core 0 reads the bytes and sees the key `"apple"`. 
4. How do we know where `"apple"` lives? We use a deterministic mathematical function (MurmurHash3):
   `Hash("apple") = 847291`
5. We determine the partition: `847291 % 271 = Partition 42`.
6. We check the cluster's partition table. Let's say Partition 42 is owned by **Machine B, Core 3**.
7. Because Machine A does not share memory with Machine B (and Core 0 doesn't share with Core 3), Machine A cannot write the data. Instead, Core 0 forwards the request over the network to Machine B.
8. Machine B receives the forwarded packet and drops it into the lock-free queue for Core 3. Core 3 saves it in its private Slab Allocator and sends the "Success" response back.

**The Smart Client Optimization:**
Bouncing requests between machines wastes time. To solve this, official "Smart Clients" download the partition table when they first connect. The client does the `Hash("apple") % 271` math *locally* and sends the TCP packet directly to the correct IP address and socket for **Machine B, Core 3**. This eliminates internal cluster hops, resulting in blazing fast, single-hop $O(1)$ latency whether you have 1 machine or 1,000 machines.

---

## 6. How to Build the System Step-by-Step (The Implementation Guide)

If you were to start this repository completely from scratch (a blank `Cargo.toml`), here is the exact order of operations to implement it without getting overwhelmed. Building distributed systems is all about creating isolated layers.

### Step 1: The Protocol Decoder (No Networking Yet)
**Goal:** Speak the client's language.
Don't write a TCP server yet! Start by writing pure functions that take a raw array of bytes (`[u8]`) and parse them into a Rust `struct`. 
You need to extract Hazelcast's binary protocol. Find out exactly where the "Message Type", "Correlation ID", and "Key/Value" fields live in the byte stream. Write unit tests to assert that `parse([0x01, 0x00...])` outputs `PutRequest { key: "hello" }`. You know you are done when your unit tests prove you can perfectly read and write the exact byte sequences expected by a real Hazelcast client.
*Files to build:* `crates/protocol`, `crates/codecs`.

### Step 2: The Slab Allocator (No Networking Yet)
**Goal:** Build the zero-allocation storage engine.
Still no networking. You need to write a deterministic memory pool. Use Rust's `mmap` or simple pre-allocated `Vec` buffers to claim a massive chunk of RAM. Write the lock-free "Free List" that hands out fixed-size blocks (slabs) in $O(1)$ time. 
You know you are done when you can write a tight loop that inserts 10 million items, deletes them, and re-inserts them without `htop` showing your program's memory footprint growing *at all*.
*Files to build:* `crates/store/allocator.rs`.

### Step 3: The Thread-Per-Core Reactor (Networking Arrives)
**Goal:** Wire up the engine to the internet.
Now, bring in `tokio-uring` and `core_affinity`. Scan the host machine for the number of CPU cores. Loop from `0` to `N_CORES`, spawning a thread, pinning it to the core, and starting an `io_uring` TCP listener. 
Hook up the Protocol Decoder from Step 1. When a client connects and sends bytes, parse them. Do not store them yet; just parse them and reply with an "OK" protocol message.
You know you are done when a Hazelcast client can successfully connect to your server without throwing a "connection reset" error.
*Files to build:* `crates/server`.

### Step 4: Cross-Core Routing and Storage Integration
**Goal:** Make the threads talk to each other without locks.
Combine Step 2 and Step 3. Give every reactor thread its own private Slab Allocator. Implement `MurmurHash3` to hash the incoming keys. 
If Thread 0 receives a key that hashes to Thread 2, Thread 0 must **not** try to lock Thread 2's storage. Instead, set up Single-Producer Single-Consumer (SPSC) channels between every pair of threads. Thread 0 drops the parsed request into Thread 2's channel. Thread 2 reads the channel, saves the data in its private Slab, and sends the response.
You know you are done when a Hazelcast client can `Put("a", "b")`, disconnect, reconnect (which might land on a different core), and successfully `Get("a")`.

---

## 7. Getting Your Hands Dirty

To see this all in action in the repo:

1. **The Codebase structure:**
   - `crates/protocol` & `crates/codecs`: The Hazelcast translation layer.
   - `crates/store`: The memory (slab allocator and HashMap).
   - `crates/server`: The Thread-per-core `io_uring` network loop.

2. **Run the server:**
   ```bash
   cargo run -p server
   ```
   This binds to `127.0.0.1:5701` (the default Hazelcast port).

3. **Run the Client Conformance Tests:**
   The ultimate proof that our system works is running an unmodified, standard Hazelcast Python client against our Rust server.
   ```bash
   # In a new terminal
   python3 -m venv conformance-python/.venv
   conformance-python/.venv/bin/pip install -r conformance-python/requirements.txt
   conformance-python/.venv/bin/python conformance-python/smoke.py
   ```
   If it prints `PYTHON SMOKE OK`, you have successfully tricked a production-grade Java-ecosystem client into talking to your bare-metal Rust engine!

Happy hacking, and welcome to distributed systems engineering!
