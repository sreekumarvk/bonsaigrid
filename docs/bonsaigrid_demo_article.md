# Guide to BonsaiGrid (Java, Python, C++, Rust)

## 1. Overview
In this article, we’ll explore **BonsaiGrid** — a revolutionary, blazingly fast, and zero-allocation drop-in replacement for the Hazelcast Server Cluster. Because BonsaiGrid faithfully implements the Hazelcast open wire protocol, you can seamlessly connect to it using standard Hazelcast clients from any language. We'll show you how to easily connect and use a distributed *Map* from Java, Python, C++, and Rust!

## 2. What Is BonsaiGrid?
Hazelcast is a popular and powerful distributed In-Memory Data Grid platform for Java. For highly specialized use cases, BonsaiGrid provides an alternative native architecture optimized for deterministic memory layouts and hardware-level performance. 

**BonsaiGrid** solves this by completely replacing the Hazelcast server backend with a native Rust implementation. It implements the exact same open binary wire protocol as Hazelcast OSS but runs on a high-performance **thread-per-core** architecture powered by `io_uring`. 

The best part? You can use your existing Hazelcast Java Client (`com.hazelcast:hazelcast`) to communicate with it without modifying any of your client-side code!

## 3. Server OS Requirements (Mac, Windows, Linux)
BonsaiGrid achieves its industry-leading performance by bypassing traditional networking bottlenecks using `io_uring`—a highly optimized async I/O API built directly into the Linux kernel (v5.1+). This means the **server engine must run on a Linux kernel**.

However, you can still easily run and test the BonsaiGrid server on Mac or Windows using standard virtualization tools:

### Docker (Mac & Windows)
The easiest way to run the server on non-Linux machines is via Docker:
```bash
# Pull and run the BonsaiGrid docker image
docker run -p 5701:5701 sreekumarvk/bonsaigrid:latest
```

### WSL2 (Windows)
If you are on Windows, you can run BonsaiGrid natively using the Windows Subsystem for Linux (WSL2), which runs a real Linux kernel:
```bash
# Inside your WSL2 Ubuntu terminal
git clone https://github.com/sreekumarvk/bonsaigrid.git
cd bonsaigrid
cargo run --release --bin server
```

*(Note: The Client libraries shown later in this guide can run on **any OS** without restrictions!)*

## 4. Maven Dependencies
To use BonsaiGrid embedded in your application, you’ll need to clone the open-source repository and build the JNI wrapper, or include the compiled `.jar` in your classpath. For this guide, we assume you have built the `bonsaigrid-embedded.jar`.

We also need the standard Hazelcast Java Client to communicate with our server:

```xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>4.2</version> <!-- Any compatible version -->
</dependency>
```

## 5. A First BonsaiGrid Application

Since BonsaiGrid implements the open Hazelcast binary protocol, it works seamlessly with any standard Hazelcast client across multiple languages!

First, start your BonsaiGrid server either standalone, via the Rust library, or embedded in Java using the JNI wrapper. Once it's running on `127.0.0.1:5701`, you can connect to it using any of the following languages:

### 5.1. Java Client Demo
Using the official `com.hazelcast:hazelcast` dependency, you can connect directly to BonsaiGrid.

```java
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class JavaDemo {
    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1:5701");
        config.setClusterName("dev");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Long, String> map = client.getMap("vehicles");

        map.put(1L, "Audi");
        map.put(2L, "BMW");

        System.out.println("Map Size: " + map.size());
        client.shutdown();
    }
}
```

### 5.2. Python Client Demo
Using the official `hazelcast-python-client`, Python applications can utilize BonsaiGrid's incredible performance.

```python
import hazelcast

if __name__ == "__main__":
    # Connect to BonsaiGrid
    client = hazelcast.HazelcastClient(
        cluster_members=["127.0.0.1:5701"],
        cluster_name="dev"
    )

    # Get the Distributed Map
    map = client.get_map("vehicles").blocking()

    map.put(1, "Audi")
    map.put(2, "BMW")

    print(f"Map Size: {map.size()}")
    client.shutdown()
```

### 5.3. C++ Client Demo
Using the official Hazelcast C++ client, high-frequency trading or gaming applications can get sub-millisecond latencies against BonsaiGrid.

```cpp
#include <hazelcast/client/hazelcast_client.h>
#include <iostream>

int main() {
    hazelcast::client::client_config config;
    config.get_network_config().add_address({"127.0.0.1", 5701});
    config.set_cluster_name("dev");

    auto client = hazelcast::new_client(std::move(config)).get();
    auto map = client.get_map("vehicles").get();

    map->put<int64_t, std::string>(1, "Audi").get();
    map->put<int64_t, std::string>(2, "BMW").get();

    std::cout << "Map Size: " << map->size().get() << std::endl;
    return 0;
}
```

### 5.4. Rust Client Demo
For native Rust applications, you can use community Rust Hazelcast clients or simply use BonsaiGrid's internal crates for direct embedded access!

```rust
use hazelcast_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ClientConfig::new();
    config.network.add_address("127.0.0.1:5701");
    config.cluster_name = "dev".to_string();

    let client = Client::start(config).await?;
    let map = client.get_map("vehicles").await?;

    map.put(1i64, "Audi".to_string()).await?;
    map.put(2i64, "BMW".to_string()).await?;

    println!("Map Size: {}", map.size().await?);
    Ok(())
}
```

## 6. Why Choose BonsaiGrid?
When you write the code above, the underlying data is stored in the BonsaiGrid Rust memory space, entirely off-heap relative to your Java application. This results in:
*   **Deterministic latency profiles** on your caching layer via native memory management.
*   **Massive throughput** via `io_uring` and lock-free thread-per-core processing.
*   **Seamless adoption** because you don't have to rewrite any of your Hazelcast client code.

## 7. Conclusion
In this article, we demonstrated how easy it is to replace an embedded Hazelcast server with the high-performance BonsaiGrid Rust engine via the native JNI wrapper. We then connected to it using the standard Hazelcast Java Client and performed basic CRUD operations on a distributed Map. 

You can find the complete source code for BonsaiGrid and the JNI wrapper on GitHub.
