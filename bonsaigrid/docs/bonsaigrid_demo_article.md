# Guide to BonsaiGrid with Java

## 1. Overview
In this article, we’ll explore **BonsaiGrid** — a revolutionary, blazingly fast, and zero-allocation drop-in replacement for the Hazelcast Server Cluster. We'll learn how to embed a BonsaiGrid server node natively inside a Java application, create a distributed *Map*, and use the standard Hazelcast Java Client to connect and query data.

## 2. What Is BonsaiGrid?
Hazelcast is a popular distributed In-Memory Data Grid platform for Java. While powerful, its server architecture relies on the JVM, which can lead to GC pauses and high memory overhead under load. 

**BonsaiGrid** solves this by completely replacing the Hazelcast server backend with a native Rust implementation. It implements the exact same open binary wire protocol as Hazelcast OSS but runs on a high-performance **thread-per-core** architecture powered by `io_uring`. 

The best part? You can use your existing Hazelcast Java Client (`com.hazelcast:hazelcast`) to communicate with it without modifying any of your client-side code!

## 3. Maven Dependencies
To use BonsaiGrid embedded in your application, you’ll need to clone the open-source repository and build the JNI wrapper, or include the compiled `.jar` in your classpath. For this guide, we assume you have built the `bonsaigrid-embedded.jar`.

We also need the standard Hazelcast Java Client to communicate with our server:

```xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>4.2</version> <!-- Any compatible version -->
</dependency>
```

## 4. A First BonsaiGrid Application

### 4.1. Start the Embedded BonsaiGrid Server
Unlike standard Hazelcast, which spawns a JVM-based server when you call `Hazelcast.newHazelcastInstance()`, BonsaiGrid provides a frictionless JNI wrapper that spawns the highly optimized Rust engine directly in the background of your JVM via FFI.

Let’s boot up our embedded node:

```java
import com.bonsaigrid.BonsaiGrid;

public class BonsaiGridDemo {
    public static void main(String[] args) {
        System.out.println("Booting up BonsaiGrid Rust Engine...");
        
        // Starts the io_uring thread-per-core server in the background natively
        BonsaiGrid.startServer(); 
        
        System.out.println("BonsaiGrid is listening on 127.0.0.1:5701");
    }
}
```
When this runs, the Java application will seamlessly extract the native dynamic library (e.g. `.so`, `.dylib`, or `.dll`) and boot the Rust server cluster on port `5701`.

### 4.2. Create the Hazelcast Java Client
Since BonsaiGrid implements the standard protocol, we can connect to our embedded Rust server using the official Hazelcast Java Client.

```java
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

// ... inside the main method ...

ClientConfig clientConfig = new ClientConfig();
clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
clientConfig.setClusterName("dev");

HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
System.out.println("Successfully connected to BonsaiGrid!");
```

### 4.3. Using the Distributed Map
Now that we are connected, let's interact with a distributed `IMap`. All network requests will route over the wire to our lightning-fast embedded Rust backend.

```java
import com.hazelcast.map.IMap;
import java.util.Map;

// ... inside the main method ...

IMap<Long, String> map = client.getMap("vehicles");

// Puts data into the BonsaiGrid store
map.put(1L, "Audi");
map.put(2L, "BMW");
map.put(3L, "Mercedes");

System.out.println("Map Size: " + map.size()); // Prints 3

// Iterating over entries
for (Map.Entry<Long, String> entry : map.entrySet()) {
    System.out.printf("Key: %d, Value: %s\n", entry.getKey(), entry.getValue());
}

// Shut down the client
client.shutdown();
```

## 5. Why Choose BonsaiGrid?
When you write the code above, the underlying data is stored in the BonsaiGrid Rust memory space, entirely off-heap relative to your Java application. This results in:
*   **Zero Garbage Collection (GC) pauses** on your caching layer.
*   **Massive throughput** via `io_uring` and lock-free thread-per-core processing.
*   **Seamless adoption** because you don't have to rewrite any of your Hazelcast client code.

## 6. Conclusion
In this article, we demonstrated how easy it is to replace an embedded Hazelcast server with the high-performance BonsaiGrid Rust engine via the native JNI wrapper. We then connected to it using the standard Hazelcast Java Client and performed basic CRUD operations on a distributed Map. 

You can find the complete source code for BonsaiGrid and the JNI wrapper on GitHub.
