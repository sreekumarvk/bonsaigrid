# BonsaiGrid Quick Start: Linux & Java

Welcome to BonsaiGrid! If you are new to In-Memory Data Grids (IMDGs) or Hazelcast, this guide is for you. 

BonsaiGrid is a blazingly fast, zero-allocation data grid written in Rust. It speaks the standard Hazelcast binary protocol, meaning you can use official Hazelcast clients to store and retrieve data across a network at extreme speeds.

In this quick start, we will:
1. Start a BonsaiGrid server on your Linux machine.
2. Create a brand new Java Maven project from scratch.
3. Write a simple Java application to connect to BonsaiGrid and interact with a distributed Map.

---

## 1. Prerequisites

Before we begin, ensure you have the following installed on your Linux machine:
- **Java 11 or higher**: (`java -version`)
- **Maven**: (`mvn -version`)
- **Rust and Cargo**: Required to compile the BonsaiGrid server (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

---

## 2. Start the BonsaiGrid Server

BonsaiGrid’s engine uses `io_uring`, which is deeply integrated into the Linux kernel for maximum performance. 

First, clone the BonsaiGrid repository and start the server:

```bash
# 1. Clone the repository
git clone https://github.com/sreekumarvk/bonsaigrid.git
cd bonsaigrid

# 2. Compile and run the server in release mode for maximum performance
cargo run --release --bin server
```

You should see output indicating that the server is listening on `127.0.0.1:5701`. Leave this terminal tab open so the server continues running in the background.

---

## 3. Create the Java Application

Open a **new terminal tab** to create your Java client application.

### Step 3.1: Initialize the Maven Project
We will create a simple directory structure for our Java project:

```bash
mkdir -p my-bonsai-app/src/main/java/com/example
cd my-bonsai-app
```

### Step 3.2: Create the `pom.xml`
Create a file named `pom.xml` in the `my-bonsai-app` directory. We only need one dependency: the official Hazelcast Java Client.

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

### Step 3.3: Write the Java Code
Create a file named `App.java` inside `src/main/java/com/example/`.

This code configures the client to connect to your local BonsaiGrid server, creates a distributed dictionary (called an `IMap`), and writes some data to it.

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

        // 3. Get a distributed map (like a standard Java HashMap, but stored over the network)
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

---

## 4. Build and Run

With your `pom.xml` and `App.java` in place, you can now compile and run your application using Maven.

Run the following command from the `my-bonsai-app` directory:

```bash
mvn clean compile exec:java -Dexec.mainClass="com.example.App"
```

### Expected Output
If everything is set up correctly, Maven will download the dependencies, compile your code, and you will see output similar to this:

```text
Starting Java Client...
... [Hazelcast Client Logs] ...
Successfully connected to BonsaiGrid!
The capital of Japan is: Tokyo
Total items in the map: 3
```

Congratulations! You have successfully built a Java application that communicates with a blazingly fast BonsaiGrid server over the network. 

## 5. Next Steps
- Try starting multiple terminals and running the Java app again. The map is persistent in the BonsaiGrid server memory!
- Explore advanced Hazelcast Client features like Distributed Queues, SQL querying, and Event Listeners — BonsaiGrid supports them all.
