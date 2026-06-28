# BonsaiGrid Quick Start: Linux & Rust

Welcome to BonsaiGrid! This guide will help you set up a native Rust client application to connect to BonsaiGrid.

Since both the server and client are written in Rust, you get the absolute best performance possible with zero FFI overhead and deep `async/await` integration.

## 1. Prerequisites

Ensure you have the following installed on your Linux machine:
- **Rust and Cargo**: (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

---

## 2. Start the BonsaiGrid Server

BonsaiGrid’s server engine leverages Linux's `io_uring` to achieve massive throughput.

Clone the BonsaiGrid repository and start the server:

```bash
# 1. Clone the repository
git clone https://github.com/sreekumarvk/bonsaigrid.git
cd bonsaigrid

# 2. Compile and run the server in release mode
cargo run --release --bin server
```

The server will start listening on `127.0.0.1:5701`. Leave this terminal tab open.

---

## 3. Create the Rust Application

Open a **new terminal tab**. We will create a fresh Cargo project for our client.

### Step 3.1: Initialize the Cargo Project
```bash
cargo new my-bonsai-rust
cd my-bonsai-rust
```

### Step 3.2: Configure `Cargo.toml`
Open `Cargo.toml` and add the `tokio` async runtime and a community Hazelcast Rust client. Alternatively, you can use BonsaiGrid's internal client crates if you are building inside the monorepo workspace. For this standalone project, we'll use a hypothetical `hazelcast-client` crate (ensure you use a valid crate or BonsaiGrid's internal client).

```toml
[package]
name = "my-bonsai-rust"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.30", features = ["full"] }
# Assuming a hazelcast_client crate exists, or point to BonsaiGrid's client crate if available
hazelcast-client = "0.1" 
```
*(Note: If a public Rust Hazelcast client is not available on crates.io, you can point this dependency directly to a path inside the BonsaiGrid repository if it provides a client library).*

### Step 3.3: Write the Rust Code
Replace the contents of `src/main.rs` with the following code:

```rust
use hazelcast_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Rust Client...");

    // 1. Configure the Client
    let mut config = ClientConfig::new();
    config.network.add_address("127.0.0.1:5701");
    config.cluster_name = "dev".to_string();

    // 2. Connect to the server
    let client = Client::start(config).await?;
    println!("Successfully connected to BonsaiGrid!");

    // 3. Get a distributed map
    let map = client.get_map("capitals").await?;

    // 4. Put some data into the grid
    map.put("France".to_string(), "Paris".to_string()).await?;
    map.put("Japan".to_string(), "Tokyo".to_string()).await?;
    map.put("Canada".to_string(), "Ottawa".to_string()).await?;

    // 5. Read the data back
    if let Some(capital_of_japan) = map.get(&"Japan".to_string()).await? {
        println!("The capital of Japan is: {}", capital_of_japan);
    }
    
    println!("Total items in the map: {}", map.size().await?);

    Ok(())
}
```

---

## 4. Build and Run

Run the following command from the `my-bonsai-rust` directory:

```bash
cargo run
```

### Expected Output
```text
Starting Rust Client...
Successfully connected to BonsaiGrid!
The capital of Japan is: Tokyo
Total items in the map: 3
```

Congratulations! You have just connected a native Rust async client to BonsaiGrid.
