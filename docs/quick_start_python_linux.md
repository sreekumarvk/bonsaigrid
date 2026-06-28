# BonsaiGrid Quick Start: Linux & Python

Welcome to BonsaiGrid! This guide will help you set up a Python application to connect to BonsaiGrid on Linux.

Because BonsaiGrid perfectly emulates the Hazelcast binary protocol, we can use the official `hazelcast-python-client` package to interact with our highly-optimized Rust server.

## 1. Prerequisites

Ensure you have the following installed on your Linux machine:
- **Python 3.6 or higher**: (`python3 --version`)
- **pip**: Python package manager (`pip3 --version`)
- **Rust and Cargo**: Required to compile the BonsaiGrid server (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

---

## 2. Start the BonsaiGrid Server

BonsaiGrid’s server utilizes `io_uring` in the Linux kernel to bypass standard networking bottlenecks.

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

## 3. Create the Python Application

Open a **new terminal tab**. We will create a fresh directory for our Python scripts.

### Step 3.1: Initialize the Project Environment
It's best practice to use a virtual environment:

```bash
mkdir -p my-bonsai-python
cd my-bonsai-python

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate
```

### Step 3.2: Install the Hazelcast Client
Install the official Hazelcast Python client via `pip`:

```bash
pip install hazelcast-python-client
```

### Step 3.3: Write the Python Code
Create a file named `app.py`:

```python
import hazelcast

def main():
    print("Starting Python Client...")

    # 1. Configure and connect the client to BonsaiGrid
    client = hazelcast.HazelcastClient(
        cluster_members=["127.0.0.1:5701"],
        cluster_name="dev"
    )
    print("Successfully connected to BonsaiGrid!")

    # 2. Get a distributed map (Note: blocking() is used for synchronous operations)
    map = client.get_map("capitals").blocking()

    # 3. Put some data into the grid
    map.put("France", "Paris")
    map.put("Japan", "Tokyo")
    map.put("Canada", "Ottawa")

    # 4. Read the data back
    capital_of_japan = map.get("Japan")
    print(f"The capital of Japan is: {capital_of_japan}")
    
    print(f"Total items in the map: {map.size()}")

    # 5. Shut down gracefully
    client.shutdown()

if __name__ == "__main__":
    main()
```

---

## 4. Run the Script

Execute the Python script in your terminal:

```bash
python app.py
```

### Expected Output
```text
Starting Python Client...
Successfully connected to BonsaiGrid!
The capital of Japan is: Tokyo
Total items in the map: 3
```

Congratulations! You have successfully built a Python application that stores data in BonsaiGrid's off-heap memory store.
