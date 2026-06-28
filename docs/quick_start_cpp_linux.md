# BonsaiGrid Quick Start: Linux & C++

Welcome to BonsaiGrid! This guide will help you set up a blazing fast C++ client application to connect to BonsaiGrid on Linux.

BonsaiGrid completely implements the open Hazelcast wire protocol, allowing us to use the highly-optimized Hazelcast C++ client to talk to the BonsaiGrid Rust engine.

## 1. Prerequisites

Ensure you have the following installed on your Linux machine:
- **C++ Compiler**: GCC (g++) or Clang (`sudo apt install build-essential`)
- **CMake**: version 3.10+ (`sudo apt install cmake`)
- **Git**: (`sudo apt install git`)
- **Rust and Cargo**: Required to compile the BonsaiGrid server (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- **Hazelcast C++ Client Library**: We will pull this via CMake, or you can install it manually from [github.com/hazelcast/hazelcast-cpp-client](https://github.com/hazelcast/hazelcast-cpp-client).

---

## 2. Start the BonsaiGrid Server

BonsaiGrid’s engine uses `io_uring` for lock-free, zero-allocation networking. 

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

## 3. Create the C++ Application

Open a **new terminal tab**. We will create a CMake project for our C++ client.

### Step 3.1: Initialize the Project
```bash
mkdir -p my-bonsai-cpp
cd my-bonsai-cpp
```

### Step 3.2: Create the `CMakeLists.txt`
Create a `CMakeLists.txt` file in the directory:

```cmake
cmake_minimum_required(VERSION 3.10)
project(BonsaiGridDemo CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# We use FetchContent to easily pull the Hazelcast C++ client
include(FetchContent)
FetchContent_Declare(
  hazelcast-cpp-client
  GIT_REPOSITORY https://github.com/hazelcast/hazelcast-cpp-client.git
  GIT_TAG        v5.3.0 # Use a compatible tag
)
FetchContent_MakeAvailable(hazelcast-cpp-client)

add_executable(demo main.cpp)
target_link_libraries(demo PRIVATE hazelcast-cpp-client)
```

### Step 3.3: Write the C++ Code
Create a file named `main.cpp`:

```cpp
#include <hazelcast/client/hazelcast_client.h>
#include <iostream>
#include <string>

int main() {
    std::cout << "Starting C++ Client..." << std::endl;

    // 1. Configure the Client
    hazelcast::client::client_config config;
    config.get_network_config().add_address({"127.0.0.1", 5701});
    config.set_cluster_name("dev");

    // 2. Connect to the server
    auto client = hazelcast::new_client(std::move(config)).get();
    std::cout << "Successfully connected to BonsaiGrid!" << std::endl;

    // 3. Get a distributed map
    auto map = client.get_map("capitals").get();

    // 4. Put some data into the grid
    map->put<std::string, std::string>("France", "Paris").get();
    map->put<std::string, std::string>("Japan", "Tokyo").get();
    map->put<std::string, std::string>("Canada", "Ottawa").get();

    // 5. Read the data back
    auto capitalOfJapan = map->get<std::string, std::string>("Japan").get();
    if (capitalOfJapan) {
        std::cout << "The capital of Japan is: " << *capitalOfJapan << std::endl;
    }
    
    std::cout << "Total items in the map: " << map->size().get() << std::endl;

    return 0;
}
```

---

## 4. Build and Run

Run the following commands from the `my-bonsai-cpp` directory to build and execute the application:

```bash
mkdir build
cd build
cmake ..
make
./demo
```

### Expected Output
```text
Starting C++ Client...
Successfully connected to BonsaiGrid!
The capital of Japan is: Tokyo
Total items in the map: 3
```

Congratulations! You have just connected a high-performance C++ client to BonsaiGrid.
