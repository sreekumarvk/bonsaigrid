//! BonsaiGrid increment-3 server binary: thread-per-core.
//!
//! Launches N worker threads, each pinned to a physical core (`core_affinity`),
//! each running its own io_uring reactor over its own `SO_REUSEPORT` listener on
//! the main port. The kernel spreads incoming connections across the N cores, so
//! throughput scales with cores. The store is partitioned into N independently
//! locked shards; a request's (map,key) deterministically selects its shard, so
//! any core serves any key correctly (the routing spec's per-core ownership,
//! realized here as per-shard locks).
//!
//! N defaults to the detected core count, capped at 8; override with BONSAI_CORES.

use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::Arc;

fn reuseport_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let sock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?; // multiple listeners on the same port, kernel load-balances
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

fn main() -> std::io::Result<()> {
    let detected = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1);
    let cap: usize = std::env::var("BONSAI_CORES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let cores = detected.min(cap).max(1);

    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
    let store = Arc::new(store::Store::with_shards(cores));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    eprintln!("BonsaiGrid listening on {addr} (thread-per-core, {cores} cores, io_uring)");

    let mut handles = Vec::new();
    for i in 0..cores {
        let store = store.clone();
        let listener = reuseport_listener(addr)?;
        let core_id = core_ids.get(i).copied();
        handles.push(std::thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let _ = server::reactor::run(listener, |req| server::handlers::dispatch(req, &store));
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}
