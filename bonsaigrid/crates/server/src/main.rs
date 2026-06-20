//! BonsaiGrid increment-3 server binary: thread-per-core with TPC.
//!
//! Launches N worker threads, each pinned to a core (`core_affinity`), each
//! running an io_uring reactor over TWO listeners:
//!   - a `SO_REUSEPORT` listener on the main port (5701) — the kernel spreads
//!     plain/smart-client connections across cores;
//!   - a dedicated TPC port (TPC_BASE + core index) — a TPC-enabled client opens
//!     one connection per port and routes each partition to channel `p % N`,
//!     i.e. directly to the owning core.
//!
//! The auth response advertises the N TPC ports. The store is partitioned into N
//! independently-locked shards keyed by (map,key), so any core serves any key
//! correctly. N defaults to detected cores capped at 8; override BONSAI_CORES.

use server::handlers::Cfg;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

const TPC_BASE: i32 = 12000;

fn reuseport_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let sock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
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
    let tpc_ports: Vec<i32> = (0..cores as i32).map(|i| TPC_BASE + i).collect();
    let cfg = Arc::new(Cfg { tpc_ports: tpc_ports.clone() });
    let store = Arc::new(store::Store::with_shards(cores));
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    eprintln!(
        "BonsaiGrid listening on {addr} (thread-per-core, {cores} cores, io_uring); TPC ports {:?}",
        tpc_ports
    );

    let mut handles = Vec::new();
    for i in 0..cores {
        let store = store.clone();
        let cfg = cfg.clone();
        let main_listener = reuseport_listener(addr)?;
        let tpc_addr: SocketAddr = format!("127.0.0.1:{}", TPC_BASE + i as i32).parse().unwrap();
        let tpc_listener = reuseport_listener(tpc_addr)?; // reuse_addr/port: robust to TIME_WAIT
        let core_id = core_ids.get(i).copied();
        handles.push(std::thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let _ = server::reactor::run(vec![main_listener, tpc_listener], |req| {
                server::handlers::dispatch(req, &store, &cfg)
            });
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}
