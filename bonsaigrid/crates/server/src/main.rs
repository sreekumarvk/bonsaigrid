//! BonsaiGrid server binary.
//!
//! Two modes, chosen by `BONSAI_MEMBERS`:
//!   - **Single node (default, N=1):** thread-per-core. N pinned io_uring
//!     reactors over a `SO_REUSEPORT` main port + per-core TPC ports.
//!   - **Multi-node (`BONSAI_MEMBERS=K`, K>1):** this process is one member of a
//!     static K-member cluster. It binds the main port `5701 + BONSAI_MEMBER_INDEX`,
//!     advertises the full membership + deterministic partition table, and serves
//!     its own partitions (single core, no TPC). Launch K processes, one per
//!     index, to form the cluster; a stock smart client routes keys to owners.

use server::handlers::{Cfg, Member};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

const TPC_BASE: i32 = 12000;
const BASE_PORT: i32 = 5701;

fn reuseport_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let sock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn cluster_members(n: usize) -> Vec<Member> {
    (0..n)
        .map(|i| Member {
            uuid: (1, (i + 1) as i64),
            host: "127.0.0.1".into(),
            port: BASE_PORT + i as i32,
        })
        .collect()
}

fn run_multi_node(members: usize, self_index: usize) -> std::io::Result<()> {
    let port = BASE_PORT + self_index as i32;
    let cfg = Arc::new(Cfg {
        members: cluster_members(members),
        self_index,
        tpc_ports: Vec::new(), // single core per member in this mode
    });
    let store = Arc::new(store::Store::with_shards(1));
    let listener = reuseport_listener(format!("127.0.0.1:{port}").parse().unwrap())?;
    eprintln!(
        "BonsaiGrid member {self_index}/{members} listening on 127.0.0.1:{port} (single core)"
    );
    if let Some(id) = core_affinity::get_core_ids().and_then(|v| v.get(self_index % 64).copied()) {
        core_affinity::set_for_current(id);
    }
    let n = members;
    server::reactor::run(
        vec![listener],
        move |msg, out| server::handlers::dispatch_bytes(msg, &store, &cfg, out),
        move |path| server::handlers::http_health(path, n),
    )
}

fn run_single_node() -> std::io::Result<()> {
    let detected = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1);
    let cores = detected.min(env_usize("BONSAI_CORES", 8)).max(1);

    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
    let tpc_ports: Vec<i32> = (0..cores as i32).map(|i| TPC_BASE + i).collect();
    let cfg = Arc::new(Cfg {
        members: vec![Member { uuid: (1, 1), host: "127.0.0.1".into(), port: BASE_PORT }],
        self_index: 0,
        tpc_ports: tpc_ports.clone(),
    });
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
        let tpc_listener = reuseport_listener(tpc_addr)?;
        let core_id = core_ids.get(i).copied();
        handles.push(std::thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let _ = server::reactor::run(
                vec![main_listener, tpc_listener],
                move |msg, out| server::handlers::dispatch_bytes(msg, &store, &cfg, out),
                |path| server::handlers::http_health(path, 1),
            );
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    let members = env_usize("BONSAI_MEMBERS", 1);
    if members > 1 {
        let self_index = env_usize("BONSAI_MEMBER_INDEX", 0);
        run_multi_node(members, self_index)
    } else {
        run_single_node()
    }
}
