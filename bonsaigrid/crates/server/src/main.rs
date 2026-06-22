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
use server::membership::{Cluster, MemberInfo};
use socket2::{Domain, Protocol, Socket, Type};
use std::cell::RefCell;
use std::net::{SocketAddr, TcpListener};
use std::rc::Rc;
use std::sync::Arc;

const TPC_BASE: i32 = 12000;
const BASE_PORT: i32 = 5701;
/// Internal member-to-member port base (member i listens on MEMBER_BASE + i).
const MEMBER_BASE: i32 = 7701;

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

/// Auth config: cluster name (default "dev") + optional username/password.
fn auth_cfg() -> (String, Option<String>, Option<String>) {
    (
        std::env::var("BONSAI_CLUSTER").unwrap_or_else(|_| "dev".into()),
        std::env::var("BONSAI_USER").ok(),
        std::env::var("BONSAI_PASS").ok(),
    )
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

/// Bootstrap members as `MemberInfo` (join_id = bootstrap index; member port =
/// MEMBER_BASE + index).
fn bootstrap_members(n: usize) -> Vec<server::membership::MemberInfo> {
    (0..n)
        .map(|i| {
            server::membership::MemberInfo::new(
                (1, (i + 1) as i64),
                "127.0.0.1".into(),
                BASE_PORT + i as i32,
                MEMBER_BASE + i as i32,
                i as u64,
            )
        })
        .collect()
}

fn run_multi_node(members: usize, self_index: usize) -> std::io::Result<()> {
    let port = BASE_PORT + self_index as i32;
    let (cluster_name, username, password) = auth_cfg();
    let cfg = Arc::new(Cfg {
        members: cluster_members(members),
        self_index,
        tpc_ports: Vec::new(), // single core per member in this mode
        cluster_name,
        username,
        password,
    });
    // Synchronous backup count K (default 1, capped at N-1).
    let backups = env_usize("BONSAI_BACKUPS", 1).min(members.saturating_sub(1));
    let quorum = env_usize("BONSAI_QUORUM", 1);
    let cluster = Rc::new(RefCell::new(Cluster::new(bootstrap_members(members), backups, quorum)));
    let store = Arc::new(store::Store::with_shards(1));
    let broker = Arc::new(server::events::EventBroker::new(cfg.members[cfg.self_index].uuid));
    let schemas = Arc::new(serialization::schema::SchemaService::new());
    let listener = reuseport_listener(format!("127.0.0.1:{port}").parse().unwrap())?;
    eprintln!(
        "BonsaiGrid member {self_index}/{members} listening on 127.0.0.1:{port} (single core, K={backups} backups)"
    );
    if let Some(id) = core_affinity::get_core_ids().and_then(|v| v.get(self_index % 64).copied()) {
        core_affinity::set_for_current(id);
    }

    // The member thread runs whenever this is a real cluster (members > 1): it
    // drives heartbeats / failure detection / migration and, when backups > 0,
    // synchronous replication. The reactor talks to it over a forward ring
    // (MemberJob) and learns membership changes over a reverse ring (ClusterEvent).
    let hb_interval = env_usize("BONSAI_HB_INTERVAL_MS", 500) as u64;
    let hb_timeout = env_usize("BONSAI_HB_TIMEOUT_MS", 3000) as u64;
    let member_ports: Vec<i32> = (0..members).map(|i| MEMBER_BASE + i as i32).collect();
    let (job_tx, job_rx) = spsc::channel::<server::member_thread::MemberJob>(8192);
    let (ev_tx, ev_rx) = spsc::channel::<server::member_thread::ClusterEvent>(1024);
    server::member_thread::spawn(
        self_index,
        member_ports,
        cluster.borrow().clone(),
        self_index as u64,
        hb_interval,
        hb_timeout,
        store.clone(),
        broker.clone(),
        job_rx,
        ev_tx,
    );
    let replicator =
        Rc::new(if backups > 0 { Some(server::member_thread::Replicator::new(job_tx, backups)) } else { None });

    let n = members;
    let (eb, cb) = (broker.clone(), broker.clone());
    let metrics = Arc::new(server::metrics::Metrics::new());
    let (md, mh) = (metrics.clone(), metrics.clone());
    let cl_d = cluster.clone();
    let rep_d = replicator.clone();
    let cl_h = cluster.clone();
    let rep_h = replicator.clone();
    // on_cluster: drain the reverse ring, apply to the authoritative Cluster, and
    // push members/partitions view events to every registered cluster-view client.
    let cl_e = cluster.clone();
    let eb_e = broker.clone();
    let on_cluster = move || {
        let mut applied = false;
        while let Some(ev) = ev_rx.pop() {
            let alive: Vec<bool> = ev.members.iter().map(|m| m.alive).collect();
            let infos: Vec<MemberInfo> = ev
                .members
                .into_iter()
                .map(|m| MemberInfo::new(m.uuid, m.host, m.client_port, m.member_port, m.join_id))
                .collect();
            if cl_e.borrow_mut().set_view(ev.generation, infos, alive) {
                applied = true;
            }
        }
        if applied {
            let c = cl_e.borrow();
            for (conn, corr) in eb_e.cluster_view_listeners() {
                for bytes in cluster_view_push(&c, corr) {
                    eb_e.enqueue(conn, bytes);
                }
            }
        }
    };
    server::reactor::run(
        vec![listener],
        move |msg, conn_id, out| {
            md.inc_request();
            let cluster = cl_d.borrow();
            server::handlers::dispatch_bytes(msg, conn_id, &store, &cfg, &broker, &schemas, &cluster, rep_d.as_ref().as_ref(), out)
        },
        move |path| {
            if let Some(dead) = parse_promote(path) {
                cl_h.borrow_mut().promote(dead as u64);
                if let Some(r) = rep_h.as_ref() {
                    r.send_membership(cl_h.borrow().clone());
                }
                let plist = cl_h.borrow().partition_list_version;
                return (
                    200,
                    "application/json",
                    format!("{{\"promoted\":{dead},\"partitionListVersion\":{plist}}}"),
                );
            }
            http_route(path, n, &mh)
        },
        move |conn_id, out| {
            for ev in eb.drain(conn_id) {
                out.extend_from_slice(&ev);
            }
        },
        move |conn_id| cb.drop_conn(conn_id),
        on_cluster,
    )
}

/// Build the (correlation-stamped) members + partitions view event messages to
/// push to a cluster-view listener on a membership change.
fn cluster_view_push(cluster: &Cluster, corr: i64) -> Vec<Vec<u8>> {
    use protocol::frame::write_message;
    use protocol::message::set_correlation_id;
    let mut mv = codecs::cluster_view::members_view_event(cluster.member_list_version, &cluster.member_tuples());
    set_correlation_id(&mut mv, corr);
    let mut pv =
        codecs::cluster_view::partitions_view_event(cluster.partition_list_version, &cluster.partition_table());
    set_correlation_id(&mut pv, corr);
    vec![write_message(&mv), write_message(&pv)]
}

/// Parse `/cluster/promote?dead=<index>` (the manual failover trigger; Phase D's
/// detector will call the same `Cluster::promote`).
fn parse_promote(path: &str) -> Option<usize> {
    let rest = path.strip_prefix("/cluster/promote")?;
    let q = rest.strip_prefix("?dead=").or_else(|| rest.strip_prefix("/?dead="))?;
    q.parse().ok()
}

/// HTTP routing on the main port: health endpoints + a Prometheus `/metrics`.
fn http_route(path: &str, cluster_size: usize, metrics: &server::metrics::Metrics) -> (u16, &'static str, String) {
    if path == "/metrics" {
        (200, "text/plain", metrics.prometheus(cluster_size))
    } else {
        server::handlers::http_health(path, cluster_size)
    }
}

fn run_single_node() -> std::io::Result<()> {
    let detected = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1);
    let cores = detected.min(env_usize("BONSAI_CORES", 8)).max(1);

    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
    let tpc_ports: Vec<i32> = (0..cores as i32).map(|i| TPC_BASE + i).collect();
    let (cluster_name, username, password) = auth_cfg();
    let cfg = Arc::new(Cfg {
        members: vec![Member { uuid: (1, 1), host: "127.0.0.1".into(), port: BASE_PORT }],
        self_index: 0,
        tpc_ports: tpc_ports.clone(),
        cluster_name,
        username,
        password,
    });
    let store = Arc::new(store::Store::with_shards(cores));
    // Single-member cluster, no backups; shared read-only across cores.
    let cluster = Arc::new(Cluster::new(bootstrap_members(1), 0, 1));
    // One broker + metrics registry shared across this member's cores.
    let broker = Arc::new(server::events::EventBroker::new(cfg.members[0].uuid));
    let metrics = Arc::new(server::metrics::Metrics::new());
    let schemas = Arc::new(serialization::schema::SchemaService::new());
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    eprintln!(
        "BonsaiGrid listening on {addr} (thread-per-core, {cores} cores, io_uring); TPC ports {:?}",
        tpc_ports
    );

    let mut handles = Vec::new();
    for i in 0..cores {
        let store = store.clone();
        let cfg = cfg.clone();
        let broker = broker.clone();
        let metrics = metrics.clone();
        let schemas = schemas.clone();
        let cluster = cluster.clone();
        let main_listener = reuseport_listener(addr)?;
        let tpc_addr: SocketAddr = format!("127.0.0.1:{}", TPC_BASE + i as i32).parse().unwrap();
        let tpc_listener = reuseport_listener(tpc_addr)?;
        let core_id = core_ids.get(i).copied();
        handles.push(std::thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let (eb, cb) = (broker.clone(), broker.clone());
            let (md, mh) = (metrics.clone(), metrics.clone());
            let _ = server::reactor::run(
                vec![main_listener, tpc_listener],
                move |msg, conn_id, out| {
                    md.inc_request();
                    server::handlers::dispatch_bytes(msg, conn_id, &store, &cfg, &broker, &schemas, &cluster, None, out)
                },
                move |path| http_route(path, 1, &mh),
                move |conn_id, out| {
                    for ev in eb.drain(conn_id) {
                        out.extend_from_slice(&ev);
                    }
                },
                move |conn_id| cb.drop_conn(conn_id),
                || {}, // single node: no membership changes
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
