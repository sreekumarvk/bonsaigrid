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
use std::collections::HashMap;
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
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Auth config: cluster name (default "dev") + optional username/password.
fn auth_cfg() -> (String, Option<String>, Option<String>) {
    (
        std::env::var("BONSAI_CLUSTER").unwrap_or_else(|_| "dev".into()),
        std::env::var("BONSAI_USER").ok(),
        std::env::var("BONSAI_PASS").ok(),
    )
}

/// Build the security context from `BONSAI_SECURITY_CONFIG` (a JSON file path);
/// falls back to the permissive open context when unset. A parse error is fatal
/// (we refuse to start with a broken security policy).
fn build_security() -> Arc<security::SecurityContext> {
    match std::env::var("BONSAI_SECURITY_CONFIG") {
        Ok(path) => {
            let json = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read BONSAI_SECURITY_CONFIG {path}: {e}"));
            let ctx = security::SecurityContext::from_json(&json)
                .unwrap_or_else(|e| panic!("invalid security config {path}: {e}"));
            eprintln!("BonsaiGrid security: RBAC enabled from {path}");
            Arc::new(ctx)
        }
        Err(_) => Arc::new(security::SecurityContext::open()),
    }
}

/// Build the client-protocol TLS acceptor from `BONSAI_TLS_MODE` + PEM paths.
/// `None` when TLS is disabled. Panics on a misconfigured cert/key (we refuse to
/// start advertising TLS we can't serve).
fn build_tls_acceptor() -> Option<security::tls::TlsAcceptor> {
    let mode = security::tls::TlsMode::parse(&std::env::var("BONSAI_TLS_MODE").unwrap_or_default());
    if !mode.tls_enabled() {
        return None;
    }
    let read = |var: &str| -> Vec<u8> {
        let path = std::env::var(var).unwrap_or_else(|_| panic!("{var} required when TLS enabled"));
        std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read {var} {path}: {e}"))
    };
    let config = security::tls::server_config(
        security::tls::load_certs(&read("BONSAI_TLS_CERT")).expect("invalid BONSAI_TLS_CERT"),
        security::tls::load_private_key(&read("BONSAI_TLS_KEY")).expect("invalid BONSAI_TLS_KEY"),
        None, // client-protocol TLS does not require client certs (that is member mTLS)
    )
    .expect("build TLS server config");
    eprintln!("BonsaiGrid TLS: client protocol mode={mode:?}");
    Some(security::tls::TlsAcceptor::new(mode, config))
}

/// Build the member-mesh mutual-TLS bundle from `BONSAI_TLS_MODE` + PEM paths
/// (cert, key, and the peer-verifying CA — all required when TLS is on). `None`
/// when TLS is disabled.
fn build_member_tls() -> Option<security::tls::MemberTls> {
    let mode = security::tls::TlsMode::parse(&std::env::var("BONSAI_TLS_MODE").unwrap_or_default());
    if !mode.tls_enabled() {
        return None;
    }
    let read = |var: &str| -> Vec<u8> {
        let path = std::env::var(var).unwrap_or_else(|_| panic!("{var} required when TLS enabled"));
        std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read {var} {path}: {e}"))
    };
    let tls = security::tls::MemberTls::new(
        mode,
        security::tls::load_certs(&read("BONSAI_TLS_CERT")).expect("invalid BONSAI_TLS_CERT"),
        security::tls::load_private_key(&read("BONSAI_TLS_KEY")).expect("invalid BONSAI_TLS_KEY"),
        security::tls::load_ca(&read("BONSAI_TLS_CA")).expect("invalid BONSAI_TLS_CA"),
    )
    .expect("build member mTLS");
    eprintln!("BonsaiGrid TLS: member mesh mTLS mode={mode:?}");
    Some(tls)
}

/// Set up local durability (Hot Restart) for `store` if `BONSAI_PERSISTENCE` is
/// enabled: recover from `BONSAI_PERSISTENCE_DIR` BEFORE serving, then attach the
/// WAL sink and spawn the persistence thread. Returns the thread handle (kept
/// alive by the caller). No-op / `None` when disabled.
fn setup_persistence(store: &Arc<store::Store>) -> Option<std::thread::JoinHandle<()>> {
    let durability =
        persistence::Durability::parse(&std::env::var("BONSAI_PERSISTENCE").unwrap_or_default());
    if !durability.enabled() {
        return None;
    }
    let dir: std::path::PathBuf = std::env::var("BONSAI_PERSISTENCE_DIR")
        .expect("BONSAI_PERSISTENCE_DIR required when persistence is enabled")
        .into();
    // Recover the in-memory state before any writes or listeners.
    persistence::recover(&dir, store).expect("recovery failed");
    let flush_ms = env_usize("BONSAI_PERSISTENCE_FLUSH_MS", 10) as u64;
    let snapshot_bytes = env_usize("BONSAI_PERSISTENCE_SNAPSHOT_MB", 64) as u64 * 1024 * 1024;
    let (tx, rx) = spsc::channel::<server::persist_thread::PersistJob>(1 << 20);
    store.set_wal_sink(Arc::new(server::persist_thread::Persister::new(tx)));
    eprintln!(
        "BonsaiGrid persistence: {durability:?} at {}",
        dir.display()
    );
    Some(server::persist_thread::spawn_persistence(
        dir,
        store.clone(),
        rx,
        flush_ms,
        snapshot_bytes,
    ))
}

/// If WAN is configured (`BONSAI_WAN_TARGETS`), attach the WAN capture sink and
/// spawn the WAN thread (inbound listener + outbound ship). Returns the thread
/// handle (kept alive by the caller). No-op / `None` when unset.
fn setup_wan(store: &Arc<store::Store>) -> Option<std::thread::JoinHandle<()>> {
    let cfg = server::wan_thread::wan_config()?;
    let dir: std::path::PathBuf = std::env::var("BONSAI_WAN_DIR")
        .unwrap_or_else(|_| "./wan-data".into())
        .into();
    let (tx, rx) = spsc::channel::<wan::WanRecord>(1 << 20);
    store.set_wan_sink(Arc::new(wan::WanPublisher::new(tx)));
    eprintln!(
        "BonsaiGrid WAN: listen :{} -> targets {:?} (batch {}, queue {} MB, {:?})",
        cfg.listen,
        cfg.targets,
        cfg.batch,
        cfg.queue_bytes / (1024 * 1024),
        cfg.backpressure
    );
    Some(server::wan_thread::spawn_wan(dir, store.clone(), rx, cfg))
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
    // A joiner (index beyond the bootstrap set) asks the master to admit it at
    // runtime; `total` slots cover the bootstrap members plus this joiner.
    let joining = self_index >= members;
    let total = members.max(self_index + 1);
    let (cluster_name, username, password) = auth_cfg();
    let cfg = Arc::new(Cfg {
        members: cluster_members(total),
        self_index,
        tpc_ports: Vec::new(), // single core per member in this mode
        cluster_name,
        username,
        password,
        security: build_security(),
    });
    // Synchronous backup count K (default 1, capped at N-1).
    let backups = env_usize("BONSAI_BACKUPS", 1).min(total.saturating_sub(1));
    // Quorum defaults to a strict majority so a partitioned minority cannot keep
    // accepting writes (split-brain protection on by default); overridable.
    let quorum = env_usize("BONSAI_QUORUM", server::membership::default_quorum(total));
    let cluster = Rc::new(RefCell::new(Cluster::new(
        bootstrap_members(total),
        backups,
        quorum,
    )));
    let self_uuid = cfg.members[self_index].uuid;
    let join_as = if joining {
        Some(cluster.borrow().members[self_index].clone())
    } else {
        None
    };
    let store = Arc::new(store::Store::with_shards_seed(1, self_index as u64));
    let _persist = setup_persistence(&store); // recover + attach WAL sink before serving
    let _wan = setup_wan(&store); // attach WAN capture + spawn the WAN thread if configured
    server::jobs::set_store(store.clone()); // streaming SQL jobs look up the IMap here
    let broker = Arc::new(server::events::EventBroker::new(self_uuid));
    let schemas = Arc::new(serialization::schema::SchemaService::new());
    let listener = reuseport_listener(format!("127.0.0.1:{port}").parse().unwrap())?;
    eprintln!(
        "BonsaiGrid member {self_index} (bootstrap {members}) listening on 127.0.0.1:{port} (single core, K={backups}, quorum={quorum}, joining={joining})"
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
    let merge_latest = server::migration::MergePolicy::parse(
        &std::env::var("BONSAI_MERGE").unwrap_or_else(|_| "LatestUpdate".into()),
    )
    .latest_update();
    let member_ports: Vec<i32> = (0..total).map(|i| MEMBER_BASE + i as i32).collect();
    let (job_tx, job_rx) = spsc::channel::<server::member_thread::MemberJob>(8192);
    let (ev_tx, ev_rx) = spsc::channel::<server::member_thread::ClusterEvent>(1024);
    server::member_thread::spawn(
        self_index,
        member_ports,
        cluster.borrow().clone(),
        self_uuid,
        hb_interval,
        hb_timeout,
        merge_latest,
        join_as,
        store.clone(),
        broker.clone(),
        job_rx,
        ev_tx,
        build_member_tls(),
        std::env::var("BONSAI_CP").is_ok_and(|v| v != "0" && !v.is_empty()),
    );
    // The reactor keeps a Replicator when there are IMap backups OR when CP is
    // enabled (CP submits ride the same member-thread job channel; a 0-backup
    // Replicator still forwards CpSubmit but never defers IMap writes).
    let cp_enabled = std::env::var("BONSAI_CP").is_ok_and(|v| v != "0" && !v.is_empty());
    let replicator = Rc::new(if backups > 0 || cp_enabled {
        Some(server::member_thread::Replicator::new(job_tx, backups))
    } else {
        None
    });

    let n = members;
    let (eb, cb) = (broker.clone(), broker.clone());
    let metrics = Arc::new(server::metrics::Metrics::new());
    let executor = server::executor::ExecutorService::new();
    let txn_service = server::txn::TransactionService::new();
    let jet_service = jet::executor::JetService::new();
    let (md, mh) = (metrics.clone(), metrics.clone());
    let cl_d = cluster.clone();
    let rep_d = replicator.clone();
    let exec_d = executor.clone();
    let txn_d = txn_service.clone();
    let jet_d = jet_service.clone();
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
    // Per-connection authenticated principal (this core only — no cross-thread
    // sharing). Defaults to the security context's anonymous principal until a
    // ClientAuthentication rebinds it; cleaned up when the connection drops.
    let conns: Rc<RefCell<HashMap<u64, Arc<security::Principal>>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let conns_drop = conns.clone();
    let anon = cfg.security.anonymous();
    let mc_store = store.clone();
    let resp_store = store.clone();
    let mc_cas = Arc::new(std::sync::atomic::AtomicU64::new(0));
    server::reactor::run(
        vec![listener],
        move |msg, conn_id, peer_cert, out| {
            md.inc_request();
            let cluster = cl_d.borrow();
            let mut principal = conns
                .borrow()
                .get(&conn_id)
                .cloned()
                .unwrap_or_else(|| anon.clone());
            server::handlers::dispatch_bytes(
                msg,
                conn_id,
                &store,
                &cfg,
                &broker,
                &schemas,
                &cluster,
                rep_d.as_ref().as_ref(),
                &exec_d,
                &txn_d,
                &jet_d,
                &mut principal,
                peer_cert,
                out,
            );
            conns.borrow_mut().insert(conn_id, principal);
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
        move |cmd, out| {
            let now_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let (reply, close) = server::memcache::execute(
                &mc_store,
                &server::memcache::parse(cmd),
                now_unix,
                &mc_cas,
                env!("CARGO_PKG_VERSION"),
            );
            out.extend_from_slice(&reply);
            close
        },
        move |cmd, out| {
            let (reply, close) = match server::resp::parse(cmd) {
                Some(args) => server::resp::execute(&resp_store, &args),
                None => (b"-ERR protocol error\r\n".to_vec(), false),
            };
            out.extend_from_slice(&reply);
            close
        },
        move |conn_id, out| {
            for ev in eb.drain(conn_id) {
                out.extend_from_slice(&ev);
            }
        },
        move |conn_id| {
            conns_drop.borrow_mut().remove(&conn_id);
            cb.drop_conn(conn_id)
        },
        on_cluster,
        build_tls_acceptor(),
    )
}

/// Build the (correlation-stamped) members + partitions view event messages to
/// push to a cluster-view listener on a membership change.
fn cluster_view_push(cluster: &Cluster, corr: i64) -> Vec<Vec<u8>> {
    use protocol::frame::write_message;
    use protocol::message::set_correlation_id;
    let mut mv = codecs::cluster_view::members_view_event(
        cluster.member_list_version,
        &cluster.member_tuples(),
    );
    set_correlation_id(&mut mv, corr);
    let mut pv = codecs::cluster_view::partitions_view_event(
        cluster.partition_list_version,
        &cluster.partition_table(),
    );
    set_correlation_id(&mut pv, corr);
    vec![write_message(&mv), write_message(&pv)]
}

/// Parse `/cluster/promote?dead=<index>` (the manual failover trigger; Phase D's
/// detector will call the same `Cluster::promote`).
fn parse_promote(path: &str) -> Option<usize> {
    let rest = path.strip_prefix("/cluster/promote")?;
    let q = rest
        .strip_prefix("?dead=")
        .or_else(|| rest.strip_prefix("/?dead="))?;
    q.parse().ok()
}

/// HTTP routing on the main port: health endpoints + a Prometheus `/metrics`.
fn http_route(
    path: &str,
    cluster_size: usize,
    metrics: &server::metrics::Metrics,
) -> (u16, &'static str, String) {
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
        members: vec![Member {
            uuid: (1, 1),
            host: "127.0.0.1".into(),
            port: BASE_PORT,
        }],
        self_index: 0,
        tpc_ports: tpc_ports.clone(),
        cluster_name,
        username,
        password,
        security: build_security(),
    });
    let store = Arc::new(store::Store::with_shards(cores));
    let _persist = setup_persistence(&store); // recover + attach WAL sink before serving
    let _wan = setup_wan(&store); // attach WAN capture + spawn the WAN thread if configured
    server::jobs::set_store(store.clone()); // streaming SQL jobs look up the IMap here
                                            // Single-member cluster, no backups; shared read-only across cores.
    let cluster = Arc::new(Cluster::new(bootstrap_members(1), 0, 1));
    let broker = Arc::new(server::events::EventBroker::new(cfg.members[0].uuid));
    let metrics = Arc::new(server::metrics::Metrics::new());
    let schemas = Arc::new(serialization::schema::SchemaService::new());
    let executor = server::executor::ExecutorService::new();
    let txn_service = server::txn::TransactionService::new();
    let jet_service = jet::executor::JetService::new();
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    eprintln!(
        "BonsaiGrid listening on {addr} (thread-per-core, {cores} cores, io_uring); TPC ports {:?}",
        tpc_ports
    );

    let mut handles = Vec::new();
    let tls_acceptor = build_tls_acceptor();
    // Shared memcached CAS counter — one global unique across all cores.
    let mc_cas = Arc::new(std::sync::atomic::AtomicU64::new(0));
    for i in 0..cores {
        let store = store.clone();
        let cfg = cfg.clone();
        let broker = broker.clone();
        let metrics = metrics.clone();
        let schemas = schemas.clone();
        let executor = executor.clone();
        let txn_service = txn_service.clone();
        let jet_service = jet_service.clone();
        let cluster = cluster.clone();
        let tls_acceptor = tls_acceptor.clone();
        let mc_cas = mc_cas.clone();
        let main_listener = reuseport_listener(addr)?;
        let tpc_addr: SocketAddr = format!("127.0.0.1:{}", TPC_BASE + i as i32)
            .parse()
            .unwrap();
        let tpc_listener = reuseport_listener(tpc_addr)?;
        let core_id = core_ids.get(i).copied();
        handles.push(std::thread::spawn(move || {
            if let Some(id) = core_id {
                core_affinity::set_for_current(id);
            }
            let (eb, cb) = (broker.clone(), broker.clone());
            let (md, mh) = (metrics.clone(), metrics.clone());
            let exec_d = executor.clone();
            let txn_d = txn_service.clone();
            let jet_d = jet_service.clone();
            let conns: Rc<RefCell<HashMap<u64, Arc<security::Principal>>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let conns_drop = conns.clone();
            let anon = cfg.security.anonymous();
            let mc_store = store.clone();
            let resp_store = store.clone();
            let _ = server::reactor::run(
                vec![main_listener, tpc_listener],
                move |msg, conn_id, peer_cert, out| {
                    md.inc_request();
                    let mut principal = conns
                        .borrow()
                        .get(&conn_id)
                        .cloned()
                        .unwrap_or_else(|| anon.clone());
                    server::handlers::dispatch_bytes(
                        msg,
                        conn_id,
                        &store,
                        &cfg,
                        &broker,
                        &schemas,
                        &cluster,
                        None,
                        &exec_d,
                        &txn_d,
                        &jet_d,
                        &mut principal,
                        peer_cert,
                        out,
                    );
                    conns.borrow_mut().insert(conn_id, principal);
                },
                move |path| http_route(path, 1, &mh),
                move |cmd, out| {
                    let now_unix = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let (reply, close) = server::memcache::execute(
                        &mc_store,
                        &server::memcache::parse(cmd),
                        now_unix,
                        &mc_cas,
                        env!("CARGO_PKG_VERSION"),
                    );
                    out.extend_from_slice(&reply);
                    close
                },
                move |cmd, out| {
                    let (reply, close) = match server::resp::parse(cmd) {
                        Some(args) => server::resp::execute(&resp_store, &args),
                        None => (b"-ERR protocol error\r\n".to_vec(), false),
                    };
                    out.extend_from_slice(&reply);
                    close
                },
                move |conn_id, out| {
                    for ev in eb.drain(conn_id) {
                        out.extend_from_slice(&ev);
                    }
                },
                move |conn_id| {
                    conns_drop.borrow_mut().remove(&conn_id);
                    cb.drop_conn(conn_id)
                },
                || {}, // single node: no membership changes
                tls_acceptor,
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
