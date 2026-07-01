use jni::objects::JClass;
use jni::JNIEnv;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::thread;

use server::handlers::{Cfg, Member};
use server::membership::{Cluster, MemberInfo};

const TPC_BASE: i32 = 12000;
const BASE_PORT: i32 = 5701;
const MEMBER_BASE: i32 = 7701;

fn reuseport_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let sock = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
    sock.listen(1024)?;
    Ok(sock.into())
}

#[no_mangle]
pub extern "system" fn Java_com_bonsaigrid_BonsaiGrid_startServer(_env: JNIEnv, _class: JClass) {
    thread::spawn(move || {
        let cores = 1; // Simplify for embedded mode
        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let tpc_ports: Vec<i32> = (0..cores as i32).map(|i| TPC_BASE + i).collect();

        let cfg = Arc::new(Cfg {
            members: vec![Member {
                uuid: (1, 1),
                host: "127.0.0.1".into(),
                port: BASE_PORT,
            }],
            self_index: 0,
            tpc_ports: tpc_ports.clone(),
            cluster_name: "dev".into(),
            username: None,
            password: None,
        });

        let store = Arc::new(store::Store::with_shards(cores));
        server::jobs::set_store(store.clone());
        let cluster = Arc::new(Cluster::new(
            vec![MemberInfo::new(
                (1, 1),
                "127.0.0.1".into(),
                BASE_PORT,
                MEMBER_BASE,
                0,
            )],
            0,
            1,
        ));
        let broker = Arc::new(server::events::EventBroker::new(cfg.members[0].uuid));
        let metrics = Arc::new(server::metrics::Metrics::new());
        let schemas = Arc::new(serialization::schema::SchemaService::new());
        let executor = server::executor::ExecutorService::new();
        let txn_service = server::txn::TransactionService::new();
        let jet_service = jet::executor::JetService::new();

        eprintln!("BonsaiGrid Embedded Server starting on {}", addr);

        let main_listener = reuseport_listener(addr).expect("Failed to bind main port");
        let tpc_addr: SocketAddr = format!("127.0.0.1:{}", TPC_BASE).parse().unwrap();
        let tpc_listener = reuseport_listener(tpc_addr).expect("Failed to bind TPC port");

        let (eb, cb) = (broker.clone(), broker.clone());
        let (md, _mh) = (metrics.clone(), metrics.clone());

        let _ = server::reactor::run(
            vec![main_listener, tpc_listener],
            move |msg, conn_id, out| {
                md.inc_request();
                server::handlers::dispatch_bytes(
                    msg,
                    conn_id,
                    &store,
                    &cfg,
                    &broker,
                    &schemas,
                    &cluster,
                    None,
                    &executor,
                    &txn_service,
                    &jet_service,
                    out,
                )
            },
            move |_path| (404, "Not Found", "Not Found".to_string()), // mock http route for now
            move |conn_id, out| {
                for ev in eb.drain(conn_id) {
                    out.extend_from_slice(&ev);
                }
            },
            move |conn_id| cb.drop_conn(conn_id),
            || {},
        );
    });
}
