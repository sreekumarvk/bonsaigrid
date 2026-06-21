//! Two real member threads (own io_uring transports, separate stores) replicate
//! a write end-to-end: the backup applies it, and the deferred client response is
//! delivered to the broker only after the backup acks.

use member::wire::Msg;
use server::events::EventBroker;
use server::handlers::Member;
use server::member_thread::{spawn, MemberJob, Replicator};
use server::membership::Cluster;
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;

const PORTS: [i32; 2] = [17811, 17812];

fn member(i: i32, port: i32) -> Member {
    Member { uuid: (1, i as i64 + 1), host: "127.0.0.1".into(), port }
}

#[test]
fn sync_backup_applies_and_delivers_deferred_response() {
    let cluster = Cluster::new(vec![member(0, 5901), member(1, 5902)], 1);
    let ports: Vec<i32> = PORTS.to_vec();

    // Backup (member 1): its own store; we assert the replicated value lands here.
    let store1 = Arc::new(Store::new());
    let broker1 = Arc::new(EventBroker::new((1, 2)));
    let (_tx1, rx1) = spsc::channel::<MemberJob>(64);
    spawn(1, ports.clone(), cluster.clone(), store1.clone(), broker1, rx1);

    // Primary (member 0): the deferred response is enqueued on broker0.
    let store0 = Arc::new(Store::new());
    let broker0 = Arc::new(EventBroker::new((1, 1)));
    let (tx0, rx0) = spsc::channel::<MemberJob>(64);
    spawn(0, ports.clone(), cluster.clone(), store0.clone(), broker0.clone(), rx0);

    // Let both listeners come up.
    std::thread::sleep(Duration::from_millis(300));

    let replicator = Replicator::new(tx0, 1);
    let conn_id = 42u64;
    let response = b"the-deferred-response".to_vec();
    // partition 0 -> owner 0, backup 1 (ring-wise).
    let deferred = replicator.replicate(0, conn_id, response.clone(), |op| Msg::BackupPut {
        op_id: op,
        name: "people".into(),
        key: b"alice".to_vec(),
        value: b"35".to_vec(),
        ttl_ms: 0,
    });
    assert!(deferred, "write with a live backup must defer");

    // Within a few seconds: backup store has the value, and the deferred response
    // has been delivered to the primary's broker for conn 42.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut backup_ok = false;
    let mut delivered: Vec<Vec<u8>> = Vec::new();
    while Instant::now() < deadline {
        if store1.get("people", b"alice") == Some(b"35".to_vec()) {
            backup_ok = true;
        }
        let drained = broker0.drain(conn_id);
        if !drained.is_empty() {
            delivered = drained;
        }
        if backup_ok && !delivered.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(backup_ok, "backup member did not apply the replicated value");
    assert_eq!(delivered, vec![response], "deferred response not delivered after ack");
}
