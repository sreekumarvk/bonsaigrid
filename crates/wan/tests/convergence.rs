//! Deterministic two-cluster WAN sim: each cluster is a Store + a capture ring +
//! a durable outbound queue; a controllable in-memory link ships batches and
//! acks. Proves one-way replication, active-active convergence, loop prevention,
//! and outage-then-replay — with no real network.

use std::path::PathBuf;
use store::Store;
use wan::{apply_batch, WanPublisher, WanQueue, WanRecord};

fn tmp(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-wan-conv-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// One cluster: store + capture ring consumer + durable queue.
struct Cluster {
    store: std::sync::Arc<Store>,
    rx: spsc::Consumer<WanRecord>,
    queue: WanQueue,
}

impl Cluster {
    fn new(dir: PathBuf) -> Cluster {
        let store = std::sync::Arc::new(Store::new());
        let (tx, rx) = spsc::channel::<WanRecord>(4096);
        store.set_wan_sink(std::sync::Arc::new(WanPublisher::new(tx)));
        Cluster {
            store,
            rx,
            queue: WanQueue::open(&dir).unwrap(),
        }
    }
    /// Drain captured records into the durable outbound queue.
    fn pump(&mut self) {
        while let Some(r) = self.rx.pop() {
            self.queue.append(&r).unwrap();
        }
    }
}

/// Ship a's unacked records to b, apply them, and ack a (if `link_up`).
fn ship(a: &mut Cluster, b: &Cluster, link_up: bool) {
    a.pump();
    if !link_up {
        return;
    }
    let un = a.queue.unacked();
    if un.is_empty() {
        return;
    }
    let recs: Vec<WanRecord> = un.iter().map(|(_, r)| r.clone()).collect();
    let up_to = un.last().unwrap().0;
    apply_batch(&b.store, &recs);
    a.queue.ack(up_to).unwrap();
}

#[test]
fn one_way_replication() {
    let mut a = Cluster::new(tmp("a1"));
    let b = Cluster::new(tmp("b1"));
    a.store.put("m", b"k".to_vec(), b"v".to_vec());
    ship(&mut a, &b, true);
    assert_eq!(b.store.get("m", b"k"), Some(b"v".to_vec()));
}

#[test]
fn active_active_converges_and_does_not_loop() {
    let mut a = Cluster::new(tmp("a2"));
    let mut b = Cluster::new(tmp("b2"));
    // Concurrent writes to the same key; the later HLC stamp wins on both sides.
    a.store.put("m", b"k".to_vec(), b"A".to_vec());
    for _ in 0..4 {
        b.store.next_stamp(); // make b's write strictly later in HLC time
    }
    b.store.put("m", b"k".to_vec(), b"B".to_vec());
    ship(&mut a, &b, true);
    ship(&mut b, &a, true);
    let (va, vb) = (a.store.get("m", b"k"), b.store.get("m", b"k"));
    assert_eq!(va, vb, "both clusters converge to the same value");
    // Loop prevention: applying b's record on a did NOT enqueue anything new on a
    // beyond a's own (already-acked) write.
    a.pump();
    assert!(
        a.queue.unacked().is_empty(),
        "WAN-applied write was not re-captured"
    );
}

#[test]
fn outage_then_replay() {
    let mut a = Cluster::new(tmp("a3"));
    let b = Cluster::new(tmp("b3"));
    // Link down: writes accumulate durably; nothing reaches b.
    for i in 0..5 {
        a.store
            .put("m", format!("k{i}").into_bytes(), b"v".to_vec());
    }
    ship(&mut a, &b, false);
    assert_eq!(b.store.get("m", b"k0"), None);
    assert_eq!(a.queue.len(), 5, "buffered durably");
    // Link restored: all buffered writes replay and b converges.
    ship(&mut a, &b, true);
    for i in 0..5 {
        assert_eq!(
            b.store.get("m", format!("k{i}").as_bytes()),
            Some(b"v".to_vec())
        );
    }
    assert_eq!(a.queue.acked(), 5);
}

#[test]
fn replicates_non_map_structures() {
    // Phase D: a Queue offer on A is captured (aux state) and replayed on B.
    let mut a = Cluster::new(tmp("a4"));
    let b = Cluster::new(tmp("b4"));
    a.store.queue_offer("q", b"one".to_vec());
    a.store.queue_offer("q", b"two".to_vec());
    ship(&mut a, &b, true);
    assert_eq!(
        b.store.queue_size("q"),
        2,
        "queue state replicated over WAN"
    );
    // Applying an aux state via install_aux did not re-capture on B (no loop).
    let mut b = b;
    b.pump();
    assert!(
        b.queue.unacked().is_empty(),
        "aux apply was not re-published"
    );
}
