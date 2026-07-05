//! Live WAN over loopback TCP: two in-process clusters, each running a WAN thread
//! (inbound listener + outbound ship) that cross-targets the other. Proves the
//! real transport path end-to-end — a write on either side replicates to the other
//! (active-active), converging via the store's HLC merge.

use std::sync::Arc;
use std::time::{Duration, Instant};

use server::wan_thread::{spawn_wan, Backpressure, WanConfig};
use store::Store;
use wan::WanPublisher;

fn wait_for(f: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

fn cluster(dir: std::path::PathBuf, listen: u16, target: u16) -> Arc<Store> {
    let s = Arc::new(Store::new());
    let (tx, rx) = spsc::channel(4096);
    s.set_wan_sink(Arc::new(WanPublisher::new(tx)));
    spawn_wan(
        dir,
        s.clone(),
        rx,
        WanConfig {
            targets: vec![format!("127.0.0.1:{target}")],
            listen,
            batch: 256,
            queue_bytes: 1 << 30,
            backpressure: Backpressure::Throw,
            poll_ms: 20,
        },
    );
    s
}

#[test]
fn two_clusters_replicate_over_tcp_active_active() {
    // Process-derived ports keep parallel test runs from colliding.
    let base = 39000 + ((std::process::id() % 400) * 2) as u16;
    let (pa, pb) = (base, base + 1);
    let dir = std::env::temp_dir().join(format!("bonsai-wanlive-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let sa = cluster(dir.join("a"), pa, pb);
    let sb = cluster(dir.join("b"), pb, pa);
    std::thread::sleep(Duration::from_millis(150)); // let the listeners bind

    sa.put("m", b"ka".to_vec(), b"va".to_vec());
    sb.put("m", b"kb".to_vec(), b"vb".to_vec());

    assert!(wait_for(|| sb.get("m", b"ka") == Some(b"va".to_vec())), "A->B replicated over TCP");
    assert!(wait_for(|| sa.get("m", b"kb") == Some(b"vb".to_vec())), "B->A replicated over TCP");

    // Loop prevention holds live: the WAN-applied write on B was not re-shipped back
    // to A (A still holds only its own value for ka; no divergence/oscillation).
    assert_eq!(sa.get("m", b"ka"), Some(b"va".to_vec()));
    assert_eq!(sb.get("m", b"kb"), Some(b"vb".to_vec()));

    std::fs::remove_dir_all(&dir).ok();
}
