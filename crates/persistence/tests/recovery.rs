//! Crash-recovery acid test: a snapshot + WAL tail (built via the codecs)
//! recover the store, including a post-snapshot remove and a torn final record.

use persistence::record::{encode_map_put, encode_map_remove};
use persistence::{recover, wal::WalSegment};
use std::path::PathBuf;
use store::Store;

fn tmpdir(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-recover-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn recovers_snapshot_plus_wal_tail_with_remove_and_torn() {
    let dir = tmpdir("acid");

    // Snapshot generation 5 holds k0..k4.
    let base = Store::new();
    for i in 0..5 {
        base.put(
            "m",
            format!("k{i}").into_bytes(),
            format!("v{i}").into_bytes(),
        );
    }
    persistence::snapshot::write_snapshot(&dir.join("snapshot.5"), &base).unwrap();

    // WAL generation 5 (after the snapshot): put k5..k9, then remove k2.
    let mut seg = WalSegment::open(&dir.join("wal.5")).unwrap();
    let mut buf = Vec::new();
    for i in 5..10 {
        encode_map_put(
            &mut buf,
            100 + i,
            0,
            "m",
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        );
    }
    encode_map_remove(&mut buf, 200, "m", b"k2");
    seg.append(&buf).unwrap();
    // Append a torn (partial) final record to simulate a crash mid-write.
    let mut torn = Vec::new();
    encode_map_put(&mut torn, 300, 0, "m", b"k99", b"never");
    torn.truncate(torn.len() - 4);
    seg.append(&torn).unwrap();
    seg.fsync().unwrap();
    drop(seg);

    // Recover into a fresh store.
    let store = Store::new();
    recover(&dir, &store).unwrap();

    // k0,k1,k3,k4 from snapshot; k5..k9 from WAL; k2 removed; k99 torn → absent.
    for i in [0, 1, 3, 4] {
        assert_eq!(
            store.get("m", format!("k{i}").as_bytes()),
            Some(format!("v{i}").into_bytes()),
            "snapshot key k{i}"
        );
    }
    for i in 5..10 {
        assert_eq!(
            store.get("m", format!("k{i}").as_bytes()),
            Some(format!("v{i}").into_bytes()),
            "WAL key k{i}"
        );
    }
    assert_eq!(store.get("m", b"k2"), None, "k2 removed by WAL");
    assert_eq!(store.get("m", b"k99"), None, "torn record dropped");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn missing_dir_is_ok() {
    let store = Store::new();
    recover(std::path::Path::new("/nonexistent/bonsai/xyz"), &store).unwrap();
    assert_eq!(store.size("m"), 0);
}
