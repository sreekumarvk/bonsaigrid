//! End-to-end persistence: mutations through a Store with a Persister sink flow
//! to the persistence thread, get fsync'd + snapshotted, and recover into a
//! fresh Store — including a snapshot+truncation mid-run.

use server::persist_thread::{spawn_persistence, PersistJob, Persister};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;

fn tmpdir(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-persist-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn wait_until(mut cond: impl FnMut() -> bool, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    cond()
}

#[test]
fn writes_persist_and_recover_across_a_snapshot() {
    let dir = tmpdir("recover");

    // Store with a persistence sink feeding the thread.
    let store = Arc::new(Store::new());
    let (tx, rx) = spsc::channel::<PersistJob>(65536);
    store.set_wal_sink(Arc::new(Persister::new(tx)));
    // Small snapshot threshold so a snapshot + truncation fires mid-run.
    let _handle = spawn_persistence(dir.clone(), store.clone(), rx, 5, 4096);

    // 500 puts + a few removes.
    for i in 0..500 {
        store.put(
            "m",
            format!("k{i}").into_bytes(),
            format!("v{i}").into_bytes(),
        );
    }
    for i in [10, 20, 30] {
        store.remove("m", format!("k{i}").as_bytes());
    }

    // Wait until the persistence thread has produced a snapshot (truncation
    // fired) so we exercise the snapshot+tail recovery path.
    let dir2 = dir.clone();
    let snapped = wait_until(
        move || {
            std::fs::read_dir(&dir2)
                .map(|rd| {
                    rd.flatten()
                        .any(|e| e.file_name().to_string_lossy().starts_with("snapshot."))
                })
                .unwrap_or(false)
        },
        5,
    );
    assert!(snapped, "expected a snapshot to be written");
    // Give the thread a moment to fsync the final records.
    std::thread::sleep(Duration::from_millis(100));

    // Recover into a fresh store and verify.
    let recovered = Store::new();
    persistence::recover(&dir, &recovered).unwrap();
    for i in 0..500 {
        if [10, 20, 30].contains(&i) {
            assert_eq!(
                recovered.get("m", format!("k{i}").as_bytes()),
                None,
                "k{i} removed"
            );
        } else {
            assert_eq!(
                recovered.get("m", format!("k{i}").as_bytes()),
                Some(format!("v{i}").into_bytes()),
                "k{i} recovered"
            );
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}
