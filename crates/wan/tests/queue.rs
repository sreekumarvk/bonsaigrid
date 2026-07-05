use std::path::PathBuf;
use wan::{WanOp, WanQueue, WanRecord};

fn tmp(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-wanq-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn rec(stamp: u64, k: &str) -> WanRecord {
    WanRecord {
        op: WanOp::Put,
        stamp,
        ttl_ms: 0,
        map: "m".into(),
        key: k.as_bytes().to_vec(),
        value: b"v".to_vec(),
    }
}

#[test]
fn append_ack_and_recover() {
    let dir = tmp("recover");
    {
        let mut q = WanQueue::open(&dir).unwrap();
        assert_eq!(q.append(&rec(1, "a")).unwrap(), 1);
        assert_eq!(q.append(&rec(2, "b")).unwrap(), 2);
        assert_eq!(q.append(&rec(3, "c")).unwrap(), 3);
        assert_eq!(q.unacked().len(), 3);
        q.ack(2).unwrap(); // remote confirmed through seq 2
        assert_eq!(q.acked(), 2);
        assert_eq!(
            q.unacked().iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![3]
        );
    }
    // Reopen: the cursor is durable, so only seq 3 is still unacked.
    let q = WanQueue::open(&dir).unwrap();
    assert_eq!(q.acked(), 2);
    let un = q.unacked();
    assert_eq!(un.len(), 1);
    assert_eq!(un[0].1.key, b"c");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn reports_when_over_the_byte_bound() {
    let dir = tmp("bound");
    let mut q = WanQueue::open(&dir).unwrap();
    assert!(!q.would_exceed(10_000));
    for i in 0..100 {
        q.append(&rec(i, "k")).unwrap();
    }
    assert!(q.would_exceed(10), "many records exceed a tiny bound");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn per_target_cursors_are_independent_and_durable() {
    let dir = tmp("pertarget");
    {
        let mut q = WanQueue::open(&dir).unwrap();
        for i in 1..=5 {
            q.append(&rec(i, "k")).unwrap();
        }
        // "fast" confirms everything; "slow" confirms nothing.
        q.ack_target("fast", 5).unwrap();
        assert_eq!(q.target_acked("fast"), 5);
        assert_eq!(q.target_acked("slow"), 0);
        assert!(
            q.unacked_for("fast").is_empty(),
            "fast has nothing to re-ship"
        );
        assert_eq!(q.unacked_for("slow").len(), 5, "slow still owes all 5");
    }
    // Per-target cursors survive reopen.
    let q = WanQueue::open(&dir).unwrap();
    assert_eq!(q.target_acked("fast"), 5);
    assert_eq!(q.target_acked("slow"), 0);
    assert_eq!(q.unacked_for("slow").len(), 5);
    std::fs::remove_dir_all(&dir).ok();
}
