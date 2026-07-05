use store::Store;
use wan::{apply_batch, WanOp, WanRecord};

#[test]
fn applies_puts_and_removes_via_merge() {
    let s = Store::new();
    let stamp = s.next_stamp();
    apply_batch(
        &s,
        &[WanRecord {
            op: WanOp::Put,
            stamp,
            ttl_ms: 0,
            map: "m".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    );
    assert_eq!(s.get("m", b"k"), Some(b"v".to_vec()));

    // A lower-stamped put loses the merge (HLC); a remove at a higher stamp wins.
    apply_batch(
        &s,
        &[WanRecord {
            op: WanOp::Put,
            stamp: 1,
            ttl_ms: 0,
            map: "m".into(),
            key: b"k".to_vec(),
            value: b"OLD".to_vec(),
        }],
    );
    assert_eq!(
        s.get("m", b"k"),
        Some(b"v".to_vec()),
        "stale put ignored by merge"
    );

    apply_batch(
        &s,
        &[WanRecord {
            op: WanOp::Remove,
            stamp: s.next_stamp(),
            ttl_ms: 0,
            map: "m".into(),
            key: b"k".to_vec(),
            value: vec![],
        }],
    );
    assert_eq!(s.get("m", b"k"), None);
}
