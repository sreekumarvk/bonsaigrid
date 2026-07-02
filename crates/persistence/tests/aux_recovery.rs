//! Phase B: all non-map data structures persist and recover — via WAL records
//! and via a snapshot.

use persistence::record::encode_aux_state;
use persistence::{recover, wal::WalSegment};
use std::path::PathBuf;
use store::{Store, AUX_LIST, AUX_MULTIMAP, AUX_PNCOUNTER, AUX_QUEUE, AUX_RINGBUFFER, AUX_SET};

fn tmpdir(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-aux-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Populate one of each structure type.
fn populate(s: &Store) {
    s.queue_offer("q", b"q1".to_vec());
    s.queue_offer("q", b"q2".to_vec());
    s.list_add("l", b"l1".to_vec());
    s.list_add("l", b"l2".to_vec());
    s.set_add("s", b"s1".to_vec());
    s.set_add("s", b"s2".to_vec());
    s.mm_put("mm", b"k".to_vec(), b"v1".to_vec());
    s.mm_put("mm", b"k".to_vec(), b"v2".to_vec());
    s.rb_add("rb", b"r1".to_vec());
    s.pn_add("pn", 42, false);
}

fn assert_recovered(s: &Store) {
    assert_eq!(s.queue_size("q"), 2, "queue");
    assert_eq!(s.queue_peek("q"), Some(b"q1".to_vec()), "queue order");
    assert_eq!(s.list_size("l"), 2, "list");
    assert_eq!(s.list_get("l", 1), Some(b"l2".to_vec()), "list order");
    assert_eq!(s.set_size("s"), 2, "set");
    assert!(s.set_contains("s", b"s1"), "set member");
    assert_eq!(s.mm_get("mm", b"k").len(), 2, "multimap");
    assert_eq!(s.rb_size("rb"), 1, "ringbuffer");
    assert_eq!(s.pn_get("pn"), 42, "pncounter");
}

#[test]
fn all_structures_recover_from_wal() {
    let dir = tmpdir("wal");
    // Emit AuxState WAL records via serialize_aux (as the sink would).
    let src = Store::new();
    populate(&src);
    let mut seg = WalSegment::open(&dir.join("wal.1")).unwrap();
    for (kind, name, state) in src.all_aux() {
        let mut buf = Vec::new();
        encode_aux_state(&mut buf, kind, &name, &state);
        seg.append(&buf).unwrap();
    }
    seg.fsync().unwrap();
    drop(seg);

    let recovered = Store::new();
    recover(&dir, &recovered).unwrap();
    assert_recovered(&recovered);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn all_structures_recover_from_snapshot() {
    let dir = tmpdir("snap");
    let src = Store::new();
    populate(&src);
    persistence::snapshot::write_snapshot(&dir.join("snapshot.1"), &src).unwrap();

    let recovered = Store::new();
    recover(&dir, &recovered).unwrap();
    assert_recovered(&recovered);
    std::fs::remove_dir_all(&dir).ok();
}

/// The kind constants are exposed so callers can reason about aux records.
#[test]
fn aux_kind_constants_distinct() {
    let all = [
        AUX_QUEUE,
        AUX_LIST,
        AUX_SET,
        AUX_MULTIMAP,
        AUX_RINGBUFFER,
        AUX_PNCOUNTER,
    ];
    let mut sorted = all.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), all.len(), "aux kinds must be distinct");
}
