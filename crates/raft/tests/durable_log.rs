//! The durable Raft log survives a restart: entries, conflict truncations, and
//! term/vote all recover, and a torn final record is dropped (not fatal).

use raft::{Entry, RaftLog};
use std::path::PathBuf;

fn tmpdir(name: &str) -> PathBuf {
    let mut d = std::env::temp_dir();
    d.push(format!("bonsai-raftlog-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn e(term: u64, index: u64, c: &[u8]) -> Entry {
    Entry {
        term,
        index,
        command: c.to_vec(),
    }
}

#[test]
fn entries_and_term_vote_recover() {
    let dir = tmpdir("recover");
    {
        let mut log = RaftLog::open_durable(&dir).unwrap();
        log.persist_term_vote(3, Some(2));
        log.append(e(1, 1, b"a"));
        log.append(e(1, 2, b"b"));
        log.append(e(2, 3, b"c"));
        log.persist_term_vote(4, Some(1)); // latest vote wins
    }
    // Reopen: everything recovers.
    let log = RaftLog::open_durable(&dir).unwrap();
    assert_eq!(log.last_index(), 3);
    assert_eq!(log.last_term(), 2);
    assert_eq!(log.command_at(3), Some(b"c".as_slice()));
    assert_eq!(log.persisted_term(), 4);
    assert_eq!(log.persisted_vote(), Some(1));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn truncation_persists_across_restart() {
    let dir = tmpdir("truncate");
    {
        let mut log = RaftLog::open_durable(&dir).unwrap();
        for i in 1..=5 {
            log.append(e(1, i, format!("v{i}").as_bytes()));
        }
        log.truncate_from(3); // drop 3,4,5
        log.append(e(2, 3, b"c3")); // conflicting entry at 3
    }
    let log = RaftLog::open_durable(&dir).unwrap();
    assert_eq!(log.last_index(), 3);
    assert_eq!(log.term_at(3), 2);
    assert_eq!(log.command_at(3), Some(b"c3".as_slice()));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn torn_tail_is_dropped() {
    let dir = tmpdir("torn");
    {
        let mut log = RaftLog::open_durable(&dir).unwrap();
        log.append(e(1, 1, b"ok1"));
        log.append(e(1, 2, b"ok2"));
    }
    // Simulate a crash mid-append: append garbage bytes to the segment tail.
    let seg = dir.join("entries.log");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02]); // partial record
    std::fs::write(&seg, &bytes).unwrap();

    let log = RaftLog::open_durable(&dir).unwrap();
    assert_eq!(log.last_index(), 2, "torn tail dropped, intact prefix kept");
    assert_eq!(log.command_at(2), Some(b"ok2".as_slice()));
    std::fs::remove_dir_all(&dir).ok();
}
