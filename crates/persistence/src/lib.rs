//! Local durability for the in-memory store: a write-ahead log + periodic
//! snapshot + recovery on restart (Hazelcast's Hot Restart Store).
//!
//! Pure I/O + codecs; the persistence *thread* and its SPSC wiring live in the
//! server crate (mirroring the member thread). Recovery applies records via the
//! store's stamp-guarded `put_merge`, so replay is idempotent.

pub mod record;
pub mod snapshot;
pub mod wal;

use record::{parse_map_put, parse_map_remove, RecordType};
use std::io;
use std::path::{Path, PathBuf};
use store::Store;

/// WAL segment file name prefix (`wal.<n>`).
pub const WAL_PREFIX: &str = "wal.";
/// Snapshot file name prefix (`snapshot.<n>`).
pub const SNAPSHOT_PREFIX: &str = "snapshot.";

/// Durability posture (`BONSAI_PERSISTENCE`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Persistence disabled.
    None,
    /// WAL appended and fsync'd off the hot path; ack does not wait.
    Async,
    /// Client ack deferred until the record is fsync'd.
    Sync,
}

impl Durability {
    pub fn parse(s: &str) -> Durability {
        match s.to_ascii_lowercase().as_str() {
            "async" => Durability::Async,
            "sync" => Durability::Sync,
            _ => Durability::None,
        }
    }
    pub fn enabled(self) -> bool {
        self != Durability::None
    }
}

/// The numeric suffix of files named `<prefix><n>` in `dir`, sorted ascending.
fn numbered(dir: &Path, prefix: &str) -> Vec<(u64, PathBuf)> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix(prefix) {
                if let Ok(n) = rest.parse::<u64>() {
                    out.push((n, e.path()));
                }
            }
        }
    }
    out.sort_by_key(|(n, _)| *n);
    out
}

/// The highest generation number in use across WAL segments and snapshots in
/// `dir` (0 if none). The persistence thread starts a new segment at
/// `latest_generation(dir) + 1` so it never appends to an already-replayed file.
pub fn latest_generation(dir: &Path) -> u64 {
    let w = numbered(dir, WAL_PREFIX)
        .last()
        .map(|(n, _)| *n)
        .unwrap_or(0);
    let s = numbered(dir, SNAPSHOT_PREFIX)
        .last()
        .map(|(n, _)| *n)
        .unwrap_or(0);
    w.max(s)
}

/// Delete WAL segments and snapshots with a generation strictly below `keep`.
pub fn prune_below(dir: &Path, keep: u64) {
    for prefix in [WAL_PREFIX, SNAPSHOT_PREFIX] {
        for (n, path) in numbered(dir, prefix) {
            if n < keep {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

/// Recover `store` from `dir`: load the newest snapshot, then replay every WAL
/// segment after it in order. Missing dir → Ok (fresh start). Idempotent.
pub fn recover(dir: &Path, store: &Store) -> io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    // Newest snapshot, and the WAL generation it superseded.
    let snaps = numbered(dir, SNAPSHOT_PREFIX);
    let snapshot_gen = snaps.last().map(|(n, _)| *n).unwrap_or(0);
    if let Some((_, path)) = snaps.last() {
        snapshot::load_snapshot(path, store)?;
    }
    // Replay WAL segments strictly after the snapshot generation, in order.
    for (n, path) in numbered(dir, WAL_PREFIX) {
        if n < snapshot_gen {
            continue; // superseded by the snapshot
        }
        wal::read_segment(&path, |rtype, payload| match rtype {
            RecordType::MapPut => {
                if let Some(mp) = parse_map_put(payload) {
                    store.put_merge(mp.map, mp.key, mp.value, mp.ttl_ms, mp.stamp, true);
                }
            }
            RecordType::MapRemove => {
                if let Some(mr) = parse_map_remove(payload) {
                    // Tail records are strictly after the snapshot point, so an
                    // ordered unconditional remove yields the correct final state.
                    store.remove(mr.map, mr.key);
                }
            }
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durability_parse() {
        assert_eq!(Durability::parse("none"), Durability::None);
        assert_eq!(Durability::parse("Async"), Durability::Async);
        assert_eq!(Durability::parse("SYNC"), Durability::Sync);
        assert_eq!(Durability::parse("x"), Durability::None);
        assert!(Durability::Async.enabled());
        assert!(!Durability::None.enabled());
    }
}
