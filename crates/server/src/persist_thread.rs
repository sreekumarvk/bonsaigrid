//! Persistence thread: drains WAL records from the reactor cores over an SPSC
//! ring, appends them to a WAL segment, group-commit fsyncs on a cadence, and
//! periodically snapshots + truncates. Mirrors the member thread's offload.

use persistence::record::{encode_aux_state, encode_map_put, encode_map_remove};
use persistence::{snapshot, wal::WalSegment};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::{Store, WalSink};

/// A framed WAL record awaiting persistence.
pub struct PersistJob(pub Vec<u8>);

/// The store-side durability sink: encodes each mutation into a framed record and
/// hands it to the persistence thread. A full ring drops the record for `async`
/// durability (best-effort, surfaced by the reactor's metrics elsewhere).
pub struct Persister {
    tx: spsc::Producer<PersistJob>,
}

impl Persister {
    pub fn new(tx: spsc::Producer<PersistJob>) -> Persister {
        Persister { tx }
    }
}

impl WalSink for Persister {
    fn map_put(&self, stamp: u64, ttl_ms: u64, map: &str, key: &[u8], value: &[u8]) {
        let mut buf = Vec::new();
        encode_map_put(&mut buf, stamp, ttl_ms, map, key, value);
        let _ = self.tx.push(PersistJob(buf));
    }
    fn map_remove(&self, stamp: u64, map: &str, key: &[u8]) {
        let mut buf = Vec::new();
        encode_map_remove(&mut buf, stamp, map, key);
        let _ = self.tx.push(PersistJob(buf));
    }
    fn aux_state(&self, kind: u8, name: &str, state: &[u8]) {
        let mut buf = Vec::new();
        encode_aux_state(&mut buf, kind, name, state);
        let _ = self.tx.push(PersistJob(buf));
    }
}

/// Spawn the persistence thread. It owns all disk state under `dir`.
pub fn spawn_persistence(
    dir: PathBuf,
    store: Arc<Store>,
    rx: spsc::Consumer<PersistJob>,
    flush_ms: u64,
    snapshot_bytes: u64,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        std::fs::create_dir_all(&dir).ok();
        // Start a fresh generation above anything recovery replayed.
        let mut generation = persistence::latest_generation(&dir) + 1;
        let mut seg =
            match WalSegment::open(&dir.join(format!("{}{generation}", persistence::WAL_PREFIX))) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("persistence: cannot open WAL segment: {e}");
                    return;
                }
            };
        let mut dirty = false;
        let mut last_flush = Instant::now();

        loop {
            // Drain everything available into the current segment.
            let mut drained = false;
            while let Some(PersistJob(bytes)) = rx.pop() {
                let _ = seg.append(&bytes);
                dirty = true;
                drained = true;
            }

            // Group-commit fsync on the flush cadence.
            if dirty && last_flush.elapsed() >= Duration::from_millis(flush_ms) {
                let _ = seg.fsync();
                dirty = false;
                last_flush = Instant::now();
            }

            // Snapshot + truncate once the segment grows past the threshold.
            if seg.len() >= snapshot_bytes {
                let _ = seg.fsync();
                let old = seg.path().to_path_buf();
                let new_gen = generation + 1;
                // 1) Roll FIRST so subsequent records land in the new segment.
                match WalSegment::open(&dir.join(format!("{}{new_gen}", persistence::WAL_PREFIX))) {
                    Ok(s) => {
                        seg = s;
                        generation = new_gen;
                    }
                    Err(e) => {
                        eprintln!("persistence: cannot roll WAL: {e}");
                        continue;
                    }
                }
                // 2) Snapshot AFTER the roll captures everything up to now,
                //    including the just-closed segment's records.
                let snap = dir.join(format!("{}{new_gen}", persistence::SNAPSHOT_PREFIX));
                let _ = snapshot::write_snapshot(&snap, &store);
                // 3) Drop the superseded segment and older files.
                let _ = std::fs::remove_file(&old);
                persistence::prune_below(&dir, new_gen);
                dirty = false;
            }

            if !drained {
                std::thread::sleep(Duration::from_millis(1)); // idle backoff
            }
        }
    })
}
