//! Durable per-target outbound buffer. Records are appended (framed, fsync'd) to
//! `records.log`; a committed cursor **per target** (target -> highest acked seq)
//! lives in `cursors` and is fsync'd on advance. On reopen, records replay (stopping
//! at a torn tail) and each target's tail is whatever sits past its cursor — so a
//! slow/lagging remote doesn't pin a fast one, and a fast remote never re-ships what
//! it already confirmed. Mirrors the persistence WAL discipline.

use crate::record::{decode, encode, Decoded, WanRecord};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const LOG_FILE: &str = "records.log";
const CURSOR_FILE: &str = "cursors";
/// The single-target convenience cursor (used by `ack`/`acked`/`unacked`).
const DEFAULT_TARGET: &str = "";

pub struct WanQueue {
    dir: PathBuf,
    seg: std::fs::File,
    records: Vec<(u64, WanRecord)>, // (seq, record), seq starts at 1
    next_seq: u64,
    cursors: HashMap<String, u64>, // target -> highest contiguously-acked seq
    bytes: u64,
}

impl WanQueue {
    pub fn open(dir: &Path) -> std::io::Result<WanQueue> {
        std::fs::create_dir_all(dir)?;
        let mut buf = Vec::new();
        match std::fs::File::open(dir.join(LOG_FILE)) {
            Ok(mut f) => {
                f.read_to_end(&mut buf)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut records = Vec::new();
        let mut off = 0;
        let mut seq = 0;
        while off < buf.len() {
            match decode(&buf[off..]) {
                Decoded::Record { rec, consumed } => {
                    seq += 1;
                    records.push((seq, rec));
                    off += consumed;
                }
                _ => break, // torn / short tail
            }
        }
        let cursors = read_cursors(&dir.join(CURSOR_FILE))?;
        let seg = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))?;
        let bytes = off as u64;
        Ok(WanQueue {
            dir: dir.to_path_buf(),
            seg,
            records,
            next_seq: seq + 1,
            cursors,
            bytes,
        })
    }

    pub fn append(&mut self, rec: &WanRecord) -> std::io::Result<u64> {
        let framed = encode(rec);
        self.seg.write_all(&framed)?;
        self.seg.sync_data()?;
        self.bytes += framed.len() as u64;
        let seq = self.next_seq;
        self.next_seq += 1;
        self.records.push((seq, rec.clone()));
        Ok(seq)
    }

    // ---- per-target shipping ----

    /// Records still unacked by `target` (its own tail).
    pub fn unacked_for(&self, target: &str) -> Vec<(u64, WanRecord)> {
        let c = self.target_acked(target);
        self.records
            .iter()
            .filter(|(s, _)| *s > c)
            .cloned()
            .collect()
    }

    /// Advance `target`'s cursor and persist it.
    pub fn ack_target(&mut self, target: &str, up_to_seq: u64) -> std::io::Result<()> {
        let capped = up_to_seq.min(self.next_seq.saturating_sub(1));
        let cur = self.cursors.entry(target.to_string()).or_insert(0);
        if capped > *cur {
            *cur = capped;
            self.persist_cursors()?;
        }
        Ok(())
    }

    pub fn target_acked(&self, target: &str) -> u64 {
        self.cursors.get(target).copied().unwrap_or(0)
    }

    /// Lowest cursor across all known targets — records at or below it are confirmed
    /// everywhere (reclaimable). 0 if no target has acked yet.
    pub fn min_acked(&self) -> u64 {
        self.cursors.values().copied().min().unwrap_or(0)
    }

    // ---- single-target convenience (the default "" cursor) ----
    pub fn unacked(&self) -> Vec<(u64, WanRecord)> {
        self.unacked_for(DEFAULT_TARGET)
    }
    pub fn ack(&mut self, up_to_seq: u64) -> std::io::Result<()> {
        self.ack_target(DEFAULT_TARGET, up_to_seq)
    }
    pub fn acked(&self) -> u64 {
        self.target_acked(DEFAULT_TARGET)
    }

    /// Count of records not yet confirmed by every target.
    pub fn len(&self) -> usize {
        let c = self.min_acked();
        self.records.iter().filter(|(s, _)| *s > c).count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// True if the durable segment already exceeds `max_bytes` (backpressure gate).
    pub fn would_exceed(&self, max_bytes: u64) -> bool {
        self.bytes >= max_bytes
    }

    fn persist_cursors(&self) -> std::io::Result<()> {
        let mut b = Vec::new();
        for (t, s) in &self.cursors {
            b.extend_from_slice(&(t.len() as u32).to_le_bytes());
            b.extend_from_slice(t.as_bytes());
            b.extend_from_slice(&s.to_le_bytes());
        }
        let tmp = self.dir.join("cursors.tmp");
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&b)?;
        f.sync_data()?;
        std::fs::rename(&tmp, self.dir.join(CURSOR_FILE))
    }
}

fn read_cursors(path: &Path) -> std::io::Result<HashMap<String, u64>> {
    let b = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };
    let mut m = HashMap::new();
    let mut p = 0;
    while p + 4 <= b.len() {
        let n = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        if p + n + 8 > b.len() {
            break; // torn tail
        }
        let target = String::from_utf8_lossy(&b[p..p + n]).into_owned();
        p += n;
        let seq = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        m.insert(target, seq);
    }
    Ok(m)
}
