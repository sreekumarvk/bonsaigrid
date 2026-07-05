//! Durable per-target outbound buffer. Records are appended (framed, fsync'd) to
//! `records.log`; a committed cursor (`acked` sequence) lives in `cursor` and is
//! fsync'd on advance. On reopen, records replay (stopping at a torn tail) and
//! only those past the cursor are unacked. Mirrors the persistence WAL discipline.

use crate::record::{decode, encode, Decoded, WanRecord};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const LOG_FILE: &str = "records.log";
const CURSOR_FILE: &str = "cursor";

pub struct WanQueue {
    dir: PathBuf,
    seg: std::fs::File,
    records: Vec<(u64, WanRecord)>, // (seq, record), seq starts at 1
    next_seq: u64,
    acked: u64,
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
        let acked = read_cursor(&dir.join(CURSOR_FILE))?;
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
            acked,
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

    pub fn unacked(&self) -> Vec<(u64, WanRecord)> {
        self.records
            .iter()
            .filter(|(s, _)| *s > self.acked)
            .cloned()
            .collect()
    }

    pub fn ack(&mut self, up_to_seq: u64) -> std::io::Result<()> {
        if up_to_seq <= self.acked {
            return Ok(());
        }
        self.acked = up_to_seq.min(self.next_seq - 1);
        write_cursor(&self.dir.join(CURSOR_FILE), self.acked)?;
        Ok(())
    }

    pub fn acked(&self) -> u64 {
        self.acked
    }
    pub fn len(&self) -> usize {
        self.records.iter().filter(|(s, _)| *s > self.acked).count()
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
}

fn read_cursor(path: &Path) -> std::io::Result<u64> {
    match std::fs::read(path) {
        Ok(b) if b.len() >= 8 => Ok(u64::from_le_bytes(b[0..8].try_into().unwrap())),
        Ok(_) => Ok(0),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

fn write_cursor(path: &Path, seq: u64) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&seq.to_le_bytes())?;
    f.sync_data()?;
    std::fs::rename(&tmp, path)
}
