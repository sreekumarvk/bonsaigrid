//! The Raft log: an ordered sequence of entries (1-based index) plus the
//! persistent `current_term`/`voted_for`.
//!
//! `RaftLog` is in-memory by default (used by the consensus core and the
//! deterministic simulation). [`RaftLog::open_durable`] attaches a crash-safe
//! WAL backing: entries are framed (`[len][crc32][term][index][cmd]`, torn-tail
//! safe) and fsync'd on append; a conflict truncation rewrites the segment; the
//! term/vote live in a small atomically-rewritten meta file (fsync'd before a
//! vote is granted). This keeps the raft crate free of the heavier persistence
//! crate while reusing the same WAL discipline.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

/// The on-disk backing for a durable log (absent for the in-memory variant).
struct Durable {
    dir: PathBuf,
    seg: std::fs::File, // append handle to entries.log
}

/// Raft log with an optional compacted prefix. Live entries cover indices
/// `snapshot_index + 1 ..= last`; everything at or before `snapshot_index` has
/// been folded into the state machine and dropped. `entries[j]` has index
/// `snapshot_index + 1 + j`.
#[derive(Default)]
pub struct RaftLog {
    entries: Vec<Entry>,
    term: u64,
    vote: Option<usize>,
    durable: Option<Durable>,
    /// Last index included in a snapshot (0 = nothing compacted).
    snapshot_index: u64,
    snapshot_term: u64,
}

const LOG_FILE: &str = "entries.log";
const META_FILE: &str = "meta";

impl RaftLog {
    pub fn new() -> RaftLog {
        RaftLog::default()
    }

    /// Open (or recover) a crash-safe log rooted at `dir`. Rebuilds the in-memory
    /// entries from the WAL (stopping at a torn tail) and the term/vote from the
    /// meta file, then keeps the segment open for appends.
    pub fn open_durable(dir: &Path) -> std::io::Result<RaftLog> {
        std::fs::create_dir_all(dir)?;
        let entries = read_log(&dir.join(LOG_FILE))?;
        let (term, vote) = read_meta(&dir.join(META_FILE))?;
        let seg = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))?;
        Ok(RaftLog {
            entries,
            term,
            vote,
            durable: Some(Durable {
                dir: dir.to_path_buf(),
                seg,
            }),
            snapshot_index: 0,
            snapshot_term: 0,
        })
    }

    /// Last index included in a snapshot (0 = nothing compacted).
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// Vec position of `index`, if it is a live (uncompacted) entry.
    fn pos(&self, index: u64) -> Option<usize> {
        if index <= self.snapshot_index {
            return None;
        }
        let off = (index - self.snapshot_index - 1) as usize;
        (off < self.entries.len()).then_some(off)
    }

    /// Index of the last entry (the snapshot index if the live log is empty).
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    /// Term of the last entry (the snapshot term if the live log is empty).
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Term of the entry at `index`: 0 before the log, the snapshot term at the
    /// snapshot boundary, the entry's term if live, else 0.
    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        if index == self.snapshot_index {
            return self.snapshot_term;
        }
        self.pos(index).map(|p| self.entries[p].term).unwrap_or(0)
    }

    /// The command at `index`, if it is a live entry.
    pub fn command_at(&self, index: u64) -> Option<&[u8]> {
        self.pos(index).map(|p| self.entries[p].command.as_slice())
    }

    /// All live entries with index >= `from`.
    pub fn entries_from(&self, from: u64) -> Vec<Entry> {
        let start = from.saturating_sub(self.snapshot_index + 1) as usize;
        self.entries.iter().skip(start).cloned().collect()
    }

    /// Discard entries at or before `up_to` (folded into the state machine). No-op
    /// if `up_to` is not past the current snapshot or exceeds the live log. Durable
    /// logs are left intact (compaction there needs a persisted SM snapshot — a
    /// follow-up), so this only bounds in-memory logs.
    pub fn compact(&mut self, up_to: u64) {
        if self.durable.is_some() || up_to <= self.snapshot_index || up_to > self.last_index() {
            return;
        }
        let up_to_term = self.term_at(up_to);
        let drop = (up_to - self.snapshot_index) as usize;
        self.entries.drain(0..drop.min(self.entries.len()));
        self.snapshot_index = up_to;
        self.snapshot_term = up_to_term;
    }

    /// Append an entry (must be the next index).
    pub fn append(&mut self, e: Entry) {
        debug_assert_eq!(
            e.index,
            self.last_index() + 1,
            "log append must be contiguous"
        );
        if let Some(d) = &mut self.durable {
            let framed = frame_entry(&e);
            // Best-effort durability; an I/O error here is fatal to safety, so
            // surface it loudly rather than silently continuing.
            if let Err(err) = d.seg.write_all(&framed).and_then(|_| d.seg.sync_data()) {
                panic!("raft: fatal error persisting log entry: {err}");
            }
        }
        self.entries.push(e);
    }

    /// Drop all live entries with index >= `index` (conflict truncation).
    pub fn truncate_from(&mut self, index: u64) {
        let keep = index.saturating_sub(self.snapshot_index + 1) as usize;
        self.entries.truncate(keep);
        if let Some(d) = &mut self.durable {
            // Truncations are rare (conflict only) — rewrite the segment.
            if let Err(err) = rewrite_log(&d.dir, &self.entries) {
                panic!("raft: fatal error rewriting log on truncate: {err}");
            }
            d.seg = std::fs::OpenOptions::new()
                .append(true)
                .open(d.dir.join(LOG_FILE))
                .expect("reopen log segment after truncate");
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // ---- persistent term/vote ----

    pub fn persist_term_vote(&mut self, term: u64, vote: Option<usize>) {
        self.term = term;
        self.vote = vote;
        if let Some(d) = &self.durable {
            if let Err(err) = write_meta(&d.dir, term, vote) {
                panic!("raft: fatal error persisting term/vote: {err}");
            }
        }
    }
    pub fn persisted_term(&self) -> u64 {
        self.term
    }
    pub fn persisted_vote(&self) -> Option<usize> {
        self.vote
    }
}

// ---- durable-log file format ----

/// Frame one entry: `[len:u32][crc32:u32][term:u64][index:u64][cmd]`, where the
/// CRC covers `term|index|cmd`. `len` is the byte count following the len field.
fn frame_entry(e: &Entry) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + e.command.len());
    body.extend_from_slice(&e.term.to_le_bytes());
    body.extend_from_slice(&e.index.to_le_bytes());
    body.extend_from_slice(&e.command);
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&((body.len() as u32 + 4).to_le_bytes())); // crc + body
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Read all framed entries from `path`, stopping cleanly at a torn/short/CRC-bad
/// tail (a crash-truncated final record is not an error). Missing file → empty.
fn read_log(path: &Path) -> std::io::Result<Vec<Entry>> {
    let mut buf = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut buf)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    }
    let mut out = Vec::new();
    let mut off = 0;
    while off + 8 <= buf.len() {
        let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        let body_start = off + 8;
        let body_end = off + 4 + len; // len counts crc(4)+body; body = len-4
        if len < 4 + 16 || body_end > buf.len() {
            break; // torn tail
        }
        let body = &buf[body_start..body_end];
        if crc32fast::hash(body) != crc {
            break; // corrupt tail
        }
        let term = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let index = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let command = body[16..].to_vec();
        out.push(Entry {
            term,
            index,
            command,
        });
        off = body_end;
    }
    Ok(out)
}

/// Rewrite the whole log segment from `entries` atomically (tmp + fsync + rename).
fn rewrite_log(dir: &Path, entries: &[Entry]) -> std::io::Result<()> {
    let tmp = dir.join("entries.log.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    for e in entries {
        f.write_all(&frame_entry(e))?;
    }
    f.sync_data()?;
    std::fs::rename(&tmp, dir.join(LOG_FILE))
}

/// Persist `term`/`vote` to the meta file atomically. Format:
/// `[term:u64][has_vote:u8][vote:u64]`.
fn write_meta(dir: &Path, term: u64, vote: Option<usize>) -> std::io::Result<()> {
    let mut body = Vec::with_capacity(17);
    body.extend_from_slice(&term.to_le_bytes());
    body.push(vote.is_some() as u8);
    body.extend_from_slice(&(vote.unwrap_or(0) as u64).to_le_bytes());
    let tmp = dir.join("meta.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&body)?;
    f.sync_data()?;
    std::fs::rename(&tmp, dir.join(META_FILE))
}

/// Read `term`/`vote` from the meta file (defaults `(0, None)` if absent/short).
fn read_meta(path: &Path) -> std::io::Result<(u64, Option<usize>)> {
    let mut buf = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut buf)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, None)),
        Err(e) => return Err(e),
    }
    if buf.len() < 17 {
        return Ok((0, None));
    }
    let term = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let vote = if buf[8] != 0 {
        Some(u64::from_le_bytes(buf[9..17].try_into().unwrap()) as usize)
    } else {
        None
    };
    Ok((term, vote))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(term: u64, index: u64, c: &[u8]) -> Entry {
        Entry {
            term,
            index,
            command: c.to_vec(),
        }
    }

    #[test]
    fn append_index_term_lookup() {
        let mut l = RaftLog::new();
        assert_eq!(l.last_index(), 0);
        assert_eq!(l.term_at(0), 0);
        l.append(e(1, 1, b"a"));
        l.append(e(1, 2, b"b"));
        l.append(e(2, 3, b"c"));
        assert_eq!(l.last_index(), 3);
        assert_eq!(l.last_term(), 2);
        assert_eq!(l.term_at(2), 1);
        assert_eq!(l.term_at(3), 2);
        assert_eq!(l.term_at(4), 0); // beyond the log
        assert_eq!(l.command_at(3), Some(b"c".as_slice()));
    }

    #[test]
    fn truncate_and_entries_from() {
        let mut l = RaftLog::new();
        for i in 1..=5 {
            l.append(e(1, i, b"x"));
        }
        assert_eq!(l.entries_from(3).len(), 3); // indices 3,4,5
        l.truncate_from(3); // drop 3,4,5
        assert_eq!(l.last_index(), 2);
        assert_eq!(l.entries_from(3).len(), 0);
    }

    #[test]
    fn persist_term_vote_roundtrip() {
        let mut l = RaftLog::new();
        l.persist_term_vote(7, Some(2));
        assert_eq!(l.persisted_term(), 7);
        assert_eq!(l.persisted_vote(), Some(2));
    }
}
