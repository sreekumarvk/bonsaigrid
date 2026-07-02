//! Append-only WAL segment file: append framed records, group-commit fsync, and
//! read them back on recovery — stopping cleanly at a crash-torn tail.

use crate::record::{decode_record, Decoded, RecordType};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// A single append-only log segment.
pub struct WalSegment {
    file: File,
    path: PathBuf,
    bytes: u64,
}

impl WalSegment {
    /// Open (creating if absent) a segment for appending.
    pub fn open(path: &Path) -> io::Result<WalSegment> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let bytes = file.metadata()?.len();
        Ok(WalSegment {
            file,
            path: path.to_path_buf(),
            bytes,
        })
    }

    /// Append already-framed record bytes (does not fsync).
    pub fn append(&mut self, framed: &[u8]) -> io::Result<()> {
        self.file.write_all(framed)?;
        self.bytes += framed.len() as u64;
        Ok(())
    }

    /// Durably flush everything appended so far.
    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Bytes written to this segment.
    pub fn len(&self) -> u64 {
        self.bytes
    }
    pub fn is_empty(&self) -> bool {
        self.bytes == 0
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Read every complete record in the segment at `path`, in order, invoking
/// `apply(rtype, payload)`. A torn/partial final record (from a crash mid-write)
/// terminates the read cleanly and is NOT an error.
pub fn read_segment(path: &Path, mut apply: impl FnMut(RecordType, &[u8])) -> io::Result<()> {
    let mut bytes = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut bytes)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    }
    let mut off = 0;
    while off < bytes.len() {
        match decode_record(&bytes[off..]) {
            Decoded::Record {
                rtype,
                payload,
                consumed,
            } => {
                apply(rtype, payload);
                off += consumed;
            }
            // Torn or incomplete tail: stop; the intact prefix is what survived.
            Decoded::NeedMore | Decoded::Torn => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{encode_map_put, parse_map_put};

    fn tmp(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("bonsai-wal-test-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&d);
        d
    }

    #[test]
    fn append_read_roundtrip() {
        let path = tmp("roundtrip");
        let mut seg = WalSegment::open(&path).unwrap();
        let mut a = Vec::new();
        encode_map_put(&mut a, 1, 0, "m", b"a", b"1");
        let mut b = Vec::new();
        encode_map_put(&mut b, 2, 0, "m", b"b", b"2");
        seg.append(&a).unwrap();
        seg.append(&b).unwrap();
        seg.fsync().unwrap();

        let mut keys = Vec::new();
        read_segment(&path, |_t, p| {
            keys.push(parse_map_put(p).unwrap().key.to_vec())
        })
        .unwrap();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn torn_tail_ignored() {
        let path = tmp("torn");
        let mut seg = WalSegment::open(&path).unwrap();
        let mut a = Vec::new();
        encode_map_put(&mut a, 1, 0, "m", b"a", b"1");
        let mut b = Vec::new();
        encode_map_put(&mut b, 2, 0, "m", b"b", b"2");
        seg.append(&a).unwrap();
        seg.append(&b).unwrap();
        seg.fsync().unwrap();
        drop(seg);
        // Simulate a crash mid-write of the second record.
        let full = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 3).unwrap();

        let mut keys = Vec::new();
        read_segment(&path, |_t, p| {
            keys.push(parse_map_put(p).unwrap().key.to_vec())
        })
        .unwrap();
        assert_eq!(
            keys,
            vec![b"a".to_vec()],
            "only the intact first record survives"
        );
        std::fs::remove_file(&path).ok();
    }
}
