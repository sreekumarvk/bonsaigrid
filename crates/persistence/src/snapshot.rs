//! Sectioned snapshot: a header followed by typed sections. v1 writes only the
//! `MapEntries` section; Phase B adds `AuxState`/`MultiMap` sections. Unknown
//! section types are skipped on load (forward-compatible).
//!
//! Installed atomically: written to `path.tmp`, fsync'd, then renamed over
//! `path`, so a crash never leaves a half-written snapshot.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;
use store::Store;

const MAGIC: &[u8; 4] = b"BSNP";
const VERSION: u16 = 1;
const SECTION_MAP_ENTRIES: u16 = 1;
const SECTION_AUX: u16 = 2;

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn get_bytes(data: &[u8], off: usize) -> Option<(&[u8], usize)> {
    let end = off.checked_add(4)?;
    if end > data.len() {
        return None;
    }
    let n = u32::from_le_bytes(data[off..end].try_into().ok()?) as usize;
    let de = end.checked_add(n)?;
    if de > data.len() {
        return None;
    }
    Some((&data[end..de], de))
}

/// Write a snapshot of `store` to `path` (atomically via `path.tmp` + rename).
pub fn write_snapshot(path: &Path, store: &Store) -> io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());

    // Section: MapEntries.
    let entries = store.all_entries_stamped();
    let mut sec = Vec::new();
    sec.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (map, key, value, stamp) in &entries {
        put_bytes(&mut sec, map.as_bytes());
        put_bytes(&mut sec, key);
        put_bytes(&mut sec, value);
        sec.extend_from_slice(&stamp.to_le_bytes());
    }
    buf.extend_from_slice(&SECTION_MAP_ENTRIES.to_le_bytes());
    buf.extend_from_slice(&(sec.len() as u32).to_le_bytes());
    buf.extend_from_slice(&sec);

    // Section: Aux (all non-map structures as (kind, name, state)).
    let aux = store.all_aux();
    let mut asec = Vec::new();
    asec.extend_from_slice(&(aux.len() as u32).to_le_bytes());
    for (kind, name, state) in &aux {
        asec.push(*kind);
        put_bytes(&mut asec, name.as_bytes());
        put_bytes(&mut asec, state);
    }
    buf.extend_from_slice(&SECTION_AUX.to_le_bytes());
    buf.extend_from_slice(&(asec.len() as u32).to_le_bytes());
    buf.extend_from_slice(&asec);

    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Load a snapshot from `path` into `store`. Missing file → Ok (nothing to do).
pub fn load_snapshot(path: &Path, store: &Store) -> io::Result<()> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    }
    if data.len() < 6 || &data[0..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad snapshot magic",
        ));
    }
    let mut off = 6; // magic + version
    while off + 6 <= data.len() {
        let stype = u16::from_le_bytes(data[off..off + 2].try_into().unwrap());
        let slen = u32::from_le_bytes(data[off + 2..off + 6].try_into().unwrap()) as usize;
        let body_start = off + 6;
        let body_end = match body_start.checked_add(slen) {
            Some(e) if e <= data.len() => e,
            _ => break, // truncated section
        };
        let body = &data[body_start..body_end];
        if stype == SECTION_MAP_ENTRIES {
            load_map_entries(body, store);
        } else if stype == SECTION_AUX {
            load_aux(body, store);
        }
        // unknown section types are skipped
        off = body_end;
    }
    Ok(())
}

fn load_map_entries(body: &[u8], store: &Store) {
    if body.len() < 4 {
        return;
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let mut off = 4;
    for _ in 0..count {
        let Some((map, o1)) = get_bytes(body, off) else {
            break;
        };
        let Some((key, o2)) = get_bytes(body, o1) else {
            break;
        };
        let Some((value, o3)) = get_bytes(body, o2) else {
            break;
        };
        if o3 + 8 > body.len() {
            break;
        }
        let stamp = u64::from_le_bytes(body[o3..o3 + 8].try_into().unwrap());
        if let Ok(m) = std::str::from_utf8(map) {
            store.put_merge(m, key, value, 0, stamp, true);
        }
        off = o3 + 8;
    }
}

fn load_aux(body: &[u8], store: &Store) {
    if body.len() < 4 {
        return;
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let mut off = 4;
    for _ in 0..count {
        if off >= body.len() {
            break;
        }
        let kind = body[off];
        off += 1;
        let Some((name, o1)) = get_bytes(body, off) else {
            break;
        };
        let Some((state, o2)) = get_bytes(body, o1) else {
            break;
        };
        if let Ok(n) = std::str::from_utf8(name) {
            store.install_aux(kind, n, state);
        }
        off = o2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!("bonsai-snap-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&d);
        d
    }

    #[test]
    fn snapshot_roundtrip_and_atomic() {
        let path = tmp("rt");
        let src = Store::new();
        src.put("a", b"k1".to_vec(), b"v1".to_vec());
        src.put("a", b"k2".to_vec(), b"v2".to_vec());
        src.put("b", b"k3".to_vec(), b"v3".to_vec());
        write_snapshot(&path, &src).unwrap();
        assert!(
            !path.with_extension("tmp").exists(),
            "tmp removed after rename"
        );

        let dst = Store::new();
        load_snapshot(&path, &dst).unwrap();
        assert_eq!(dst.get("a", b"k1"), Some(b"v1".to_vec()));
        assert_eq!(dst.get("a", b"k2"), Some(b"v2".to_vec()));
        assert_eq!(dst.get("b", b"k3"), Some(b"v3".to_vec()));
        std::fs::remove_file(&path).ok();
    }
}
