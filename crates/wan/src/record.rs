//! `WanRecord` frame codec: `[len:u32][crc32:u32][op:u8][stamp:u64][ttl:u64]
//! [map][key][value]` (little-endian; each blob length-prefixed; CRC over
//! `op..value`; `len` counts crc+body). Mirrors the persistence WAL discipline.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WanOp {
    Put,
    Remove,
    /// A full non-map structure state (`kind` = one of the store's `AUX_*`
    /// constants). For an aux record `map` carries the structure name, `value`
    /// the serialized state, and `key` is empty.
    Aux(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WanRecord {
    pub op: WanOp,
    pub stamp: u64,
    pub ttl_ms: u64,
    pub map: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub enum Decoded {
    Record { rec: WanRecord, consumed: usize },
    NeedMore,
    Torn,
}

fn put_blob(b: &mut Vec<u8>, x: &[u8]) {
    b.extend_from_slice(&(x.len() as u32).to_le_bytes());
    b.extend_from_slice(x);
}

fn get_blob(b: &[u8], p: usize) -> Option<(&[u8], usize)> {
    let n = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
    let s = b.get(p + 4..p + 4 + n)?;
    Some((s, p + 4 + n))
}

pub fn encode(rec: &WanRecord) -> Vec<u8> {
    let mut body = Vec::with_capacity(18 + rec.map.len() + rec.key.len() + rec.value.len());
    match rec.op {
        WanOp::Put => body.push(1),
        WanOp::Remove => body.push(2),
        WanOp::Aux(kind) => {
            body.push(3);
            body.push(kind);
        }
    }
    body.extend_from_slice(&rec.stamp.to_le_bytes());
    body.extend_from_slice(&rec.ttl_ms.to_le_bytes());
    put_blob(&mut body, rec.map.as_bytes());
    put_blob(&mut body, &rec.key);
    put_blob(&mut body, &rec.value);
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&((body.len() as u32 + 4).to_le_bytes())); // crc + body
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

pub fn decode(bytes: &[u8]) -> Decoded {
    if bytes.len() < 8 {
        return Decoded::NeedMore;
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let end = 4 + len; // len counts crc(4) + body
    if len < 4 + 17 {
        return Decoded::Torn;
    }
    if bytes.len() < end {
        return Decoded::NeedMore;
    }
    let body = &bytes[8..end];
    if crc32fast::hash(body) != crc {
        return Decoded::Torn;
    }
    // op byte, then (aux only) a kind byte; `base` is where the fixed header starts.
    let (op, base) = match body.first() {
        Some(1) => (WanOp::Put, 1),
        Some(2) => (WanOp::Remove, 1),
        Some(3) => match body.get(1) {
            Some(&kind) => (WanOp::Aux(kind), 2),
            None => return Decoded::Torn,
        },
        _ => return Decoded::Torn,
    };
    if body.len() < base + 16 {
        return Decoded::Torn;
    }
    let stamp = u64::from_le_bytes(body[base..base + 8].try_into().unwrap());
    let ttl_ms = u64::from_le_bytes(body[base + 8..base + 16].try_into().unwrap());
    let Some((map, o1)) = get_blob(body, base + 16) else {
        return Decoded::Torn;
    };
    let Some((key, o2)) = get_blob(body, o1) else {
        return Decoded::Torn;
    };
    let Some((value, _)) = get_blob(body, o2) else {
        return Decoded::Torn;
    };
    let Ok(map) = std::str::from_utf8(map) else {
        return Decoded::Torn;
    };
    Decoded::Record {
        rec: WanRecord {
            op,
            stamp,
            ttl_ms,
            map: map.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        },
        consumed: end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_roundtrips() {
        let rec = WanRecord {
            op: WanOp::Put,
            stamp: 42,
            ttl_ms: 1000,
            map: "m".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let bytes = encode(&rec);
        match decode(&bytes) {
            Decoded::Record { rec: got, consumed } => {
                assert_eq!(got, rec);
                assert_eq!(consumed, bytes.len());
            }
            _ => panic!("expected a decoded record"),
        }
    }

    #[test]
    fn aux_roundtrips_with_kind() {
        let rec = WanRecord {
            op: WanOp::Aux(7),
            stamp: 0,
            ttl_ms: 0,
            map: "myqueue".into(),
            key: Vec::new(),
            value: vec![1, 2, 3, 4],
        };
        let bytes = encode(&rec);
        match decode(&bytes) {
            Decoded::Record { rec: got, consumed } => {
                assert_eq!(got, rec);
                assert_eq!(got.op, WanOp::Aux(7));
                assert_eq!(consumed, bytes.len());
            }
            _ => panic!("expected a decoded aux record"),
        }
    }

    #[test]
    fn short_buffer_needs_more_and_flip_is_torn() {
        let rec = WanRecord {
            op: WanOp::Remove,
            stamp: 7,
            ttl_ms: 0,
            map: "m".into(),
            key: b"k".to_vec(),
            value: Vec::new(),
        };
        let bytes = encode(&rec);
        assert!(matches!(decode(&bytes[..4]), Decoded::NeedMore));
        let mut bad = bytes.clone();
        *bad.last_mut().unwrap() ^= 0xFF;
        assert!(matches!(decode(&bad), Decoded::Torn));
    }
}
