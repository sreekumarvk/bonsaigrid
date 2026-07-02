//! Structure-agnostic WAL record envelope.
//!
//! Frame: `[len: u32][crc32: u32][record_type: u16][payload: len-2 bytes]`.
//! `len` counts `record_type + payload`; `crc32` covers those same bytes so
//! recovery can detect a torn/corrupt tail from a crash mid-write. New record
//! types (Phase B structures) are additive — no format change.

/// What kind of mutation a record carries. New variants are appended (the
/// numeric tag is the on-disk identity and must stay stable).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum RecordType {
    MapPut = 1,
    MapRemove = 2,
}

impl RecordType {
    fn from_u16(v: u16) -> Option<RecordType> {
        Some(match v {
            1 => RecordType::MapPut,
            2 => RecordType::MapRemove,
            _ => return None,
        })
    }
}

/// Result of decoding the next record from a byte stream.
#[derive(Debug)]
pub enum Decoded<'a> {
    /// A complete, CRC-valid record. `consumed` is the full frame length.
    Record {
        rtype: RecordType,
        payload: &'a [u8],
        consumed: usize,
    },
    /// Not enough bytes yet for a full frame — read more.
    NeedMore,
    /// A corrupt/torn frame (bad CRC, unknown type, or overrunning length): stop.
    Torn,
}

const HEADER: usize = 8; // len(4) + crc(4)

/// Append a framed record (`rtype` + `payload`) to `buf`, computing the CRC.
fn frame(buf: &mut Vec<u8>, rtype: RecordType, payload: &[u8]) {
    let body_len = 2 + payload.len(); // record_type + payload
    let start = buf.len();
    buf.extend_from_slice(&(body_len as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // crc placeholder
    buf.extend_from_slice(&(rtype as u16).to_le_bytes());
    buf.extend_from_slice(payload);
    let crc = crc32fast::hash(&buf[start + HEADER..]); // over record_type + payload
    buf[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// Append a length-prefixed byte string.
fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

/// Read a length-prefixed byte string at `off`; returns `(slice, next_off)`.
fn get_bytes(payload: &[u8], off: usize) -> Option<(&[u8], usize)> {
    let end = off.checked_add(4)?;
    if end > payload.len() {
        return None;
    }
    let n = u32::from_le_bytes(payload[off..end].try_into().ok()?) as usize;
    let data_end = end.checked_add(n)?;
    if data_end > payload.len() {
        return None;
    }
    Some((&payload[end..data_end], data_end))
}

/// Encode a `MapPut` record into `buf`. Payload: `stamp | ttl_ms | map | key | value`.
pub fn encode_map_put(
    buf: &mut Vec<u8>,
    stamp: u64,
    ttl_ms: u64,
    map: &str,
    key: &[u8],
    value: &[u8],
) {
    let mut p = Vec::with_capacity(16 + map.len() + key.len() + value.len() + 12);
    p.extend_from_slice(&stamp.to_le_bytes());
    p.extend_from_slice(&ttl_ms.to_le_bytes());
    put_bytes(&mut p, map.as_bytes());
    put_bytes(&mut p, key);
    put_bytes(&mut p, value);
    frame(buf, RecordType::MapPut, &p);
}

/// Encode a `MapRemove` record into `buf`. Payload: `stamp | map | key`.
pub fn encode_map_remove(buf: &mut Vec<u8>, stamp: u64, map: &str, key: &[u8]) {
    let mut p = Vec::with_capacity(8 + map.len() + key.len() + 8);
    p.extend_from_slice(&stamp.to_le_bytes());
    put_bytes(&mut p, map.as_bytes());
    put_bytes(&mut p, key);
    frame(buf, RecordType::MapRemove, &p);
}

/// Decoded `MapPut` fields.
pub struct MapPut<'a> {
    pub stamp: u64,
    pub ttl_ms: u64,
    pub map: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Decoded `MapRemove` fields.
pub struct MapRemove<'a> {
    pub stamp: u64,
    pub map: &'a str,
    pub key: &'a [u8],
}

pub fn parse_map_put(payload: &[u8]) -> Option<MapPut<'_>> {
    if payload.len() < 16 {
        return None;
    }
    let stamp = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let ttl_ms = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let (map, o1) = get_bytes(payload, 16)?;
    let (key, o2) = get_bytes(payload, o1)?;
    let (value, _) = get_bytes(payload, o2)?;
    Some(MapPut {
        stamp,
        ttl_ms,
        map: std::str::from_utf8(map).ok()?,
        key,
        value,
    })
}

pub fn parse_map_remove(payload: &[u8]) -> Option<MapRemove<'_>> {
    if payload.len() < 8 {
        return None;
    }
    let stamp = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let (map, o1) = get_bytes(payload, 8)?;
    let (key, _) = get_bytes(payload, o1)?;
    Some(MapRemove {
        stamp,
        map: std::str::from_utf8(map).ok()?,
        key,
    })
}

/// Decode the next record at the start of `bytes`.
pub fn decode_record(bytes: &[u8]) -> Decoded<'_> {
    if bytes.len() < HEADER {
        return Decoded::NeedMore;
    }
    let body_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let frame_len = HEADER + body_len;
    if body_len < 2 {
        return Decoded::Torn; // must at least carry record_type
    }
    if bytes.len() < frame_len {
        return Decoded::NeedMore;
    }
    let body = &bytes[HEADER..frame_len];
    if crc32fast::hash(body) != crc {
        return Decoded::Torn;
    }
    let rtype = match RecordType::from_u16(u16::from_le_bytes(body[0..2].try_into().unwrap())) {
        Some(t) => t,
        None => return Decoded::Torn,
    };
    Decoded::Record {
        rtype,
        payload: &body[2..],
        consumed: frame_len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_put_roundtrip() {
        let mut buf = Vec::new();
        encode_map_put(&mut buf, 42, 1000, "orders", b"k1", b"value1");
        match decode_record(&buf) {
            Decoded::Record {
                rtype,
                payload,
                consumed,
            } => {
                assert_eq!(rtype, RecordType::MapPut);
                assert_eq!(consumed, buf.len());
                let mp = parse_map_put(payload).unwrap();
                assert_eq!(mp.stamp, 42);
                assert_eq!(mp.ttl_ms, 1000);
                assert_eq!(mp.map, "orders");
                assert_eq!(mp.key, b"k1");
                assert_eq!(mp.value, b"value1");
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn map_remove_roundtrip() {
        let mut buf = Vec::new();
        encode_map_remove(&mut buf, 7, "m", b"gone");
        match decode_record(&buf) {
            Decoded::Record { rtype, payload, .. } => {
                assert_eq!(rtype, RecordType::MapRemove);
                let mr = parse_map_remove(payload).unwrap();
                assert_eq!(mr.stamp, 7);
                assert_eq!(mr.map, "m");
                assert_eq!(mr.key, b"gone");
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn two_records_decode_sequentially() {
        let mut buf = Vec::new();
        encode_map_put(&mut buf, 1, 0, "m", b"a", b"1");
        encode_map_put(&mut buf, 2, 0, "m", b"b", b"2");
        let Decoded::Record { consumed: c1, .. } = decode_record(&buf) else {
            panic!("first record");
        };
        let Decoded::Record {
            payload, consumed, ..
        } = decode_record(&buf[c1..])
        else {
            panic!("second record");
        };
        assert_eq!(c1 + consumed, buf.len());
        assert_eq!(parse_map_put(payload).unwrap().key, b"b");
    }

    #[test]
    fn truncated_is_need_more() {
        let mut buf = Vec::new();
        encode_map_put(&mut buf, 1, 0, "m", b"k", b"v");
        buf.truncate(buf.len() - 3);
        assert!(matches!(decode_record(&buf), Decoded::NeedMore));
    }

    #[test]
    fn corrupt_payload_is_torn() {
        let mut buf = Vec::new();
        encode_map_put(&mut buf, 1, 0, "m", b"k", b"v");
        let last = buf.len() - 1;
        buf[last] ^= 0xFF; // flip a payload byte → CRC mismatch
        assert!(matches!(decode_record(&buf), Decoded::Torn));
    }
}
