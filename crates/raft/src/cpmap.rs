//! `CPMap` — a linearizable key/value map as a deterministic replicated state
//! machine over Raft (Hazelcast's CP `CPMap`). Every op is a committed command, so
//! all replicas apply the same sequence and reads observe a linearizable snapshot.
//! Keys and values are opaque bytes.

use crate::cp::CpReply;
use std::collections::HashMap;

const TAG_GET: u8 = 0;
const TAG_PUT: u8 = 1;
const TAG_SET: u8 = 2;
const TAG_PUT_IF_ABSENT: u8 = 3;
const TAG_REMOVE: u8 = 4;
const TAG_REMOVE_IF_EQ: u8 = 5;
const TAG_REPLACE: u8 = 6;
const TAG_CAS: u8 = 7;
const TAG_CONTAINS: u8 = 8;
const TAG_SIZE: u8 = 9;
const TAG_CLEAR: u8 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapOp {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
    PutIfAbsent(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
    RemoveIfEquals(Vec<u8>, Vec<u8>),
    Replace(Vec<u8>, Vec<u8>),
    CompareAndSet(Vec<u8>, Vec<u8>, Vec<u8>), // key, expected, new
    ContainsKey(Vec<u8>),
    Size,
    Clear,
}

fn put_blob(b: &mut Vec<u8>, x: &[u8]) {
    b.extend_from_slice(&(x.len() as u32).to_le_bytes());
    b.extend_from_slice(x);
}
fn get_blob(b: &[u8], p: &mut usize) -> Option<Vec<u8>> {
    let n = u32::from_le_bytes(b.get(*p..*p + 4)?.try_into().ok()?) as usize;
    *p += 4;
    let s = b.get(*p..*p + n)?.to_vec();
    *p += n;
    Some(s)
}

/// Encode a `[tag][name][args…]` command body (the `OBJ_CP_MAP` prefix is added by
/// `cp::cm_command`).
pub fn encode(name: &str, op: &MapOp) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(match op {
        MapOp::Get(_) => TAG_GET,
        MapOp::Put(..) => TAG_PUT,
        MapOp::Set(..) => TAG_SET,
        MapOp::PutIfAbsent(..) => TAG_PUT_IF_ABSENT,
        MapOp::Remove(_) => TAG_REMOVE,
        MapOp::RemoveIfEquals(..) => TAG_REMOVE_IF_EQ,
        MapOp::Replace(..) => TAG_REPLACE,
        MapOp::CompareAndSet(..) => TAG_CAS,
        MapOp::ContainsKey(_) => TAG_CONTAINS,
        MapOp::Size => TAG_SIZE,
        MapOp::Clear => TAG_CLEAR,
    });
    put_blob(&mut b, name.as_bytes());
    match op {
        MapOp::Get(k) | MapOp::Remove(k) | MapOp::ContainsKey(k) => put_blob(&mut b, k),
        MapOp::Put(k, v)
        | MapOp::Set(k, v)
        | MapOp::PutIfAbsent(k, v)
        | MapOp::RemoveIfEquals(k, v)
        | MapOp::Replace(k, v) => {
            put_blob(&mut b, k);
            put_blob(&mut b, v);
        }
        MapOp::CompareAndSet(k, e, n) => {
            put_blob(&mut b, k);
            put_blob(&mut b, e);
            put_blob(&mut b, n);
        }
        MapOp::Size | MapOp::Clear => {}
    }
    b
}

pub fn decode(bytes: &[u8]) -> Option<(String, MapOp)> {
    let (&tag, rest) = bytes.split_first()?;
    let mut p = 0;
    let name = String::from_utf8(get_blob(rest, &mut p)?).ok()?;
    let op = match tag {
        TAG_GET => MapOp::Get(get_blob(rest, &mut p)?),
        TAG_PUT => MapOp::Put(get_blob(rest, &mut p)?, get_blob(rest, &mut p)?),
        TAG_SET => MapOp::Set(get_blob(rest, &mut p)?, get_blob(rest, &mut p)?),
        TAG_PUT_IF_ABSENT => MapOp::PutIfAbsent(get_blob(rest, &mut p)?, get_blob(rest, &mut p)?),
        TAG_REMOVE => MapOp::Remove(get_blob(rest, &mut p)?),
        TAG_REMOVE_IF_EQ => MapOp::RemoveIfEquals(get_blob(rest, &mut p)?, get_blob(rest, &mut p)?),
        TAG_REPLACE => MapOp::Replace(get_blob(rest, &mut p)?, get_blob(rest, &mut p)?),
        TAG_CAS => MapOp::CompareAndSet(
            get_blob(rest, &mut p)?,
            get_blob(rest, &mut p)?,
            get_blob(rest, &mut p)?,
        ),
        TAG_CONTAINS => MapOp::ContainsKey(get_blob(rest, &mut p)?),
        TAG_SIZE => MapOp::Size,
        TAG_CLEAR => MapOp::Clear,
        _ => return None,
    };
    Some((name, op))
}

/// The replicated CPMap state machine (one keyspace per named map).
#[derive(Default)]
pub struct CpMapSm {
    maps: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
}

impl CpMapSm {
    pub fn new() -> CpMapSm {
        CpMapSm::default()
    }

    /// Apply a committed command (deterministic). Unknown commands are a no-op.
    pub fn apply(&mut self, body: &[u8]) -> CpReply {
        let Some((name, op)) = decode(body) else {
            return CpReply::Nil;
        };
        let m = self.maps.entry(name).or_default();
        match op {
            MapOp::Get(k) => CpReply::Data(m.get(&k).cloned()),
            MapOp::Put(k, v) => CpReply::Data(m.insert(k, v)),
            MapOp::Set(k, v) => {
                m.insert(k, v);
                CpReply::Nil
            }
            MapOp::PutIfAbsent(k, v) => match m.get(&k) {
                Some(existing) => CpReply::Data(Some(existing.clone())),
                None => {
                    m.insert(k, v);
                    CpReply::Data(None)
                }
            },
            MapOp::Remove(k) => CpReply::Data(m.remove(&k)),
            MapOp::RemoveIfEquals(k, v) => {
                if m.get(&k) == Some(&v) {
                    m.remove(&k);
                    CpReply::Bool(true)
                } else {
                    CpReply::Bool(false)
                }
            }
            MapOp::Replace(k, v) => {
                if m.contains_key(&k) {
                    CpReply::Data(m.insert(k, v))
                } else {
                    CpReply::Data(None)
                }
            }
            MapOp::CompareAndSet(k, e, n) => {
                if m.get(&k) == Some(&e) {
                    m.insert(k, n);
                    CpReply::Bool(true)
                } else {
                    CpReply::Bool(false)
                }
            }
            MapOp::ContainsKey(k) => CpReply::Bool(m.contains_key(&k)),
            MapOp::Size => CpReply::Long(m.len() as i64),
            MapOp::Clear => {
                m.clear();
                CpReply::Nil
            }
        }
    }

    pub fn get(&self, name: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.maps.get(name).and_then(|m| m.get(key).cloned())
    }
    pub fn size(&self, name: &str) -> usize {
        self.maps.get(name).map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let ops = [
            MapOp::Get(b"k".to_vec()),
            MapOp::Put(b"k".to_vec(), b"v".to_vec()),
            MapOp::Set(b"k".to_vec(), b"v".to_vec()),
            MapOp::PutIfAbsent(b"k".to_vec(), b"v".to_vec()),
            MapOp::Remove(b"k".to_vec()),
            MapOp::RemoveIfEquals(b"k".to_vec(), b"v".to_vec()),
            MapOp::Replace(b"k".to_vec(), b"v".to_vec()),
            MapOp::CompareAndSet(b"k".to_vec(), b"e".to_vec(), b"n".to_vec()),
            MapOp::ContainsKey(b"k".to_vec()),
            MapOp::Size,
            MapOp::Clear,
        ];
        for op in ops {
            let b = encode("m", &op);
            let (name, got) = decode(&b).unwrap();
            assert_eq!(name, "m");
            assert_eq!(got, op);
        }
    }

    #[test]
    fn linearizable_map_semantics() {
        let mut sm = CpMapSm::new();
        let ap = |sm: &mut CpMapSm, op: MapOp| sm.apply(&encode("m", &op));

        // put returns previous (None first, then old)
        assert_eq!(
            ap(&mut sm, MapOp::Put(b"a".to_vec(), b"1".to_vec())),
            CpReply::Data(None)
        );
        assert_eq!(
            ap(&mut sm, MapOp::Put(b"a".to_vec(), b"2".to_vec())),
            CpReply::Data(Some(b"1".to_vec()))
        );
        assert_eq!(
            ap(&mut sm, MapOp::Get(b"a".to_vec())),
            CpReply::Data(Some(b"2".to_vec()))
        );

        // putIfAbsent keeps existing
        assert_eq!(
            ap(&mut sm, MapOp::PutIfAbsent(b"a".to_vec(), b"9".to_vec())),
            CpReply::Data(Some(b"2".to_vec()))
        );
        assert_eq!(
            ap(&mut sm, MapOp::PutIfAbsent(b"b".to_vec(), b"3".to_vec())),
            CpReply::Data(None)
        );

        // CAS
        assert_eq!(
            ap(
                &mut sm,
                MapOp::CompareAndSet(b"a".to_vec(), b"2".to_vec(), b"4".to_vec())
            ),
            CpReply::Bool(true)
        );
        assert_eq!(
            ap(
                &mut sm,
                MapOp::CompareAndSet(b"a".to_vec(), b"2".to_vec(), b"5".to_vec())
            ),
            CpReply::Bool(false)
        );
        assert_eq!(sm.get("m", b"a"), Some(b"4".to_vec()));

        // replace only if present
        assert_eq!(
            ap(&mut sm, MapOp::Replace(b"absent".to_vec(), b"x".to_vec())),
            CpReply::Data(None)
        );
        assert_eq!(sm.get("m", b"absent"), None);

        // removeIfEquals + size + contains + clear
        assert_eq!(
            ap(&mut sm, MapOp::ContainsKey(b"a".to_vec())),
            CpReply::Bool(true)
        );
        assert_eq!(ap(&mut sm, MapOp::Size), CpReply::Long(2));
        assert_eq!(
            ap(
                &mut sm,
                MapOp::RemoveIfEquals(b"a".to_vec(), b"WRONG".to_vec())
            ),
            CpReply::Bool(false)
        );
        assert_eq!(
            ap(&mut sm, MapOp::RemoveIfEquals(b"a".to_vec(), b"4".to_vec())),
            CpReply::Bool(true)
        );
        assert_eq!(
            ap(&mut sm, MapOp::Remove(b"b".to_vec())),
            CpReply::Data(Some(b"3".to_vec()))
        );
        assert_eq!(ap(&mut sm, MapOp::Size), CpReply::Long(0));
        ap(&mut sm, MapOp::Put(b"x".to_vec(), b"1".to_vec()));
        assert_eq!(ap(&mut sm, MapOp::Clear), CpReply::Nil);
        assert_eq!(sm.size("m"), 0);
    }
}
