//! `IAtomicReference` as a deterministic replicated state machine: a named cell
//! holding an opaque byte value (or null). Mirrors [`crate::atomiclong`] but the
//! value is `Option<Vec<u8>>` (serialized `Data`), so replies are `Data`/`Bool`.

use crate::cp::CpReply;
use std::collections::HashMap;

/// An AtomicReference operation. Values are opaque bytes; `None` is the null ref.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArOp {
    Get,
    Set(Option<Vec<u8>>),
    GetAndSet(Option<Vec<u8>>),
    /// `(expected, update)` — set to `update` iff the current value equals `expected`.
    CompareAndSet(Option<Vec<u8>>, Option<Vec<u8>>),
    Contains(Option<Vec<u8>>),
    IsNull,
    Clear,
}

const TAG_GET: u8 = 0;
const TAG_SET: u8 = 1;
const TAG_GET_AND_SET: u8 = 2;
const TAG_COMPARE_AND_SET: u8 = 3;
const TAG_CONTAINS: u8 = 4;
const TAG_IS_NULL: u8 = 5;
const TAG_CLEAR: u8 = 6;

fn put_opt(buf: &mut Vec<u8>, v: &Option<Vec<u8>>) {
    match v {
        Some(b) => {
            buf.push(1);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        None => buf.push(0),
    }
}

fn get_opt(b: &[u8], p: &mut usize) -> Option<Option<Vec<u8>>> {
    let tag = *b.get(*p)?;
    *p += 1;
    if tag == 0 {
        return Some(None);
    }
    let len = u32::from_le_bytes(b.get(*p..*p + 4)?.try_into().ok()?) as usize;
    *p += 4;
    let v = b.get(*p..*p + len)?.to_vec();
    *p += len;
    Some(Some(v))
}

/// Encode `(name, op)` into a command body: `[tag][args...][name]`.
pub fn encode(name: &str, op: &ArOp) -> Vec<u8> {
    let mut buf = Vec::new();
    match op {
        ArOp::Get => buf.push(TAG_GET),
        ArOp::Set(v) => {
            buf.push(TAG_SET);
            put_opt(&mut buf, v);
        }
        ArOp::GetAndSet(v) => {
            buf.push(TAG_GET_AND_SET);
            put_opt(&mut buf, v);
        }
        ArOp::CompareAndSet(e, u) => {
            buf.push(TAG_COMPARE_AND_SET);
            put_opt(&mut buf, e);
            put_opt(&mut buf, u);
        }
        ArOp::Contains(v) => {
            buf.push(TAG_CONTAINS);
            put_opt(&mut buf, v);
        }
        ArOp::IsNull => buf.push(TAG_IS_NULL),
        ArOp::Clear => buf.push(TAG_CLEAR),
    }
    // Name terminates the command (rest of the buffer).
    buf.extend_from_slice(name.as_bytes());
    buf
}

/// Decode a command body produced by [`encode`].
pub fn decode(body: &[u8]) -> Option<(String, ArOp)> {
    let tag = *body.first()?;
    let mut p = 1;
    let op = match tag {
        TAG_GET => ArOp::Get,
        TAG_SET => ArOp::Set(get_opt(body, &mut p)?),
        TAG_GET_AND_SET => ArOp::GetAndSet(get_opt(body, &mut p)?),
        TAG_COMPARE_AND_SET => ArOp::CompareAndSet(get_opt(body, &mut p)?, get_opt(body, &mut p)?),
        TAG_CONTAINS => ArOp::Contains(get_opt(body, &mut p)?),
        TAG_IS_NULL => ArOp::IsNull,
        TAG_CLEAR => ArOp::Clear,
        _ => return None,
    };
    let name = std::str::from_utf8(&body[p..]).ok()?.to_string();
    Some((name, op))
}

/// The replicated AtomicReference state machine.
#[derive(Default)]
pub struct AtomicReferenceSm {
    values: HashMap<String, Vec<u8>>, // absent key == null ref
}

impl AtomicReferenceSm {
    pub fn new() -> AtomicReferenceSm {
        AtomicReferenceSm::default()
    }

    /// Apply a committed command (deterministic). Unknown commands are a no-op.
    pub fn apply(&mut self, body: &[u8]) -> CpReply {
        let Some((name, op)) = decode(body) else {
            return CpReply::Nil;
        };
        match op {
            ArOp::Get => CpReply::Data(self.values.get(&name).cloned()),
            ArOp::Set(v) => {
                self.store(&name, v);
                CpReply::Nil
            }
            ArOp::GetAndSet(v) => {
                let old = self.values.get(&name).cloned();
                self.store(&name, v);
                CpReply::Data(old)
            }
            ArOp::CompareAndSet(expected, update) => {
                let cur = self.values.get(&name).cloned();
                if cur == expected {
                    self.store(&name, update);
                    CpReply::Bool(true)
                } else {
                    CpReply::Bool(false)
                }
            }
            ArOp::Contains(v) => CpReply::Bool(self.values.get(&name).cloned() == v),
            ArOp::IsNull => CpReply::Bool(!self.values.contains_key(&name)),
            ArOp::Clear => {
                self.values.remove(&name);
                CpReply::Nil
            }
        }
    }

    fn store(&mut self, name: &str, v: Option<Vec<u8>>) {
        match v {
            Some(b) => {
                self.values.insert(name.to_string(), b);
            }
            None => {
                self.values.remove(name);
            }
        }
    }

    /// Current value of `name` (None if null/unset).
    pub fn get(&self, name: &str) -> Option<Vec<u8>> {
        self.values.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for op in [
            ArOp::Get,
            ArOp::Set(Some(b"v".to_vec())),
            ArOp::Set(None),
            ArOp::GetAndSet(Some(b"x".to_vec())),
            ArOp::CompareAndSet(Some(b"a".to_vec()), Some(b"b".to_vec())),
            ArOp::CompareAndSet(None, Some(b"b".to_vec())),
            ArOp::Contains(Some(b"c".to_vec())),
            ArOp::IsNull,
            ArOp::Clear,
        ] {
            let b = encode("ref", &op);
            let (name, decoded) = decode(&b).unwrap();
            assert_eq!(name, "ref");
            assert_eq!(decoded, op);
        }
    }

    #[test]
    fn apply_semantics() {
        let mut sm = AtomicReferenceSm::new();
        assert_eq!(sm.apply(&encode("r", &ArOp::IsNull)), CpReply::Bool(true));
        assert_eq!(sm.apply(&encode("r", &ArOp::Get)), CpReply::Data(None));
        assert_eq!(
            sm.apply(&encode("r", &ArOp::Set(Some(b"hi".to_vec())))),
            CpReply::Nil
        );
        assert_eq!(
            sm.apply(&encode("r", &ArOp::Get)),
            CpReply::Data(Some(b"hi".to_vec()))
        );
        assert_eq!(
            sm.apply(&encode(
                "r",
                &ArOp::CompareAndSet(Some(b"hi".to_vec()), Some(b"bye".to_vec()))
            )),
            CpReply::Bool(true)
        );
        assert_eq!(
            sm.apply(&encode(
                "r",
                &ArOp::CompareAndSet(Some(b"hi".to_vec()), None)
            )),
            CpReply::Bool(false)
        );
        assert_eq!(
            sm.apply(&encode("r", &ArOp::GetAndSet(None))),
            CpReply::Data(Some(b"bye".to_vec()))
        );
        assert_eq!(sm.apply(&encode("r", &ArOp::IsNull)), CpReply::Bool(true));
    }
}
