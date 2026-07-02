//! `IAtomicLong` as a deterministic replicated state machine over Raft.
//!
//! A command is `(name, op)` encoded into a Raft entry's bytes; `apply` runs it
//! against the replicated `HashMap<String, i64>` and returns the reply. Because
//! Raft delivers a single committed total order and `apply` is deterministic,
//! every replica converges to identical state — the linearizable guarantee (see
//! `tests/linearizability.rs`). The raft consensus core does not depend on this
//! module; it is the first CP state machine (server-side wiring is Phase C).

use std::collections::HashMap;

/// An AtomicLong operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlOp {
    Get,
    Set(i64),
    GetAndSet(i64),
    AddAndGet(i64),
    GetAndAdd(i64),
    /// `(expected, new)` — set to `new` iff the current value equals `expected`.
    CompareAndSet(i64, i64),
}

/// The reply from applying an op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlReply {
    Long(i64),
    Bool(bool),
    None,
}

const TAG_GET: u8 = 0;
const TAG_SET: u8 = 1;
const TAG_GET_AND_SET: u8 = 2;
const TAG_ADD_AND_GET: u8 = 3;
const TAG_GET_AND_ADD: u8 = 4;
const TAG_COMPARE_AND_SET: u8 = 5;

/// Encode `(name, op)` into a Raft entry command: `[tag][a:i64][b:i64][name]`.
pub fn encode(name: &str, op: &AlOp) -> Vec<u8> {
    let (tag, a, b) = match *op {
        AlOp::Get => (TAG_GET, 0, 0),
        AlOp::Set(v) => (TAG_SET, v, 0),
        AlOp::GetAndSet(v) => (TAG_GET_AND_SET, v, 0),
        AlOp::AddAndGet(d) => (TAG_ADD_AND_GET, d, 0),
        AlOp::GetAndAdd(d) => (TAG_GET_AND_ADD, d, 0),
        AlOp::CompareAndSet(e, n) => (TAG_COMPARE_AND_SET, e, n),
    };
    let mut buf = Vec::with_capacity(17 + name.len());
    buf.push(tag);
    buf.extend_from_slice(&a.to_le_bytes());
    buf.extend_from_slice(&b.to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf
}

/// Decode a command produced by [`encode`].
pub fn decode(bytes: &[u8]) -> Option<(String, AlOp)> {
    if bytes.len() < 17 {
        return None;
    }
    let tag = bytes[0];
    let a = i64::from_le_bytes(bytes[1..9].try_into().ok()?);
    let b = i64::from_le_bytes(bytes[9..17].try_into().ok()?);
    let name = std::str::from_utf8(&bytes[17..]).ok()?.to_string();
    let op = match tag {
        TAG_GET => AlOp::Get,
        TAG_SET => AlOp::Set(a),
        TAG_GET_AND_SET => AlOp::GetAndSet(a),
        TAG_ADD_AND_GET => AlOp::AddAndGet(a),
        TAG_GET_AND_ADD => AlOp::GetAndAdd(a),
        TAG_COMPARE_AND_SET => AlOp::CompareAndSet(a, b),
        _ => return None,
    };
    Some((name, op))
}

/// The replicated AtomicLong state machine.
#[derive(Default)]
pub struct AtomicLongSm {
    values: HashMap<String, i64>,
}

impl AtomicLongSm {
    pub fn new() -> AtomicLongSm {
        AtomicLongSm::default()
    }

    /// Apply a committed command (deterministic). Unknown commands are a no-op.
    pub fn apply(&mut self, command: &[u8]) -> AlReply {
        let Some((name, op)) = decode(command) else {
            return AlReply::None;
        };
        let v = self.values.entry(name).or_insert(0);
        match op {
            AlOp::Get => AlReply::Long(*v),
            AlOp::Set(n) => {
                *v = n;
                AlReply::None
            }
            AlOp::GetAndSet(n) => {
                let old = *v;
                *v = n;
                AlReply::Long(old)
            }
            AlOp::AddAndGet(d) => {
                *v += d;
                AlReply::Long(*v)
            }
            AlOp::GetAndAdd(d) => {
                let old = *v;
                *v += d;
                AlReply::Long(old)
            }
            AlOp::CompareAndSet(e, n) => {
                if *v == e {
                    *v = n;
                    AlReply::Bool(true)
                } else {
                    AlReply::Bool(false)
                }
            }
        }
    }

    /// Current value of `name` (0 if never set).
    pub fn get(&self, name: &str) -> i64 {
        self.values.get(name).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        for op in [
            AlOp::Get,
            AlOp::Set(42),
            AlOp::GetAndSet(-7),
            AlOp::AddAndGet(3),
            AlOp::GetAndAdd(9),
            AlOp::CompareAndSet(1, 2),
        ] {
            let b = encode("counter", &op);
            let (name, decoded) = decode(&b).unwrap();
            assert_eq!(name, "counter");
            assert_eq!(decoded, op);
        }
    }

    #[test]
    fn apply_semantics() {
        let mut sm = AtomicLongSm::new();
        assert_eq!(sm.apply(&encode("c", &AlOp::Get)), AlReply::Long(0));
        assert_eq!(
            sm.apply(&encode("c", &AlOp::AddAndGet(5))),
            AlReply::Long(5)
        );
        assert_eq!(
            sm.apply(&encode("c", &AlOp::GetAndAdd(3))),
            AlReply::Long(5)
        );
        assert_eq!(sm.get("c"), 8);
        assert_eq!(
            sm.apply(&encode("c", &AlOp::GetAndSet(100))),
            AlReply::Long(8)
        );
        assert_eq!(
            sm.apply(&encode("c", &AlOp::CompareAndSet(100, 200))),
            AlReply::Bool(true)
        );
        assert_eq!(
            sm.apply(&encode("c", &AlOp::CompareAndSet(100, 999))),
            AlReply::Bool(false)
        );
        assert_eq!(sm.get("c"), 200);
    }

    #[test]
    fn keys_are_independent() {
        let mut sm = AtomicLongSm::new();
        sm.apply(&encode("a", &AlOp::Set(1)));
        sm.apply(&encode("b", &AlOp::Set(2)));
        assert_eq!(sm.get("a"), 1);
        assert_eq!(sm.get("b"), 2);
    }
}
