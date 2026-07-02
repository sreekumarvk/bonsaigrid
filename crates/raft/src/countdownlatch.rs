//! `ICountDownLatch` as a deterministic replicated state machine: a named counter
//! that only counts down. Blocking `await` is a follow-up (needs CP sessions /
//! waiter tracking); v1 covers the linearizable mutating + read ops.

use crate::cp::CpReply;
use std::collections::HashMap;

/// A CountDownLatch operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CdlOp {
    GetCount,
    CountDown,
    /// Set the count, but only if it is currently zero (Hazelcast semantics).
    TrySetCount(i32),
}

const TAG_GET: u8 = 0;
const TAG_COUNT_DOWN: u8 = 1;
const TAG_TRY_SET: u8 = 2;

/// Encode `(name, op)` into a command body: `[tag][i32?][name]`.
pub fn encode(name: &str, op: &CdlOp) -> Vec<u8> {
    let mut buf = Vec::new();
    match *op {
        CdlOp::GetCount => buf.push(TAG_GET),
        CdlOp::CountDown => buf.push(TAG_COUNT_DOWN),
        CdlOp::TrySetCount(n) => {
            buf.push(TAG_TRY_SET);
            buf.extend_from_slice(&n.to_le_bytes());
        }
    }
    buf.extend_from_slice(name.as_bytes());
    buf
}

pub fn decode(body: &[u8]) -> Option<(String, CdlOp)> {
    let tag = *body.first()?;
    let mut p = 1;
    let op = match tag {
        TAG_GET => CdlOp::GetCount,
        TAG_COUNT_DOWN => CdlOp::CountDown,
        TAG_TRY_SET => {
            let n = i32::from_le_bytes(body.get(1..5)?.try_into().ok()?);
            p = 5;
            CdlOp::TrySetCount(n)
        }
        _ => return None,
    };
    let name = std::str::from_utf8(&body[p..]).ok()?.to_string();
    Some((name, op))
}

/// The replicated CountDownLatch state machine.
#[derive(Default)]
pub struct CountDownLatchSm {
    counts: HashMap<String, i32>,
}

impl CountDownLatchSm {
    pub fn new() -> CountDownLatchSm {
        CountDownLatchSm::default()
    }

    pub fn apply(&mut self, body: &[u8]) -> CpReply {
        let Some((name, op)) = decode(body) else {
            return CpReply::Nil;
        };
        match op {
            CdlOp::GetCount => CpReply::Long(*self.counts.get(&name).unwrap_or(&0) as i64),
            CdlOp::CountDown => {
                let c = self.counts.entry(name).or_insert(0);
                *c = (*c - 1).max(0);
                CpReply::Nil
            }
            CdlOp::TrySetCount(n) => {
                let c = self.counts.entry(name).or_insert(0);
                if *c == 0 {
                    *c = n.max(0);
                    CpReply::Bool(true)
                } else {
                    CpReply::Bool(false)
                }
            }
        }
    }

    pub fn count(&self, name: &str) -> i32 {
        *self.counts.get(name).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_semantics() {
        for op in [CdlOp::GetCount, CdlOp::CountDown, CdlOp::TrySetCount(3)] {
            let (n, d) = decode(&encode("l", &op)).unwrap();
            assert_eq!(n, "l");
            assert_eq!(d, op);
        }
        let mut sm = CountDownLatchSm::new();
        assert_eq!(
            sm.apply(&encode("l", &CdlOp::TrySetCount(2))),
            CpReply::Bool(true)
        );
        // TrySetCount fails while the latch is still counting.
        assert_eq!(
            sm.apply(&encode("l", &CdlOp::TrySetCount(9))),
            CpReply::Bool(false)
        );
        assert_eq!(sm.apply(&encode("l", &CdlOp::GetCount)), CpReply::Long(2));
        sm.apply(&encode("l", &CdlOp::CountDown));
        sm.apply(&encode("l", &CdlOp::CountDown));
        assert_eq!(sm.count("l"), 0);
        sm.apply(&encode("l", &CdlOp::CountDown)); // floors at 0
        assert_eq!(sm.count("l"), 0);
    }
}
