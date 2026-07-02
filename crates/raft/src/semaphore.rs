//! `ISemaphore` as a deterministic replicated state machine: a named permit
//! counter. Blocking `acquire` is a follow-up (needs CP sessions / a wait queue);
//! v1 treats acquire as a non-blocking `tryAcquire` and covers the other ops.

use crate::cp::CpReply;
use std::collections::HashMap;

/// A Semaphore operation. Permit counts are non-negative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemOp {
    /// Initialise the permit count, only if not already initialised.
    Init(i32),
    /// Acquire `n` permits if available (non-blocking in v1).
    Acquire(i32),
    Release(i32),
    /// Take all available permits, returning how many.
    Drain,
    AvailablePermits,
}

const TAG_INIT: u8 = 0;
const TAG_ACQUIRE: u8 = 1;
const TAG_RELEASE: u8 = 2;
const TAG_DRAIN: u8 = 3;
const TAG_AVAILABLE: u8 = 4;

pub fn encode(name: &str, op: &SemOp) -> Vec<u8> {
    let mut buf = Vec::new();
    match *op {
        SemOp::Init(n) => {
            buf.push(TAG_INIT);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        SemOp::Acquire(n) => {
            buf.push(TAG_ACQUIRE);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        SemOp::Release(n) => {
            buf.push(TAG_RELEASE);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        SemOp::Drain => buf.push(TAG_DRAIN),
        SemOp::AvailablePermits => buf.push(TAG_AVAILABLE),
    }
    buf.extend_from_slice(name.as_bytes());
    buf
}

pub fn decode(body: &[u8]) -> Option<(String, SemOp)> {
    let tag = *body.first()?;
    let mut p = 1;
    let n =
        |body: &[u8]| -> Option<i32> { Some(i32::from_le_bytes(body.get(1..5)?.try_into().ok()?)) };
    let op = match tag {
        TAG_INIT => {
            p = 5;
            SemOp::Init(n(body)?)
        }
        TAG_ACQUIRE => {
            p = 5;
            SemOp::Acquire(n(body)?)
        }
        TAG_RELEASE => {
            p = 5;
            SemOp::Release(n(body)?)
        }
        TAG_DRAIN => SemOp::Drain,
        TAG_AVAILABLE => SemOp::AvailablePermits,
        _ => return None,
    };
    let name = std::str::from_utf8(&body[p..]).ok()?.to_string();
    Some((name, op))
}

/// The replicated Semaphore state machine.
#[derive(Default)]
pub struct SemaphoreSm {
    permits: HashMap<String, i32>,
}

impl SemaphoreSm {
    pub fn new() -> SemaphoreSm {
        SemaphoreSm::default()
    }

    pub fn apply(&mut self, body: &[u8]) -> CpReply {
        let Some((name, op)) = decode(body) else {
            return CpReply::Nil;
        };
        match op {
            SemOp::Init(n) => match self.permits.entry(name) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(n.max(0));
                    CpReply::Bool(true)
                }
                std::collections::hash_map::Entry::Occupied(_) => CpReply::Bool(false),
            },
            SemOp::Acquire(n) => {
                let p = self.permits.entry(name).or_insert(0);
                if n >= 0 && *p >= n {
                    *p -= n;
                    CpReply::Bool(true)
                } else {
                    CpReply::Bool(false)
                }
            }
            SemOp::Release(n) => {
                let p = self.permits.entry(name).or_insert(0);
                *p += n.max(0);
                CpReply::Bool(true)
            }
            SemOp::Drain => {
                let p = self.permits.entry(name).or_insert(0);
                let drained = *p;
                *p = 0;
                CpReply::Long(drained as i64)
            }
            SemOp::AvailablePermits => CpReply::Long(*self.permits.get(&name).unwrap_or(&0) as i64),
        }
    }

    pub fn available(&self, name: &str) -> i32 {
        *self.permits.get(name).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_semantics() {
        for op in [
            SemOp::Init(5),
            SemOp::Acquire(2),
            SemOp::Release(1),
            SemOp::Drain,
            SemOp::AvailablePermits,
        ] {
            let (n, d) = decode(&encode("s", &op)).unwrap();
            assert_eq!(n, "s");
            assert_eq!(d, op);
        }
        let mut sm = SemaphoreSm::new();
        assert_eq!(sm.apply(&encode("s", &SemOp::Init(3))), CpReply::Bool(true));
        assert_eq!(
            sm.apply(&encode("s", &SemOp::Init(9))),
            CpReply::Bool(false)
        ); // already init
        assert_eq!(
            sm.apply(&encode("s", &SemOp::Acquire(2))),
            CpReply::Bool(true)
        );
        assert_eq!(
            sm.apply(&encode("s", &SemOp::Acquire(2))),
            CpReply::Bool(false)
        ); // only 1 left
        assert_eq!(
            sm.apply(&encode("s", &SemOp::AvailablePermits)),
            CpReply::Long(1)
        );
        assert_eq!(
            sm.apply(&encode("s", &SemOp::Release(4))),
            CpReply::Bool(true)
        );
        assert_eq!(sm.apply(&encode("s", &SemOp::Drain)), CpReply::Long(5));
        assert_eq!(sm.available("s"), 0);
    }
}
