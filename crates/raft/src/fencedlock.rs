//! `FencedLock` as a deterministic replicated state machine: mutual exclusion
//! with a **monotonic fencing token**. Each successful acquisition of a free lock
//! returns a strictly increasing fence, so a client that stalls and loses the
//! lock can be fenced off by comparing tokens. Reentrant for the same
//! `(session, thread)` owner.
//!
//! v1 is non-blocking (lock == tryLock: it fails immediately if another owner
//! holds it) and has no session-expiry auto-release — that arrives with CP
//! sessions. `getLockOwnershipState` (a 4-field query) is also a session follow-up.

use crate::cp::CpReply;
use std::collections::HashMap;

/// A FencedLock operation, identified by its `(session, thread)` owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlOp {
    Lock { session: i64, thread: i64 },
    TryLock { session: i64, thread: i64 },
    Unlock { session: i64, thread: i64 },
}

const TAG_LOCK: u8 = 0;
const TAG_TRY_LOCK: u8 = 1;
const TAG_UNLOCK: u8 = 2;

fn owner(op: &FlOp) -> (i64, i64) {
    match *op {
        FlOp::Lock { session, thread }
        | FlOp::TryLock { session, thread }
        | FlOp::Unlock { session, thread } => (session, thread),
    }
}

pub fn encode(name: &str, op: &FlOp) -> Vec<u8> {
    let tag = match op {
        FlOp::Lock { .. } => TAG_LOCK,
        FlOp::TryLock { .. } => TAG_TRY_LOCK,
        FlOp::Unlock { .. } => TAG_UNLOCK,
    };
    let (session, thread) = owner(op);
    let mut buf = Vec::with_capacity(17 + name.len());
    buf.push(tag);
    buf.extend_from_slice(&session.to_le_bytes());
    buf.extend_from_slice(&thread.to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf
}

pub fn decode(body: &[u8]) -> Option<(String, FlOp)> {
    if body.len() < 17 {
        return None;
    }
    let tag = body[0];
    let session = i64::from_le_bytes(body[1..9].try_into().ok()?);
    let thread = i64::from_le_bytes(body[9..17].try_into().ok()?);
    let op = match tag {
        TAG_LOCK => FlOp::Lock { session, thread },
        TAG_TRY_LOCK => FlOp::TryLock { session, thread },
        TAG_UNLOCK => FlOp::Unlock { session, thread },
        _ => return None,
    };
    let name = std::str::from_utf8(&body[17..]).ok()?.to_string();
    Some((name, op))
}

#[derive(Clone)]
struct LockState {
    fence: i64,
    session: i64,
    thread: i64,
    count: i32,
}

/// The replicated FencedLock state machine.
#[derive(Default)]
pub struct FencedLockSm {
    locks: HashMap<String, LockState>,
    /// Group-wide monotonic fence source (only ever increases).
    next_fence: i64,
}

impl FencedLockSm {
    pub fn new() -> FencedLockSm {
        FencedLockSm::default()
    }

    pub fn apply(&mut self, body: &[u8]) -> CpReply {
        let Some((name, op)) = decode(body) else {
            return CpReply::Nil;
        };
        match op {
            FlOp::Lock { session, thread } | FlOp::TryLock { session, thread } => {
                CpReply::Long(self.acquire(&name, session, thread))
            }
            FlOp::Unlock { session, thread } => CpReply::Bool(self.release(&name, session, thread)),
        }
    }

    /// Acquire (or reenter). Returns the fence, or 0 if held by another owner.
    fn acquire(&mut self, name: &str, session: i64, thread: i64) -> i64 {
        match self.locks.get_mut(name) {
            Some(l) if l.count > 0 => {
                if l.session == session && l.thread == thread {
                    l.count += 1;
                    l.fence // reentrant: same fence
                } else {
                    0 // held by another owner
                }
            }
            _ => {
                self.next_fence += 1;
                self.locks.insert(
                    name.to_string(),
                    LockState {
                        fence: self.next_fence,
                        session,
                        thread,
                        count: 1,
                    },
                );
                self.next_fence
            }
        }
    }

    /// Release one hold. Returns true if this owner held it.
    fn release(&mut self, name: &str, session: i64, thread: i64) -> bool {
        match self.locks.get_mut(name) {
            Some(l) if l.count > 0 && l.session == session && l.thread == thread => {
                l.count -= 1;
                if l.count == 0 {
                    self.locks.remove(name);
                }
                true
            }
            _ => false,
        }
    }

    /// Release every lock held by an expired/closed session (auto-release).
    pub fn release_session(&mut self, session: i64) {
        self.locks.retain(|_, l| l.session != session);
    }

    /// Current fence of `name` (0 if unlocked).
    pub fn fence(&self, name: &str) -> i64 {
        self.locks.get(name).map(|l| l.fence).unwrap_or(0)
    }
    pub fn is_locked(&self, name: &str) -> bool {
        self.locks.get(name).is_some_and(|l| l.count > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for op in [
            FlOp::Lock {
                session: 1,
                thread: 2,
            },
            FlOp::TryLock {
                session: 3,
                thread: 4,
            },
            FlOp::Unlock {
                session: 5,
                thread: 6,
            },
        ] {
            let (n, d) = decode(&encode("lk", &op)).unwrap();
            assert_eq!(n, "lk");
            assert_eq!(d, op);
        }
    }

    #[test]
    fn fencing_is_monotonic_and_exclusive() {
        let mut sm = FencedLockSm::new();
        // A acquires -> fence f1.
        let f1 = match sm.apply(&encode(
            "lk",
            &FlOp::Lock {
                session: 1,
                thread: 1,
            },
        )) {
            CpReply::Long(f) => f,
            _ => panic!(),
        };
        assert!(f1 > 0);
        // B cannot acquire while A holds it.
        assert_eq!(
            sm.apply(&encode(
                "lk",
                &FlOp::TryLock {
                    session: 2,
                    thread: 2
                }
            )),
            CpReply::Long(0)
        );
        // A reenters -> same fence.
        assert_eq!(
            sm.apply(&encode(
                "lk",
                &FlOp::Lock {
                    session: 1,
                    thread: 1
                }
            )),
            CpReply::Long(f1)
        );
        // Two unlocks release (count was 2).
        assert_eq!(
            sm.apply(&encode(
                "lk",
                &FlOp::Unlock {
                    session: 1,
                    thread: 1
                }
            )),
            CpReply::Bool(true)
        );
        assert!(sm.is_locked("lk"));
        assert_eq!(
            sm.apply(&encode(
                "lk",
                &FlOp::Unlock {
                    session: 1,
                    thread: 1
                }
            )),
            CpReply::Bool(true)
        );
        assert!(!sm.is_locked("lk"));
        // B now acquires -> a STRICTLY GREATER fence (the fencing guarantee).
        let f2 = match sm.apply(&encode(
            "lk",
            &FlOp::Lock {
                session: 2,
                thread: 2,
            },
        )) {
            CpReply::Long(f) => f,
            _ => panic!(),
        };
        assert!(f2 > f1, "fence must strictly increase across acquisitions");
        // A's stale unlock is rejected.
        assert_eq!(
            sm.apply(&encode(
                "lk",
                &FlOp::Unlock {
                    session: 1,
                    thread: 1
                }
            )),
            CpReply::Bool(false)
        );
    }
}
