//! CP sessions: a replicated lease registry. A client holds a session (a
//! monotonic id) and heartbeats it; if it stops (client death), the session
//! expires and the resources it held — FencedLock holds, etc. — are auto-released.
//!
//! Expiry must be identical on every replica, so it cannot read wall-clock time.
//! Instead a logical `clock` advances by one per committed `Tick` command (the
//! leader proposes ticks periodically); a session expires when the clock passes
//! its deadline. Session TTL and the tick cadence are expressed in ticks.

use std::collections::HashMap;

/// Advertised session TTL / heartbeat period (client-facing milliseconds).
pub const TTL_MILLIS: i64 = 30_000;
pub const HEARTBEAT_MILLIS: i64 = 5_000;
/// Session lifetime in logical ticks (a tick is proposed roughly every second).
const TTL_TICKS: i64 = 30;

/// A CP session operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessOp {
    Create,
    Heartbeat(i64),
    Close(i64),
    /// Advance the logical clock (leader-proposed; expires stale sessions).
    Tick,
    GenerateThreadId,
}

const TAG_CREATE: u8 = 0;
const TAG_HEARTBEAT: u8 = 1;
const TAG_CLOSE: u8 = 2;
const TAG_TICK: u8 = 3;
const TAG_GEN_THREAD: u8 = 4;

pub fn encode(op: &SessOp) -> Vec<u8> {
    let mut buf = Vec::new();
    match *op {
        SessOp::Create => buf.push(TAG_CREATE),
        SessOp::Heartbeat(id) => {
            buf.push(TAG_HEARTBEAT);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        SessOp::Close(id) => {
            buf.push(TAG_CLOSE);
            buf.extend_from_slice(&id.to_le_bytes());
        }
        SessOp::Tick => buf.push(TAG_TICK),
        SessOp::GenerateThreadId => buf.push(TAG_GEN_THREAD),
    }
    buf
}

pub fn decode(body: &[u8]) -> Option<SessOp> {
    let tag = *body.first()?;
    let id = || -> Option<i64> { Some(i64::from_le_bytes(body.get(1..9)?.try_into().ok()?)) };
    Some(match tag {
        TAG_CREATE => SessOp::Create,
        TAG_HEARTBEAT => SessOp::Heartbeat(id()?),
        TAG_CLOSE => SessOp::Close(id()?),
        TAG_TICK => SessOp::Tick,
        TAG_GEN_THREAD => SessOp::GenerateThreadId,
        _ => return None,
    })
}

/// The replicated session registry.
#[derive(Default)]
pub struct SessionSm {
    /// session id -> expiry deadline (in logical ticks).
    sessions: HashMap<i64, i64>,
    clock: i64,
    next_id: i64,
    next_thread_id: i64,
}

impl SessionSm {
    pub fn new() -> SessionSm {
        SessionSm::default()
    }

    /// Create a session; returns its id (its deadline is `clock + TTL`).
    pub fn create(&mut self) -> i64 {
        self.next_id += 1;
        let id = self.next_id;
        self.sessions.insert(id, self.clock + TTL_TICKS);
        id
    }

    /// Renew a session's lease; returns whether it existed.
    pub fn heartbeat(&mut self, id: i64) -> bool {
        if let Some(d) = self.sessions.get_mut(&id) {
            *d = self.clock + TTL_TICKS;
            true
        } else {
            false
        }
    }

    /// Close a session explicitly; returns whether it existed.
    pub fn close(&mut self, id: i64) -> bool {
        self.sessions.remove(&id).is_some()
    }

    /// Advance the logical clock one tick and return newly-expired session ids.
    pub fn tick(&mut self) -> Vec<i64> {
        self.clock += 1;
        let now = self.clock;
        let expired: Vec<i64> = self
            .sessions
            .iter()
            .filter(|(_, &d)| d <= now)
            .map(|(&id, _)| id)
            .collect();
        for id in &expired {
            self.sessions.remove(id);
        }
        expired
    }

    /// A fresh, unique thread id.
    pub fn generate_thread_id(&mut self) -> i64 {
        self.next_thread_id += 1;
        self.next_thread_id
    }

    pub fn is_active(&self, id: i64) -> bool {
        self.sessions.contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for op in [
            SessOp::Create,
            SessOp::Heartbeat(7),
            SessOp::Close(7),
            SessOp::Tick,
            SessOp::GenerateThreadId,
        ] {
            assert_eq!(decode(&encode(&op)).unwrap(), op);
        }
    }

    #[test]
    fn expiry_and_heartbeat() {
        let mut sm = SessionSm::new();
        let s = sm.create();
        assert!(sm.is_active(s));
        // Heartbeating keeps it alive across many ticks.
        for _ in 0..TTL_TICKS * 2 {
            sm.heartbeat(s);
            assert!(sm.tick().is_empty(), "heartbeated session never expires");
        }
        assert!(sm.is_active(s));
        // Stop heartbeating -> expires within TTL ticks.
        let mut expired = Vec::new();
        for _ in 0..=TTL_TICKS {
            expired.extend(sm.tick());
        }
        assert!(expired.contains(&s), "stale session expires");
        assert!(!sm.is_active(s));
    }
}
