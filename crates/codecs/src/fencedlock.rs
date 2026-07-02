//! Client-protocol codec for `FencedLock` (Hazelcast CP `FencedLock*` messages).
//!
//! Requests carry `sessionId`(long @16) + `threadId`(long @24) + invocationUid as
//! fixed init-frame fields; the object name is the trailing string frame. Lock /
//! tryLock respond with a `long` fence (reuse the AtomicLong long response);
//! unlock responds with a bool. v1 ignores the invocationUid (no CP sessions).

use protocol::fixed::{read_i64_le, write_i32_le, write_i64_le};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame};

pub const LOCK_REQ: i32 = 459008;
pub const LOCK_RESP: i32 = 459009;
pub const TRY_LOCK_REQ: i32 = 459264;
pub const TRY_LOCK_RESP: i32 = 459265;
pub const UNLOCK_REQ: i32 = 459520;
pub const UNLOCK_RESP: i32 = 459521;

const SESSION_OFFSET: usize = 16;
const THREAD_OFFSET: usize = 24;

/// A decoded FencedLock request (owner identity + operation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlReq {
    Lock,
    TryLock,
    Unlock,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FencedLockRequest {
    pub name: String,
    pub session: i64,
    pub thread: i64,
    pub op: FlReq,
}

pub fn is_fencedlock(req_type: i32) -> bool {
    response_type(req_type).is_some()
}

pub fn response_type(req_type: i32) -> Option<i32> {
    Some(match req_type {
        LOCK_REQ => LOCK_RESP,
        TRY_LOCK_REQ => TRY_LOCK_RESP,
        UNLOCK_REQ => UNLOCK_RESP,
        _ => return None,
    })
}

fn long_at(frames: &[Frame], off: usize) -> i64 {
    frames
        .first()
        .filter(|f| f.content.len() >= off + 8)
        .map(|f| read_i64_le(&f.content, off))
        .unwrap_or(0)
}

pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<FencedLockRequest> {
    let op = match req_type {
        LOCK_REQ => FlReq::Lock,
        TRY_LOCK_REQ => FlReq::TryLock,
        UNLOCK_REQ => FlReq::Unlock,
        _ => return None,
    };
    Some(FencedLockRequest {
        name: decode_string(frames.last()?),
        session: long_at(frames, SESSION_OFFSET),
        thread: long_at(frames, THREAD_OFFSET),
        op,
    })
}

/// Build a request as a client would (tests / any driver).
pub fn encode_request(
    req_type: i32,
    group: &str,
    name: &str,
    session: i64,
    thread: i64,
) -> Vec<Frame> {
    use crate::{begin_frame, end_frame};
    use protocol::primitives::string_frame;
    let mut init = vec![0u8; 49]; // header + session + thread + invocation-uid space
    write_i32_le(&mut init, 0, req_type);
    write_i64_le(&mut init, SESSION_OFFSET, session);
    write_i64_le(&mut init, THREAD_OFFSET, thread);
    vec![
        initial_frame(init),
        begin_frame(),
        initial_frame(vec![0u8; 16]),
        string_frame(group),
        end_frame(),
        string_frame(name),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        for (t, op) in [
            (LOCK_REQ, FlReq::Lock),
            (TRY_LOCK_REQ, FlReq::TryLock),
            (UNLOCK_REQ, FlReq::Unlock),
        ] {
            let f = encode_request(t, "g", "lk", 11, 22);
            let r = decode_request(t, &f).unwrap();
            assert_eq!(r.name, "lk");
            assert_eq!(r.session, 11);
            assert_eq!(r.thread, 22);
            assert_eq!(r.op, op);
        }
        assert!(is_fencedlock(LOCK_REQ));
        assert!(!is_fencedlock(1));
        assert_eq!(response_type(UNLOCK_REQ), Some(UNLOCK_RESP));
    }
}
