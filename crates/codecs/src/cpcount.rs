//! Client-protocol codec for the CP integer-counter primitives: `ICountDownLatch`
//! and `ISemaphore`. Their parameters are fixed fields in the initial frame; the
//! object name is the trailing string frame (past the `RaftGroupId`). Responses
//! are an int / bool at offset 13, or void. Session/thread/invocation-uid fields
//! present on the blocking ops are parsed past (v1 has no CP sessions).

use protocol::fixed::{read_i32_le, write_i32_le};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame};

// CountDownLatch
pub const CDL_GET_COUNT_REQ: i32 = 721920;
pub const CDL_GET_COUNT_RESP: i32 = 721921;
pub const CDL_COUNT_DOWN_REQ: i32 = 721664;
pub const CDL_COUNT_DOWN_RESP: i32 = 721665;
pub const CDL_TRY_SET_COUNT_REQ: i32 = 721152;
pub const CDL_TRY_SET_COUNT_RESP: i32 = 721153;
// Semaphore
pub const SEM_INIT_REQ: i32 = 786688;
pub const SEM_INIT_RESP: i32 = 786689;
pub const SEM_ACQUIRE_REQ: i32 = 786944;
pub const SEM_ACQUIRE_RESP: i32 = 786945;
pub const SEM_RELEASE_REQ: i32 = 787200;
pub const SEM_RELEASE_RESP: i32 = 787201;
pub const SEM_DRAIN_REQ: i32 = 787456;
pub const SEM_DRAIN_RESP: i32 = 787457;
pub const SEM_AVAILABLE_REQ: i32 = 787968;
pub const SEM_AVAILABLE_RESP: i32 = 787969;

/// A decoded CP counter request (name + operation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CpCountReq {
    CdlGetCount,
    CdlCountDown,
    CdlTrySetCount(i32),
    SemInit(i32),
    SemAcquire(i32),
    SemRelease(i32),
    SemDrain,
    SemAvailablePermits,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpCountRequest {
    pub name: String,
    pub op: CpCountReq,
}

pub fn is_cp_count(req_type: i32) -> bool {
    response_type(req_type).is_some()
}

pub fn response_type(req_type: i32) -> Option<i32> {
    Some(match req_type {
        CDL_GET_COUNT_REQ => CDL_GET_COUNT_RESP,
        CDL_COUNT_DOWN_REQ => CDL_COUNT_DOWN_RESP,
        CDL_TRY_SET_COUNT_REQ => CDL_TRY_SET_COUNT_RESP,
        SEM_INIT_REQ => SEM_INIT_RESP,
        SEM_ACQUIRE_REQ => SEM_ACQUIRE_RESP,
        SEM_RELEASE_REQ => SEM_RELEASE_RESP,
        SEM_DRAIN_REQ => SEM_DRAIN_RESP,
        SEM_AVAILABLE_REQ => SEM_AVAILABLE_RESP,
        _ => return None,
    })
}

fn int_at(frames: &[Frame], off: usize) -> i32 {
    frames
        .first()
        .filter(|f| f.content.len() >= off + 4)
        .map(|f| read_i32_le(&f.content, off))
        .unwrap_or(0)
}

/// Decode a request. The object name is the trailing string frame; integer
/// parameters are fixed fields in the initial frame at op-specific offsets
/// (count/permits @16 for the simple ops; @49 for Semaphore acquire/release,
/// which are preceded by sessionId+threadId+invocationUid).
pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<CpCountRequest> {
    let name = decode_string(frames.last()?);
    let op = match req_type {
        CDL_GET_COUNT_REQ => CpCountReq::CdlGetCount,
        CDL_COUNT_DOWN_REQ => CpCountReq::CdlCountDown,
        CDL_TRY_SET_COUNT_REQ => CpCountReq::CdlTrySetCount(int_at(frames, 16)),
        SEM_INIT_REQ => CpCountReq::SemInit(int_at(frames, 16)),
        SEM_ACQUIRE_REQ => CpCountReq::SemAcquire(int_at(frames, 49)),
        SEM_RELEASE_REQ => CpCountReq::SemRelease(int_at(frames, 49)),
        SEM_DRAIN_REQ => CpCountReq::SemDrain,
        SEM_AVAILABLE_REQ => CpCountReq::SemAvailablePermits,
        _ => return None,
    };
    Some(CpCountRequest { name, op })
}

/// Encode an int response (`resp_type` at 0, value at 13).
pub fn encode_int_response(resp_type: i32, value: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 17]; // type@0, corr@4, backupAcks@12, int@13
    write_i32_le(&mut c, 0, resp_type);
    write_i32_le(&mut c, 13, value);
    vec![initial_frame(c)]
}

/// Encode a bool response (bool byte at 13).
pub fn encode_bool_response(resp_type: i32, value: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 14];
    write_i32_le(&mut c, 0, resp_type);
    c[13] = value as u8;
    vec![initial_frame(c)]
}

/// Encode a void response (header only).
pub fn encode_void_response(resp_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, resp_type);
    vec![initial_frame(c)]
}

/// Build a request as a client would (tests / any driver): init frame (with the
/// integer param at `int_off`, if any) + `RaftGroupId` + object name.
pub fn encode_request(
    req_type: i32,
    group: &str,
    name: &str,
    int_off: usize,
    value: i32,
) -> Vec<Frame> {
    use crate::{begin_frame, end_frame};
    use protocol::primitives::string_frame;
    let mut init = vec![0u8; int_off.max(16) + 4];
    write_i32_le(&mut init, 0, req_type);
    if int_off >= 16 {
        write_i32_le(&mut init, int_off, value);
    }
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
    fn cdl_trysetcount_roundtrip() {
        let f = encode_request(CDL_TRY_SET_COUNT_REQ, "g", "latch", 16, 5);
        let r = decode_request(CDL_TRY_SET_COUNT_REQ, &f).unwrap();
        assert_eq!(r.name, "latch");
        assert_eq!(r.op, CpCountReq::CdlTrySetCount(5));
    }

    #[test]
    fn sem_acquire_permits_at_offset_49() {
        let f = encode_request(SEM_ACQUIRE_REQ, "g", "sem", 49, 3);
        let r = decode_request(SEM_ACQUIRE_REQ, &f).unwrap();
        assert_eq!(r.name, "sem");
        assert_eq!(r.op, CpCountReq::SemAcquire(3));
    }

    #[test]
    fn no_param_ops_and_responses() {
        let f = encode_request(SEM_AVAILABLE_REQ, "g", "sem", 0, 0);
        assert_eq!(
            decode_request(SEM_AVAILABLE_REQ, &f).unwrap().op,
            CpCountReq::SemAvailablePermits
        );
        assert_eq!(
            read_i32_le(&encode_int_response(SEM_AVAILABLE_RESP, 7)[0].content, 13),
            7
        );
        assert_eq!(encode_bool_response(SEM_INIT_RESP, true)[0].content[13], 1);
    }

    #[test]
    fn response_type_mapping() {
        assert_eq!(response_type(CDL_GET_COUNT_REQ), Some(CDL_GET_COUNT_RESP));
        assert!(is_cp_count(SEM_DRAIN_REQ));
        assert!(!is_cp_count(12345));
    }
}
