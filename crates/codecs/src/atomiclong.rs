//! `IAtomicLong` client-protocol codec (Hazelcast CP `AtomicLong*` messages).
//!
//! Request initial frame: `type@0, correlationId@4, partitionId@12`, then the
//! op's long arguments at offset 16 (and 24 for compare-and-set). The variable
//! part is a `RaftGroupId` (BEGIN_DS, `[seed@0][id@8]`, group-name string, END_DS)
//! followed by the object-name string — so the object name is the final frame.
//! Response initial frame: `type@0, correlationId@4, backupAcks@12`, then the
//! result at offset 13 (a long, or a single bool byte for compare-and-set).
//!
//! v1 targets one default CP group, so the `RaftGroupId` seed/id are parsed but
//! not used for routing; only the object name selects the AtomicLong.

use protocol::fixed::{read_i64_le, write_i32_le, write_i64_le};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame, string_frame};

use crate::{begin_frame, end_frame};

// Request / response message-type ids (from the reference codecs).
pub const GET_REQ: i32 = 591104;
pub const GET_RESP: i32 = 591105;
pub const ADD_AND_GET_REQ: i32 = 590592;
pub const ADD_AND_GET_RESP: i32 = 590593;
pub const GET_AND_ADD_REQ: i32 = 591360;
pub const GET_AND_ADD_RESP: i32 = 591361;
pub const COMPARE_AND_SET_REQ: i32 = 590848;
pub const COMPARE_AND_SET_RESP: i32 = 590849;
pub const GET_AND_SET_REQ: i32 = 591616;
pub const GET_AND_SET_RESP: i32 = 591617;
pub const SET_REQ: i32 = 591872;
pub const SET_RESP: i32 = 591873;

/// A decoded AtomicLong operation (raw values — the server maps these to its
/// replicated state-machine command).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AtomicLongOp {
    Get,
    Set(i64),
    GetAndSet(i64),
    AddAndGet(i64),
    GetAndAdd(i64),
    CompareAndSet(i64, i64),
}

/// A decoded AtomicLong request: which object, which op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicLongRequest {
    pub name: String,
    pub op: AtomicLongOp,
}

/// The response message-type for a given request message-type.
pub fn response_type(req_type: i32) -> Option<i32> {
    Some(match req_type {
        GET_REQ => GET_RESP,
        ADD_AND_GET_REQ => ADD_AND_GET_RESP,
        GET_AND_ADD_REQ => GET_AND_ADD_RESP,
        COMPARE_AND_SET_REQ => COMPARE_AND_SET_RESP,
        GET_AND_SET_REQ => GET_AND_SET_RESP,
        SET_REQ => SET_RESP,
        _ => return None,
    })
}

/// True if this request is an AtomicLong op we handle.
pub fn is_atomiclong(req_type: i32) -> bool {
    response_type(req_type).is_some()
}

/// Decode a request. The op's long args are in the initial frame at offset 16
/// (and 24 for compare-and-set); the object name is the final frame.
pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<AtomicLongRequest> {
    let init = &frames.first()?.content;
    let name = decode_string(frames.last()?);
    let op = match req_type {
        GET_REQ => AtomicLongOp::Get,
        SET_REQ => AtomicLongOp::Set(read_i64_le(init, 16)),
        GET_AND_SET_REQ => AtomicLongOp::GetAndSet(read_i64_le(init, 16)),
        ADD_AND_GET_REQ => AtomicLongOp::AddAndGet(read_i64_le(init, 16)),
        GET_AND_ADD_REQ => AtomicLongOp::GetAndAdd(read_i64_le(init, 16)),
        COMPARE_AND_SET_REQ => {
            AtomicLongOp::CompareAndSet(read_i64_le(init, 16), read_i64_le(init, 24))
        }
        _ => return None,
    };
    Some(AtomicLongRequest { name, op })
}

/// Encode a `long`-returning response (`resp_type` at 0, value at 13).
pub fn encode_long_response(resp_type: i32, value: i64) -> Vec<Frame> {
    let mut c = vec![0u8; 21]; // type@0, corr@4, backupAcks@12, value@13
    write_i32_le(&mut c, 0, resp_type);
    write_i64_le(&mut c, 13, value);
    vec![initial_frame(c)]
}

/// Encode a `bool`-returning response (compare-and-set; bool byte at 13).
pub fn encode_bool_response(resp_type: i32, value: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 14]; // type@0, corr@4, backupAcks@12, bool@13
    write_i32_le(&mut c, 0, resp_type);
    c[13] = value as u8;
    vec![initial_frame(c)]
}

/// Encode a `void` response (AtomicLong.set; header only).
pub fn encode_void_response(resp_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, resp_type);
    vec![initial_frame(c)]
}

/// Build a request as a client would (used by tests and any member-side driver):
/// initial frame + `RaftGroupId(BEGIN, [seed,id], group, END)` + object name.
pub fn encode_request(req_type: i32, group: &str, name: &str, op: &AtomicLongOp) -> Vec<Frame> {
    let init_len = match op {
        AtomicLongOp::Get => 16,
        AtomicLongOp::CompareAndSet(..) => 32,
        _ => 24,
    };
    let mut init = vec![0u8; init_len];
    write_i32_le(&mut init, 0, req_type);
    match *op {
        AtomicLongOp::Set(v)
        | AtomicLongOp::GetAndSet(v)
        | AtomicLongOp::AddAndGet(v)
        | AtomicLongOp::GetAndAdd(v) => write_i64_le(&mut init, 16, v),
        AtomicLongOp::CompareAndSet(e, u) => {
            write_i64_le(&mut init, 16, e);
            write_i64_le(&mut init, 24, u);
        }
        AtomicLongOp::Get => {}
    }
    vec![
        initial_frame(init),
        begin_frame(),
        initial_frame(vec![0u8; 16]), // RaftGroupId seed@0, id@8
        string_frame(group),
        end_frame(),
        string_frame(name),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::fixed::read_i32_le;

    fn roundtrip(req_type: i32, op: AtomicLongOp) {
        let frames = encode_request(req_type, "default", "counter", &op);
        let req = decode_request(req_type, &frames).expect("decodes");
        assert_eq!(req.name, "counter");
        assert_eq!(req.op, op);
    }

    #[test]
    fn request_roundtrip_all_ops() {
        roundtrip(GET_REQ, AtomicLongOp::Get);
        roundtrip(SET_REQ, AtomicLongOp::Set(42));
        roundtrip(GET_AND_SET_REQ, AtomicLongOp::GetAndSet(-7));
        roundtrip(ADD_AND_GET_REQ, AtomicLongOp::AddAndGet(5));
        roundtrip(GET_AND_ADD_REQ, AtomicLongOp::GetAndAdd(9));
        roundtrip(COMPARE_AND_SET_REQ, AtomicLongOp::CompareAndSet(1, 2));
    }

    #[test]
    fn long_response_layout() {
        let f = encode_long_response(ADD_AND_GET_RESP, 123);
        assert_eq!(read_i32_le(&f[0].content, 0), ADD_AND_GET_RESP);
        assert_eq!(read_i64_le(&f[0].content, 13), 123);
    }

    #[test]
    fn bool_response_layout() {
        let f = encode_bool_response(COMPARE_AND_SET_RESP, true);
        assert_eq!(read_i32_le(&f[0].content, 0), COMPARE_AND_SET_RESP);
        assert_eq!(f[0].content[13], 1);
    }

    #[test]
    fn response_type_mapping() {
        assert_eq!(response_type(ADD_AND_GET_REQ), Some(ADD_AND_GET_RESP));
        assert_eq!(
            response_type(COMPARE_AND_SET_REQ),
            Some(COMPARE_AND_SET_RESP)
        );
        assert!(is_atomiclong(GET_REQ));
        assert!(!is_atomiclong(12345));
    }
}
