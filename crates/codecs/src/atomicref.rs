//! `IAtomicReference` client-protocol codec (Hazelcast CP `AtomicRef*` messages).
//!
//! Like [`crate::atomiclong`] the variable part starts with a `RaftGroupId`
//! (BEGIN_DS, `[seed@0][id@8]`, group-name string, END_DS) — 4 frames — then the
//! object-name string (frame index 5), then any nullable `Data` values. So the
//! object name is at a fixed frame position and the values follow it. Responses
//! carry a nullable `Data` (Get/Set) or a bool (CompareAndSet/Contains) at
//! offset 13.

use protocol::fixed::{write_i32_le, write_i64_le};
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame, string_frame};

use crate::{begin_frame, end_frame};

pub const GET_REQ: i32 = 656384;
pub const GET_RESP: i32 = 656385;
pub const SET_REQ: i32 = 656640;
pub const SET_RESP: i32 = 656641;
pub const COMPARE_AND_SET_REQ: i32 = 655872;
pub const COMPARE_AND_SET_RESP: i32 = 655873;
pub const CONTAINS_REQ: i32 = 656128;
pub const CONTAINS_RESP: i32 = 656129;

/// The frame index of the object name (past the 4-frame RaftGroupId + init).
const NAME_FRAME: usize = 5;

/// A decoded AtomicReference request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArReq {
    Get,
    Set {
        new: Option<Vec<u8>>,
        return_old: bool,
    },
    CompareAndSet {
        old: Option<Vec<u8>>,
        new: Option<Vec<u8>>,
    },
    Contains {
        value: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicRefRequest {
    pub name: String,
    pub op: ArReq,
}

pub fn response_type(req_type: i32) -> Option<i32> {
    Some(match req_type {
        GET_REQ => GET_RESP,
        SET_REQ => SET_RESP,
        COMPARE_AND_SET_REQ => COMPARE_AND_SET_RESP,
        CONTAINS_REQ => CONTAINS_RESP,
        _ => return None,
    })
}

pub fn is_atomicref(req_type: i32) -> bool {
    response_type(req_type).is_some()
}

/// A nullable `Data` frame at `idx`: `None` if absent or the null frame.
fn nullable_data(frames: &[Frame], idx: usize) -> Option<Vec<u8>> {
    frames
        .get(idx)
        .filter(|f| !f.is_null())
        .map(|f| f.content.clone())
}

/// Decode a request. Values follow the object-name frame.
pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<AtomicRefRequest> {
    let name = decode_string(frames.get(NAME_FRAME)?);
    let op = match req_type {
        GET_REQ => ArReq::Get,
        SET_REQ => ArReq::Set {
            new: nullable_data(frames, NAME_FRAME + 1),
            return_old: frames
                .first()
                .map(|f| f.content.get(16).copied() == Some(1))
                == Some(true),
        },
        COMPARE_AND_SET_REQ => ArReq::CompareAndSet {
            old: nullable_data(frames, NAME_FRAME + 1),
            new: nullable_data(frames, NAME_FRAME + 2),
        },
        CONTAINS_REQ => ArReq::Contains {
            value: nullable_data(frames, NAME_FRAME + 1),
        },
        _ => return None,
    };
    Some(AtomicRefRequest { name, op })
}

/// Encode a nullable-`Data` response (Get / Set-with-return).
pub fn encode_data_response(resp_type: i32, value: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, resp_type);
    let mut out = vec![initial_frame(c)];
    match value {
        Some(v) => out.push(data_frame(v)),
        None => out.push(null_frame()),
    }
    out
}

/// Encode a bool response (CompareAndSet / Contains; bool byte at 13).
pub fn encode_bool_response(resp_type: i32, value: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 14];
    write_i32_le(&mut c, 0, resp_type);
    c[13] = value as u8;
    vec![initial_frame(c)]
}

/// Build a request as a client would (tests / any driver): init frame +
/// `RaftGroupId` + object name + trailing nullable `Data` values.
pub fn encode_request(req_type: i32, group: &str, name: &str, op: &ArReq) -> Vec<Frame> {
    let mut init = vec![
        0u8;
        if matches!(op, ArReq::Set { .. }) {
            17
        } else {
            16
        }
    ];
    write_i32_le(&mut init, 0, req_type);
    if let ArReq::Set { return_old, .. } = op {
        init[16] = *return_old as u8;
    }
    let nullable = |v: &Option<Vec<u8>>| match v {
        Some(b) => data_frame(b),
        None => null_frame(),
    };
    let mut frames = vec![
        initial_frame(init),
        begin_frame(),
        initial_frame(vec![0u8; 16]), // RaftGroupId seed@0, id@8
        string_frame(group),
        end_frame(),
        string_frame(name),
    ];
    match op {
        ArReq::Get => {}
        ArReq::Set { new, .. } => frames.push(nullable(new)),
        ArReq::CompareAndSet { old, new } => {
            frames.push(nullable(old));
            frames.push(nullable(new));
        }
        ArReq::Contains { value } => frames.push(nullable(value)),
    }
    frames
}

/// Patch the correlation id into a response's initial frame (at content offset 4).
pub fn set_correlation(frames: &mut [Frame], corr: i64) {
    if let Some(f) = frames.first_mut() {
        write_i64_le(&mut f.content, 4, corr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::fixed::{read_i32_le, read_i64_le};

    fn roundtrip(req_type: i32, op: ArReq) {
        let frames = encode_request(req_type, "default", "ref", &op);
        let req = decode_request(req_type, &frames).expect("decodes");
        assert_eq!(req.name, "ref");
        assert_eq!(req.op, op);
    }

    #[test]
    fn request_roundtrip_all_ops() {
        roundtrip(GET_REQ, ArReq::Get);
        roundtrip(
            SET_REQ,
            ArReq::Set {
                new: Some(b"v".to_vec()),
                return_old: true,
            },
        );
        roundtrip(
            SET_REQ,
            ArReq::Set {
                new: None,
                return_old: false,
            },
        );
        roundtrip(
            COMPARE_AND_SET_REQ,
            ArReq::CompareAndSet {
                old: Some(b"a".to_vec()),
                new: Some(b"b".to_vec()),
            },
        );
        roundtrip(
            COMPARE_AND_SET_REQ,
            ArReq::CompareAndSet {
                old: None,
                new: Some(b"b".to_vec()),
            },
        );
        roundtrip(
            CONTAINS_REQ,
            ArReq::Contains {
                value: Some(b"c".to_vec()),
            },
        );
    }

    #[test]
    fn data_response_layout() {
        let mut f = encode_data_response(GET_RESP, Some(b"hi"));
        set_correlation(&mut f, 77);
        assert_eq!(read_i32_le(&f[0].content, 0), GET_RESP);
        assert_eq!(read_i64_le(&f[0].content, 4), 77);
        assert_eq!(f[1].content, b"hi");
        // Null value -> a null frame.
        let n = encode_data_response(GET_RESP, None);
        assert!(n[1].is_null());
    }

    #[test]
    fn bool_response_layout() {
        let f = encode_bool_response(COMPARE_AND_SET_RESP, true);
        assert_eq!(read_i32_le(&f[0].content, 0), COMPARE_AND_SET_RESP);
        assert_eq!(f[0].content[13], 1);
    }
}
