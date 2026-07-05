//! CPMap client codec. A CP request is `[init][RaftGroupId (BEGIN, [seed@0,id@8],
//! group-name, END) = 4 frames][name][Data args…]`, so the object name is at frame
//! 5 and the key/value Data frames follow — identical framing to AtomicReference.
//! Replies (nullable Data / bool / void) reuse the atomicref/atomiclong encoders in
//! `member_thread::build_response`.

use crate::{begin_frame, end_frame};
use protocol::fixed::write_i32_le;
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, string_frame};

pub const GET_REQ: i32 = 2294016;
pub const PUT_REQ: i32 = 2294272;
pub const SET_REQ: i32 = 2294528;
pub const REMOVE_REQ: i32 = 2294784;
pub const DELETE_REQ: i32 = 2295040;
pub const CAS_REQ: i32 = 2295296;
pub const PUT_IF_ABSENT_REQ: i32 = 2295552;

const NAME_FRAME: usize = 5;

/// Whether `msg_type` is a CPMap client request.
pub fn is_cpmap(msg_type: i32) -> bool {
    matches!(
        msg_type,
        GET_REQ | PUT_REQ | SET_REQ | REMOVE_REQ | DELETE_REQ | CAS_REQ | PUT_IF_ABSENT_REQ
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CpMapReq {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
    Delete(Vec<u8>),
    CompareAndSet(Vec<u8>, Vec<u8>, Vec<u8>),
    PutIfAbsent(Vec<u8>, Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CpMapRequest {
    pub name: String,
    pub op: CpMapReq,
}

fn data(frames: &[Frame], idx: usize) -> Vec<u8> {
    frames
        .get(idx)
        .filter(|f| !f.is_null())
        .map(|f| f.content.clone())
        .unwrap_or_default()
}

pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<CpMapRequest> {
    let name = decode_string(frames.get(NAME_FRAME)?);
    let key = || data(frames, NAME_FRAME + 1);
    let val = || data(frames, NAME_FRAME + 2);
    let op = match req_type {
        GET_REQ => CpMapReq::Get(key()),
        REMOVE_REQ => CpMapReq::Remove(key()),
        DELETE_REQ => CpMapReq::Delete(key()),
        PUT_REQ => CpMapReq::Put(key(), val()),
        SET_REQ => CpMapReq::Set(key(), val()),
        PUT_IF_ABSENT_REQ => CpMapReq::PutIfAbsent(key(), val()),
        CAS_REQ => CpMapReq::CompareAndSet(
            key(),
            data(frames, NAME_FRAME + 2),
            data(frames, NAME_FRAME + 3),
        ),
        _ => return None,
    };
    Some(CpMapRequest { name, op })
}

/// Build a request as a client would (tests / any driver): init + RaftGroupId +
/// object name + trailing `Data` args.
pub fn encode_request(req_type: i32, group: &str, name: &str, op: &CpMapReq) -> Vec<Frame> {
    let mut init = vec![0u8; 16];
    write_i32_le(&mut init, 0, req_type);
    let mut frames = vec![
        initial_frame(init),
        begin_frame(),
        initial_frame(vec![0u8; 16]), // RaftGroupId seed@0, id@8
        string_frame(group),
        end_frame(),
        string_frame(name),
    ];
    match op {
        CpMapReq::Get(k) | CpMapReq::Remove(k) | CpMapReq::Delete(k) => frames.push(data_frame(k)),
        CpMapReq::Put(k, v) | CpMapReq::Set(k, v) | CpMapReq::PutIfAbsent(k, v) => {
            frames.push(data_frame(k));
            frames.push(data_frame(v));
        }
        CpMapReq::CompareAndSet(k, e, n) => {
            frames.push(data_frame(k));
            frames.push(data_frame(e));
            frames.push(data_frame(n));
        }
    }
    frames
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips() {
        let cases = [
            (GET_REQ, CpMapReq::Get(b"k".to_vec())),
            (PUT_REQ, CpMapReq::Put(b"k".to_vec(), b"v".to_vec())),
            (SET_REQ, CpMapReq::Set(b"k".to_vec(), b"v".to_vec())),
            (REMOVE_REQ, CpMapReq::Remove(b"k".to_vec())),
            (DELETE_REQ, CpMapReq::Delete(b"k".to_vec())),
            (
                PUT_IF_ABSENT_REQ,
                CpMapReq::PutIfAbsent(b"k".to_vec(), b"v".to_vec()),
            ),
            (
                CAS_REQ,
                CpMapReq::CompareAndSet(b"k".to_vec(), b"e".to_vec(), b"n".to_vec()),
            ),
        ];
        for (ty, op) in cases {
            let frames = encode_request(ty, "default", "m", &op);
            let dec = decode_request(ty, &frames).expect("decodes");
            assert_eq!(dec.name, "m");
            assert_eq!(dec.op, op);
        }
    }
}
