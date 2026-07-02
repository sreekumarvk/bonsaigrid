//! Client-protocol codec for CP session lifecycle (`CPSession*` messages):
//! create / close / heartbeat / generate-thread-id. Create's response carries
//! `sessionId + ttlMillis + heartbeatMillis`; close returns a bool; heartbeat is
//! void; generate-thread-id returns a long. `sessionId` (close/heartbeat) is a
//! fixed init-frame field at offset 16.

use protocol::fixed::{read_i64_le, write_i32_le, write_i64_le};
use protocol::frame::Frame;
use protocol::primitives::initial_frame;

pub const CREATE_REQ: i32 = 2031872;
pub const CREATE_RESP: i32 = 2031873;
pub const CLOSE_REQ: i32 = 2032128;
pub const CLOSE_RESP: i32 = 2032129;
pub const HEARTBEAT_REQ: i32 = 2032384;
pub const HEARTBEAT_RESP: i32 = 2032385;
pub const GENERATE_THREAD_ID_REQ: i32 = 2032640;
pub const GENERATE_THREAD_ID_RESP: i32 = 2032641;

const SESSION_ID_OFFSET: usize = 16;

/// A decoded CP session request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CpSessionReq {
    Create,
    Close(i64),
    Heartbeat(i64),
    GenerateThreadId,
}

pub fn is_cp_session(req_type: i32) -> bool {
    response_type(req_type).is_some()
}

pub fn response_type(req_type: i32) -> Option<i32> {
    Some(match req_type {
        CREATE_REQ => CREATE_RESP,
        CLOSE_REQ => CLOSE_RESP,
        HEARTBEAT_REQ => HEARTBEAT_RESP,
        GENERATE_THREAD_ID_REQ => GENERATE_THREAD_ID_RESP,
        _ => return None,
    })
}

fn session_id(frames: &[Frame]) -> i64 {
    frames
        .first()
        .filter(|f| f.content.len() >= SESSION_ID_OFFSET + 8)
        .map(|f| read_i64_le(&f.content, SESSION_ID_OFFSET))
        .unwrap_or(0)
}

pub fn decode_request(req_type: i32, frames: &[Frame]) -> Option<CpSessionReq> {
    Some(match req_type {
        CREATE_REQ => CpSessionReq::Create,
        CLOSE_REQ => CpSessionReq::Close(session_id(frames)),
        HEARTBEAT_REQ => CpSessionReq::Heartbeat(session_id(frames)),
        GENERATE_THREAD_ID_REQ => CpSessionReq::GenerateThreadId,
        _ => return None,
    })
}

/// Create response: `sessionId@13, ttlMillis@21, heartbeatMillis@29`.
pub fn encode_create_response(
    resp_type: i32,
    session_id: i64,
    ttl_ms: i64,
    hb_ms: i64,
) -> Vec<Frame> {
    let mut c = vec![0u8; 37];
    write_i32_le(&mut c, 0, resp_type);
    write_i64_le(&mut c, 13, session_id);
    write_i64_le(&mut c, 21, ttl_ms);
    write_i64_le(&mut c, 29, hb_ms);
    vec![initial_frame(c)]
}

/// Long response (generate-thread-id): value at 13.
pub fn encode_long_response(resp_type: i32, value: i64) -> Vec<Frame> {
    let mut c = vec![0u8; 21];
    write_i32_le(&mut c, 0, resp_type);
    write_i64_le(&mut c, 13, value);
    vec![initial_frame(c)]
}

/// Bool response (close): bool byte at 13.
pub fn encode_bool_response(resp_type: i32, value: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 14];
    write_i32_le(&mut c, 0, resp_type);
    c[13] = value as u8;
    vec![initial_frame(c)]
}

/// Void response (heartbeat): header only.
pub fn encode_void_response(resp_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, resp_type);
    vec![initial_frame(c)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_and_responses() {
        assert_eq!(
            decode_request(CREATE_REQ, &[initial_frame(vec![0u8; 16])]),
            Some(CpSessionReq::Create)
        );
        // sessionId at offset 16 for close/heartbeat.
        let mut init = vec![0u8; 24];
        write_i64_le(&mut init, 16, 42);
        assert_eq!(
            decode_request(CLOSE_REQ, &[initial_frame(init.clone())]),
            Some(CpSessionReq::Close(42))
        );
        assert_eq!(
            decode_request(HEARTBEAT_REQ, &[initial_frame(init)]),
            Some(CpSessionReq::Heartbeat(42))
        );
        // Create response fields.
        let f = encode_create_response(CREATE_RESP, 7, 30000, 5000);
        assert_eq!(read_i64_le(&f[0].content, 13), 7);
        assert_eq!(read_i64_le(&f[0].content, 21), 30000);
        assert_eq!(read_i64_le(&f[0].content, 29), 5000);
        assert!(is_cp_session(GENERATE_THREAD_ID_REQ));
        assert!(!is_cp_session(1));
    }
}
