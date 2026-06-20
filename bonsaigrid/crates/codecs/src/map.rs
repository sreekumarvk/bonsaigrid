//! MapPutCodec (65792/65793) and MapGetCodec (66048/66049).
//!
//! Put request initial-frame offsets: threadId@16, ttl@24. Var-frames: name,
//! key (Data), value (Data). Get request: threadId@16; var-frames: name, key.
//! Responses carry a single nullable Data (the previous/looked-up value).

use protocol::fixed::{read_i64_le, write_i32_le};
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame};

pub struct PutRequest {
    pub name: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub thread_id: i64,
    pub ttl: i64,
}

pub fn decode_put(frames: &[Frame]) -> PutRequest {
    let initial = &frames[0].content;
    PutRequest {
        thread_id: read_i64_le(initial, 16),
        ttl: read_i64_le(initial, 24),
        name: decode_string(&frames[1]),
        key: frames[2].content.clone(),
        value: frames[3].content.clone(),
    }
}

pub struct GetRequest {
    pub name: String,
    pub key: Vec<u8>,
    pub thread_id: i64,
}

pub fn decode_get(frames: &[Frame]) -> GetRequest {
    let initial = &frames[0].content;
    GetRequest {
        thread_id: read_i64_le(initial, 16),
        name: decode_string(&frames[1]),
        key: frames[2].content.clone(),
    }
}

fn response(msg_type: i32, value: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, msg_type);
    let mut out = vec![initial_frame(c)];
    match value {
        Some(v) => out.push(data_frame(v)),
        None => out.push(null_frame()),
    }
    out
}

pub fn encode_put_response(old: Option<&[u8]>) -> Vec<Frame> {
    response(65793, old)
}

pub fn encode_get_response(val: Option<&[u8]>) -> Vec<Frame> {
    response(66049, val)
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::message::msg_type;

    #[test]
    fn put_response_null_is_one_null_frame() {
        let f = encode_put_response(None);
        assert_eq!(msg_type(&f), 65793);
        assert!(f[1].is_null());
    }

    #[test]
    fn get_response_carries_value_blob() {
        let f = encode_get_response(Some(&[9, 9, 9]));
        assert_eq!(msg_type(&f), 66049);
        assert_eq!(f[1].content, vec![9, 9, 9]);
    }
}
