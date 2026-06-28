use protocol::fixed::{read_i32_le, write_i32_le};
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame};

pub fn decode_cache_put(frames: &[Frame]) -> (String, Vec<u8>, Vec<u8>, bool) {
    let c = &frames[0].content;
    let get = c[16] == 1;
    let _completion_id = read_i32_le(c, 17);
    let name = decode_string(&frames[1]);
    let key = frames[2].content.clone();
    let value = frames[3].content.clone();
    (name, key, value, get)
}

pub fn decode_cache_get(frames: &[Frame]) -> (String, Vec<u8>) {
    let name = decode_string(&frames[1]);
    let key = frames[2].content.clone();
    (name, key)
}

pub fn encode_cache_put_response(old: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 22]; // just initial frame
    write_i32_le(&mut c, 0, 1250049); // RESPONSE_MESSAGE_TYPE
    if let Some(v) = old {
        vec![initial_frame(c), data_frame(v)]
    } else {
        vec![initial_frame(c), null_frame()]
    }
}

pub fn encode_cache_get_response(val: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 1248513); // RESPONSE_MESSAGE_TYPE
    if let Some(v) = val {
        vec![initial_frame(c), data_frame(v)]
    } else {
        vec![initial_frame(c), null_frame()]
    }
}

pub fn decode_cache_remove(frames: &[Frame]) -> (String, Vec<u8>, Option<Vec<u8>>) {
    let name = decode_string(&frames[1]);
    let key = frames[2].content.clone();
    let current_val = if frames.len() > 3 && !frames[3].content.is_empty() {
        Some(frames[3].content.clone())
    } else {
        None
    };
    (name, key, current_val)
}

pub fn encode_cache_remove_response(removed: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 23];
    write_i32_le(&mut c, 0, 1250817); // RESPONSE_MESSAGE_TYPE
    c[22] = if removed { 1 } else { 0 };
    vec![initial_frame(c)]
}
