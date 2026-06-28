use protocol::fixed::{read_i64_le, write_i32_le};
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame};

pub fn decode_submit_to_partition(frames: &[Frame]) -> (String, (i64, i64), Vec<u8>) {
    let c = &frames[0].content;
    let uuid = (read_i64_le(c, 16), read_i64_le(c, 24));
    let name = decode_string(&frames[1]);
    let callable = frames[2].content.clone();
    (name, uuid, callable)
}

pub fn decode_submit_to_member(frames: &[Frame]) -> (String, (i64, i64), Vec<u8>, (i64, i64)) {
    let c = &frames[0].content;
    let uuid = (read_i64_le(c, 16), read_i64_le(c, 24));
    let member_uuid = (read_i64_le(c, 32), read_i64_le(c, 40));
    let name = decode_string(&frames[1]);
    let callable = frames[2].content.clone();
    (name, uuid, callable, member_uuid)
}

pub fn encode_submit_response(msg_type: i32, response_data: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 22]; // initial frame
    write_i32_le(&mut c, 0, msg_type);
    if let Some(data) = response_data {
        vec![initial_frame(c), data_frame(data)]
    } else {
        vec![initial_frame(c), null_frame()]
    }
}
