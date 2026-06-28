use protocol::fixed::{read_i32_le, read_i64_le, write_i32_le, write_uuid};
use protocol::frame::Frame;
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame};

pub fn decode_transaction_create(frames: &[Frame]) -> (i64, i64) {
    let _timeout = read_i64_le(&frames[0].content, 16);
    // Usually it doesn't take UUID as input, it generates one.
    // Wait, let's check TransactionCreateCodec.java. We will return generated UUID later.
    (0, 0)
}

pub fn encode_transaction_create_response(uuid: (i64, i64)) -> Vec<Frame> {
    let mut c = vec![0u8; 38];
    write_i32_le(&mut c, 0, 1376769); // RESPONSE_MESSAGE_TYPE
    write_uuid(&mut c, 22, Some(uuid));
    vec![initial_frame(c)]
}

pub fn decode_transaction_commit(frames: &[Frame]) -> (i64, i64) {
    let c = &frames[0].content;
    (read_i64_le(c, 16), read_i64_le(c, 24))
}

pub fn encode_transaction_commit_response() -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 1376513); // RESPONSE_MESSAGE_TYPE
    vec![initial_frame(c)]
}

pub fn decode_transaction_rollback(frames: &[Frame]) -> (i64, i64) {
    let c = &frames[0].content;
    (read_i64_le(c, 16), read_i64_le(c, 24))
}

pub fn encode_transaction_rollback_response() -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 1377025);
    vec![initial_frame(c)]
}

pub fn decode_transactional_map_put(frames: &[Frame]) -> (String, (i64, i64), Vec<u8>, Vec<u8>) {
    let c = &frames[0].content;
    let txn_id = (read_i64_le(c, 16), read_i64_le(c, 24));
    let name = decode_string(&frames[1]);
    let key = frames[2].content.clone();
    let value = frames[3].content.clone();
    (name, txn_id, key, value)
}

pub fn encode_transactional_map_put_response(old: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 919041);
    if let Some(v) = old {
        vec![initial_frame(c), data_frame(v)]
    } else {
        vec![initial_frame(c), null_frame()]
    }
}
