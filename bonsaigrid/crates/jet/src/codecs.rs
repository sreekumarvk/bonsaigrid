use protocol::frame::Frame;
use protocol::fixed::{read_i64_le, write_i32_le, write_i64_le};
use protocol::primitives::{initial_frame, data_frame, decode_string, null_frame};
use crate::dag::{Dag, Edge, Vertex};

// Decode JetSubmitJob (16646400)
// For simplicity, we just pretend it returns a JobId. In reality we'd parse the DAG.
pub fn decode_submit_job(frames: &[Frame]) -> (i64, Vec<u8>) {
    let job_id = read_i64_le(&frames[0].content, 16);
    // The DAG bytes are usually in frames[1] or similar
    // Just a placeholder implementation.
    (job_id, Vec::new())
}

pub fn encode_submit_job_response(job_id: i64) -> Vec<Frame> {
    let mut c = vec![0u8; 22]; // Assuming standard response length
    write_i32_le(&mut c, 0, 16646401); // RESPONSE
    write_i64_le(&mut c, 14, job_id); // offset might be wrong, dummy implementation
    vec![initial_frame(c)]
}

// JetGetJobStatus (16646912)
pub fn decode_get_job_status(frames: &[Frame]) -> i64 {
    read_i64_le(&frames[0].content, 16)
}

pub fn encode_get_job_status_response(status: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 16646913); // RESPONSE
    write_i32_le(&mut c, 14, status); // RUNNING=1, etc.
    vec![initial_frame(c)]
}

// JetJoinSubmittedJob (16647424)
pub fn decode_join_submitted_job(frames: &[Frame]) -> i64 {
    read_i64_le(&frames[0].content, 16)
}

pub fn encode_join_submitted_job_response() -> Vec<Frame> {
    let mut c = vec![0u8; 22];
    write_i32_le(&mut c, 0, 16647425);
    vec![initial_frame(c)]
}
