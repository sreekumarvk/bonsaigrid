use protocol::fixed::write_i32_le;
use protocol::frame::{Frame, UNFRAGMENTED};
use protocol::primitives::{initial_frame, string_frame};

/// Encodes MCGetTimedMemberStateResponse (2099969) which carries a single
/// JSON string representing the TimedMemberState.
pub fn encode_get_timed_member_state_response(json_state: &str) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, 2099969); // RESPONSE_MESSAGE_TYPE
    vec![initial_frame(c), string_frame(json_state)]
}
