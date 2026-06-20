//! ClientAddClusterViewListenerCodec: response (769) + members-view (770) and
//! partitions-view (771) events. Events set IS_EVENT on the initial frame and
//! carry `version` at offset 16 (after partitionId).

use crate::auth::MemberTuple;
use crate::{member_info, partition_table};
use protocol::fixed::write_i32_le;
use protocol::frame::{Frame, IS_EVENT, UNFRAGMENTED};
use protocol::primitives::initial_frame;

pub fn encode_response() -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, 769);
    vec![initial_frame(c)]
}

fn event_initial(msg_type: i32, version: i32) -> Frame {
    let mut c = vec![0u8; 20]; // type@0, corr@4, partitionId@12, version@16
    write_i32_le(&mut c, 0, msg_type);
    write_i32_le(&mut c, 12, -1); // partitionId
    write_i32_le(&mut c, 16, version);
    Frame { flags: UNFRAGMENTED | IS_EVENT, content: c }
}

pub fn members_view_event(version: i32, members: &[MemberTuple]) -> Vec<Frame> {
    let mut out = vec![event_initial(770, version)];
    member_info::encode_list(&mut out, members);
    out
}

pub fn partitions_view_event(version: i32, partitions: &[((i64, i64), Vec<i32>)]) -> Vec<Frame> {
    let mut out = vec![event_initial(771, version)];
    partition_table::encode(&mut out, partitions);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::frame::IS_EVENT;
    use protocol::message::msg_type;

    #[test]
    fn response_is_type_769() {
        assert_eq!(msg_type(&encode_response()), 769);
    }

    #[test]
    fn members_event_sets_event_flag_and_type_770() {
        let ev = members_view_event(1, &[((1, 1), "127.0.0.1".into(), 5701, false, (5, 8, 0))]);
        assert_eq!(msg_type(&ev), 770);
        assert!(ev[0].flags & IS_EVENT != 0);
    }

    #[test]
    fn partitions_event_type_771() {
        let ev = partitions_view_event(1, &[((1, 1), (0..271).collect())]);
        assert_eq!(msg_type(&ev), 771);
        assert!(ev[0].flags & IS_EVENT != 0);
    }
}
