//! Accessors over a message's initial frame header.
//! Header offsets in the initial frame content: type @0 (i32), correlationId @4 (i64),
//! partitionId @12 (i32, requests) / backupAcks @12 (u8, responses).

use crate::fixed::{read_i32_le, read_i64_le, write_i64_le};
use crate::frame::Frame;

pub fn msg_type(frames: &[Frame]) -> i32 {
    read_i32_le(&frames[0].content, 0)
}
pub fn correlation_id(frames: &[Frame]) -> i64 {
    read_i64_le(&frames[0].content, 4)
}
pub fn set_correlation_id(frames: &mut [Frame], id: i64) {
    write_i64_le(&mut frames[0].content, 4, id);
}
pub fn partition_id(frames: &[Frame]) -> i32 {
    read_i32_le(&frames[0].content, 12)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Frame, UNFRAGMENTED};

    #[test]
    fn reads_type_correlation_partition_from_initial_frame() {
        let mut content = vec![0u8; 16];
        crate::fixed::write_i32_le(&mut content, 0, 66048); // type
        crate::fixed::write_i64_le(&mut content, 4, 42); // correlation
        crate::fixed::write_i32_le(&mut content, 12, 7); // partition
        let frames = vec![Frame {
            flags: UNFRAGMENTED,
            content,
        }];
        assert_eq!(msg_type(&frames), 66048);
        assert_eq!(correlation_id(&frames), 42);
        assert_eq!(partition_id(&frames), 7);
    }

    #[test]
    fn set_correlation_id_overwrites() {
        let mut frames = vec![Frame {
            flags: UNFRAGMENTED,
            content: vec![0u8; 16],
        }];
        set_correlation_id(&mut frames, 99);
        assert_eq!(correlation_id(&frames), 99);
    }
}
