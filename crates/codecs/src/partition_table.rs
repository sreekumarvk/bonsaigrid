//! EntryListUUIDListIntegerCodec.encode — the partition table.
//!
//! Layout (mirrors `EntryListUUIDListIntegerCodec.java`):
//!   BEGIN
//!   one frame per entry: packed i32 LE partition ids (the value list)
//!   END
//!   one frame: packed UUIDs (17B each) — the key list
//!
//! Key and value lists are positionally aligned: entry i pairs keyList[i] with
//! the i-th value frame.

use crate::{begin_frame, end_frame};
use protocol::fixed::{write_i32_le, write_uuid, UUID_SIZE};
use protocol::frame::Frame;

pub fn encode(out: &mut Vec<Frame>, entries: &[((i64, i64), Vec<i32>)]) {
    out.push(begin_frame());
    for (_uuid, parts) in entries {
        let mut content = vec![0u8; parts.len() * 4];
        for (i, p) in parts.iter().enumerate() {
            write_i32_le(&mut content, i * 4, *p);
        }
        out.push(Frame { flags: 0, content });
    }
    out.push(end_frame());

    let mut keys = vec![0u8; entries.len() * UUID_SIZE];
    for (i, (uuid, _)) in entries.iter().enumerate() {
        write_uuid(&mut keys, i * UUID_SIZE, Some(*uuid));
    }
    out.push(Frame {
        flags: 0,
        content: keys,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::fixed::{read_i32_le, read_uuid};
    use protocol::frame::{BEGIN_DS, END_DS};

    #[test]
    fn single_member_owns_all_partitions() {
        let parts: Vec<i32> = (0..271).collect();
        let mut f = Vec::new();
        encode(&mut f, &[((7, 9), parts.clone())]);
        // BEGIN, value-frame, END, key-frame
        assert!(f[0].flags & BEGIN_DS != 0);
        assert_eq!(f[1].content.len(), 271 * 4);
        assert_eq!(read_i32_le(&f[1].content, 0), 0);
        assert_eq!(read_i32_le(&f[1].content, 270 * 4), 270);
        assert!(f[2].flags & END_DS != 0);
        assert_eq!(read_uuid(&f[3].content, 0), Some((7, 9)));
    }
}
