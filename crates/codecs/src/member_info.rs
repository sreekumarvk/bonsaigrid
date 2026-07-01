//! MemberInfoCodec.encode for the single member this node advertises.
//!
//! Layout (mirrors `MemberInfoCodec.java`):
//!   BEGIN
//!   initial frame: uuid (17B) | liteMember (1B bool)
//!   Address (BEGIN/port/host/END)
//!   attributes Map<String,String>  (BEGIN/END — empty here)
//!   MemberVersion (BEGIN/[major,minor,patch]/END)
//!   addressMap Map (BEGIN/END — empty here)
//!   END

use crate::{address, begin_frame, end_frame};
use protocol::fixed::write_uuid;
use protocol::frame::Frame;

/// Encode one member. `uuid` = (msb, lsb); `version` = (major, minor, patch).
pub fn encode(
    out: &mut Vec<Frame>,
    uuid: (i64, i64),
    host: &str,
    port: i32,
    lite: bool,
    version: (u8, u8, u8),
) {
    out.push(begin_frame());

    // initial frame: 17-byte uuid + 1-byte lite flag
    let mut initial = vec![0u8; 18];
    write_uuid(&mut initial, 0, Some(uuid));
    initial[17] = if lite { 1 } else { 0 };
    out.push(Frame {
        flags: 0,
        content: initial,
    });

    address::encode(out, host, port);

    // empty attributes map
    out.push(begin_frame());
    out.push(end_frame());

    // MemberVersion: BEGIN, [major, minor, patch], END
    out.push(begin_frame());
    out.push(Frame {
        flags: 0,
        content: vec![version.0, version.1, version.2],
    });
    out.push(end_frame());

    // empty addressMap
    out.push(begin_frame());
    out.push(end_frame());

    out.push(end_frame());
}

/// Encode a List<MemberInfo> (ListMultiFrame: BEGIN, items..., END).
pub fn encode_list(
    out: &mut Vec<Frame>,
    members: &[((i64, i64), String, i32, bool, (u8, u8, u8))],
) {
    out.push(begin_frame());
    for (uuid, host, port, lite, version) in members {
        encode(out, *uuid, host, *port, *lite, *version);
    }
    out.push(end_frame());
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::fixed::read_uuid;
    use protocol::frame::{BEGIN_DS, END_DS};

    #[test]
    fn member_encodes_begin_uuid_lite_then_nested_then_end() {
        let mut f = Vec::new();
        encode(
            &mut f,
            (123456789, 987654321),
            "127.0.0.1",
            5701,
            false,
            (5, 8, 0),
        );
        assert!(f[0].flags & BEGIN_DS != 0);
        assert_eq!(read_uuid(&f[1].content, 0), Some((123456789, 987654321)));
        assert_eq!(f[1].content[17], 0); // not lite
        assert!(f.last().unwrap().flags & END_DS != 0);
    }

    #[test]
    fn member_list_wraps_in_begin_end() {
        let mut f = Vec::new();
        encode_list(
            &mut f,
            &[((1, 1), "127.0.0.1".into(), 5701, false, (5, 8, 0))],
        );
        assert!(f[0].flags & BEGIN_DS != 0);
        assert!(f.last().unwrap().flags & END_DS != 0);
    }
}
