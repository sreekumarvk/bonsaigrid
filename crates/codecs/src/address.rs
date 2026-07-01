//! AddressCodec: BEGIN, initial frame [port i32 @0], host string, END.

use crate::{begin_frame, end_frame};
use protocol::fixed::write_i32_le;
use protocol::frame::Frame;
use protocol::primitives::string_frame;

pub fn encode(out: &mut Vec<Frame>, host: &str, port: i32) {
    out.push(begin_frame());
    let mut initial = vec![0u8; 4];
    write_i32_le(&mut initial, 0, port);
    out.push(Frame {
        flags: 0,
        content: initial,
    });
    out.push(string_frame(host));
    out.push(end_frame());
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::fixed::read_i32_le;
    use protocol::frame::{BEGIN_DS, END_DS};
    use protocol::primitives::decode_string;

    #[test]
    fn address_encodes_begin_port_host_end() {
        let mut frames = Vec::new();
        encode(&mut frames, "127.0.0.1", 5701);
        assert!(frames[0].flags & BEGIN_DS != 0);
        assert_eq!(read_i32_le(&frames[1].content, 0), 5701);
        assert_eq!(decode_string(&frames[2]), "127.0.0.1");
        assert!(frames[3].flags & END_DS != 0);
    }
}
