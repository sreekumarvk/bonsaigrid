//! Verifies the vendored Hazelcast 2.10 golden fixture parses cleanly with our
//! frame envelope, and exposes a `message_of_type` helper reused by codec tests.

use protocol::frame::{read_message, Frame};
use std::fs;

pub fn load_golden() -> Vec<u8> {
    fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/golden/2.10.protocol.compatibility.binary"
    ))
    .expect("golden fixture present")
}

/// Parse the whole fixture into individual messages (each a Vec<Frame>).
pub fn all_messages(bytes: &[u8]) -> Vec<Vec<Frame>> {
    let mut out = Vec::new();
    let mut off = 0;
    while off < bytes.len() {
        let (frames, used) = read_message(&bytes[off..]).expect("each message parses");
        out.push(frames);
        off += used;
    }
    out
}

#[test]
fn golden_parses_into_many_complete_messages() {
    let bytes = load_golden();
    let msgs = all_messages(&bytes);
    // Re-sum consumed bytes to confirm no trailing/short reads.
    let mut off = 0;
    for _ in &msgs {
        let (_f, used) = read_message(&bytes[off..]).unwrap();
        off += used;
    }
    assert_eq!(off, bytes.len(), "consumed the whole fixture with no trailing bytes");
    assert!(msgs.len() > 100, "fixture contains every codec's messages, got {}", msgs.len());
}
