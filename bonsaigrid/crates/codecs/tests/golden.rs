//! Conformance against Hazelcast's committed 2.10 golden fixture.
//! Decoders must recover the `ReferenceObjects` values; encoders must reproduce
//! the exact bytes. Reference values (from `ReferenceObjects.java`):
//!   aString = "localhost"; aData = b"111313123131313131"; aLong = -50992225;
//!   aByte = 113; aUUID = (123456789, 987654321).

use protocol::frame::{read_message, write_message, Frame};
use protocol::message::{msg_type, set_correlation_id};
use std::fs;

const A_STRING: &str = "localhost";
const A_DATA: &[u8] = b"111313123131313131";
const A_LONG: i64 = -50992225;
const A_BYTE: u8 = 113;

fn load_golden() -> Vec<u8> {
    fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/golden/2.10.protocol.compatibility.binary"
    ))
    .expect("golden fixture present")
}

fn message_of_type(t: i32) -> Vec<Frame> {
    let bytes = load_golden();
    let mut off = 0;
    while off < bytes.len() {
        let (frames, used) = read_message(&bytes[off..]).unwrap();
        if msg_type(&frames) == t {
            return frames;
        }
        off += used;
    }
    panic!("no message of type {t} in fixture");
}

/// Compare two messages ignoring the correlation-id field (the fixture and our
/// encoders both leave it 0 pre-dispatch, but normalize to be safe).
fn assert_same_message(mut a: Vec<Frame>, mut b: Vec<Frame>) {
    set_correlation_id(&mut a, 0);
    set_correlation_id(&mut b, 0);
    assert_eq!(write_message(&a), write_message(&b));
}

#[test]
fn auth_request_decodes_reference_values() {
    let req = codecs::auth::decode_request(&message_of_type(256));
    assert_eq!(req.cluster_name, A_STRING);
    assert_eq!(req.client_type, A_STRING);
    assert_eq!(req.serialization_version, A_BYTE);
    assert_eq!(req.routing_mode, A_BYTE);
}

#[test]
fn map_put_request_decodes_reference_values() {
    let req = codecs::map::decode_put(&message_of_type(65792));
    assert_eq!(req.name, A_STRING);
    assert_eq!(req.key, A_DATA);
    assert_eq!(req.value, A_DATA);
    assert_eq!(req.thread_id, A_LONG);
    assert_eq!(req.ttl, A_LONG);
}

#[test]
fn map_get_request_decodes_reference_values() {
    let req = codecs::map::decode_get(&message_of_type(66048));
    assert_eq!(req.name, A_STRING);
    assert_eq!(req.key, A_DATA);
    assert_eq!(req.thread_id, A_LONG);
}

#[test]
fn map_put_response_encodes_to_golden_bytes() {
    assert_same_message(codecs::map::encode_put_response(Some(A_DATA)), message_of_type(65793));
}

#[test]
fn map_get_response_encodes_to_golden_bytes() {
    assert_same_message(codecs::map::encode_get_response(Some(A_DATA)), message_of_type(66049));
}
