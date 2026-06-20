//! Message dispatch: the single-node handshake and map ops.
//!
//! Single-core degenerate case of the cross-core routing invariant: this one
//! member owns all 271 partitions (`core 0 owns p where p % 1 == 0`).

use codecs::auth::{self, AuthResponse, MemberTuple};
use codecs::{cluster_view, map};
use protocol::fixed::write_i32_le;
use protocol::frame::{Frame, UNFRAGMENTED};
use protocol::message::{correlation_id, msg_type, set_correlation_id};
use store::Store;

pub const MEMBER_UUID: (i64, i64) = (1, 1);
pub const CLUSTER_ID: (i64, i64) = (2, 2);
pub const PARTITION_COUNT: i32 = 271;
pub const HOST: &str = "127.0.0.1";
pub const PORT: i32 = 5701;
pub const SERVER_VERSION: &str = "5.8.0";
pub const VERSION: (u8, u8, u8) = (5, 8, 0);

fn members() -> Vec<MemberTuple> {
    vec![(MEMBER_UUID, HOST.to_string(), PORT, false, VERSION)]
}

fn partitions() -> Vec<((i64, i64), Vec<i32>)> {
    vec![(MEMBER_UUID, (0..PARTITION_COUNT).collect())]
}

fn auth_response() -> Vec<Frame> {
    let mem = members();
    let parts = partitions();
    auth::encode_response(&AuthResponse {
        status: 0, // AUTHENTICATED
        member_uuid: MEMBER_UUID,
        serialization_version: 1,
        partition_count: PARTITION_COUNT,
        cluster_id: CLUSTER_ID,
        server_version: SERVER_VERSION,
        address_host: HOST,
        address_port: PORT,
        member_list_version: 1,
        members: &mem,
        partition_list_version: 1,
        partitions: &parts,
    })
}

fn empty_response(msg_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, msg_type);
    vec![Frame { flags: UNFRAGMENTED, content: c }]
}

pub fn dispatch(req: Vec<Frame>, store: &Store) -> Vec<Vec<Frame>> {
    let corr = correlation_id(&req);
    let mut replies: Vec<Vec<Frame>> = match msg_type(&req) {
        256 => vec![auth_response()],
        768 => vec![
            cluster_view::encode_response(),
            cluster_view::members_view_event(1, &members()),
            cluster_view::partitions_view_event(1, &partitions()),
        ],
        65792 => {
            let r = map::decode_put(&req);
            let old = store.put(&r.name, r.key, r.value);
            vec![map::encode_put_response(old.as_deref())]
        }
        66048 => {
            let r = map::decode_get(&req);
            let v = store.get(&r.name, &r.key);
            vec![map::encode_get_response(v.as_deref())]
        }
        // Unknown op: ack with an empty response of type+1 so the client does
        // not hang (covers e.g. CreateProxy). The live client reveals any op
        // that needs a richer reply (per plan's empirical-risk note).
        other => vec![empty_response(other + 1)],
    };
    for reply in replies.iter_mut() {
        set_correlation_id(reply, corr);
    }
    replies
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(msg_type: i32, corr: i64) -> Vec<Frame> {
        let mut c = vec![0u8; 16];
        write_i32_le(&mut c, 0, msg_type);
        let mut f = vec![Frame { flags: UNFRAGMENTED, content: c }];
        set_correlation_id(&mut f, corr);
        f
    }

    #[test]
    fn auth_replies_257_with_echoed_correlation() {
        let store = Store::new();
        let out = dispatch(request(256, 99), &store);
        assert_eq!(out.len(), 1);
        assert_eq!(msg_type(&out[0]), 257);
        assert_eq!(correlation_id(&out[0]), 99);
    }

    #[test]
    fn cluster_view_replies_response_plus_two_events() {
        let store = Store::new();
        let out = dispatch(request(768, 5), &store);
        assert_eq!(out.len(), 3);
        assert_eq!(msg_type(&out[0]), 769);
        assert_eq!(msg_type(&out[1]), 770);
        assert_eq!(msg_type(&out[2]), 771);
        for m in &out {
            assert_eq!(correlation_id(m), 5);
        }
    }

    fn put_request(name: &str, key: &[u8], value: &[u8], corr: i64) -> Vec<Frame> {
        let mut c = vec![0u8; 32]; // threadId@16, ttl@24
        write_i32_le(&mut c, 0, 65792);
        let mut f = vec![
            Frame { flags: UNFRAGMENTED, content: c },
            protocol::primitives::string_frame(name),
            protocol::primitives::data_frame(key),
            protocol::primitives::data_frame(value),
        ];
        set_correlation_id(&mut f, corr);
        f
    }

    fn get_request(name: &str, key: &[u8], corr: i64) -> Vec<Frame> {
        let mut c = vec![0u8; 24]; // threadId@16
        write_i32_le(&mut c, 0, 66048);
        let mut f = vec![
            Frame { flags: UNFRAGMENTED, content: c },
            protocol::primitives::string_frame(name),
            protocol::primitives::data_frame(key),
        ];
        set_correlation_id(&mut f, corr);
        f
    }

    #[test]
    fn put_then_get_roundtrips_through_store() {
        let store = Store::new();
        let out = dispatch(put_request("m", &[1, 2], &[9], 1), &store);
        assert_eq!(msg_type(&out[0]), 65793);
        assert!(out[0][1].is_null()); // no prior value

        let out = dispatch(get_request("m", &[1, 2], 2), &store);
        assert_eq!(msg_type(&out[0]), 66049);
        assert_eq!(out[0][1].content, vec![9]);
    }
}
