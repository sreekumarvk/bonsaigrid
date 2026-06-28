//! ClientAuthenticationCodec: decode request (256), encode response (257).
//!
//! Request initial-frame offsets: type@0, correlationId@4, partitionId@12,
//! uuid@16 (17B), serializationVersion@33, routingMode@34, cpDirectToLeader@35.
//! Request var-frames: clusterName, username?, password?, clientType,
//! clientHazelcastVersion, clientName, labels.

use crate::{address, member_info, partition_table};
use protocol::fixed::{write_i32_le, write_uuid};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame, null_frame, string_frame};

pub struct AuthRequest {
    pub cluster_name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_type: String,
    pub serialization_version: u8,
    pub routing_mode: u8,
}

pub fn decode_request(frames: &[Frame]) -> AuthRequest {
    let initial = &frames[0].content;
    let serialization_version = initial[33];
    let routing_mode = if initial.len() > 34 { initial[34] } else { 0 };
    // Var-frames: each nullable field is exactly one frame, so positions are fixed:
    // [1]=clusterName [2]=username [3]=password [4]=clientType ...
    let nullable = |f: &Frame| if f.is_null() { None } else { Some(decode_string(f)) };
    AuthRequest {
        cluster_name: decode_string(&frames[1]),
        username: nullable(&frames[2]),
        password: nullable(&frames[3]),
        client_type: decode_string(&frames[4]),
        serialization_version,
        routing_mode,
    }
}

/// Member tuple: (uuid, host, port, lite, version(major,minor,patch)).
pub type MemberTuple = ((i64, i64), String, i32, bool, (u8, u8, u8));

pub struct AuthResponse<'a> {
    pub status: u8,
    pub member_uuid: (i64, i64),
    pub serialization_version: u8,
    pub partition_count: i32,
    pub cluster_id: (i64, i64),
    pub server_version: &'a str,
    pub address_host: &'a str,
    pub address_port: i32,
    pub member_list_version: i32,
    pub members: &'a [MemberTuple],
    pub partition_list_version: i32,
    pub partitions: &'a [((i64, i64), Vec<i32>)],
    /// One TPC port per core, or None to disable TPC.
    pub tpc_ports: Option<&'a [i32]>,
    pub tpc_token: Option<&'a [u8]>,
}

pub fn encode_response(r: &AuthResponse) -> Vec<Frame> {
    // Response initial-frame size = 62 (see ClientAuthenticationCodec offsets).
    let mut c = vec![0u8; 62];
    write_i32_le(&mut c, 0, 257); // RESPONSE_MESSAGE_TYPE
    c[12] = 0; // backupAcks
    c[13] = r.status;
    write_uuid(&mut c, 14, Some(r.member_uuid));
    c[31] = r.serialization_version;
    write_i32_le(&mut c, 32, r.partition_count);
    write_uuid(&mut c, 36, Some(r.cluster_id));
    c[53] = 0; // failoverSupported = false
    write_i32_le(&mut c, 54, r.member_list_version);
    write_i32_le(&mut c, 58, r.partition_list_version);

    let mut out = vec![initial_frame(c)];
    address::encode(&mut out, r.address_host, r.address_port); // nullable address (present)
    out.push(string_frame(r.server_version)); // serverHazelcastVersion
    match r.tpc_ports {
        // ListIntegerCodec: one frame of N little-endian i32 ports.
        Some(ports) => {
            let mut pc = vec![0u8; ports.len() * 4];
            for (i, p) in ports.iter().enumerate() {
                write_i32_le(&mut pc, i * 4, *p);
            }
            out.push(Frame { flags: 0, content: pc });
        }
        None => out.push(null_frame()),
    }
    match r.tpc_token {
        // ByteArrayCodec: one frame of raw bytes.
        Some(tok) => out.push(Frame { flags: 0, content: tok.to_vec() }),
        None => out.push(null_frame()),
    }
    member_info::encode_list(&mut out, r.members);
    partition_table::encode(&mut out, r.partitions);
    // keyValuePairs: empty map
    out.push(crate::begin_frame());
    out.push(crate::end_frame());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::message::msg_type;

    #[test]
    fn response_starts_with_type_257() {
        let resp = AuthResponse {
            status: 0,
            member_uuid: (1, 1),
            serialization_version: 1,
            partition_count: 271,
            cluster_id: (2, 2),
            server_version: "5.8.0",
            address_host: "127.0.0.1",
            address_port: 5701,
            member_list_version: 1,
            members: &[((1, 1), "127.0.0.1".into(), 5701, false, (5, 8, 0))],
            partition_list_version: 1,
            partitions: &[((1, 1), (0..271).collect())],
            tpc_ports: None,
            tpc_token: None,
        };
        let frames = encode_response(&resp);
        assert_eq!(msg_type(&frames), 257);
    }
}
