//! Message dispatch: the cluster handshake and map ops.
//!
//! The handshake advertises the full member list and a deterministic partition
//! table (partition `p` is owned by member `p % N`). A stock smart client routes
//! each key to its partition's owner, so each member stores/serves only its own
//! partitions — no server-side partition hashing or member-to-member traffic is
//! needed for correctness.

use codecs::auth::{self, AuthResponse, MemberTuple};
use codecs::{cluster_view, map};
use protocol::fixed::{read_i32_le, read_i64_le, write_i32_le, write_i64_le, write_u16_le, write_uuid};
use protocol::frame::{read_message, write_message, Frame, IS_FINAL, IS_NULL, UNFRAGMENTED};
use protocol::message::{correlation_id, msg_type, set_correlation_id};
use store::Store;

// ---- Zero-allocation hot path (MapGet) -------------------------------------
//
// The reactor hands us a complete request message as a byte slice and a reused
// output buffer. For the hottest op (MapGet) we parse the request frames in
// place and encode the response straight into the output buffer, copying the
// value directly out of the slab under the shard lock — no `Vec<Frame>`, no
// intermediate value `Vec`, no allocation after warmup. All other ops fall back
// to the frame-based path below.

fn frame_at(msg: &[u8], off: usize) -> Option<(&[u8], usize)> {
    if off + 6 > msg.len() {
        return None;
    }
    let len = read_i32_le(msg, off) as usize;
    if len < 6 || off + len > msg.len() {
        return None;
    }
    Some((&msg[off + 6..off + len], off + len))
}

fn encode_get_into(out: &mut Vec<u8>, corr: i64, v: Option<&[u8]>) {
    // initial frame: 13-byte content [type 66049 @0, corr @4, backupAcks @12]
    let mut hdr = [0u8; 19];
    write_i32_le(&mut hdr, 0, 19); // frame length = 6 + 13
    write_u16_le(&mut hdr, 4, UNFRAGMENTED);
    write_i32_le(&mut hdr, 6, 66049);
    write_i64_le(&mut hdr, 10, corr);
    out.extend_from_slice(&hdr);
    let mut p = [0u8; 6];
    match v {
        Some(val) => {
            write_i32_le(&mut p, 0, (6 + val.len()) as i32);
            write_u16_le(&mut p, 4, IS_FINAL);
            out.extend_from_slice(&p);
            out.extend_from_slice(val);
        }
        None => {
            write_i32_le(&mut p, 0, 6);
            write_u16_le(&mut p, 4, IS_NULL | IS_FINAL);
            out.extend_from_slice(&p);
        }
    }
}

fn try_fast_get(msg: &[u8], store: &Store, out: &mut Vec<u8>) -> bool {
    let Some((c0, off1)) = frame_at(msg, 0) else { return false };
    if c0.len() < 12 || read_i32_le(c0, 0) != 66048 {
        return false; // not MapGet
    }
    let corr = read_i64_le(c0, 4);
    let Some((name_b, off2)) = frame_at(msg, off1) else { return false };
    let Some((key_b, _)) = frame_at(msg, off2) else { return false };
    let Ok(name) = std::str::from_utf8(name_b) else { return false };
    store.get_with(name, key_b, |v| encode_get_into(out, corr, v));
    true
}

/// Hazelcast REST health endpoints (served on the main port via protocol
/// detection). Operators' existing health checks / k8s probes / load balancers
/// keep working unchanged. Returns (status, content-type, body).
pub fn http_health(path: &str, cluster_size: usize) -> (u16, &'static str, String) {
    let text = "text/plain";
    match path {
        "/hazelcast/health/node-state" => (200, text, "ACTIVE".into()),
        "/hazelcast/health/cluster-state" => (200, text, "ACTIVE".into()),
        "/hazelcast/health/cluster-safe" => (200, text, "TRUE".into()),
        "/hazelcast/health/migration-queue-size" => (200, text, "0".into()),
        "/hazelcast/health/cluster-size" => (200, text, cluster_size.to_string()),
        "/hazelcast/health/ready" => (200, text, String::new()),
        "/hazelcast/health" | "/hazelcast/health/" => (
            200,
            "application/json",
            format!(
                "{{\"nodeState\":\"ACTIVE\",\"clusterState\":\"ACTIVE\",\"clusterSafe\":true,\"migrationQueueSize\":0,\"clusterSize\":{cluster_size}}}"
            ),
        ),
        _ => (404, text, "Not Found".into()),
    }
}

/// Feed one complete request message; append framed reply bytes to `out`.
pub fn dispatch_bytes(msg: &[u8], store: &Store, cfg: &Cfg, out: &mut Vec<u8>) {
    if try_fast_get(msg, store, out) {
        return;
    }
    if let Some((frames, _)) = read_message(msg) {
        for reply in dispatch(frames, store, cfg) {
            out.extend_from_slice(&write_message(&reply));
        }
    }
}

pub const CLUSTER_ID: (i64, i64) = (2, 2);
pub const PARTITION_COUNT: i32 = 271;
pub const SERVER_VERSION: &str = "5.8.0";
pub const VERSION: (u8, u8, u8) = (5, 8, 0);
const TPC_TOKEN: &[u8] = b"bonsaigrid-tpc-token";

/// Stable registration id handed back for listener registrations.
pub const REGISTRATION_UUID: (i64, i64) = (3, 3);

/// One cluster member.
#[derive(Clone)]
pub struct Member {
    pub uuid: (i64, i64),
    pub host: String,
    pub port: i32,
}

/// Runtime config the handshake needs: the full membership, which member *this*
/// process is, and this member's TPC ports (empty disables TPC advertisement).
#[derive(Clone)]
pub struct Cfg {
    pub members: Vec<Member>,
    pub self_index: usize,
    pub tpc_ports: Vec<i32>,
}

impl Cfg {
    /// Single-node, single-member cluster.
    pub fn single() -> Cfg {
        Cfg {
            members: vec![Member { uuid: (1, 1), host: "127.0.0.1".into(), port: 5701 }],
            self_index: 0,
            tpc_ports: Vec::new(),
        }
    }

    fn member_tuples(&self) -> Vec<MemberTuple> {
        self.members
            .iter()
            .map(|m| (m.uuid, m.host.clone(), m.port, false, VERSION))
            .collect()
    }

    /// Deterministic table: member `i` owns partitions `{p : p % N == i}`.
    fn partition_table(&self) -> Vec<((i64, i64), Vec<i32>)> {
        let n = self.members.len() as i32;
        self.members
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let parts = (0..PARTITION_COUNT).filter(|p| p % n == i as i32).collect();
                (m.uuid, parts)
            })
            .collect()
    }

    fn self_member(&self) -> &Member {
        &self.members[self.self_index]
    }
}

fn auth_response(cfg: &Cfg) -> Vec<Frame> {
    let mem = cfg.member_tuples();
    let parts = cfg.partition_table();
    let me = cfg.self_member();
    let tpc = if cfg.tpc_ports.is_empty() {
        (None, None)
    } else {
        (Some(cfg.tpc_ports.as_slice()), Some(TPC_TOKEN))
    };
    auth::encode_response(&AuthResponse {
        status: 0, // AUTHENTICATED
        member_uuid: me.uuid,
        serialization_version: 1,
        partition_count: PARTITION_COUNT,
        cluster_id: CLUSTER_ID,
        server_version: SERVER_VERSION,
        address_host: me.host.as_str(),
        address_port: me.port,
        member_list_version: 1,
        members: &mem,
        partition_list_version: 1,
        partitions: &parts,
        tpc_ports: tpc.0,
        tpc_token: tpc.1,
    })
}

fn empty_response(msg_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, msg_type);
    vec![Frame { flags: UNFRAGMENTED, content: c }]
}

/// A response whose initial frame carries a single UUID at offset 13 (after
/// backupAcks). Used by listener-registration responses that return a
/// registration id. Since this increment sends no events for these listeners,
/// any stable id works.
fn uuid_response(msg_type: i32, uuid: (i64, i64)) -> Vec<Frame> {
    let mut c = vec![0u8; 30]; // type@0, corr@4, backupAcks@12, uuid@13 (17B)
    write_i32_le(&mut c, 0, msg_type);
    write_uuid(&mut c, 13, Some(uuid));
    vec![Frame { flags: UNFRAGMENTED, content: c }]
}

pub fn dispatch(req: Vec<Frame>, store: &Store, cfg: &Cfg) -> Vec<Vec<Frame>> {
    let corr = correlation_id(&req);
    let mut replies: Vec<Vec<Frame>> = match msg_type(&req) {
        256 => vec![auth_response(cfg)],
        // ClientTpcAuthentication: a TPC client authenticates each per-core
        // channel with the token from the main auth. Response is an empty ack.
        5632 => vec![empty_response(5633)],
        768 => {
            let mem = cfg.member_tuples();
            let parts = cfg.partition_table();
            vec![
                cluster_view::encode_response(),
                cluster_view::members_view_event(1, &mem),
                cluster_view::partitions_view_event(1, &parts),
            ]
        }
        // MapPut (with TTL ms; <=0 means no expiry)
        65792 => {
            let r = map::decode_put(&req);
            // Verify server-side partition computation matches the client's
            // routing: a member only receives keys whose partition it owns, so
            // computed_partition % N must equal this member's index.
            if std::env::var_os("BONSAI_VERIFY_PARTITION").is_some() {
                let n = cfg.members.len() as i32;
                let computed = serialization::partition_id(&r.key, PARTITION_COUNT);
                let owner = computed % n;
                eprintln!(
                    "PARTITION {} computed={computed} owner={owner} self={}",
                    if owner == cfg.self_index as i32 { "OK" } else { "MISMATCH" },
                    cfg.self_index
                );
            }
            let ttl = if r.ttl > 0 { r.ttl as u64 } else { 0 };
            let old = store.put_ttl(&r.name, r.key, r.value, ttl);
            vec![map::encode_put_response(old.as_deref())]
        }
        // MapGet
        66048 => {
            let r = map::decode_get(&req);
            let v = store.get(&r.name, &r.key);
            vec![map::encode_get_response(v.as_deref())]
        }
        // MapRemove -> old value
        66304 => {
            let r = map::decode_get(&req);
            let old = store.remove(&r.name, &r.key);
            vec![map::data_response(66305, old.as_deref())]
        }
        // MapDelete -> void
        67840 => {
            let r = map::decode_get(&req);
            store.remove(&r.name, &r.key);
            vec![empty_response(67841)]
        }
        // MapContainsKey -> bool
        67072 => {
            let r = map::decode_get(&req);
            vec![map::bool_response(67073, store.contains_key(&r.name, &r.key))]
        }
        // MapContainsValue -> bool
        67328 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(67329, store.contains_value(&name, &value))]
        }
        // MapSize -> int
        76288 => {
            let name = map::decode_name(&req);
            vec![map::int_response(76289, store.size(&name) as i32)]
        }
        // MapIsEmpty -> bool
        76544 => {
            let name = map::decode_name(&req);
            vec![map::bool_response(76545, store.is_empty(&name))]
        }
        // MapPutIfAbsent -> existing value (or null)
        69120 => {
            let r = map::decode_put(&req);
            let ttl = if r.ttl > 0 { r.ttl as u64 } else { 0 };
            let existing = store.put_if_absent(&r.name, r.key, r.value, ttl);
            vec![map::data_response(69121, existing.as_deref())]
        }
        // MapReplace -> old value (only if present)
        66560 => {
            let r = map::decode_replace(&req);
            let old = store.replace(&r.name, r.key, r.value);
            vec![map::data_response(66561, old.as_deref())]
        }
        // MapClear -> void
        77056 => {
            let name = map::decode_name(&req);
            store.clear(&name);
            vec![empty_response(77057)]
        }
        // ClientLocalBackupListener: smart clients register it; response is a
        // UUID registration id at offset 13. We never push backup events.
        3840 => vec![uuid_response(3841, REGISTRATION_UUID)],
        // ClientCreateProxy: client creates a distributed-object proxy (e.g. on
        // getMap). Response is an empty ack.
        1024 => vec![empty_response(1025)],
        // ClientStatistics: periodic client metrics push. Empty ack.
        3072 => vec![empty_response(3073)],
        // Unknown op: ack with an empty response of type+1 so the client does
        // not hang (covers e.g. CreateProxy). The live client reveals any op
        // that needs a richer reply (per plan's empirical-risk note).
        other => {
            eprintln!("UNKNOWN op type {other} (0x{other:06x}) -> empty ack {}", other + 1);
            vec![empty_response(other + 1)]
        }
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

    fn cluster_cfg(n: usize, self_index: usize) -> Cfg {
        Cfg {
            members: (0..n)
                .map(|i| Member {
                    uuid: (1, (i + 1) as i64),
                    host: "127.0.0.1".into(),
                    port: 5701 + i as i32,
                })
                .collect(),
            self_index,
            tpc_ports: Vec::new(),
        }
    }

    #[test]
    fn partition_table_covers_all_partitions_by_p_mod_n() {
        let cfg = cluster_cfg(3, 0);
        let table = cfg.partition_table();
        assert_eq!(table.len(), 3);
        let total: usize = table.iter().map(|(_, p)| p.len()).sum();
        assert_eq!(total, PARTITION_COUNT as usize, "every partition assigned exactly once");
        for (i, (_, parts)) in table.iter().enumerate() {
            for &p in parts {
                assert_eq!((p as usize) % 3, i, "member {i} owns p%3==i");
            }
        }
    }

    #[test]
    fn auth_reports_self_member_identity() {
        // member index 2 should report its own uuid (1,3) in the response header.
        let cfg = cluster_cfg(3, 2);
        let out = dispatch(request(256, 1), &Store::new(), &cfg);
        assert_eq!(msg_type(&out[0]), 257);
        // member_uuid lives at offset 14 (after backupAcks@12 + status@13).
        assert_eq!(protocol::fixed::read_uuid(&out[0][0].content, 14), Some((1, 3)));
    }

    #[test]
    fn http_health_endpoints() {
        assert_eq!(http_health("/hazelcast/health/node-state", 1).2, "ACTIVE");
        assert_eq!(http_health("/hazelcast/health/cluster-state", 3).2, "ACTIVE");
        assert_eq!(http_health("/hazelcast/health/cluster-safe", 1).2, "TRUE");
        assert_eq!(http_health("/hazelcast/health/cluster-size", 3).2, "3");
        assert_eq!(http_health("/hazelcast/health/migration-queue-size", 1).2, "0");
        let (status, ctype, body) = http_health("/hazelcast/health", 5);
        assert_eq!(status, 200);
        assert_eq!(ctype, "application/json");
        assert!(body.contains("\"clusterSize\":5"));
        assert_eq!(http_health("/nope", 1).0, 404);
    }

    #[test]
    fn auth_replies_257_with_echoed_correlation() {
        let store = Store::new();
        let out = dispatch(request(256, 99), &store, &Cfg::single());
        assert_eq!(out.len(), 1);
        assert_eq!(msg_type(&out[0]), 257);
        assert_eq!(correlation_id(&out[0]), 99);
    }

    #[test]
    fn cluster_view_replies_response_plus_two_events() {
        let store = Store::new();
        let out = dispatch(request(768, 5), &store, &Cfg::single());
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
    fn local_backup_listener_replies_3841_with_uuid() {
        use protocol::fixed::read_uuid;
        let store = Store::new();
        let out = dispatch(request(3840, 7), &store, &Cfg::single());
        assert_eq!(msg_type(&out[0]), 3841);
        // initial frame must be 30 bytes with the registration UUID at offset 13
        assert_eq!(out[0][0].content.len(), 30);
        assert_eq!(read_uuid(&out[0][0].content, 13), Some(REGISTRATION_UUID));
        assert_eq!(correlation_id(&out[0]), 7);
    }

    #[test]
    fn create_proxy_replies_empty_1025() {
        let store = Store::new();
        let out = dispatch(request(1024, 8), &store, &Cfg::single());
        assert_eq!(msg_type(&out[0]), 1025);
        assert_eq!(correlation_id(&out[0]), 8);
    }

    #[test]
    fn put_then_get_roundtrips_through_store() {
        let store = Store::new();
        let out = dispatch(put_request("m", &[1, 2], &[9], 1), &store, &Cfg::single());
        assert_eq!(msg_type(&out[0]), 65793);
        assert!(out[0][1].is_null()); // no prior value

        let out = dispatch(get_request("m", &[1, 2], 2), &store, &Cfg::single());
        assert_eq!(msg_type(&out[0]), 66049);
        assert_eq!(out[0][1].content, vec![9]);
    }
}
