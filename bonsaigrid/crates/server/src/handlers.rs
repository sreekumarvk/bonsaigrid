//! Message dispatch: the cluster handshake and map ops.
//!
//! The handshake advertises the full member list and a deterministic partition
//! table (partition `p` is owned by member `p % N`). A stock smart client routes
//! each key to its partition's owner, so each member stores/serves only its own
//! partitions — no server-side partition hashing or member-to-member traffic is
//! needed for correctness.

use crate::events::EventBroker;
use codecs::auth::{self, AuthResponse, MemberTuple};
use codecs::{cluster_view, map};
use serialization::compact::CompactExtractor;
use serialization::schema::SchemaService;
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
#[allow(clippy::too_many_arguments)]
pub fn dispatch_bytes(
    msg: &[u8],
    conn_id: u64,
    store: &Store,
    cfg: &Cfg,
    broker: &EventBroker,
    schemas: &SchemaService,
    out: &mut Vec<u8>,
) {
    if try_fast_get(msg, store, out) {
        return;
    }
    if let Some((frames, _)) = read_message(msg) {
        for reply in dispatch(frames, conn_id, store, cfg, broker, schemas) {
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
    /// Cluster name a client must present (the Hazelcast OSS auth gate).
    pub cluster_name: String,
    /// Optional username/password; when set, clients must match.
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Cfg {
    /// Single-node, single-member cluster.
    pub fn single() -> Cfg {
        Cfg {
            members: vec![Member { uuid: (1, 1), host: "127.0.0.1".into(), port: 5701 }],
            self_index: 0,
            tpc_ports: Vec::new(),
            cluster_name: "dev".into(),
            username: None,
            password: None,
        }
    }

    /// Authentication status for a request (0 AUTHENTICATED, 1 CREDENTIALS_FAILED,
    /// 3 NOT_ALLOWED_IN_CLUSTER).
    fn auth_status(&self, req: &codecs::auth::AuthRequest) -> u8 {
        if req.cluster_name != self.cluster_name {
            return 3;
        }
        if let Some(user) = &self.username {
            if req.username.as_deref() != Some(user.as_str())
                || req.password.as_deref() != self.password.as_deref()
            {
                return 1;
            }
        }
        0
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

fn auth_response(cfg: &Cfg, status: u8) -> Vec<Frame> {
    let mem = cfg.member_tuples();
    let parts = cfg.partition_table();
    let me = cfg.self_member();
    let tpc = if cfg.tpc_ports.is_empty() {
        (None, None)
    } else {
        (Some(cfg.tpc_ports.as_slice()), Some(TPC_TOKEN))
    };
    auth::encode_response(&AuthResponse {
        status,
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

/// Namespace a ReplicatedMap so it doesn't collide with an IMap of the same name.
fn repl(name: &str) -> String {
    format!("\u{1}repl:{name}")
}

/// Deliver a deferred MapLock (69633) grant to a waiting connection.
fn grant_lock_to_waiter(broker: &EventBroker, conn_id: u64, corr: i64) {
    let mut resp = empty_response(69633);
    set_correlation_id(&mut resp, corr);
    broker.enqueue(conn_id, write_message(&resp));
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

#[allow(clippy::too_many_arguments)]
pub fn dispatch(
    req: Vec<Frame>,
    conn_id: u64,
    store: &Store,
    cfg: &Cfg,
    broker: &EventBroker,
    schemas: &SchemaService,
) -> Vec<Vec<Frame>> {
    let corr = correlation_id(&req);
    let mut replies: Vec<Vec<Frame>> = match msg_type(&req) {
        // ---- Compact schema service ----
        // ClientSendSchema -> replicated members (single node: just us)
        4864 => {
            let schema = codecs::schema::decode_schema(&req, 1);
            schemas.put(schema);
            vec![codecs::schema::encode_uuid_list_response(4865, &[cfg.members[cfg.self_index].uuid])]
        }
        // ClientFetchSchema -> the schema (schemaId is a long @16)
        5120 => {
            let id = read_i64_le(&req[0].content, 16);
            vec![codecs::schema::encode_fetch_schema_response(5121, schemas.get(id).as_ref())]
        }
        // ClientSendAllSchemas -> empty ack (the Python client uses SendSchema)
        5376 => vec![empty_response(5377)],
        256 => {
            let areq = codecs::auth::decode_request(&req);
            vec![auth_response(cfg, cfg.auth_status(&areq))]
        }
        // MapAddNearCacheInvalidationListener: register + registration id.
        81664 => {
            let name = map::decode_name(&req);
            broker.register_near_cache(&name, conn_id, corr);
            vec![uuid_response(81665, REGISTRATION_UUID)]
        }
        // MapFetchNearCacheInvalidationMetadata: empty baseline.
        81152 => vec![map::encode_metadata_response(81153)],
        // MapAddEntryListener: register the listener; reply with a registration id.
        71936 => {
            let (name, flags, include_value) = map::decode_add_entry_listener(&req);
            broker.register(&name, conn_id, corr, flags, include_value);
            vec![uuid_response(71937, REGISTRATION_UUID)]
        }
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
            let el = broker.has_listeners(&r.name);
            let nc = broker.has_near_cache(&r.name);
            if el || nc {
                let old = store.put_ttl(&r.name, r.key.clone(), r.value.clone(), ttl);
                if el {
                    let etype = if old.is_some() { map::UPDATED } else { map::ADDED };
                    broker.publish(&r.name, etype, &r.key, Some(&r.value), old.as_deref());
                }
                if nc {
                    broker.invalidate(&r.name, &r.key);
                }
                vec![map::encode_put_response(old.as_deref())]
            } else {
                let old = store.put_ttl(&r.name, r.key, r.value, ttl);
                vec![map::encode_put_response(old.as_deref())]
            }
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
            if old.is_some() {
                if broker.has_listeners(&r.name) {
                    broker.publish(&r.name, map::REMOVED, &r.key, None, old.as_deref());
                }
                if broker.has_near_cache(&r.name) {
                    broker.invalidate(&r.name, &r.key);
                }
            }
            vec![map::data_response(66305, old.as_deref())]
        }
        // MapDelete -> void
        67840 => {
            let r = map::decode_get(&req);
            let old = store.remove(&r.name, &r.key);
            if old.is_some() {
                if broker.has_listeners(&r.name) {
                    broker.publish(&r.name, map::REMOVED, &r.key, None, old.as_deref());
                }
                if broker.has_near_cache(&r.name) {
                    broker.invalidate(&r.name, &r.key);
                }
            }
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
        // ---- Per-key locking ----
        // MapLock (blocking): grant now, or defer the response until granted.
        69632 => {
            let (name, key) = map::decode_name_key(&req);
            let tid = read_i64_le(&req[0].content, 16);
            if store.lock_or_wait(&name, &key, tid, conn_id, corr) {
                vec![empty_response(69633)]
            } else {
                vec![] // queued: the granting unlock will deliver the response
            }
        }
        // MapTryLock -> bool
        69888 => {
            let (name, key) = map::decode_name_key(&req);
            let tid = read_i64_le(&req[0].content, 16);
            vec![map::bool_response(69889, store.try_lock(&name, &key, tid))]
        }
        // MapUnlock -> void (may grant a waiter, delivered out-of-band)
        70400 => {
            let (name, key) = map::decode_name_key(&req);
            let tid = read_i64_le(&req[0].content, 16);
            if let Some((wc, wcorr)) = store.unlock(&name, &key, tid) {
                grant_lock_to_waiter(broker, wc, wcorr);
            }
            vec![empty_response(70401)]
        }
        // MapIsLocked -> bool
        70144 => {
            let (name, key) = map::decode_name_key(&req);
            vec![map::bool_response(70145, store.is_locked(&name, &key))]
        }
        // MapForceUnlock -> void
        78592 => {
            let (name, key) = map::decode_name_key(&req);
            if let Some((wc, wcorr)) = store.force_unlock(&name, &key) {
                grant_lock_to_waiter(broker, wc, wcorr);
            }
            vec![empty_response(78593)]
        }
        // MapGetAll -> EntryList<Data,Data>
        74496 => {
            let name = map::decode_name(&req);
            let keys = map::decode_data_list(&req, 2);
            let entries = store.get_all(&name, &keys);
            vec![map::encode_entry_list_response(74497, &entries)]
        }
        // MapPutAll -> void
        76800 => {
            let name = map::decode_name(&req);
            let entries = map::decode_entry_list(&req, 2);
            store.put_all(&name, entries);
            vec![empty_response(76801)]
        }
        // MapKeySet -> List<Data>
        74240 => {
            let name = map::decode_name(&req);
            let keys: Vec<Vec<u8>> = store.entries(&name).into_iter().map(|(k, _)| k).collect();
            vec![map::encode_data_list_response(74241, &keys)]
        }
        // MapValues -> List<Data>
        74752 => {
            let name = map::decode_name(&req);
            let vals: Vec<Vec<u8>> = store.entries(&name).into_iter().map(|(_, v)| v).collect();
            vec![map::encode_data_list_response(74753, &vals)]
        }
        // MapEntrySet -> EntryList<Data,Data>
        75008 => {
            let name = map::decode_name(&req);
            let entries = store.entries(&name);
            vec![map::encode_entry_list_response(75009, &entries)]
        }
        // ---- Predicate queries (full scan; Compact values) ----
        // MapKeySetWithPredicate -> List<Data> (keys of matching entries)
        75264 => {
            let (name, pred) = map::decode_name_value(&req);
            let matches = query::scan(&query::decode(&pred), store.entries(&name), schemas, &CompactExtractor);
            let keys: Vec<Vec<u8>> = matches.into_iter().map(|(k, _)| k).collect();
            vec![map::encode_data_list_response(75265, &keys)]
        }
        // MapValuesWithPredicate -> List<Data> (values of matching entries)
        75520 => {
            let (name, pred) = map::decode_name_value(&req);
            let matches = query::scan(&query::decode(&pred), store.entries(&name), schemas, &CompactExtractor);
            let vals: Vec<Vec<u8>> = matches.into_iter().map(|(_, v)| v).collect();
            vec![map::encode_data_list_response(75521, &vals)]
        }
        // MapEntriesWithPredicate -> EntryList<Data,Data> (matching entries)
        75776 => {
            let (name, pred) = map::decode_name_value(&req);
            let matches = query::scan(&query::decode(&pred), store.entries(&name), schemas, &CompactExtractor);
            vec![map::encode_entry_list_response(75777, &matches)]
        }
        // ---- Topic (pub/sub) ----
        262400 => {
            let (name, message) = map::decode_name_value(&req);
            broker.publish_topic(&name, &message); // no-ops if no subscribers
            vec![empty_response(262401)]
        }
        262656 => {
            let name = map::decode_name(&req);
            broker.register_topic(&name, conn_id, corr);
            vec![uuid_response(262657, REGISTRATION_UUID)]
        }
        // ---- Distributed Set ----
        394240 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(394241, store.set_add(&name, value))]
        }
        394496 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(394497, store.set_remove(&name, &value))]
        }
        393728 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(393729, store.set_contains(&name, &value))]
        }
        393472 => {
            let name = map::decode_name(&req);
            vec![map::int_response(393473, store.set_size(&name) as i32)]
        }
        395776 => {
            let name = map::decode_name(&req);
            vec![map::encode_data_list_response(395777, &store.set_get_all(&name))]
        }
        395520 => {
            let name = map::decode_name(&req);
            store.set_clear(&name);
            vec![empty_response(395521)]
        }
        // ---- MultiMap (Set semantics) ----
        131328 => {
            let name = map::decode_name(&req);
            let key = req[2].content.clone();
            let value = req[3].content.clone();
            vec![map::bool_response(131329, store.mm_put(&name, key, value))]
        }
        131584 => {
            let (name, key) = map::decode_name_key(&req);
            vec![map::encode_data_list_response(131585, &store.mm_get(&name, &key))]
        }
        131840 => {
            let (name, key) = map::decode_name_key(&req);
            vec![map::encode_data_list_response(131841, &store.mm_remove(&name, &key))]
        }
        133632 => {
            let name = map::decode_name(&req);
            vec![map::int_response(133633, store.mm_size(&name) as i32)]
        }
        134144 => {
            let (name, key) = map::decode_name_key(&req);
            vec![map::int_response(134145, store.mm_value_count(&name, &key) as i32)]
        }
        // ---- Distributed List ----
        328704 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(328705, store.list_add(&name, value))]
        }
        331520 => {
            let name = map::decode_name(&req);
            let index = read_i32_le(&req[0].content, 16); // ListGet index @16
            vec![map::data_response(331521, store.list_get(&name, index).as_deref())]
        }
        327936 => {
            let name = map::decode_name(&req);
            vec![map::int_response(327937, store.list_size(&name) as i32)]
        }
        328192 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(328193, store.list_contains(&name, &value))]
        }
        328960 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(328961, store.list_remove(&name, &value))]
        }
        330240 => {
            let name = map::decode_name(&req);
            vec![map::encode_data_list_response(330241, &store.list_get_all(&name))]
        }
        331008 => {
            let name = map::decode_name(&req);
            vec![map::bool_response(331009, store.list_is_empty(&name))]
        }
        329984 => {
            let name = map::decode_name(&req);
            store.list_clear(&name);
            vec![empty_response(329985)]
        }
        // ---- Distributed Queue ----
        196864 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(196865, store.queue_offer(&name, value))]
        }
        197888 => {
            let name = map::decode_name(&req);
            vec![map::data_response(197889, store.queue_poll(&name).as_deref())]
        }
        198400 => {
            let name = map::decode_name(&req);
            vec![map::data_response(198401, store.queue_peek(&name).as_deref())]
        }
        197376 => {
            let name = map::decode_name(&req);
            vec![map::int_response(197377, store.queue_size(&name) as i32)]
        }
        197632 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(197633, store.queue_remove(&name, &value))]
        }
        199424 => {
            let (name, value) = map::decode_name_value(&req);
            vec![map::bool_response(199425, store.queue_contains(&name, &value))]
        }
        201728 => {
            let name = map::decode_name(&req);
            vec![map::bool_response(201729, store.queue_is_empty(&name))]
        }
        200448 => {
            let name = map::decode_name(&req);
            store.queue_clear(&name);
            vec![empty_response(200449)]
        }
        // ---- ReplicatedMap (single-node: an IMap in a private namespace) ----
        852224 => {
            let r = repl(&map::decode_name(&req));
            let old = store.put(&r, req[2].content.clone(), req[3].content.clone());
            vec![map::data_response(852225, old.as_deref())]
        }
        853504 => {
            let (n, k) = map::decode_name_key(&req);
            vec![map::data_response(853505, store.get(&repl(&n), &k).as_deref())]
        }
        853760 => {
            let (n, k) = map::decode_name_key(&req);
            vec![map::data_response(853761, store.remove(&repl(&n), &k).as_deref())]
        }
        852992 => {
            let (n, k) = map::decode_name_key(&req);
            vec![map::bool_response(852993, store.contains_key(&repl(&n), &k))]
        }
        853248 => {
            let (n, v) = map::decode_name_value(&req);
            vec![map::bool_response(853249, store.contains_value(&repl(&n), &v))]
        }
        852480 => vec![map::int_response(852481, store.size(&repl(&map::decode_name(&req))) as i32)],
        852736 => vec![map::bool_response(852737, store.is_empty(&repl(&map::decode_name(&req))))],
        854272 => {
            store.clear(&repl(&map::decode_name(&req)));
            vec![empty_response(854273)]
        }
        855808 => {
            let ks: Vec<Vec<u8>> = store.entries(&repl(&map::decode_name(&req))).into_iter().map(|(k, _)| k).collect();
            vec![map::encode_data_list_response(855809, &ks)]
        }
        856064 => {
            let vs: Vec<Vec<u8>> = store.entries(&repl(&map::decode_name(&req))).into_iter().map(|(_, v)| v).collect();
            vec![map::encode_data_list_response(856065, &vs)]
        }
        856320 => {
            let es = store.entries(&repl(&map::decode_name(&req)));
            vec![map::encode_entry_list_response(856321, &es)]
        }
        // ---- Ringbuffer ----
        1508864 => {
            let (n, v) = map::decode_name_value(&req);
            vec![map::long_response(1508865, store.rb_add(&n, v))]
        }
        1509120 => {
            let n = map::decode_name(&req);
            let seq = read_i64_le(&req[0].content, 16);
            vec![map::data_response(1509121, store.rb_read_one(&n, seq).as_deref())]
        }
        1507584 => vec![map::long_response(1507585, store.rb_size(&map::decode_name(&req)))],
        1508352 => vec![map::long_response(1508353, store.rb_capacity(&map::decode_name(&req)))],
        1507840 => vec![map::long_response(1507841, store.rb_tail(&map::decode_name(&req)))],
        1508096 => vec![map::long_response(1508097, store.rb_head(&map::decode_name(&req)))],
        // ---- PNCounter ----
        // PNCounterGetConfiguredReplicaCount -> int (we run a single replica)
        1901312 => vec![map::int_response(1901313, 1)],
        1900800 => {
            let uuid = cfg.members[cfg.self_index].uuid;
            let v = store.pn_get(&map::decode_name(&req));
            vec![map::pncounter_response(1900801, v, 1, uuid, store.pn_tick())]
        }
        1901056 => {
            let n = map::decode_name(&req);
            let delta = read_i64_le(&req[0].content, 16);
            let get_before = req[0].content[24] == 1;
            let uuid = cfg.members[cfg.self_index].uuid;
            let v = store.pn_add(&n, delta, get_before);
            vec![map::pncounter_response(1901057, v, 1, uuid, store.pn_tick())]
        }
        // ---- FlakeIdGenerator ----
        1835264 => {
            let n = map::decode_name(&req);
            let batch = read_i32_le(&req[0].content, 16);
            let (base, inc, size) = store.flake_batch(&n, batch);
            vec![map::flakeid_response(1835265, base, inc, size)]
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

    fn auth_request(corr: i64) -> Vec<Frame> {
        let mut c = vec![0u8; 36];
        write_i32_le(&mut c, 0, 256);
        let mut f = vec![
            Frame { flags: UNFRAGMENTED, content: c },
            protocol::primitives::string_frame("dev"), // clusterName matches default
            protocol::primitives::null_frame(),        // username
            protocol::primitives::null_frame(),        // password
            protocol::primitives::string_frame("rust-test"),
        ];
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
            cluster_name: "dev".into(),
            username: None,
            password: None,
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
        let out = dispatch(auth_request(1), 0, &Store::new(), &cfg, &EventBroker::new((1, 1)), &SchemaService::new());
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
        let out = dispatch(auth_request(99), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
        assert_eq!(out.len(), 1);
        assert_eq!(msg_type(&out[0]), 257);
        assert_eq!(correlation_id(&out[0]), 99);
    }

    #[test]
    fn cluster_view_replies_response_plus_two_events() {
        let store = Store::new();
        let out = dispatch(request(768, 5), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
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
        let out = dispatch(request(3840, 7), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
        assert_eq!(msg_type(&out[0]), 3841);
        // initial frame must be 30 bytes with the registration UUID at offset 13
        assert_eq!(out[0][0].content.len(), 30);
        assert_eq!(read_uuid(&out[0][0].content, 13), Some(REGISTRATION_UUID));
        assert_eq!(correlation_id(&out[0]), 7);
    }

    #[test]
    fn create_proxy_replies_empty_1025() {
        let store = Store::new();
        let out = dispatch(request(1024, 8), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
        assert_eq!(msg_type(&out[0]), 1025);
        assert_eq!(correlation_id(&out[0]), 8);
    }

    #[test]
    fn put_then_get_roundtrips_through_store() {
        let store = Store::new();
        let out = dispatch(put_request("m", &[1, 2], &[9], 1), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
        assert_eq!(msg_type(&out[0]), 65793);
        assert!(out[0][1].is_null()); // no prior value

        let out = dispatch(get_request("m", &[1, 2], 2), 0, &store, &Cfg::single(), &EventBroker::new((1, 1)), &SchemaService::new());
        assert_eq!(msg_type(&out[0]), 66049);
        assert_eq!(out[0][1].content, vec![9]);
    }
}
