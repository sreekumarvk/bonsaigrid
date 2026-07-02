//! Member thread: drives the io_uring member transport, ships IMap writes to
//! backups, and delivers each deferred client response once its backups ack.
//!
//! The reactor (client) thread owns a [`Replicator`] holding the SPSC producer;
//! on a replicated write it applies locally, builds the client response bytes,
//! pushes a [`MemberJob::Replicate`], and returns no immediate reply. This thread
//! consumes the job, fans the mutation to `backups_of(partition)`, and on the
//! last ack calls `broker.enqueue(conn_id, response)` — the reactor flushes it on
//! its next event tick (same path as a deferred lock grant).

use crate::cluster_coordinator::Coordinator;
use crate::events::EventBroker;
use crate::membership::{Cluster, MemberInfo};
use member::replication::{apply, Pending};
use member::transport::{Handler, Peers, Transport};
use member::wire::{MemberRec, Msg};
use raft::cp::{CpGroup, CpMsg, CpReply};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;

/// The shape of an AtomicLong reply (chosen by the request op).
#[derive(Clone, Copy, Debug)]
pub enum ReplyKind {
    Long,
    Bool,
    Void,
}

/// CP-subsystem state owned by the member thread: the default group's Raft node +
/// AtomicLong SM, plus the per-op client bookkeeping needed to answer clients.
struct CpState {
    group: CpGroup,
    /// client_id -> (conn, correlation, response msg-type, reply shape).
    pending: HashMap<u64, (u64, i64, i32, ReplyKind)>,
    next_client: u64,
}

impl CpState {
    fn new(group: CpGroup) -> CpState {
        CpState {
            group,
            pending: HashMap::new(),
            next_client: 1,
        }
    }

    fn route(cpout: Vec<(usize, CpMsg)>, outbox: &mut Vec<(usize, Msg)>) {
        for (to, m) in cpout {
            outbox.push((
                to,
                Msg::Cp {
                    payload: raft::cp::encode_msg(&m),
                },
            ));
        }
    }

    /// Submit a client op; record how to answer it once it commits.
    fn submit(
        &mut self,
        conn: u64,
        corr: i64,
        resp_type: i32,
        kind: ReplyKind,
        command: Vec<u8>,
        outbox: &mut Vec<(usize, Msg)>,
    ) {
        let client = self.next_client;
        self.next_client += 1;
        self.pending.insert(client, (conn, corr, resp_type, kind));
        let mut cpout = Vec::new();
        self.group.submit(client, command, &mut cpout);
        Self::route(cpout, outbox);
    }

    /// Feed an inbound CP message and emit any resulting CP messages.
    fn step(&mut self, src: usize, msg: CpMsg, outbox: &mut Vec<(usize, Msg)>) {
        let mut cpout = Vec::new();
        self.group.step(src, msg, &mut cpout);
        Self::route(cpout, outbox);
    }

    /// Advance CP time.
    fn tick(&mut self, outbox: &mut Vec<(usize, Msg)>) {
        let mut cpout = Vec::new();
        self.group.tick(&mut cpout);
        Self::route(cpout, outbox);
    }

    /// Drain committed client ops into `(conn_id, response_bytes)` deliveries.
    fn drain_deliveries(&mut self) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        for comp in self.group.take_completions() {
            if let Some((conn, corr, resp_type, kind)) = self.pending.remove(&comp.client) {
                out.push((conn, build_response(resp_type, kind, &comp.reply, corr)));
            }
        }
        out
    }
}

/// Build the AtomicLong response wire bytes for a committed reply, with the
/// request's correlation id patched into the initial frame.
fn build_response(resp_type: i32, kind: ReplyKind, reply: &CpReply, corr: i64) -> Vec<u8> {
    let mut frames = match kind {
        ReplyKind::Long => {
            let v = match reply {
                CpReply::Long(v) => *v,
                _ => 0,
            };
            codecs::atomiclong::encode_long_response(resp_type, v)
        }
        ReplyKind::Bool => codecs::atomiclong::encode_bool_response(
            resp_type,
            matches!(reply, CpReply::Bool(true)),
        ),
        ReplyKind::Void => codecs::atomiclong::encode_void_response(resp_type),
    };
    protocol::fixed::write_i64_le(&mut frames[0].content, 4, corr);
    protocol::frame::write_message(&frames)
}

/// Entries per MigrateChunk.
const MIG_CHUNK: usize = 256;

/// ~5 s at the transport's 1 ms tick: a write whose backups never ack is
/// force-completed so a dead backup can't wedge the primary.
const ACK_TIMEOUT_TICKS: u32 = 5000;

pub enum MemberJob {
    /// Replicate a write; deliver `response` to `conn_id` once backups ack.
    Replicate {
        partition: i32,
        op_id: u64,
        msg: Msg,
        conn_id: u64,
        response: Vec<u8>,
    },
    /// Replace the member thread's membership view (after a manual promotion).
    Membership(Cluster),
    /// A client CP (AtomicLong) op to submit to the default group; the reply is
    /// delivered to `conn_id` (with `correlation`) once it commits.
    CpSubmit {
        conn_id: u64,
        correlation: i64,
        resp_type: i32,
        kind: ReplyKind,
        command: Vec<u8>,
    },
}

/// Member → reactor signal: the coordinator changed the membership; the reactor
/// updates its authoritative `Cluster` and pushes cluster-view events to clients.
pub struct ClusterEvent {
    pub generation: u64,
    pub members: Vec<MemberRec>,
}

/// Reactor-thread handle that hands replicated writes to the member thread.
pub struct Replicator {
    tx: spsc::Producer<MemberJob>,
    next_op: Cell<u64>,
    backups: usize,
}

impl Replicator {
    pub fn new(tx: spsc::Producer<MemberJob>, backups: usize) -> Replicator {
        Replicator {
            tx,
            next_op: Cell::new(1),
            backups,
        }
    }

    /// True if this cluster keeps backups at all (callers skip the deferral path
    /// entirely when false).
    pub fn has_backups(&self) -> bool {
        self.backups > 0
    }

    /// Queue a replicated write. `mk` builds the backup message from the assigned
    /// `op_id`. Returns `true` if the write was deferred (caller must withhold the
    /// client reply); `false` if it should reply normally (no backups, or the ring
    /// was full).
    pub fn replicate(
        &self,
        partition: i32,
        conn_id: u64,
        response: Vec<u8>,
        mk: impl FnOnce(u64) -> Msg,
    ) -> bool {
        if self.backups == 0 {
            return false;
        }
        let op_id = self.next_op.get();
        self.next_op.set(op_id.wrapping_add(1));
        let job = MemberJob::Replicate {
            partition,
            op_id,
            msg: mk(op_id),
            conn_id,
            response,
        };
        self.tx.push(job).is_ok()
    }

    /// Push an updated membership view to the member thread (after promotion).
    pub fn send_membership(&self, cluster: Cluster) {
        let _ = self.tx.push(MemberJob::Membership(cluster));
    }

    /// Submit a client AtomicLong op to the CP subsystem; the reply is delivered
    /// to `conn_id` once it commits. Returns `false` if the ring was full.
    pub fn submit_cp(
        &self,
        conn_id: u64,
        correlation: i64,
        resp_type: i32,
        kind: ReplyKind,
        command: Vec<u8>,
    ) -> bool {
        self.tx
            .push(MemberJob::CpSubmit {
                conn_id,
                correlation,
                resp_type,
                kind,
                command,
            })
            .is_ok()
    }
}

pub(crate) struct MemberHandler {
    store: Arc<Store>,
    broker: Arc<EventBroker>,
    rx: spsc::Consumer<MemberJob>,
    coord: Coordinator,
    pending: Pending,
    events: spsc::Producer<ClusterEvent>,
    /// Merge policy for inbound migrated entries (true = LatestUpdate).
    merge_latest: bool,
    /// Shared transport peer-address table; refreshed from the cluster on change.
    peers: Peers,
    /// CP subsystem (default group); `None` unless CP is enabled for this node.
    cp: Option<CpState>,
}

impl MemberHandler {
    /// Assemble a handler from its collaborators. Shared by the production
    /// [`spawn`] path and the deterministic simulation harness (`sim`), so both
    /// exercise the identical state machine.
    pub(crate) fn new(
        store: Arc<Store>,
        broker: Arc<EventBroker>,
        rx: spsc::Consumer<MemberJob>,
        coord: Coordinator,
        events: spsc::Producer<ClusterEvent>,
        merge_latest: bool,
        peers: Peers,
    ) -> MemberHandler {
        MemberHandler {
            store,
            broker,
            rx,
            coord,
            pending: Pending::new(),
            events,
            merge_latest,
            peers,
            cp: None,
        }
    }

    /// Enable the CP subsystem on this member with `group` as the default group.
    pub(crate) fn set_cp(&mut self, group: CpGroup) {
        self.cp = Some(CpState::new(group));
    }

    /// The membership view this member currently believes (test/sim only).
    #[cfg(test)]
    pub(crate) fn cluster(&self) -> &Cluster {
        &self.coord.cluster
    }

    /// Forward the current membership view to the reactor.
    fn emit_view(&self) {
        let (generation, members) = self.coord.view_recs();
        let _ = self.events.push(ClusterEvent {
            generation,
            members,
        });
    }

    /// Refresh the transport's peer addresses from the current cluster (so a
    /// runtime-joined member becomes reachable).
    fn sync_peers(&self) {
        let mut p = self.peers.borrow_mut();
        for (i, m) in self.coord.cluster.members.iter().enumerate() {
            if let Ok(addr) = format!("{}:{}", m.host, m.member_port).parse() {
                p.insert(i, addr);
            }
        }
    }

    /// React to a membership change: refresh peers, notify the reactor, and stream
    /// out the partitions this member must migrate.
    fn apply_change(
        &mut self,
        ch: crate::cluster_coordinator::Change,
        outbox: &mut Vec<(usize, Msg)>,
    ) {
        if !ch.changed {
            return;
        }
        self.sync_peers();
        self.emit_view();
        if ch.migrations.is_empty() {
            return;
        }
        // Bucket this member's live entries by partition once, then stream each
        // migrating partition to its new owner.
        let generation = self.coord.cluster.generation;
        let mut by_part: HashMap<i32, Vec<(String, Vec<u8>, Vec<u8>, u64)>> = HashMap::new();
        for (map, key, val, stamp) in self.store.all_entries_stamped() {
            let p = serialization::partition_id(&key, crate::handlers::PARTITION_COUNT);
            by_part.entry(p).or_default().push((map, key, val, stamp));
        }
        for (partition, dest) in ch.migrations {
            outbox.push((
                dest,
                Msg::MigrateStart {
                    generation,
                    partition,
                },
            ));
            if let Some(entries) = by_part.get(&partition) {
                for chunk in entries.chunks(MIG_CHUNK) {
                    outbox.push((
                        dest,
                        Msg::MigrateChunk {
                            generation,
                            partition,
                            entries: chunk.to_vec(),
                        },
                    ));
                }
            }
            // Auxiliary-structure state for this partition (queues/lists/sets/...).
            let aux = self
                .store
                .aux_state_for_partition(partition, crate::handlers::PARTITION_COUNT);
            outbox.push((
                dest,
                Msg::MigrateAux {
                    generation,
                    partition,
                    payload: aux,
                },
            ));
            // MultiMap entries (key-partitioned) for this partition.
            let mm = self
                .store
                .mm_entries_for_partition(partition, crate::handlers::PARTITION_COUNT);
            if !mm.is_empty() {
                outbox.push((
                    dest,
                    Msg::MigrateMm {
                        generation,
                        partition,
                        entries: mm,
                    },
                ));
            }
            outbox.push((
                dest,
                Msg::MigrateEnd {
                    generation,
                    partition,
                },
            ));
        }
    }
}

impl Handler for MemberHandler {
    fn on_msg(&mut self, src: usize, msg: Msg, outbox: &mut Vec<(usize, Msg)>) {
        match msg {
            Msg::BackupPut { op_id, .. } | Msg::BackupRemove { op_id, .. } => {
                apply(&self.store, &msg); // backup side: write locally, then ack
                outbox.push((src, Msg::Ack { op_id }));
            }
            Msg::Ack { op_id } => {
                if let Some((conn, resp)) = self.pending.ack(op_id) {
                    self.broker.enqueue(conn, resp);
                }
            }
            Msg::Heartbeat {
                from_join_id,
                generation,
            } => {
                self.coord.on_heartbeat(from_join_id, generation);
            }
            Msg::MemberView {
                generation,
                members,
            } => {
                let ch = self.coord.on_view(generation, members);
                self.apply_change(ch, outbox);
            }
            Msg::JoinRequest {
                uuid,
                host,
                client_port,
                member_port,
            } => {
                let info = MemberInfo::new(uuid, host, client_port, member_port, 0);
                let ch = self.coord.on_join(info, outbox);
                self.apply_change(ch, outbox);
            }
            Msg::MigrateChunk { entries, .. } => {
                for (map, key, val, stamp) in entries {
                    self.store
                        .put_merge(&map, &key, &val, 0, stamp, self.merge_latest);
                }
            }
            Msg::BackupState { op_id, payload, .. } => {
                self.store.install_aux_state(&payload); // backup side: install aux state, then ack
                outbox.push((src, Msg::Ack { op_id }));
            }
            Msg::MigrateAux { payload, .. } => {
                self.store.install_aux_state(&payload);
            }
            Msg::BackupMm {
                op_id,
                name,
                key,
                values,
            } => {
                self.store.mm_install(&name, key, values);
                outbox.push((src, Msg::Ack { op_id }));
            }
            Msg::MigrateMm { entries, .. } => {
                for (name, key, values) in entries {
                    self.store.mm_install(&name, key, values);
                }
            }
            Msg::Cp { payload } => {
                let deliveries = if let (Some(cp), Some(m)) =
                    (self.cp.as_mut(), raft::cp::decode_msg(&payload))
                {
                    cp.step(src, m, outbox);
                    cp.drain_deliveries()
                } else {
                    Vec::new()
                };
                for (conn, bytes) in deliveries {
                    self.broker.enqueue(conn, bytes);
                }
            }
            Msg::MigrateStart { .. } | Msg::MigrateEnd { .. } => {}
            Msg::Hello { .. } => {}
        }
    }

    fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> bool {
        while let Some(job) = self.rx.pop() {
            match job {
                MemberJob::Replicate {
                    partition,
                    op_id,
                    msg,
                    conn_id,
                    response,
                } => {
                    let backups = self.coord.cluster.backups_of(partition);
                    if backups.is_empty() {
                        self.broker.enqueue(conn_id, response); // nobody to wait on
                    } else {
                        for b in &backups {
                            outbox.push((*b, msg.clone()));
                        }
                        if let Some((conn, resp)) =
                            self.pending
                                .register(op_id, backups.len() as u32, conn_id, response)
                        {
                            self.broker.enqueue(conn, resp);
                        }
                    }
                }
                MemberJob::Membership(c) => self.coord.cluster = c,
                MemberJob::CpSubmit {
                    conn_id,
                    correlation,
                    resp_type,
                    kind,
                    command,
                } => {
                    if let Some(cp) = self.cp.as_mut() {
                        cp.submit(conn_id, correlation, resp_type, kind, command, outbox);
                    }
                }
            }
        }
        for (conn, resp) in self.pending.sweep_expired(ACK_TIMEOUT_TICKS) {
            self.broker.enqueue(conn, resp);
        }
        // Drive the CP subsystem's clock and flush any committed client replies.
        let cp_deliveries = if let Some(cp) = self.cp.as_mut() {
            cp.tick(outbox);
            cp.drain_deliveries()
        } else {
            Vec::new()
        };
        for (conn, bytes) in cp_deliveries {
            self.broker.enqueue(conn, bytes);
        }
        let ch = self.coord.on_tick(outbox);
        self.apply_change(ch, outbox);
        true
    }
}

/// Spawn the member thread. `member_ports[self_index]` is this member's inbound
/// member port; the others are dialed on demand.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    self_index: usize,
    member_ports: Vec<i32>,
    cluster: Cluster,
    self_uuid: (i64, i64),
    hb_interval_ticks: u64,
    hb_timeout_ticks: u64,
    merge_latest: bool,
    join_as: Option<MemberInfo>,
    store: Arc<Store>,
    broker: Arc<EventBroker>,
    rx: spsc::Consumer<MemberJob>,
    events: spsc::Producer<ClusterEvent>,
    member_tls: Option<security::tls::MemberTls>,
    cp_enabled: bool,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let cp_group_size = cluster.len();
        let transport = match Transport::bind(self_index, &member_ports) {
            Ok(t) => t.with_tls(member_tls),
            Err(e) => {
                eprintln!(
                    "member transport bind failed on port {}: {e}",
                    member_ports[self_index]
                );
                return;
            }
        };
        let peers = transport.peers();
        let mut coord = Coordinator::new(cluster, self_uuid, hb_interval_ticks, hb_timeout_ticks);
        if let Some(info) = join_as {
            coord.set_pending_join(info);
        }
        let mut handler = MemberHandler::new(store, broker, rx, coord, events, merge_latest, peers);
        if cp_enabled {
            // Default CP group = all bootstrap members (NodeId = member index).
            let node = raft::RaftNode::new(
                self_index,
                (0..cp_group_size).collect(),
                raft::RaftLog::new(),
                self_index as u64 + 1,
            );
            handler.set_cp(CpGroup::new(node));
            eprintln!("BonsaiGrid CP: AtomicLong enabled (default group, {cp_group_size} members)");
        }
        let _ = transport.run(handler);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codecs::atomiclong::ADD_AND_GET_RESP;
    use member::wire::Msg;
    use protocol::fixed::{read_i32_le, read_i64_le};
    use raft::atomiclong::AlOp;
    use raft::cp::al_command;
    use raft::{RaftLog, RaftNode};

    // A single-node default CP group elects itself and commits immediately.
    fn single_node_cp() -> CpState {
        let mut node = RaftNode::new(0, vec![0], RaftLog::new(), 1);
        node.set_heartbeat_period(2);
        CpState::new(CpGroup::new(node))
    }

    #[test]
    fn build_response_patches_type_correlation_value() {
        // Response frame bytes: [len:4][flags:2][content]; content: type@0,
        // corr@4, backupAcks@12, value@13 → byte offsets 6, 10, 19.
        let bytes = build_response(ADD_AND_GET_RESP, ReplyKind::Long, &CpReply::Long(42), 99);
        assert_eq!(read_i32_le(&bytes, 6), ADD_AND_GET_RESP);
        assert_eq!(read_i64_le(&bytes, 10), 99); // correlation
        assert_eq!(read_i64_le(&bytes, 19), 42); // value
    }

    #[test]
    fn cp_submit_commits_and_delivers_reply() {
        let mut cp = single_node_cp();
        let mut outbox = Vec::new();
        for _ in 0..40 {
            cp.tick(&mut outbox); // elect self as leader
        }
        cp.submit(
            5, // conn
            99,
            ADD_AND_GET_RESP,
            ReplyKind::Long,
            al_command("c", &AlOp::AddAndGet(3)),
            &mut outbox,
        );
        let mut delivery = None;
        for _ in 0..40 {
            cp.tick(&mut outbox);
            let d = cp.drain_deliveries();
            if !d.is_empty() {
                delivery = Some(d);
                break;
            }
        }
        let d = delivery.expect("a reply is delivered");
        assert_eq!(d[0].0, 5, "delivered to the originating connection");
        assert_eq!(read_i64_le(&d[0].1, 10), 99, "correlation preserved");
        assert_eq!(read_i64_le(&d[0].1, 19), 3, "AddAndGet(3) committed value");
    }

    #[test]
    fn replicate_defers_only_when_backups_exist() {
        let (tx, rx) = spsc::channel::<MemberJob>(8);
        let no_backup = Replicator::new(tx, 0);
        assert!(!no_backup.replicate(0, 1, vec![9], |op| Msg::BackupRemove {
            op_id: op,
            name: "m".into(),
            key: b"k".to_vec()
        }));
        assert!(rx.pop().is_none()); // nothing queued

        let (tx, rx) = spsc::channel::<MemberJob>(8);
        let with_backup = Replicator::new(tx, 1);
        assert!(
            with_backup.replicate(3, 7, vec![1, 2], |op| Msg::BackupPut {
                op_id: op,
                name: "m".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0
            })
        );
        match rx.pop() {
            Some(MemberJob::Replicate {
                partition,
                op_id,
                conn_id,
                ..
            }) => {
                assert_eq!(partition, 3);
                assert_eq!(conn_id, 7);
                assert!(op_id >= 1);
            }
            _ => panic!("expected a Replicate job"),
        }
    }
}
