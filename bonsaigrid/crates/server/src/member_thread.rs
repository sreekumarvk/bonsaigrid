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
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use store::Store;

/// Entries per MigrateChunk.
const MIG_CHUNK: usize = 256;

/// ~5 s at the transport's 1 ms tick: a write whose backups never ack is
/// force-completed so a dead backup can't wedge the primary.
const ACK_TIMEOUT_TICKS: u32 = 5000;

pub enum MemberJob {
    /// Replicate a write; deliver `response` to `conn_id` once backups ack.
    Replicate { partition: i32, op_id: u64, msg: Msg, conn_id: u64, response: Vec<u8> },
    /// Replace the member thread's membership view (after a manual promotion).
    Membership(Cluster),
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
        Replicator { tx, next_op: Cell::new(1), backups }
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
        let job = MemberJob::Replicate { partition, op_id, msg: mk(op_id), conn_id, response };
        self.tx.push(job).is_ok()
    }

    /// Push an updated membership view to the member thread (after promotion).
    pub fn send_membership(&self, cluster: Cluster) {
        let _ = self.tx.push(MemberJob::Membership(cluster));
    }
}

struct MemberHandler {
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
}

impl MemberHandler {
    /// Forward the current membership view to the reactor.
    fn emit_view(&self) {
        let (generation, members) = self.coord.view_recs();
        let _ = self.events.push(ClusterEvent { generation, members });
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
    fn apply_change(&mut self, ch: crate::cluster_coordinator::Change, outbox: &mut Vec<(usize, Msg)>) {
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
            outbox.push((dest, Msg::MigrateStart { generation, partition }));
            if let Some(entries) = by_part.get(&partition) {
                for chunk in entries.chunks(MIG_CHUNK) {
                    outbox.push((dest, Msg::MigrateChunk { generation, partition, entries: chunk.to_vec() }));
                }
            }
            outbox.push((dest, Msg::MigrateEnd { generation, partition }));
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
            Msg::Heartbeat { from_join_id, generation } => {
                self.coord.on_heartbeat(from_join_id, generation);
            }
            Msg::MemberView { generation, members } => {
                let ch = self.coord.on_view(generation, members);
                self.apply_change(ch, outbox);
            }
            Msg::JoinRequest { uuid, host, client_port, member_port } => {
                let info = MemberInfo::new(uuid, host, client_port, member_port, 0);
                let ch = self.coord.on_join(info, outbox);
                self.apply_change(ch, outbox);
            }
            Msg::MigrateChunk { entries, .. } => {
                for (map, key, val, stamp) in entries {
                    self.store.put_merge(&map, &key, &val, 0, stamp, self.merge_latest);
                }
            }
            Msg::BackupState { op_id, payload, .. } => {
                self.store.install_aux_state(&payload); // backup side: install aux state, then ack
                outbox.push((src, Msg::Ack { op_id }));
            }
            Msg::MigrateAux { payload, .. } => {
                self.store.install_aux_state(&payload);
            }
            Msg::MigrateStart { .. } | Msg::MigrateEnd { .. } => {}
            Msg::Hello { .. } => {}
        }
    }

    fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> bool {
        while let Some(job) = self.rx.pop() {
            match job {
                MemberJob::Replicate { partition, op_id, msg, conn_id, response } => {
                    let backups = self.coord.cluster.backups_of(partition);
                    if backups.is_empty() {
                        self.broker.enqueue(conn_id, response); // nobody to wait on
                    } else {
                        for b in &backups {
                            outbox.push((*b, msg.clone()));
                        }
                        if let Some((conn, resp)) =
                            self.pending.register(op_id, backups.len() as u32, conn_id, response)
                        {
                            self.broker.enqueue(conn, resp);
                        }
                    }
                }
                MemberJob::Membership(c) => self.coord.cluster = c,
            }
        }
        for (conn, resp) in self.pending.sweep_expired(ACK_TIMEOUT_TICKS) {
            self.broker.enqueue(conn, resp);
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
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let transport = match Transport::bind(self_index, &member_ports) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("member transport bind failed on port {}: {e}", member_ports[self_index]);
                return;
            }
        };
        let peers = transport.peers();
        let mut coord = Coordinator::new(cluster, self_uuid, hb_interval_ticks, hb_timeout_ticks);
        if let Some(info) = join_as {
            coord.set_pending_join(info);
        }
        let handler =
            MemberHandler { store, broker, rx, coord, pending: Pending::new(), events, merge_latest, peers };
        let _ = transport.run(handler);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use member::wire::Msg;

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
        assert!(with_backup.replicate(3, 7, vec![1, 2], |op| Msg::BackupPut {
            op_id: op,
            name: "m".into(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0
        }));
        match rx.pop() {
            Some(MemberJob::Replicate { partition, op_id, conn_id, .. }) => {
                assert_eq!(partition, 3);
                assert_eq!(conn_id, 7);
                assert!(op_id >= 1);
            }
            _ => panic!("expected a Replicate job"),
        }
    }
}
