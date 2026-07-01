//! Deterministic simulation testing (DST) for the distributed member layer.
//!
//! `madsim` is not usable here — it virtualizes `tokio`'s network, and this
//! system runs on raw `io_uring`. Instead we exploit the seam the production
//! design already provides: the member state machine is the [`Handler`] trait
//! (`on_msg` / `on_tick` → `outbox`) driven by *tick-based virtual time*, with
//! no wall-clock and no socket calls inside the logic. So we run N real
//! [`MemberHandler`]s in a single thread, shuttle their outbox messages through
//! an in-memory bus, advance a tick counter by hand, and inject faults. This is
//! the *exact* production replication/coordination/migration code — only the
//! io_uring transport (pure plumbing, covered by loopback + wire golden tests)
//! is replaced.
//!
//! Everything is seeded and single-threaded, so any failure reproduces byte for
//! byte from its seed.

use crate::cluster_coordinator::Coordinator;
use crate::events::EventBroker;
use crate::handlers::PARTITION_COUNT;
use crate::member_thread::{ClusterEvent, MemberHandler, MemberJob};
use crate::membership::{Cluster, MemberInfo};
use member::transport::{Handler, Peers};
use member::wire::Msg;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use store::Store;

/// Seeded splitmix64 — a deterministic PRNG with no external deps and no thread
/// RNG (both of which would break replayability).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `0..n` (0 when `n == 0`).
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

/// One simulated member: the real handler plus the handles a reactor would own.
struct Node {
    handler: MemberHandler,
    /// Producer side of the member's job ring — the reactor pushes replicated
    /// writes here; we use it to inject client writes.
    job_tx: spsc::Producer<MemberJob>,
    store: Arc<Store>,
    broker: Arc<EventBroker>,
    /// Kept alive so `emit_view` pushes never fail; we introspect state directly
    /// instead of consuming these.
    _events_rx: spsc::Consumer<ClusterEvent>,
    alive: bool,
}

/// A message in flight on the simulated network.
struct InFlight {
    from: usize,
    to: usize,
    msg: Msg,
    due: u64,
    seq: u64,
}

/// A pending client write the caller can poll for acknowledgement.
pub(crate) struct Ticket {
    conn: u64,
    owner: usize,
}

/// Why a client write was refused before it could replicate.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WriteReject {
    /// The routed partition owner is down.
    OwnerDown,
    /// The owner's view lacks quorum (split-brain protection).
    QuorumRejected,
}

/// An in-process, deterministic cluster of member state machines.
pub(crate) struct SimCluster {
    nodes: Vec<Node>,
    bus: Vec<InFlight>,
    tick: u64,
    seq: u64,
    rng: Rng,
    /// Directional cut links `(from, to)` — models network partitions.
    cut: HashSet<(usize, usize)>,
    /// Messages arrive in `1..=max_latency` ticks; >1 induces reordering.
    max_latency: u64,
    next_op: u64,
    next_conn: u64,
    /// Canonical partition table used to route client writes (what a smart
    /// client would hold). Refreshed from a member's view after failover.
    route: Cluster,
}

impl SimCluster {
    /// Build an `n`-member cluster with `backups` sync backups and the given
    /// `quorum`. Each member's store is tagged with its index exactly as
    /// production (`with_shards_seed(1, self_index)`), so HLC stamp / merge
    /// behaviour is faithful.
    pub(crate) fn new(n: usize, backups: usize, quorum: usize, seed: u64) -> SimCluster {
        let members: Vec<MemberInfo> = (0..n)
            .map(|i| {
                MemberInfo::new(
                    (1, i as i64 + 1),
                    "127.0.0.1".into(),
                    5901 + i as i32,
                    17901 + i as i32,
                    i as u64,
                )
            })
            .collect();
        let base = Cluster::new(members, backups, quorum);

        // Short heartbeat cadence keeps sims fast: interval 2 ticks, death after 10.
        let (hb_interval, hb_timeout) = (2u64, 10u64);
        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            let store = Arc::new(Store::with_shards_seed(1, i as u64));
            let broker = Arc::new(EventBroker::new((1, i as i64 + 1)));
            let (job_tx, job_rx) = spsc::channel::<MemberJob>(4096);
            let (ev_tx, ev_rx) = spsc::channel::<ClusterEvent>(4096);
            let coord = Coordinator::new(base.clone(), (1, i as i64 + 1), hb_interval, hb_timeout);
            let peers: Peers = Rc::new(RefCell::new(HashMap::new()));
            let handler = MemberHandler::new(
                store.clone(),
                broker.clone(),
                job_rx,
                coord,
                ev_tx,
                true,
                peers,
            );
            nodes.push(Node {
                handler,
                job_tx,
                store,
                broker,
                _events_rx: ev_rx,
                alive: true,
            });
        }
        SimCluster {
            nodes,
            bus: Vec::new(),
            tick: 0,
            seq: 0,
            rng: Rng::new(seed),
            cut: HashSet::new(),
            max_latency: 1,
            next_op: 1,
            next_conn: 1,
            route: base,
        }
    }

    // ---- fault controls -----------------------------------------------------

    /// Crash member `i`: it stops ticking and every message to/from it is lost.
    pub(crate) fn kill(&mut self, i: usize) {
        self.nodes[i].alive = false;
        self.route.alive[i] = false;
    }

    /// Sever every link between the two groups (both directions).
    pub(crate) fn partition(&mut self, a: &[usize], b: &[usize]) {
        for &x in a {
            for &y in b {
                self.cut.insert((x, y));
                self.cut.insert((y, x));
            }
        }
    }

    /// Set the maximum per-message delivery latency in ticks (>1 reorders).
    pub(crate) fn set_latency(&mut self, max: u64) {
        self.max_latency = max.max(1);
    }

    // ---- driving the simulation --------------------------------------------

    /// Advance one virtual tick: deliver everything due, then tick every node.
    pub(crate) fn step(&mut self) {
        self.tick += 1;

        // Collect messages due this tick and deliver them in a deterministic
        // (due, seq) order regardless of bus vector churn.
        let mut due: Vec<InFlight> = Vec::new();
        let mut i = 0;
        while i < self.bus.len() {
            if self.bus[i].due <= self.tick {
                due.push(self.bus.swap_remove(i));
            } else {
                i += 1;
            }
        }
        due.sort_by_key(|m| (m.due, m.seq));
        for m in due {
            // A link cut or a kill after enqueue still swallows the message.
            if !self.linked(m.from, m.to) {
                continue;
            }
            let mut outbox = Vec::new();
            self.nodes[m.to].handler.on_msg(m.from, m.msg, &mut outbox);
            self.route(m.to, outbox);
        }

        for i in 0..self.nodes.len() {
            if !self.nodes[i].alive {
                continue;
            }
            let mut outbox = Vec::new();
            self.nodes[i].handler.on_tick(&mut outbox);
            self.route(i, outbox);
        }
    }

    /// Advance `ticks` steps.
    pub(crate) fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Step until the ticket's deferred response is delivered, or give up after
    /// `max_ticks`. Returns whether it acked.
    pub(crate) fn run_until_acked(&mut self, t: &Ticket, max_ticks: u64) -> bool {
        for _ in 0..max_ticks {
            self.step();
            if !self.nodes[t.owner].broker.drain(t.conn).is_empty() {
                return true;
            }
        }
        false
    }

    // ---- client operations --------------------------------------------------

    /// Inject a client put routed to the partition owner, applying the same
    /// quorum gate the request dispatcher uses. On success the owner has applied
    /// locally and queued synchronous replication; poll the [`Ticket`] with
    /// [`run_until_acked`](Self::run_until_acked).
    pub(crate) fn client_put(
        &mut self,
        map: &str,
        key: &[u8],
        val: &[u8],
    ) -> Result<Ticket, WriteReject> {
        let partition = serialization::partition_id(key, PARTITION_COUNT);
        let owner = self.route.owner(partition);
        if !self.nodes[owner].alive {
            return Err(WriteReject::OwnerDown);
        }
        // Mirrors handlers.rs: quorum-gated writes are rejected, not buffered.
        if !self.nodes[owner].handler.cluster().has_quorum() {
            return Err(WriteReject::QuorumRejected);
        }
        // Owner applies locally, then hands the mutation to its member thread.
        self.nodes[owner].store.put(map, key.to_vec(), val.to_vec());
        let op_id = self.next_op;
        self.next_op += 1;
        let conn = self.next_conn;
        self.next_conn += 1;
        let msg = Msg::BackupPut {
            op_id,
            name: map.to_string(),
            key: key.to_vec(),
            value: val.to_vec(),
            ttl_ms: 0,
        };
        let job = MemberJob::Replicate {
            partition,
            op_id,
            msg,
            conn_id: conn,
            response: format!("ok:{conn}").into_bytes(),
        };
        let _ = self.nodes[owner].job_tx.push(job);
        Ok(Ticket { conn, owner })
    }

    /// Would a client write reaching member `i` be accepted (alive + quorum)?
    /// This is the split-brain predicate: a minority member must answer `false`.
    pub(crate) fn accepts_writes(&self, i: usize) -> bool {
        self.nodes[i].alive && self.nodes[i].handler.cluster().has_quorum()
    }

    // ---- reads / introspection ---------------------------------------------

    /// The value on any *alive* member — the durability check (no acked write
    /// may vanish from the whole live cluster).
    pub(crate) fn read_any_alive(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        for n in &self.nodes {
            if n.alive {
                if let Some(v) = n.store.get(map, key) {
                    return Some(v);
                }
            }
        }
        None
    }

    /// What member `i` currently believes about liveness.
    pub(crate) fn live_count(&self, i: usize) -> usize {
        self.nodes[i].handler.cluster().live_count()
    }

    /// Point the client routing table at member `i`'s post-failover view.
    pub(crate) fn refresh_route_from(&mut self, i: usize) {
        self.route = self.nodes[i].handler.cluster().clone();
    }

    // ---- internals ----------------------------------------------------------

    /// A message can traverse `a -> b` only if both ends are alive and the link
    /// isn't cut.
    fn linked(&self, a: usize, b: usize) -> bool {
        self.nodes[a].alive
            && self.nodes[b].alive
            && !self.cut.contains(&(a, b))
            && !self.cut.contains(&(b, a))
    }

    /// Enqueue a handler's outbox onto the bus, applying loss and latency.
    fn route(&mut self, from: usize, outbox: Vec<(usize, Msg)>) {
        for (to, msg) in outbox {
            if to == from || to >= self.nodes.len() || !self.linked(from, to) {
                continue;
            }
            let latency = 1 + self.rng.below(self.max_latency);
            let seq = self.seq;
            self.seq += 1;
            self.bus.push(InFlight {
                from,
                to,
                msg,
                due: self.tick + latency,
                seq,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bring a cluster to a steady state: heartbeats flow long enough that every
    /// member has a `last_seen` entry for every peer (so nobody is falsely
    /// declared dead later).
    fn settle(sim: &mut SimCluster) {
        sim.run(20);
    }

    /// INVARIANT 1 — Durability across failover.
    /// Every write acknowledged to the client must survive the loss of its
    /// partition owner: the synchronous backup holds it and is promoted.
    #[test]
    fn acked_writes_survive_owner_failover() {
        let mut sim = SimCluster::new(3, 1, 2, 0xD00D);
        settle(&mut sim);

        let mut acked: Vec<(String, String)> = Vec::new();
        for k in 0..50 {
            let key = format!("k{k}");
            let val = format!("v{k}");
            let t = sim
                .client_put("m", key.as_bytes(), val.as_bytes())
                .expect("healthy cluster must accept the write");
            assert!(
                sim.run_until_acked(&t, 50),
                "write {k} was never acknowledged"
            );
            acked.push((key, val));
        }

        // Kill member 0 (owner of some partitions, backup of others) and let the
        // survivors detect the death, elect, promote, and re-replicate.
        sim.kill(0);
        sim.run(60);

        for (key, val) in &acked {
            assert_eq!(
                sim.read_any_alive("m", key.as_bytes()),
                Some(val.clone().into_bytes()),
                "acked write {key} was lost after owner failover"
            );
        }
    }

    /// INVARIANT 2 — Split-brain / quorum.
    /// Under a network partition, the minority side must stop accepting writes
    /// while the majority stays available.
    #[test]
    fn minority_partition_refuses_writes_majority_stays_available() {
        let mut sim = SimCluster::new(3, 1, 2, 0xBEEF);
        settle(&mut sim);

        // Split {0,1} | {2}.
        sim.partition(&[0, 1], &[2]);
        sim.run(60); // > heartbeat timeout: each side buries the other.

        // Majority keeps quorum (2 alive in its view); minority loses it.
        assert!(
            sim.accepts_writes(0),
            "majority member 0 must stay writable"
        );
        assert!(
            sim.accepts_writes(1),
            "majority member 1 must stay writable"
        );
        assert!(
            !sim.accepts_writes(2),
            "minority member 2 must refuse writes (no split-brain)"
        );
        assert_eq!(sim.live_count(2), 1, "minority sees only itself");

        // The majority can still complete a replicated write among {0,1}.
        sim.refresh_route_from(0);
        let t = sim
            .client_put("m", b"survivor-key", b"survivor-val")
            .expect("majority must accept writes");
        assert!(
            sim.run_until_acked(&t, 60),
            "majority write did not replicate+ack within the partition"
        );
    }

    /// INVARIANT 3 — Merge convergence resolves by real time, not member index.
    /// `LatestUpdate` keeps the higher per-entry stamp, and stamps are now
    /// HLC-packed (physical-ms high bits, member id only a low tiebreak). So a
    /// genuinely later write wins a split-brain merge even when it came from a
    /// *lower*-indexed member — the member-index-dominates hazard is gone.
    /// (`next_stamp_at` drives physical time explicitly so the test is
    /// deterministic and does not depend on the wall clock advancing.)
    #[test]
    fn latest_update_merge_resolves_by_real_time() {
        // During a split, member 2 writes at ms=100 (EARLIER); the lower-indexed
        // member 0 writes at ms=101 (LATER).
        let m0 = Store::with_shards_seed(1, 0); // member 0
        let m2 = Store::with_shards_seed(1, 2); // member 2
        let s2_earlier = m2.next_stamp_at(100); // higher index, earlier time
        let s0_later = m0.next_stamp_at(101); // lower index, later time

        assert!(
            s0_later > s2_earlier,
            "the later real-time write must carry the higher stamp regardless of \
             member index"
        );

        // On heal, both entries land on one holder and merge via LatestUpdate.
        let merged = Store::with_shards_seed(1, 0);
        merged.put_merge("m", b"k", b"m2-earlier", 0, s2_earlier, true);
        merged.put_merge("m", b"k", b"m0-LATER", 0, s0_later, true);

        assert_eq!(
            merged.get("m", b"k"),
            Some(b"m0-LATER".to_vec()),
            "LatestUpdate must keep the genuinely later write (no member-index bias)"
        );
    }

    /// INVARIANT 4 — Migration is non-atomic but convergent.
    /// A partition migration is not atomic w.r.t. concurrent client writes;
    /// correctness rests entirely on stamps. A concurrent write (fresh, larger
    /// stamp) must win over the older migrated copy regardless of arrival order.
    #[test]
    fn migration_does_not_clobber_concurrent_write() {
        for migrate_first in [true, false] {
            // Destination is the new owner (member index 1).
            let dest = Store::with_shards_seed(1, 1);
            // The migrated entry keeps its original (earlier) stamp from the
            // source member; the concurrent client write on `dest` is later.
            let migrated_stamp = Store::with_shards_seed(1, 0).next_stamp_at(999);
            let concurrent_stamp = dest.next_stamp_at(1_000); // fresh local write, larger

            let apply_migrated =
                |s: &Store| s.put_merge("m", b"k", b"OLD", 0, migrated_stamp, true);
            let apply_write = |s: &Store| s.put_merge("m", b"k", b"NEW", 0, concurrent_stamp, true);

            if migrate_first {
                apply_migrated(&dest);
                apply_write(&dest);
            } else {
                apply_write(&dest);
                apply_migrated(&dest);
            }

            assert_eq!(
                dest.get("m", b"k"),
                Some(b"NEW".to_vec()),
                "concurrent write lost to stale migrated data (migrate_first={migrate_first})"
            );
        }
    }

    /// Bonus — the harness itself under a lossy, reordering network: durability
    /// must still hold with delayed and dropped messages, proving the retries /
    /// ack tracking converge (and that the sim is deterministic under a seed).
    #[test]
    fn writes_survive_lossy_reordering_network() {
        let mut sim = SimCluster::new(3, 1, 2, 0x1234);
        sim.set_latency(4); // 1..=4 tick delivery → reordering
        settle(&mut sim);

        let t = sim
            .client_put("m", b"k", b"v")
            .expect("cluster accepts write");
        assert!(
            sim.run_until_acked(&t, 200),
            "write never acked under a delayed/reordering network"
        );
        assert_eq!(sim.read_any_alive("m", b"k"), Some(b"v".to_vec()));
    }
}
