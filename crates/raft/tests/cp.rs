//! The CP group driver: a client op submitted to any member — leader or
//! follower — commits through Raft and completes on the member the client is on,
//! with the correct linearizable reply. Exercises forward-to-leader routing.

use raft::atomiclong::AlOp;
use raft::cp::{
    al_command, fl_command, sess_command, ClientId, Completion, CpGroup, CpMsg, CpReply,
};
use raft::fencedlock::FlOp;
use raft::session::SessOp;
use raft::{NodeId, RaftLog, RaftNode};
use std::collections::HashSet;

struct CpSim {
    groups: Vec<CpGroup>,
    alive: Vec<bool>,
    bus: Vec<(NodeId, NodeId, CpMsg)>,
    cut: HashSet<(NodeId, NodeId)>,
    /// Completions observed at each member (client op answered here).
    done: Vec<Vec<Completion>>,
}

impl CpSim {
    fn new(n: usize) -> CpSim {
        let peers: Vec<NodeId> = (0..n).collect();
        let groups = (0..n)
            .map(|i| {
                let mut node = RaftNode::new(i, peers.clone(), RaftLog::new(), 0x6C7 + i as u64);
                node.set_heartbeat_period(2);
                CpGroup::new(node)
            })
            .collect();
        CpSim {
            groups,
            alive: vec![true; n],
            bus: Vec::new(),
            cut: HashSet::new(),
            done: vec![Vec::new(); n],
        }
    }

    fn linked(&self, a: NodeId, b: NodeId) -> bool {
        self.alive[a] && self.alive[b] && !self.cut.contains(&(a, b)) && !self.cut.contains(&(b, a))
    }

    fn route(&mut self, from: NodeId, out: Vec<(NodeId, CpMsg)>) {
        for (to, msg) in out {
            if self.linked(from, to) {
                self.bus.push((from, to, msg));
            }
        }
    }

    fn step(&mut self) {
        for (from, to, msg) in std::mem::take(&mut self.bus) {
            if !self.linked(from, to) {
                continue;
            }
            let mut out = Vec::new();
            self.groups[to].step(from, msg, &mut out);
            self.route(to, out);
        }
        for i in 0..self.groups.len() {
            if !self.alive[i] {
                continue;
            }
            let mut out = Vec::new();
            self.groups[i].tick(&mut out);
            self.route(i, out);
            let c = self.groups[i].take_completions();
            self.done[i].extend(c);
        }
    }

    fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = (0..self.groups.len())
            .filter(|&i| self.alive[i] && self.groups[i].is_leader())
            .collect();
        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    /// Submit a client op at member `member`.
    fn submit(&mut self, member: NodeId, client: ClientId, command: Vec<u8>) {
        let mut out = Vec::new();
        self.groups[member].submit(client, command, &mut out);
        self.route(member, out);
    }

    /// Find the completion for `client` across all members.
    fn completion(&self, client: ClientId) -> Option<&Completion> {
        self.done.iter().flatten().find(|c| c.client == client)
    }
}

#[test]
fn op_on_leader_completes_locally() {
    let mut sim = CpSim::new(3);
    sim.run(80);
    let leader = sim.leader().expect("a leader");
    sim.submit(leader, 1, al_command("c", &AlOp::AddAndGet(5)));
    sim.run(60);
    let done = sim.completion(1).expect("op 1 completed");
    assert_eq!(done.reply, CpReply::Long(5));
    // The completion surfaced on the member the client was connected to.
    assert!(sim.done[leader].iter().any(|c| c.client == 1));
}

#[test]
fn op_on_follower_forwards_and_completes() {
    let mut sim = CpSim::new(3);
    sim.run(80);
    let leader = sim.leader().expect("a leader");
    let follower = (0..3).find(|&i| i != leader).unwrap();
    sim.submit(follower, 7, al_command("c", &AlOp::AddAndGet(3)));
    sim.run(80);
    let done = sim.completion(7).expect("op 7 completed");
    assert_eq!(
        done.reply,
        CpReply::Long(3),
        "reply routed back to the follower"
    );
    assert!(
        sim.done[follower].iter().any(|c| c.client == 7),
        "completion surfaced on the follower the client is on"
    );
    // Every replica applied the increment.
    for i in 0..3 {
        assert_eq!(
            sim.groups[i].sm().atomic_long().get("c"),
            3,
            "replica {i} applied"
        );
    }
}

#[test]
fn interleaved_ops_from_all_members_are_linearizable() {
    let mut sim = CpSim::new(5);
    sim.run(80);
    // Each member's client fires an increment; replies must reflect a consistent
    // total order (values 1..=5 each observed exactly once).
    for member in 0..5 {
        sim.submit(
            member,
            member as ClientId,
            al_command("c", &AlOp::AddAndGet(1)),
        );
    }
    sim.run(150);
    let replies: Vec<i64> = (0..5)
        .map(|c| match sim.completion(c as ClientId).map(|x| &x.reply) {
            Some(CpReply::Long(v)) => *v,
            other => panic!("op {c} not completed: {other:?}"),
        })
        .collect();
    let mut sorted = replies.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![1, 2, 3, 4, 5],
        "replies form a total order: {replies:?}"
    );
    // Final replicated value is 5 on every alive replica.
    for i in 0..5 {
        assert_eq!(
            sim.groups[i].sm().atomic_long().get("c"),
            5,
            "replica {i} final value"
        );
    }
}

#[test]
fn cpmap_ops_replicate_through_raft() {
    use raft::cp::cm_command;
    use raft::cpmap::MapOp;
    let mut sim = CpSim::new(3);
    sim.run(80);
    let leader = sim.leader().expect("a leader");

    // First put → no previous; second put → the old value; both via the log.
    sim.submit(
        leader,
        1,
        cm_command("m", &MapOp::Put(b"k".to_vec(), b"v1".to_vec())),
    );
    sim.run(40);
    assert_eq!(sim.completion(1).unwrap().reply, CpReply::Data(None));
    sim.submit(
        leader,
        2,
        cm_command("m", &MapOp::Put(b"k".to_vec(), b"v2".to_vec())),
    );
    sim.run(40);
    assert_eq!(
        sim.completion(2).unwrap().reply,
        CpReply::Data(Some(b"v1".to_vec()))
    );

    // A stale CAS fails linearizably.
    sim.submit(
        leader,
        3,
        cm_command(
            "m",
            &MapOp::CompareAndSet(b"k".to_vec(), b"WRONG".to_vec(), b"v3".to_vec()),
        ),
    );
    sim.run(40);
    assert_eq!(sim.completion(3).unwrap().reply, CpReply::Bool(false));

    // Every replica applied the same committed state.
    for i in 0..3 {
        assert_eq!(
            sim.groups[i].sm().cp_map().get("m", b"k"),
            Some(b"v2".to_vec()),
            "replica {i}"
        );
    }
}

/// Helper: extract a Long reply for `client`.
fn long_reply(sim: &CpSim, client: ClientId) -> i64 {
    match sim.completion(client).map(|c| &c.reply) {
        Some(CpReply::Long(v)) => *v,
        other => panic!("client {client} not a Long: {other:?}"),
    }
}

#[test]
fn session_expiry_auto_releases_fenced_lock() {
    let mut sim = CpSim::new(3);
    sim.run(80);
    let leader = sim.leader().expect("a leader");

    // Client A creates a session and locks "lk".
    sim.submit(leader, 1, sess_command(&SessOp::Create));
    sim.run(20);
    let sid_a = long_reply(&sim, 1);
    assert!(sid_a > 0, "session created");

    sim.submit(
        leader,
        2,
        fl_command(
            "lk",
            &FlOp::Lock {
                session: sid_a,
                thread: 1,
            },
        ),
    );
    sim.run(20);
    let fence_a = long_reply(&sim, 2);
    assert!(fence_a > 0, "A holds the lock");

    // Another owner cannot take it while A's session is alive.
    sim.submit(
        leader,
        3,
        fl_command(
            "lk",
            &FlOp::Lock {
                session: 999,
                thread: 2,
            },
        ),
    );
    sim.run(20);
    assert_eq!(long_reply(&sim, 3), 0, "lock is held; B refused");

    // Advance the session clock past the TTL WITHOUT heartbeating A (drive Ticks
    // directly rather than wait for the leader's ~1s cadence). A expires.
    for i in 0..40 {
        sim.submit(leader, 1000 + i, sess_command(&SessOp::Tick));
        sim.run(4);
    }

    // B can now acquire "lk" — A's lock was auto-released — with a HIGHER fence.
    sim.submit(
        leader,
        4,
        fl_command(
            "lk",
            &FlOp::Lock {
                session: 999,
                thread: 2,
            },
        ),
    );
    sim.run(30);
    let fence_b = long_reply(&sim, 4);
    assert!(
        fence_b > fence_a,
        "auto-released; B acquires with a strictly greater fence ({fence_b} > {fence_a})"
    );
}
