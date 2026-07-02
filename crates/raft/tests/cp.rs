//! The CP group driver: a client op submitted to any member — leader or
//! follower — commits through Raft and completes on the member the client is on,
//! with the correct linearizable reply. Exercises forward-to-leader routing.

use raft::atomiclong::{encode, AlOp, AlReply};
use raft::cp::{ClientId, Completion, CpGroup, CpMsg};
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
    sim.submit(leader, 1, encode("c", &AlOp::AddAndGet(5)));
    sim.run(60);
    let done = sim.completion(1).expect("op 1 completed");
    assert_eq!(done.reply, AlReply::Long(5));
    // The completion surfaced on the member the client was connected to.
    assert!(sim.done[leader].iter().any(|c| c.client == 1));
}

#[test]
fn op_on_follower_forwards_and_completes() {
    let mut sim = CpSim::new(3);
    sim.run(80);
    let leader = sim.leader().expect("a leader");
    let follower = (0..3).find(|&i| i != leader).unwrap();
    sim.submit(follower, 7, encode("c", &AlOp::AddAndGet(3)));
    sim.run(80);
    let done = sim.completion(7).expect("op 7 completed");
    assert_eq!(
        done.reply,
        AlReply::Long(3),
        "reply routed back to the follower"
    );
    assert!(
        sim.done[follower].iter().any(|c| c.client == 7),
        "completion surfaced on the follower the client is on"
    );
    // Every replica applied the increment.
    for i in 0..3 {
        assert_eq!(sim.groups[i].sm().get("c"), 3, "replica {i} applied");
    }
}

#[test]
fn interleaved_ops_from_all_members_are_linearizable() {
    let mut sim = CpSim::new(5);
    sim.run(80);
    // Each member's client fires an increment; replies must reflect a consistent
    // total order (values 1..=5 each observed exactly once).
    for member in 0..5 {
        sim.submit(member, member as ClientId, encode("c", &AlOp::AddAndGet(1)));
    }
    sim.run(150);
    let replies: Vec<i64> = (0..5)
        .map(|c| match sim.completion(c as ClientId).map(|x| &x.reply) {
            Some(AlReply::Long(v)) => *v,
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
        assert_eq!(sim.groups[i].sm().get("c"), 5, "replica {i} final value");
    }
}
