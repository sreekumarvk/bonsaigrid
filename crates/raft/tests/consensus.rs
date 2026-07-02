//! Deterministic simulation of a Raft group: N nodes in one process over an
//! in-memory bus with virtual time and fault injection. Asserts the core safety
//! and liveness properties — single leader, replication + commit, re-election
//! after a crash, minority cannot commit, and committed logs never diverge.

use raft::{NodeId, RaftLog, RaftMsg, RaftNode};
use std::collections::HashSet;

struct Sim {
    nodes: Vec<RaftNode>,
    alive: Vec<bool>,
    bus: Vec<(NodeId, NodeId, RaftMsg)>,
    cut: HashSet<(NodeId, NodeId)>,
    applied: Vec<Vec<(u64, Vec<u8>)>>,
}

impl Sim {
    fn new(n: usize) -> Sim {
        let peers: Vec<NodeId> = (0..n).collect();
        let nodes = (0..n)
            .map(|i| {
                let mut node = RaftNode::new(i, peers.clone(), RaftLog::new(), 0x51ED + i as u64);
                node.set_heartbeat_period(2);
                node
            })
            .collect();
        Sim {
            nodes,
            alive: vec![true; n],
            bus: Vec::new(),
            cut: HashSet::new(),
            applied: vec![Vec::new(); n],
        }
    }

    fn linked(&self, a: NodeId, b: NodeId) -> bool {
        self.alive[a] && self.alive[b] && !self.cut.contains(&(a, b)) && !self.cut.contains(&(b, a))
    }

    fn route(&mut self, from: NodeId, out: Vec<(NodeId, RaftMsg)>) {
        for (to, msg) in out {
            if self.linked(from, to) {
                self.bus.push((from, to, msg));
            }
        }
    }

    fn step(&mut self) {
        // Deliver messages queued last step (1-tick latency).
        for (from, to, msg) in std::mem::take(&mut self.bus) {
            if !self.linked(from, to) {
                continue;
            }
            let mut out = Vec::new();
            self.nodes[to].step(from, msg, &mut out);
            self.route(to, out);
        }
        // Tick each alive node; collect committed commands.
        for i in 0..self.nodes.len() {
            if !self.alive[i] {
                continue;
            }
            let mut out = Vec::new();
            self.nodes[i].tick(&mut out);
            self.route(i, out);
            let c = self.nodes[i].take_committed();
            self.applied[i].extend(c);
        }
    }

    fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// The single leader of the newest term among alive nodes, if unambiguous.
    fn leader(&self) -> Option<NodeId> {
        let max_term = (0..self.nodes.len())
            .filter(|&i| self.alive[i])
            .map(|i| self.nodes[i].term())
            .max()
            .unwrap_or(0);
        let leaders: Vec<NodeId> = (0..self.nodes.len())
            .filter(|&i| {
                self.alive[i] && self.nodes[i].is_leader() && self.nodes[i].term() == max_term
            })
            .collect();
        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    fn propose(&mut self, cmd: &[u8]) -> bool {
        if let Some(l) = self.leader() {
            self.nodes[l].propose(cmd.to_vec()).is_some()
        } else {
            false
        }
    }

    fn kill(&mut self, i: NodeId) {
        self.alive[i] = false;
    }
    fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        for &x in a {
            for &y in b {
                self.cut.insert((x, y));
                self.cut.insert((y, x));
            }
        }
    }
}

/// No two leaders ever coexist for the same term (Raft election safety).
fn assert_no_two_leaders_same_term(sim: &Sim) {
    let mut seen = std::collections::HashMap::new();
    for i in 0..sim.nodes.len() {
        if sim.alive[i] && sim.nodes[i].is_leader() {
            let t = sim.nodes[i].term();
            assert!(
                seen.insert(t, i).is_none(),
                "two leaders in term {t}: {} and {i}",
                seen[&t]
            );
        }
    }
}

#[test]
fn elects_a_single_leader() {
    let mut sim = Sim::new(3);
    sim.run(80);
    assert_no_two_leaders_same_term(&sim);
    assert!(sim.leader().is_some(), "a leader must be elected");
}

#[test]
fn replicates_and_commits_to_all() {
    let mut sim = Sim::new(3);
    sim.run(80);
    assert!(sim.propose(b"x=1"), "leader accepts a proposal");
    sim.run(80);
    for i in 0..3 {
        let cmds: Vec<Vec<u8>> = sim.applied[i].iter().map(|(_, c)| c.clone()).collect();
        assert!(
            cmds.contains(&b"x=1".to_vec()),
            "node {i} applied the command"
        );
    }
}

#[test]
fn reelects_after_leader_crash() {
    let mut sim = Sim::new(5);
    sim.run(80);
    let old = sim.leader().expect("initial leader");
    sim.kill(old);
    sim.run(120);
    let new = sim.leader().expect("a new leader after crash");
    assert_ne!(new, old, "the dead node is not the leader");
    assert_no_two_leaders_same_term(&sim);
}

#[test]
fn partitioned_minority_cannot_commit() {
    let mut sim = Sim::new(5);
    sim.run(80);
    // Split {0,1} | {2,3,4}. The majority side keeps/elects a leader and commits;
    // the minority cannot.
    sim.partition(&[0, 1], &[2, 3, 4]);
    sim.run(150);
    // Propose on EVERY node that currently claims leadership — a partitioned
    // stale leader in the minority may still think it leads, but must never
    // commit; only the majority-side leader can.
    for i in 0..5 {
        if sim.alive[i] && sim.nodes[i].is_leader() {
            sim.nodes[i].propose(b"maj=1".to_vec());
        }
    }
    sim.run(150);
    // Minority nodes 0,1 must NOT have applied the command.
    for i in [0, 1] {
        let cmds: Vec<Vec<u8>> = sim.applied[i].iter().map(|(_, c)| c.clone()).collect();
        assert!(
            !cmds.contains(&b"maj=1".to_vec()),
            "minority node {i} must not commit"
        );
    }
    // A majority node must have it.
    let maj_has = [2, 3, 4]
        .iter()
        .any(|&i| sim.applied[i].iter().any(|(_, c)| c == b"maj=1"));
    assert!(maj_has, "a majority node commits the entry");
}

#[test]
fn committed_logs_never_diverge_under_chaos() {
    let mut sim = Sim::new(5);
    sim.run(60);
    // Chaos: propose, partition, heal, kill+revive-equivalent (re-link).
    for round in 0..6 {
        sim.propose(format!("c{round}").as_bytes());
        sim.run(30);
        sim.partition(&[round % 5], &[(round + 1) % 5, (round + 2) % 5]);
        sim.run(30);
        sim.cut.clear(); // heal
        sim.run(30);
    }
    sim.run(120);
    // Any two nodes' committed command sequences must agree on their common prefix.
    for a in 0..5 {
        for b in (a + 1)..5 {
            let ca: Vec<&Vec<u8>> = sim.applied[a].iter().map(|(_, c)| c).collect();
            let cb: Vec<&Vec<u8>> = sim.applied[b].iter().map(|(_, c)| c).collect();
            let n = ca.len().min(cb.len());
            assert_eq!(
                &ca[..n],
                &cb[..n],
                "committed logs of {a} and {b} diverge on their common prefix"
            );
        }
    }
}
