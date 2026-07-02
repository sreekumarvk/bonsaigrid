//! Deterministic Raft simulation harness shared by the consensus and
//! linearizability tests: N nodes in one process over an in-memory bus with
//! virtual time and fault injection (partition / crash).
//!
//! Each integration-test binary compiles this module independently and uses a
//! different subset of its helpers, so allow unused items here.
#![allow(dead_code)]

use raft::{NodeId, RaftLog, RaftMsg, RaftNode};
use std::collections::HashSet;

pub struct Sim {
    pub nodes: Vec<RaftNode>,
    pub alive: Vec<bool>,
    pub bus: Vec<(NodeId, NodeId, RaftMsg)>,
    pub cut: HashSet<(NodeId, NodeId)>,
    /// Committed `(index, command)` stream observed at each node.
    pub applied: Vec<Vec<(u64, Vec<u8>)>>,
}

impl Sim {
    pub fn new(n: usize) -> Sim {
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

    pub fn linked(&self, a: NodeId, b: NodeId) -> bool {
        self.alive[a] && self.alive[b] && !self.cut.contains(&(a, b)) && !self.cut.contains(&(b, a))
    }

    fn route(&mut self, from: NodeId, out: Vec<(NodeId, RaftMsg)>) {
        for (to, msg) in out {
            if self.linked(from, to) {
                self.bus.push((from, to, msg));
            }
        }
    }

    pub fn step(&mut self) {
        for (from, to, msg) in std::mem::take(&mut self.bus) {
            if !self.linked(from, to) {
                continue;
            }
            let mut out = Vec::new();
            self.nodes[to].step(from, msg, &mut out);
            self.route(to, out);
        }
        for i in 0..self.nodes.len() {
            if !self.alive[i] {
                continue;
            }
            let mut out = Vec::new();
            self.nodes[i].tick(&mut out);
            self.route(i, out);
            let c = self.nodes[i].take_committed();
            self.applied[i].extend(c);
            // Continuously exercise compaction so every safety test also stresses
            // the snapshot/log-compaction path with a small retention window.
            self.nodes[i].maybe_compact(8);
        }
    }

    pub fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }

    /// The single leader of the newest term among alive nodes, if unambiguous.
    pub fn leader(&self) -> Option<NodeId> {
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

    /// Propose via the current leader; returns true if accepted.
    pub fn propose(&mut self, cmd: &[u8]) -> bool {
        if let Some(l) = self.leader() {
            self.nodes[l].propose(cmd.to_vec()).is_some()
        } else {
            false
        }
    }

    pub fn kill(&mut self, i: NodeId) {
        self.alive[i] = false;
    }

    pub fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        for &x in a {
            for &y in b {
                self.cut.insert((x, y));
                self.cut.insert((y, x));
            }
        }
    }
}

/// No two leaders ever coexist for the same term (Raft election safety).
pub fn assert_no_two_leaders_same_term(sim: &Sim) {
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
