//! CP group driver: wires a [`RaftNode`] + the AtomicLong state machine into a
//! client-facing service with **forward-to-leader** routing and reply delivery.
//!
//! A client is connected to some member. That member submits the command here;
//! if it is the leader it proposes directly, otherwise it forwards to the leader.
//! Whichever member proposed records the pending client op against the assigned
//! log index; when that index commits and applies, the reply is delivered to the
//! member the client is on (locally, or via a `Reply` message), which completes
//! the client. This is the orchestration the live server's member transport +
//! AtomicLong client codecs plug into (Phase C3).
//!
//! Not yet handled (follow-ups, per the spec): CP sessions / dedup of retried
//! commands (so a client retry after a leader change can double-apply a
//! non-idempotent op), and read-index optimization (reads go through the log).

use crate::atomiclong::{AlReply, AtomicLongSm};
use crate::{NodeId, RaftMsg, RaftNode};
use std::collections::HashMap;

/// Correlation id for an in-flight client operation.
pub type ClientId = u64;

/// A message exchanged between CP group members.
#[derive(Clone, Debug)]
pub enum CpMsg {
    /// A raw Raft RPC.
    Raft(RaftMsg),
    /// A member forwards a client's command to the leader.
    Forward {
        client: ClientId,
        origin: NodeId,
        command: Vec<u8>,
    },
    /// The proposing member returns a committed reply to the origin member.
    Reply { client: ClientId, reply: AlReply },
}

/// A client op that has committed and is ready to answer the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub client: ClientId,
    pub reply: AlReply,
}

/// One CP group member: a Raft node + the AtomicLong SM + client-op routing.
pub struct CpGroup {
    id: NodeId,
    raft: RaftNode,
    sm: AtomicLongSm,
    /// Log index -> (client, origin member) for ops this node proposed.
    pending: HashMap<u64, (ClientId, NodeId)>,
    /// Locally-originated ops buffered until a leader is known.
    waiting: Vec<(ClientId, Vec<u8>)>,
    completions: Vec<Completion>,
}

impl CpGroup {
    pub fn new(raft: RaftNode) -> CpGroup {
        let id = raft.id();
        CpGroup {
            id,
            raft,
            sm: AtomicLongSm::new(),
            pending: HashMap::new(),
            waiting: Vec::new(),
            completions: Vec::new(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.raft.leader()
    }
    pub fn sm(&self) -> &AtomicLongSm {
        &self.sm
    }

    /// A client connected to THIS member submits an AtomicLong command (encoded
    /// via `atomiclong::encode`).
    pub fn submit(&mut self, client: ClientId, command: Vec<u8>, out: &mut Vec<(NodeId, CpMsg)>) {
        self.route(client, self.id, command, out);
    }

    /// Route a command originating at member `origin` to wherever it can be
    /// proposed: propose locally if leader, forward if a leader is known, else
    /// buffer (locally-originated only — a forwarded op with no known leader is
    /// dropped so the origin's retry/timeout drives progress).
    fn route(
        &mut self,
        client: ClientId,
        origin: NodeId,
        command: Vec<u8>,
        out: &mut Vec<(NodeId, CpMsg)>,
    ) {
        if self.raft.is_leader() {
            if let Some(index) = self.raft.propose(command) {
                self.pending.insert(index, (client, origin));
            }
        } else if let Some(leader) = self.raft.leader() {
            out.push((
                leader,
                CpMsg::Forward {
                    client,
                    origin,
                    command,
                },
            ));
        } else if origin == self.id {
            self.waiting.push((client, command));
        }
    }

    /// Handle an incoming CP message.
    pub fn step(&mut self, from: NodeId, msg: CpMsg, out: &mut Vec<(NodeId, CpMsg)>) {
        match msg {
            CpMsg::Raft(rm) => {
                let mut rout = Vec::new();
                self.raft.step(from, rm, &mut rout);
                for (to, m) in rout {
                    out.push((to, CpMsg::Raft(m)));
                }
            }
            CpMsg::Forward {
                client,
                origin,
                command,
            } => {
                // Expected to be the leader; if leadership moved, re-route.
                self.route(client, origin, command, out);
            }
            CpMsg::Reply { client, reply } => {
                self.completions.push(Completion { client, reply });
            }
        }
        self.apply_committed(out);
    }

    /// Advance logical time: drive Raft, flush buffered ops once a leader exists,
    /// and apply newly-committed commands.
    pub fn tick(&mut self, out: &mut Vec<(NodeId, CpMsg)>) {
        let mut rout = Vec::new();
        self.raft.tick(&mut rout);
        for (to, m) in rout {
            out.push((to, CpMsg::Raft(m)));
        }
        if self.raft.is_leader() || self.raft.leader().is_some() {
            for (client, command) in std::mem::take(&mut self.waiting) {
                self.route(client, self.id, command, out);
            }
        }
        self.apply_committed(out);
    }

    /// Apply committed commands to the SM; deliver replies for pending ops.
    fn apply_committed(&mut self, out: &mut Vec<(NodeId, CpMsg)>) {
        for (index, command) in self.raft.take_committed() {
            let reply = self.sm.apply(&command);
            if let Some((client, origin)) = self.pending.remove(&index) {
                if origin == self.id {
                    self.completions.push(Completion { client, reply });
                } else {
                    out.push((origin, CpMsg::Reply { client, reply }));
                }
            }
        }
    }

    /// Drain client ops that have committed (to answer their clients).
    pub fn take_completions(&mut self) -> Vec<Completion> {
        std::mem::take(&mut self.completions)
    }
}
