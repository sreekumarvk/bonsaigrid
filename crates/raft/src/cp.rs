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

/// Live log entries retained before compaction kicks in.
const COMPACT_KEEP: usize = 256;

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
    /// apply newly-committed commands, and bound the log by compaction.
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
        // Applied state lives in `sm`, so committed log entries are redundant once
        // every member has them — fold them away to bound memory.
        self.raft.maybe_compact(COMPACT_KEEP);
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

// ---- CpMsg wire codec (carried opaquely by the member transport) ----

use crate::log::Entry;

fn put_u64(o: &mut Vec<u8>, v: u64) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_blob(o: &mut Vec<u8>, b: &[u8]) {
    put_u64(o, b.len() as u64);
    o.extend_from_slice(b);
}
fn get_u64(b: &[u8], p: &mut usize) -> Option<u64> {
    let v = u64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
    *p += 8;
    Some(v)
}
fn get_blob(b: &[u8], p: &mut usize) -> Option<Vec<u8>> {
    let n = get_u64(b, p)? as usize;
    let s = b.get(*p..*p + n)?.to_vec();
    *p += n;
    Some(s)
}

fn put_entry(o: &mut Vec<u8>, e: &Entry) {
    put_u64(o, e.term);
    put_u64(o, e.index);
    put_blob(o, &e.command);
}
fn get_entry(b: &[u8], p: &mut usize) -> Option<Entry> {
    Some(Entry {
        term: get_u64(b, p)?,
        index: get_u64(b, p)?,
        command: get_blob(b, p)?,
    })
}

fn put_reply(o: &mut Vec<u8>, r: &AlReply) {
    match r {
        AlReply::Long(v) => {
            o.push(0);
            o.extend_from_slice(&v.to_le_bytes());
        }
        AlReply::Bool(v) => {
            o.push(1);
            o.push(*v as u8);
        }
        AlReply::None => o.push(2),
    }
}
fn get_reply(b: &[u8], p: &mut usize) -> Option<AlReply> {
    let tag = *b.get(*p)?;
    *p += 1;
    Some(match tag {
        0 => {
            let v = i64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
            *p += 8;
            AlReply::Long(v)
        }
        1 => {
            let v = *b.get(*p)? != 0;
            *p += 1;
            AlReply::Bool(v)
        }
        _ => AlReply::None,
    })
}

/// Serialize a [`CpMsg`] for the member transport (little-endian).
pub fn encode_msg(msg: &CpMsg) -> Vec<u8> {
    let mut o = Vec::new();
    match msg {
        CpMsg::Raft(rm) => {
            o.push(0);
            encode_raft(&mut o, rm);
        }
        CpMsg::Forward {
            client,
            origin,
            command,
        } => {
            o.push(1);
            put_u64(&mut o, *client);
            put_u64(&mut o, *origin as u64);
            put_blob(&mut o, command);
        }
        CpMsg::Reply { client, reply } => {
            o.push(2);
            put_u64(&mut o, *client);
            put_reply(&mut o, reply);
        }
    }
    o
}

/// Deserialize a [`CpMsg`] produced by [`encode_msg`].
pub fn decode_msg(b: &[u8]) -> Option<CpMsg> {
    let mut p = 1;
    match *b.first()? {
        0 => Some(CpMsg::Raft(decode_raft(b, &mut p)?)),
        1 => Some(CpMsg::Forward {
            client: get_u64(b, &mut p)?,
            origin: get_u64(b, &mut p)? as NodeId,
            command: get_blob(b, &mut p)?,
        }),
        2 => Some(CpMsg::Reply {
            client: get_u64(b, &mut p)?,
            reply: get_reply(b, &mut p)?,
        }),
        _ => None,
    }
}

fn encode_raft(o: &mut Vec<u8>, m: &RaftMsg) {
    match m {
        RaftMsg::RequestVote {
            term,
            candidate,
            last_log_index,
            last_log_term,
        } => {
            o.push(0);
            put_u64(o, *term);
            put_u64(o, *candidate as u64);
            put_u64(o, *last_log_index);
            put_u64(o, *last_log_term);
        }
        RaftMsg::RequestVoteResp { term, granted } => {
            o.push(1);
            put_u64(o, *term);
            o.push(*granted as u8);
        }
        RaftMsg::AppendEntries {
            term,
            leader,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            o.push(2);
            put_u64(o, *term);
            put_u64(o, *leader as u64);
            put_u64(o, *prev_log_index);
            put_u64(o, *prev_log_term);
            put_u64(o, entries.len() as u64);
            for e in entries {
                put_entry(o, e);
            }
            put_u64(o, *leader_commit);
        }
        RaftMsg::AppendEntriesResp {
            term,
            success,
            match_index,
        } => {
            o.push(3);
            put_u64(o, *term);
            o.push(*success as u8);
            put_u64(o, *match_index);
        }
    }
}

fn decode_raft(b: &[u8], p: &mut usize) -> Option<RaftMsg> {
    let tag = *b.get(*p)?;
    *p += 1;
    Some(match tag {
        0 => RaftMsg::RequestVote {
            term: get_u64(b, p)?,
            candidate: get_u64(b, p)? as NodeId,
            last_log_index: get_u64(b, p)?,
            last_log_term: get_u64(b, p)?,
        },
        1 => RaftMsg::RequestVoteResp {
            term: get_u64(b, p)?,
            granted: {
                let g = *b.get(*p)? != 0;
                *p += 1;
                g
            },
        },
        2 => {
            let term = get_u64(b, p)?;
            let leader = get_u64(b, p)? as NodeId;
            let prev_log_index = get_u64(b, p)?;
            let prev_log_term = get_u64(b, p)?;
            let n = get_u64(b, p)? as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                entries.push(get_entry(b, p)?);
            }
            let leader_commit = get_u64(b, p)?;
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }
        }
        3 => RaftMsg::AppendEntriesResp {
            term: get_u64(b, p)?,
            success: {
                let s = *b.get(*p)? != 0;
                *p += 1;
                s
            },
            match_index: get_u64(b, p)?,
        },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomiclong::{encode, AlOp};

    fn roundtrip(m: CpMsg) {
        let bytes = encode_msg(&m);
        let back = decode_msg(&bytes).expect("decodes");
        // Compare via re-encoding (CpMsg has no PartialEq due to AlReply nesting).
        assert_eq!(encode_msg(&back), bytes, "roundtrip stable");
    }

    #[test]
    fn cpmsg_wire_roundtrip() {
        roundtrip(CpMsg::Raft(RaftMsg::RequestVote {
            term: 3,
            candidate: 2,
            last_log_index: 5,
            last_log_term: 2,
        }));
        roundtrip(CpMsg::Raft(RaftMsg::RequestVoteResp {
            term: 3,
            granted: true,
        }));
        roundtrip(CpMsg::Raft(RaftMsg::AppendEntries {
            term: 4,
            leader: 1,
            prev_log_index: 2,
            prev_log_term: 3,
            entries: vec![
                Entry {
                    term: 4,
                    index: 3,
                    command: b"cmd1".to_vec(),
                },
                Entry {
                    term: 4,
                    index: 4,
                    command: b"cmd2".to_vec(),
                },
            ],
            leader_commit: 2,
        }));
        roundtrip(CpMsg::Raft(RaftMsg::AppendEntriesResp {
            term: 4,
            success: true,
            match_index: 4,
        }));
        roundtrip(CpMsg::Forward {
            client: 42,
            origin: 3,
            command: encode("c", &AlOp::AddAndGet(7)),
        });
        roundtrip(CpMsg::Reply {
            client: 42,
            reply: AlReply::Long(7),
        });
        roundtrip(CpMsg::Reply {
            client: 43,
            reply: AlReply::Bool(false),
        });
    }
}
