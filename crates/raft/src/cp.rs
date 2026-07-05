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

use crate::atomiclong::{AlOp, AlReply, AtomicLongSm};
use crate::atomicref::{ArOp, AtomicReferenceSm};
use crate::countdownlatch::{CdlOp, CountDownLatchSm};
use crate::fencedlock::{FencedLockSm, FlOp};
use crate::semaphore::{SemOp, SemaphoreSm};
use crate::session::{SessOp, SessionSm};
use crate::{NodeId, RaftMsg, RaftNode};
use std::collections::HashMap;

/// Correlation id for an in-flight client operation.
pub type ClientId = u64;

/// Live log entries retained before compaction kicks in.
const COMPACT_KEEP: usize = 256;

/// Member ticks between leader-proposed session clock advances (~1s at a 1ms
/// member tick). One session TTL is `session::TTL_MILLIS` worth of these.
const SESSION_TICK_INTERVAL: u64 = 1000;

/// Object-type tag prefixing every replicated command (selects the state machine).
pub const OBJ_ATOMIC_LONG: u8 = 0;
pub const OBJ_ATOMIC_REF: u8 = 1;
pub const OBJ_COUNTDOWN_LATCH: u8 = 2;
pub const OBJ_SEMAPHORE: u8 = 3;
pub const OBJ_FENCED_LOCK: u8 = 4;
pub const OBJ_SESSION: u8 = 5;
pub const OBJ_CP_MAP: u8 = 6;

/// A committed reply, shaped by the operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CpReply {
    Long(i64),
    Bool(bool),
    Data(Option<Vec<u8>>),
    Nil,
}

/// Build an AtomicLong command: `[OBJ_ATOMIC_LONG][atomiclong body]`.
pub fn al_command(name: &str, op: &AlOp) -> Vec<u8> {
    let mut c = vec![OBJ_ATOMIC_LONG];
    c.extend_from_slice(&crate::atomiclong::encode(name, op));
    c
}

/// Build an AtomicReference command: `[OBJ_ATOMIC_REF][atomicref body]`.
pub fn ar_command(name: &str, op: &ArOp) -> Vec<u8> {
    let mut c = vec![OBJ_ATOMIC_REF];
    c.extend_from_slice(&crate::atomicref::encode(name, op));
    c
}

/// Build a CountDownLatch command: `[OBJ_COUNTDOWN_LATCH][body]`.
pub fn cdl_command(name: &str, op: &CdlOp) -> Vec<u8> {
    let mut c = vec![OBJ_COUNTDOWN_LATCH];
    c.extend_from_slice(&crate::countdownlatch::encode(name, op));
    c
}

/// Build a Semaphore command: `[OBJ_SEMAPHORE][body]`.
pub fn sem_command(name: &str, op: &SemOp) -> Vec<u8> {
    let mut c = vec![OBJ_SEMAPHORE];
    c.extend_from_slice(&crate::semaphore::encode(name, op));
    c
}

/// Build a FencedLock command: `[OBJ_FENCED_LOCK][body]`.
pub fn fl_command(name: &str, op: &FlOp) -> Vec<u8> {
    let mut c = vec![OBJ_FENCED_LOCK];
    c.extend_from_slice(&crate::fencedlock::encode(name, op));
    c
}

/// Build a CPMap command: `[OBJ_CP_MAP][cpmap body]`.
pub fn cm_command(name: &str, op: &crate::cpmap::MapOp) -> Vec<u8> {
    let mut c = vec![OBJ_CP_MAP];
    c.extend_from_slice(&crate::cpmap::encode(name, op));
    c
}

/// Build a CP-session command: `[OBJ_SESSION][body]`.
pub fn sess_command(op: &SessOp) -> Vec<u8> {
    let mut c = vec![OBJ_SESSION];
    c.extend_from_slice(&crate::session::encode(op));
    c
}

/// The replicated CP state machine: a registry that dispatches an object-tagged
/// command to the owning per-type machine. New primitives add a tag + a field.
#[derive(Default)]
pub struct CpSm {
    atomic_long: AtomicLongSm,
    atomic_ref: AtomicReferenceSm,
    countdown_latch: CountDownLatchSm,
    semaphore: SemaphoreSm,
    fenced_lock: FencedLockSm,
    sessions: SessionSm,
    cp_map: crate::cpmap::CpMapSm,
}

impl CpSm {
    pub fn new() -> CpSm {
        CpSm::default()
    }

    /// Apply a committed `[obj_type][body]` command; unknown types are a no-op.
    pub fn apply(&mut self, command: &[u8]) -> CpReply {
        let Some((&obj, body)) = command.split_first() else {
            return CpReply::Nil;
        };
        match obj {
            OBJ_ATOMIC_LONG => match self.atomic_long.apply(body) {
                AlReply::Long(v) => CpReply::Long(v),
                AlReply::Bool(b) => CpReply::Bool(b),
                AlReply::None => CpReply::Nil,
            },
            OBJ_ATOMIC_REF => self.atomic_ref.apply(body),
            OBJ_COUNTDOWN_LATCH => self.countdown_latch.apply(body),
            OBJ_SEMAPHORE => self.semaphore.apply(body),
            OBJ_FENCED_LOCK => self.fenced_lock.apply(body),
            OBJ_SESSION => self.apply_session(body),
            OBJ_CP_MAP => self.cp_map.apply(body),
            _ => CpReply::Nil,
        }
    }

    pub fn atomic_long(&self) -> &AtomicLongSm {
        &self.atomic_long
    }
    pub fn atomic_ref(&self) -> &AtomicReferenceSm {
        &self.atomic_ref
    }
    pub fn countdown_latch(&self) -> &CountDownLatchSm {
        &self.countdown_latch
    }
    pub fn semaphore(&self) -> &SemaphoreSm {
        &self.semaphore
    }
    pub fn fenced_lock(&self) -> &FencedLockSm {
        &self.fenced_lock
    }
    pub fn cp_map(&self) -> &crate::cpmap::CpMapSm {
        &self.cp_map
    }
    pub fn sessions(&self) -> &SessionSm {
        &self.sessions
    }

    /// Apply a session op, auto-releasing resources held by closed/expired
    /// sessions across the resource state machines (v1: FencedLock).
    fn apply_session(&mut self, body: &[u8]) -> CpReply {
        let Some(op) = crate::session::decode(body) else {
            return CpReply::Nil;
        };
        match op {
            SessOp::Create => CpReply::Long(self.sessions.create()),
            SessOp::Heartbeat(id) => {
                self.sessions.heartbeat(id);
                CpReply::Nil
            }
            SessOp::Close(id) => {
                let existed = self.sessions.close(id);
                self.fenced_lock.release_session(id);
                CpReply::Bool(existed)
            }
            SessOp::Tick => {
                for id in self.sessions.tick() {
                    self.fenced_lock.release_session(id);
                }
                CpReply::Nil
            }
            SessOp::GenerateThreadId => CpReply::Long(self.sessions.generate_thread_id()),
        }
    }
}

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
    Reply { client: ClientId, reply: CpReply },
}

/// A client op that has committed and is ready to answer the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub client: ClientId,
    pub reply: CpReply,
}

/// One CP group member: a Raft node + the CP state machine + client-op routing.
pub struct CpGroup {
    id: NodeId,
    raft: RaftNode,
    sm: CpSm,
    /// Log index -> (client, origin member) for ops this node proposed.
    pending: HashMap<u64, (ClientId, NodeId)>,
    /// Locally-originated ops buffered until a leader is known.
    waiting: Vec<(ClientId, Vec<u8>)>,
    completions: Vec<Completion>,
    /// Ticks since the leader last proposed a session clock advance.
    ticks_since_session: u64,
}

impl CpGroup {
    pub fn new(raft: RaftNode) -> CpGroup {
        let id = raft.id();
        CpGroup {
            id,
            raft,
            sm: CpSm::new(),
            pending: HashMap::new(),
            waiting: Vec::new(),
            completions: Vec::new(),
            ticks_since_session: 0,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }
    /// True if this member can answer a linearizable read from its local `sm()`
    /// without appending to the log (the leader read-lease / ReadIndex-lease).
    pub fn has_read_lease(&self) -> bool {
        self.raft.has_read_lease()
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.raft.leader()
    }
    pub fn sm(&self) -> &CpSm {
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
        // The leader advances the session clock periodically so idle sessions
        // expire (and their locks auto-release) with no client action required.
        if self.raft.is_leader() {
            self.ticks_since_session += 1;
            if self.ticks_since_session >= SESSION_TICK_INTERVAL {
                self.ticks_since_session = 0;
                self.raft.propose(sess_command(&SessOp::Tick));
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

fn put_reply(o: &mut Vec<u8>, r: &CpReply) {
    match r {
        CpReply::Long(v) => {
            o.push(0);
            o.extend_from_slice(&v.to_le_bytes());
        }
        CpReply::Bool(v) => {
            o.push(1);
            o.push(*v as u8);
        }
        CpReply::Data(d) => {
            o.push(2);
            match d {
                Some(b) => put_blob(o, b),
                None => put_u64(o, u64::MAX), // sentinel for null
            }
        }
        CpReply::Nil => o.push(3),
    }
}
fn get_reply(b: &[u8], p: &mut usize) -> Option<CpReply> {
    let tag = *b.get(*p)?;
    *p += 1;
    Some(match tag {
        0 => {
            let v = i64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
            *p += 8;
            CpReply::Long(v)
        }
        1 => {
            let v = *b.get(*p)? != 0;
            *p += 1;
            CpReply::Bool(v)
        }
        2 => {
            let len = u64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
            if len == u64::MAX {
                *p += 8;
                CpReply::Data(None)
            } else {
                CpReply::Data(Some(get_blob(b, p)?))
            }
        }
        _ => CpReply::Nil,
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
    use crate::atomiclong::AlOp;

    fn roundtrip(m: CpMsg) {
        let bytes = encode_msg(&m);
        let back = decode_msg(&bytes).expect("decodes");
        // Compare via re-encoding (CpMsg holds CpReply, which nests bytes).
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
            command: al_command("c", &AlOp::AddAndGet(7)),
        });
        roundtrip(CpMsg::Reply {
            client: 42,
            reply: CpReply::Long(7),
        });
        roundtrip(CpMsg::Reply {
            client: 43,
            reply: CpReply::Bool(false),
        });
        roundtrip(CpMsg::Reply {
            client: 44,
            reply: CpReply::Data(Some(b"hello".to_vec())),
        });
        roundtrip(CpMsg::Reply {
            client: 45,
            reply: CpReply::Data(None),
        });
    }
}
