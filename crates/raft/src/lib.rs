//! A from-scratch Raft consensus core (Ongaro & Ousterhout).
//!
//! Pure and message-driven: no threads, no I/O, no clock. The caller feeds
//! messages via [`RaftNode::step`], advances logical time via [`RaftNode::tick`],
//! proposes commands via [`RaftNode::propose`], and drains committed commands via
//! [`RaftNode::take_committed`]. This seam makes the node deterministically
//! simulation-testable (see `tests/`). Durability of the log + `current_term`/
//! `voted_for` is layered on top by the caller (see `log` module).

pub mod atomiclong;
pub mod log;

pub use log::{Entry, RaftLog};

/// Member index within the CP group.
pub type NodeId = usize;

/// Raft RPCs exchanged between group members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftMsg {
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        /// On success, the follower's last log index (for `match_index`).
        match_index: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A single Raft node for one CP group.
pub struct RaftNode {
    id: NodeId,
    peers: Vec<NodeId>, // all members incl. self
    role: Role,

    // Persistent state (the caller mirrors these to disk before responding).
    current_term: u64,
    voted_for: Option<NodeId>,
    log: RaftLog,

    // Volatile state.
    commit_index: u64,
    last_applied: u64,
    leader: Option<NodeId>,

    // Candidate/leader bookkeeping.
    votes: Vec<bool>,      // indexed by NodeId; votes received this election
    next_index: Vec<u64>,  // leader: next entry to send each peer
    match_index: Vec<u64>, // leader: highest replicated index per peer

    // Deterministic timers driven by `tick`. Units are ticks.
    elapsed: u64,          // ticks since last leader contact / heartbeat
    election_timeout: u64, // randomized per follower/candidate
    heartbeat_period: u64,
    rng: u64, // seeded splitmix64 state for timeout jitter
}

impl RaftNode {
    /// Create a node. `peers` is the full member set (including `id`). `seed`
    /// deterministically drives the randomized election timeout.
    pub fn new(id: NodeId, peers: Vec<NodeId>, log: RaftLog, seed: u64) -> RaftNode {
        let n = peers.iter().copied().max().unwrap_or(0) + 1;
        let mut node = RaftNode {
            id,
            peers,
            role: Role::Follower,
            current_term: log.persisted_term(),
            voted_for: log.persisted_vote(),
            log,
            commit_index: 0,
            last_applied: 0,
            leader: None,
            votes: vec![false; n],
            next_index: vec![1; n],
            match_index: vec![0; n],
            elapsed: 0,
            election_timeout: 0,
            heartbeat_period: 1,
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
        };
        node.reset_election_timeout();
        node
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn term(&self) -> u64 {
        self.current_term
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    fn majority(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    fn next_rand(&mut self) -> u64 {
        // splitmix64
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Election timeout in `[base, 2*base)` ticks. `base` is 10 heartbeat periods.
    fn reset_election_timeout(&mut self) {
        let base = 10 * self.heartbeat_period.max(1);
        self.election_timeout = base + self.next_rand() % base;
        self.elapsed = 0;
    }

    /// Set the heartbeat period (ticks). Election timeout is derived from it.
    pub fn set_heartbeat_period(&mut self, ticks: u64) {
        self.heartbeat_period = ticks.max(1);
        self.reset_election_timeout();
    }

    /// Adopt a newer term, revert to follower, and clear the vote.
    fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader = None;
        self.log
            .persist_term_vote(self.current_term, self.voted_for);
        self.reset_election_timeout();
    }

    /// Propose a command (leader only). Returns the assigned log index, or `None`
    /// if this node isn't the leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.log.last_index() + 1;
        self.log.append(Entry {
            term: self.current_term,
            index,
            command,
        });
        self.match_index[self.id] = index;
        Some(index)
    }

    /// Advance logical time. Emits RPCs (heartbeats, or a new election's votes).
    pub fn tick(&mut self, out: &mut Vec<(NodeId, RaftMsg)>) {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed >= self.heartbeat_period {
                    self.elapsed = 0;
                    self.broadcast_append(out);
                }
            }
            Role::Follower | Role::Candidate => {
                if self.elapsed >= self.election_timeout {
                    self.start_election(out);
                }
            }
        }
    }

    fn start_election(&mut self, out: &mut Vec<(NodeId, RaftMsg)>) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader = None;
        self.log
            .persist_term_vote(self.current_term, self.voted_for);
        self.votes.iter_mut().for_each(|v| *v = false);
        self.votes[self.id] = true; // vote for self
        self.reset_election_timeout();
        let (lli, llt) = (self.log.last_index(), self.log.last_term());
        for &p in &self.peers {
            if p != self.id {
                out.push((
                    p,
                    RaftMsg::RequestVote {
                        term: self.current_term,
                        candidate: self.id,
                        last_log_index: lli,
                        last_log_term: llt,
                    },
                ));
            }
        }
        // Single-node group: immediately a leader.
        self.maybe_become_leader(out);
    }

    fn maybe_become_leader(&mut self, out: &mut Vec<(NodeId, RaftMsg)>) {
        if self.role != Role::Candidate {
            return;
        }
        let count = self.votes.iter().filter(|&&v| v).count();
        if count >= self.majority() {
            self.role = Role::Leader;
            self.leader = Some(self.id);
            let last = self.log.last_index();
            for p in &self.peers {
                self.next_index[*p] = last + 1;
                self.match_index[*p] = 0;
            }
            self.match_index[self.id] = last;
            self.elapsed = self.heartbeat_period; // send heartbeats immediately
            self.broadcast_append(out);
        }
    }

    fn broadcast_append(&mut self, out: &mut Vec<(NodeId, RaftMsg)>) {
        for p in self.peers.clone() {
            if p != self.id {
                self.send_append(p, out);
            }
        }
    }

    fn send_append(&mut self, peer: NodeId, out: &mut Vec<(NodeId, RaftMsg)>) {
        let next = self.next_index[peer];
        let prev_log_index = next - 1;
        let prev_log_term = self.log.term_at(prev_log_index);
        let entries = self.log.entries_from(next);
        out.push((
            peer,
            RaftMsg::AppendEntries {
                term: self.current_term,
                leader: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        ));
    }

    /// Handle an incoming RPC. Emits any responses/RPCs into `out`.
    pub fn step(&mut self, from: NodeId, msg: RaftMsg, out: &mut Vec<(NodeId, RaftMsg)>) {
        // Any message with a newer term reverts us to a follower of that term.
        if msg_term(&msg) > self.current_term {
            self.become_follower(msg_term(&msg));
        }
        match msg {
            RaftMsg::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(term, candidate, last_log_index, last_log_term, out),
            RaftMsg::RequestVoteResp { term, granted } => {
                self.on_vote_resp(from, term, granted, out)
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append(
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                out,
            ),
            RaftMsg::AppendEntriesResp {
                term,
                success,
                match_index,
            } => self.on_append_resp(from, term, success, match_index, out),
        }
    }

    fn on_request_vote(
        &mut self,
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        out: &mut Vec<(NodeId, RaftMsg)>,
    ) {
        let mut granted = false;
        if term >= self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
            // Election restriction: candidate's log must be at least as up-to-date.
            let up_to_date = last_log_term > self.log.last_term()
                || (last_log_term == self.log.last_term()
                    && last_log_index >= self.log.last_index());
            if can_vote && up_to_date {
                granted = true;
                self.voted_for = Some(candidate);
                self.log
                    .persist_term_vote(self.current_term, self.voted_for);
                self.reset_election_timeout(); // granting a vote defers our own election
            }
        }
        out.push((
            candidate,
            RaftMsg::RequestVoteResp {
                term: self.current_term,
                granted,
            },
        ));
    }

    fn on_vote_resp(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        out: &mut Vec<(NodeId, RaftMsg)>,
    ) {
        if self.role != Role::Candidate || term != self.current_term {
            return;
        }
        if granted {
            self.votes[from] = true;
            self.maybe_become_leader(out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(
        &mut self,
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        out: &mut Vec<(NodeId, RaftMsg)>,
    ) {
        let mut success = false;
        let mut match_index = 0;
        if term >= self.current_term {
            // Valid leader for this term.
            if self.role != Role::Follower {
                self.role = Role::Follower;
            }
            self.leader = Some(leader);
            self.reset_election_timeout();
            // Log-matching: prev entry must agree.
            if prev_log_index == 0 || self.log.term_at(prev_log_index) == prev_log_term {
                success = true;
                // Append/overwrite entries, truncating on the first conflict.
                let mut idx = prev_log_index;
                for e in entries {
                    idx = e.index;
                    match self.log.term_at(e.index) {
                        t if t == e.term && self.log.last_index() >= e.index => {} // already have it
                        _ => {
                            self.log.truncate_from(e.index);
                            self.log.append(e);
                        }
                    }
                }
                match_index = idx.max(prev_log_index);
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(self.log.last_index());
                }
            }
        }
        out.push((
            leader,
            RaftMsg::AppendEntriesResp {
                term: self.current_term,
                success,
                match_index,
            },
        ));
    }

    fn on_append_resp(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        out: &mut Vec<(NodeId, RaftMsg)>,
    ) {
        if self.role != Role::Leader || term != self.current_term {
            return;
        }
        if success {
            self.match_index[from] = self.match_index[from].max(match_index);
            self.next_index[from] = self.match_index[from] + 1;
            self.advance_commit();
        } else {
            // Back off and retry.
            self.next_index[from] = self.next_index[from].saturating_sub(1).max(1);
            self.send_append(from, out);
        }
    }

    /// Advance `commit_index` to the highest index replicated on a majority whose
    /// entry is from the current term (Raft's commit rule).
    fn advance_commit(&mut self) {
        let last = self.log.last_index();
        for idx in (self.commit_index + 1..=last).rev() {
            if self.log.term_at(idx) != self.current_term {
                continue; // can only commit current-term entries directly
            }
            let replicas = self
                .peers
                .iter()
                .filter(|&&p| self.match_index[p] >= idx)
                .count();
            if replicas >= self.majority() {
                self.commit_index = idx;
                break;
            }
        }
    }

    /// Drain commands newly safe to apply (in `index` order).
    pub fn take_committed(&mut self) -> Vec<(u64, Vec<u8>)> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(cmd) = self.log.command_at(self.last_applied) {
                out.push((self.last_applied, cmd.to_vec()));
            }
        }
        out
    }
}

fn msg_term(m: &RaftMsg) -> u64 {
    match m {
        RaftMsg::RequestVote { term, .. }
        | RaftMsg::RequestVoteResp { term, .. }
        | RaftMsg::AppendEntries { term, .. }
        | RaftMsg::AppendEntriesResp { term, .. } => *term,
    }
}
