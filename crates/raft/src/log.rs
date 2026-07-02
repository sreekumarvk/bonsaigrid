//! The Raft log: an ordered sequence of entries (1-based index) plus the
//! persistent `current_term`/`voted_for`. This is the in-memory implementation
//! used by the consensus core and the deterministic simulation; a WAL-backed
//! durable variant layers on top (Phase A3) by persisting on append / term-vote.

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

/// In-memory Raft log. `entries[i]` has index `i + 1`.
#[derive(Default)]
pub struct RaftLog {
    entries: Vec<Entry>,
    term: u64,
    vote: Option<usize>,
}

impl RaftLog {
    pub fn new() -> RaftLog {
        RaftLog::default()
    }

    /// Index of the last entry (0 if empty).
    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    /// Term of the last entry (0 if empty).
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Term of the entry at `index` (0 if index is 0 or beyond the log).
    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.entries
            .get((index - 1) as usize)
            .map(|e| e.term)
            .unwrap_or(0)
    }

    /// The command at `index`, if present.
    pub fn command_at(&self, index: u64) -> Option<&[u8]> {
        if index == 0 {
            return None;
        }
        self.entries
            .get((index - 1) as usize)
            .map(|e| e.command.as_slice())
    }

    /// All entries with index >= `from`.
    pub fn entries_from(&self, from: u64) -> Vec<Entry> {
        if from == 0 {
            return self.entries.clone();
        }
        self.entries
            .iter()
            .skip((from - 1) as usize)
            .cloned()
            .collect()
    }

    /// Append an entry (must be the next index).
    pub fn append(&mut self, e: Entry) {
        debug_assert_eq!(
            e.index,
            self.last_index() + 1,
            "log append must be contiguous"
        );
        self.entries.push(e);
    }

    /// Drop all entries with index >= `index` (conflict truncation).
    pub fn truncate_from(&mut self, index: u64) {
        if index == 0 {
            self.entries.clear();
        } else {
            self.entries.truncate((index - 1) as usize);
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // ---- persistent term/vote ----

    pub fn persist_term_vote(&mut self, term: u64, vote: Option<usize>) {
        self.term = term;
        self.vote = vote;
    }
    pub fn persisted_term(&self) -> u64 {
        self.term
    }
    pub fn persisted_vote(&self) -> Option<usize> {
        self.vote
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(term: u64, index: u64, c: &[u8]) -> Entry {
        Entry {
            term,
            index,
            command: c.to_vec(),
        }
    }

    #[test]
    fn append_index_term_lookup() {
        let mut l = RaftLog::new();
        assert_eq!(l.last_index(), 0);
        assert_eq!(l.term_at(0), 0);
        l.append(e(1, 1, b"a"));
        l.append(e(1, 2, b"b"));
        l.append(e(2, 3, b"c"));
        assert_eq!(l.last_index(), 3);
        assert_eq!(l.last_term(), 2);
        assert_eq!(l.term_at(2), 1);
        assert_eq!(l.term_at(3), 2);
        assert_eq!(l.term_at(4), 0); // beyond the log
        assert_eq!(l.command_at(3), Some(b"c".as_slice()));
    }

    #[test]
    fn truncate_and_entries_from() {
        let mut l = RaftLog::new();
        for i in 1..=5 {
            l.append(e(1, i, b"x"));
        }
        assert_eq!(l.entries_from(3).len(), 3); // indices 3,4,5
        l.truncate_from(3); // drop 3,4,5
        assert_eq!(l.last_index(), 2);
        assert_eq!(l.entries_from(3).len(), 0);
    }

    #[test]
    fn persist_term_vote_roundtrip() {
        let mut l = RaftLog::new();
        l.persist_term_vote(7, Some(2));
        assert_eq!(l.persisted_term(), 7);
        assert_eq!(l.persisted_vote(), Some(2));
    }
}
