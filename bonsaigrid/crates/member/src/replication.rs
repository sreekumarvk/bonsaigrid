//! Synchronous-backup bookkeeping on the member thread.
//!
//! `Pending` tracks, per `op_id`, how many backup acks remain before the primary
//! may deliver the deferred client response. When the count reaches zero the
//! response bytes + the client connection id are handed back for delivery.
//!
//! `apply` is the backup side: it writes an inbound `BackupPut`/`BackupRemove`
//! into the local store. Backups never re-replicate.

use crate::wire::Msg;
use std::collections::HashMap;
use store::Store;

struct PendingOp {
    remaining: u32,
    conn_id: u64,
    response: Vec<u8>,
    /// Poll ticks elapsed since registration; used for the ack-timeout sweep.
    age: u32,
}

#[derive(Default)]
pub struct Pending {
    ops: HashMap<u64, PendingOp>,
}

impl Pending {
    pub fn new() -> Pending {
        Pending::default()
    }

    /// Register a write awaiting `remaining` acks. If `remaining == 0` (no live
    /// backups) the response is returned immediately for delivery and nothing is
    /// stored.
    pub fn register(
        &mut self,
        op_id: u64,
        remaining: u32,
        conn_id: u64,
        response: Vec<u8>,
    ) -> Option<(u64, Vec<u8>)> {
        if remaining == 0 {
            return Some((conn_id, response));
        }
        self.ops.insert(op_id, PendingOp { remaining, conn_id, response, age: 0 });
        None
    }

    /// Record one backup ack for `op_id`. When the last ack arrives, the op is
    /// removed and its `(conn_id, response)` returned for delivery.
    pub fn ack(&mut self, op_id: u64) -> Option<(u64, Vec<u8>)> {
        let done = match self.ops.get_mut(&op_id) {
            Some(op) => {
                op.remaining = op.remaining.saturating_sub(1);
                op.remaining == 0
            }
            None => return None,
        };
        if done {
            let op = self.ops.remove(&op_id).unwrap();
            Some((op.conn_id, op.response))
        } else {
            None
        }
    }

    /// Advance the age of every pending op by one poll tick and force-complete any
    /// that have exceeded `max_age` ticks (a dead/slow backup must not wedge the
    /// primary forever). Returns the responses to deliver anyway.
    pub fn sweep_expired(&mut self, max_age: u32) -> Vec<(u64, Vec<u8>)> {
        let mut expired = Vec::new();
        for op in self.ops.values_mut() {
            op.age += 1;
        }
        let ids: Vec<u64> =
            self.ops.iter().filter(|(_, op)| op.age >= max_age).map(|(&id, _)| id).collect();
        for id in ids {
            let op = self.ops.remove(&id).unwrap();
            expired.push((op.conn_id, op.response));
        }
        expired
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// Apply an inbound backup mutation to the local store (backup side).
pub fn apply(store: &Store, msg: &Msg) {
    match msg {
        Msg::BackupPut { name, key, value, ttl_ms, .. } => {
            store.put_ttl(name, key.clone(), value.clone(), *ttl_ms);
        }
        Msg::BackupRemove { name, key, .. } => {
            store.remove(name, key);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acks_complete_once() {
        let mut p = Pending::new();
        assert!(p.register(1, 2, 42, vec![9]).is_none()); // needs 2 acks
        assert!(p.ack(1).is_none()); // 1 of 2
        assert_eq!(p.ack(1), Some((42, vec![9]))); // 2 of 2 -> deliver
        assert!(p.ack(1).is_none()); // already delivered
        assert!(p.is_empty());
        // 0 backups -> immediate delivery, nothing stored
        assert_eq!(p.register(2, 0, 7, vec![1]), Some((7, vec![1])));
        assert!(p.is_empty());
    }

    #[test]
    fn sweep_force_completes_after_timeout() {
        let mut p = Pending::new();
        p.register(5, 1, 99, vec![3]);
        assert!(p.sweep_expired(3).is_empty()); // age 1
        assert!(p.sweep_expired(3).is_empty()); // age 2
        assert_eq!(p.sweep_expired(3), vec![(99, vec![3])]); // age 3 -> expire
        assert!(p.is_empty());
    }

    #[test]
    fn apply_writes_and_removes() {
        let s = Store::new();
        apply(&s, &Msg::BackupPut { op_id: 1, name: "m".into(), key: b"k".to_vec(), value: b"v".to_vec(), ttl_ms: 0 });
        assert_eq!(s.get("m", b"k"), Some(b"v".to_vec()));
        apply(&s, &Msg::BackupRemove { op_id: 2, name: "m".into(), key: b"k".to_vec() });
        assert_eq!(s.get("m", b"k"), None);
    }
}
