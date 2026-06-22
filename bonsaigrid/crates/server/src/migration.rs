//! Partition migration planning + merge policy.
//!
//! On a membership change the master recomputes ownership; every partition whose
//! owner moved is streamed from its old owner to its new owner. `outgoing`
//! computes the partitions THIS member must send. The actual streaming (reading
//! the store, batching `MigrateChunk`s) lives in the member thread, which has the
//! store; the destination applies entries via `Store::put_merge` under the
//! configured `MergePolicy`.

use crate::membership::Cluster;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergePolicy {
    /// Higher per-entry stamp wins (Hazelcast LatestUpdateMergePolicy).
    LatestUpdate,
    /// Keep the existing entry if present (Hazelcast PutIfAbsentMergePolicy).
    PutIfAbsent,
}

impl MergePolicy {
    pub fn from_str(s: &str) -> MergePolicy {
        match s.to_ascii_lowercase().as_str() {
            "putifabsent" => MergePolicy::PutIfAbsent,
            _ => MergePolicy::LatestUpdate,
        }
    }
    /// Whether `Store::put_merge` should use LatestUpdate semantics.
    pub fn latest_update(&self) -> bool {
        matches!(self, MergePolicy::LatestUpdate)
    }
}

/// Partitions `self_uuid` must send because ownership moved away from it between
/// `old` and `new`. Returns `(partition, dest_index_in_new)`.
pub fn outgoing(old: &Cluster, new: &Cluster, count: i32, self_uuid: (i64, i64)) -> Vec<(i32, usize)> {
    let mut out = Vec::new();
    if old.is_empty() || new.is_empty() {
        return out;
    }
    for p in 0..count {
        let old_owner = old.members[old.owner(p)].uuid;
        let new_idx = new.owner(p);
        let new_owner = new.members[new_idx].uuid;
        if old_owner == self_uuid && new_owner != self_uuid {
            out.push((p, new_idx));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::MemberInfo;

    fn m(i: u64) -> MemberInfo {
        MemberInfo::new((1, i as i64 + 1), "127.0.0.1".into(), 5701 + i as i32, 7701 + i as i32, i)
    }

    #[test]
    fn join_moves_some_partitions_to_the_newcomer() {
        let old = Cluster::new(vec![m(0), m(1)], 1, 1);
        let new = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        // Member 0 sends the partitions it owned that now belong elsewhere.
        let from0 = outgoing(&old, &new, 271, (1, 1));
        assert!(!from0.is_empty(), "growth must move some of member 0's partitions");
        // The new member (index 2, uuid (1,3)) must receive at least one partition.
        let to_new: Vec<_> = (0..3)
            .flat_map(|i| outgoing(&old, &new, 271, (1, i as i64 + 1)))
            .filter(|(_, dest)| new.members[*dest].uuid == (1, 3))
            .collect();
        assert!(!to_new.is_empty(), "the newcomer must gain partitions");
    }

    #[test]
    fn no_change_no_migration() {
        let c = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        assert!(outgoing(&c, &c, 271, (1, 1)).is_empty());
    }

    #[test]
    fn merge_policy_from_str() {
        assert_eq!(MergePolicy::from_str("PutIfAbsent"), MergePolicy::PutIfAbsent);
        assert_eq!(MergePolicy::from_str("LatestUpdate"), MergePolicy::LatestUpdate);
        assert_eq!(MergePolicy::from_str("garbage"), MergePolicy::LatestUpdate);
        assert!(MergePolicy::LatestUpdate.latest_update());
        assert!(!MergePolicy::PutIfAbsent.latest_update());
    }
}
