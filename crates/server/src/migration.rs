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
    pub fn parse(s: &str) -> MergePolicy {
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

/// Holders of a partition: owner first, then backups (by uuid).
fn holder_uuids(c: &Cluster, p: i32) -> Vec<(i64, i64)> {
    let mut v = vec![c.members[c.owner(p)].uuid];
    for b in c.backups_of(p) {
        v.push(c.members[b].uuid);
    }
    v
}

fn alive_in(c: &Cluster, uuid: (i64, i64)) -> bool {
    c.members.iter().zip(&c.alive).any(|(m, &a)| a && m.uuid == uuid)
}

/// Migrations `self_uuid` must send after a change from `old` to `new`. A
/// partition's holders = owner ∪ backups; whenever the holder set gains a member,
/// the **first old holder still alive in `new`** (a member that has the data)
/// sends the partition to each new holder that didn't already hold it. This single
/// rule covers join (data to the newcomer), death-rebalance (surviving backup →
/// new holders), and restore-K (owner → a fresh backup). Returns
/// `(partition, dest_index_in_new)`.
pub fn plan(old: &Cluster, new: &Cluster, count: i32, self_uuid: (i64, i64)) -> Vec<(i32, usize)> {
    let mut out = Vec::new();
    if old.is_empty() || new.is_empty() {
        return out;
    }
    for p in 0..count {
        let old_holders = holder_uuids(old, p);
        // The sole sender is the first old holder that survives into `new`.
        let Some(&sender) = old_holders.iter().find(|&&u| alive_in(new, u)) else { continue };
        if sender != self_uuid {
            continue;
        }
        let mut new_holders = vec![new.owner(p)];
        new_holders.extend(new.backups_of(p));
        for h in new_holders {
            let h_uuid = new.members[h].uuid;
            if h_uuid != self_uuid && !old_holders.contains(&h_uuid) {
                out.push((p, h)); // a new holder that didn't have the data
            }
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
        // Across all senders, the new member (uuid (1,3)) must gain partitions.
        let to_new: Vec<_> = (0..3)
            .flat_map(|i| plan(&old, &new, 271, (1, i as i64 + 1)))
            .filter(|(_, dest)| new.members[*dest].uuid == (1, 3))
            .collect();
        assert!(!to_new.is_empty(), "the newcomer must gain partitions");
    }

    #[test]
    fn death_restores_k_to_a_fresh_backup() {
        // 3 members, K=1. Member 0 dies (tombstone). For partitions member 0 owned
        // (backed by member 1), member 1 becomes owner and must re-replicate to a
        // fresh backup (member 2) to restore K.
        let old = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let mut new = old.clone();
        new.remove_member_by_uuid((1, 1)); // member 0 dead
        // Member 1 (the surviving backup → new owner) sends to member 2 (fresh backup).
        let from1 = plan(&old, &new, 271, (1, 2));
        assert!(!from1.is_empty(), "restore-K must re-replicate to a fresh backup");
        assert!(from1.iter().all(|(_, dest)| new.members[*dest].uuid == (1, 3)));
    }

    #[test]
    fn no_change_no_migration() {
        let c = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        assert!(plan(&c, &c, 271, (1, 1)).is_empty());
    }

    #[test]
    fn merge_policy_from_str() {
        assert_eq!(MergePolicy::parse("PutIfAbsent"), MergePolicy::PutIfAbsent);
        assert_eq!(MergePolicy::parse("LatestUpdate"), MergePolicy::LatestUpdate);
        assert_eq!(MergePolicy::parse("garbage"), MergePolicy::LatestUpdate);
        assert!(MergePolicy::LatestUpdate.latest_update());
        assert!(!MergePolicy::PutIfAbsent.latest_update());
    }
}
