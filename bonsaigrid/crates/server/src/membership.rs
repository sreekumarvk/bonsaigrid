//! Dynamic cluster membership + ring-wise replica assignment.
//!
//! Replaces the static partition table that `Cfg` used to compute. The reactor
//! thread owns the authoritative `Cluster`; promotion mutates it and reconnecting
//! clients read the updated member list + partition table from `auth`/cluster-view.
//!
//! Assignment is ring-wise: the *home* of partition `p` is `p % N`. `owner(p)`
//! walks the ring from the home to the first **alive** member, so a dead home
//! falls through to its backup. `backups_of(p)` are the next alive members after
//! the owner. This means `promote(dead)` needs no explicit reassignment — marking
//! a member dead automatically advances every affected partition to its backup.

use crate::handlers::{Member, PARTITION_COUNT, VERSION};
use codecs::auth::MemberTuple;

#[derive(Clone, Debug)]
pub struct Cluster {
    pub members: Vec<Member>,
    pub alive: Vec<bool>,
    /// Synchronous backup count K (already capped at N-1 by the caller).
    pub backups: usize,
    pub member_list_version: i32,
    pub partition_list_version: i32,
}

impl Cluster {
    pub fn new(members: Vec<Member>, backups: usize) -> Cluster {
        let n = members.len();
        Cluster {
            members,
            alive: vec![true; n],
            backups,
            member_list_version: 1,
            partition_list_version: 1,
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
    pub fn live_count(&self) -> usize {
        self.alive.iter().filter(|&&a| a).count()
    }

    /// The member index that clients are told owns `partition`: the first alive
    /// member walking the ring from the partition's home (`p % N`).
    pub fn owner(&self, partition: i32) -> usize {
        let n = self.len();
        let home = (partition.rem_euclid(n as i32)) as usize;
        for j in 0..n {
            let i = (home + j) % n;
            if self.alive[i] {
                return i;
            }
        }
        home // no live member (degenerate); caller handles empty cluster
    }

    /// The alive backup member indices for `partition`: the next
    /// `min(backups, live_count-1)` alive members after the owner.
    pub fn backups_of(&self, partition: i32) -> Vec<usize> {
        let n = self.len();
        let owner = self.owner(partition);
        let want = self.backups.min(self.live_count().saturating_sub(1));
        let mut out = Vec::with_capacity(want);
        let mut j = 1;
        while out.len() < want && j < n {
            let i = (owner + j) % n;
            if self.alive[i] {
                out.push(i);
            }
            j += 1;
        }
        out
    }

    /// Mark `dead` as down and bump both view versions. Idempotent.
    pub fn promote(&mut self, dead: usize) {
        if dead < self.alive.len() && self.alive[dead] {
            self.alive[dead] = false;
            self.member_list_version += 1;
            self.partition_list_version += 1;
        }
    }

    /// Alive members as wire tuples (uuid, host, port, lite=false, version).
    pub fn member_tuples(&self) -> Vec<MemberTuple> {
        self.members
            .iter()
            .zip(&self.alive)
            .filter(|(_, &a)| a)
            .map(|(m, _)| (m.uuid, m.host.clone(), m.port, false, VERSION))
            .collect()
    }

    /// Per-alive-member partition lists: partition `p` is listed under `owner(p)`.
    pub fn partition_table(&self) -> Vec<((i64, i64), Vec<i32>)> {
        let mut by_member: Vec<((i64, i64), Vec<i32>)> = self
            .members
            .iter()
            .zip(&self.alive)
            .filter(|(_, &a)| a)
            .map(|(m, _)| (m.uuid, Vec::new()))
            .collect();
        // index alive members by their position in `members`
        for p in 0..PARTITION_COUNT {
            let owner_uuid = self.members[self.owner(p)].uuid;
            if let Some(entry) = by_member.iter_mut().find(|(u, _)| *u == owner_uuid) {
                entry.1.push(p);
            }
        }
        by_member
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(i: i32) -> Member {
        Member { uuid: (i as i64, i as i64), host: "127.0.0.1".into(), port: 5701 + i }
    }

    #[test]
    fn assignment_and_promote() {
        let mut c = Cluster::new(vec![m(0), m(1), m(2)], 1);
        assert_eq!(c.owner(0), 0);
        assert_eq!(c.backups_of(0), vec![1]);
        assert_eq!(c.owner(1), 1);
        assert_eq!(c.backups_of(1), vec![2]);
        assert_eq!(c.owner(2), 2);
        assert_eq!(c.backups_of(2), vec![0]);

        // promote(0): partition-0 owner falls through to member 1 (its backup).
        c.promote(0);
        assert!(!c.alive[0]);
        assert_eq!(c.owner(0), 1);
        assert_eq!(c.member_tuples().len(), 2);
        assert!(c.partition_table().iter().all(|(u, _)| *u != (0, 0)));
        assert!(c.member_list_version >= 2 && c.partition_list_version >= 2);

        // every partition still owned by an alive member after promotion
        let total: usize = c.partition_table().iter().map(|(_, ps)| ps.len()).sum();
        assert_eq!(total, PARTITION_COUNT as usize);
    }

    #[test]
    fn promote_is_idempotent() {
        let mut c = Cluster::new(vec![m(0), m(1)], 1);
        c.promote(0);
        let v = c.member_list_version;
        c.promote(0);
        assert_eq!(c.member_list_version, v); // no further bump
    }

    #[test]
    fn backups_capped_by_live_count() {
        let c = Cluster::new(vec![m(0), m(1)], 1);
        assert_eq!(c.backups_of(0), vec![1]);
        let single = Cluster::new(vec![m(0)], 1);
        assert!(single.backups_of(0).is_empty()); // nobody to back up to
    }
}
