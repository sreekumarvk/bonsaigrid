//! Dynamic cluster membership, master election, and ring-wise replica assignment.
//!
//! The member list is **join-ordered** (each member has a monotonic `join_id`;
//! lower = older). The **master** is the alive member with the smallest join_id.
//! A monotonic `generation` stamps every change; a view with a lower generation
//! is ignored (stale).
//!
//! Ownership uses ring-walk over the join-ordered list with dead members kept as
//! tombstones (`alive=false`): `owner(p)` walks from `p % total` to the first
//! alive member, so a dead owner's partitions fall through to its backup — which
//! holds the data from synchronous replication. Tombstones keep live indices
//! stable, so a death does NOT reshuffle ownership (only joins do, which migrate).

use codecs::auth::MemberTuple;

const PARTITION_COUNT: i32 = crate::handlers::PARTITION_COUNT;
const VERSION: (u8, u8, u8) = crate::handlers::VERSION;

#[derive(Clone, Debug)]
pub struct MemberInfo {
    pub uuid: (i64, i64),
    pub host: String,
    pub client_port: i32,
    pub member_port: i32,
    pub join_id: u64,
}

impl MemberInfo {
    pub fn new(uuid: (i64, i64), host: String, client_port: i32, member_port: i32, join_id: u64) -> MemberInfo {
        MemberInfo { uuid, host, client_port, member_port, join_id }
    }
}

#[derive(Clone, Debug)]
pub struct Cluster {
    /// Join-ordered (by join_id); dead members kept as tombstones for stable
    /// ring indices.
    pub members: Vec<MemberInfo>,
    pub alive: Vec<bool>,
    pub backups: usize,
    pub quorum: usize,
    pub generation: u64,
    pub member_list_version: i32,
    pub partition_list_version: i32,
}

impl Cluster {
    pub fn new(members: Vec<MemberInfo>, backups: usize, quorum: usize) -> Cluster {
        let n = members.len();
        Cluster {
            members,
            alive: vec![true; n],
            backups,
            quorum: quorum.max(1),
            generation: 1,
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
    pub fn has_quorum(&self) -> bool {
        self.live_count() >= self.quorum.max(1)
    }

    /// Index of the alive member with the smallest join_id (the master), if any.
    pub fn master(&self) -> Option<usize> {
        self.members
            .iter()
            .enumerate()
            .filter(|(i, _)| self.alive[*i])
            .min_by_key(|(_, m)| m.join_id)
            .map(|(i, _)| i)
    }

    /// True if the member with `join_id` is the current master.
    pub fn is_master(&self, join_id: u64) -> bool {
        self.master().map(|i| self.members[i].join_id) == Some(join_id)
    }

    pub fn index_of_join(&self, join_id: u64) -> Option<usize> {
        self.members.iter().position(|m| m.join_id == join_id)
    }
    pub fn index_of_uuid(&self, uuid: (i64, i64)) -> Option<usize> {
        self.members.iter().position(|m| m.uuid == uuid)
    }
    pub fn max_join_id(&self) -> u64 {
        self.members.iter().map(|m| m.join_id).max().unwrap_or(0)
    }

    /// Owner of `partition`: first alive member walking the ring from `p % total`.
    pub fn owner(&self, partition: i32) -> usize {
        let n = self.len();
        let home = (partition.rem_euclid(n as i32)) as usize;
        for j in 0..n {
            let i = (home + j) % n;
            if self.alive[i] {
                return i;
            }
        }
        home
    }

    /// Alive backups for `partition`: the next `min(backups, live-1)` alive members
    /// after the owner.
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

    /// Mark a member dead by uuid (tombstone) and bump generation. Idempotent.
    pub fn remove_member_by_uuid(&mut self, uuid: (i64, i64)) -> bool {
        match self.index_of_uuid(uuid) {
            Some(i) if self.alive[i] => {
                self.alive[i] = false;
                self.bump();
                true
            }
            _ => false,
        }
    }

    /// Manual promote (Phase C endpoint): mark the bootstrap member with this
    /// join_id dead.
    pub fn promote(&mut self, join_id: u64) {
        if let Some(i) = self.index_of_join(join_id) {
            if self.alive[i] {
                self.alive[i] = false;
                self.bump();
            }
        }
    }

    /// Add (or revive) a member. Returns the assigned join_id.
    pub fn add_member(&mut self, mut info: MemberInfo) -> u64 {
        if let Some(i) = self.index_of_uuid(info.uuid) {
            self.alive[i] = true; // revive a rejoining member
            self.bump();
            return self.members[i].join_id;
        }
        if info.join_id == 0 || self.index_of_join(info.join_id).is_some() {
            info.join_id = self.max_join_id() + 1;
        }
        let jid = info.join_id;
        self.members.push(info);
        self.alive.push(true);
        self.bump();
        jid
    }

    /// Apply a master's published view (members **with** alive flags / tombstones)
    /// if it is newer. Preserving tombstones is essential: ring ownership must
    /// shift a dead member's partitions to its backup, not reshuffle by modulo.
    pub fn set_view(&mut self, generation: u64, members: Vec<MemberInfo>, alive: Vec<bool>) -> bool {
        if generation <= self.generation {
            return false;
        }
        self.members = members;
        self.alive = alive;
        self.generation = generation;
        self.member_list_version += 1;
        self.partition_list_version += 1;
        true
    }

    /// Convenience for tests: apply a view of all-alive members.
    pub fn apply_view(&mut self, generation: u64, members: Vec<MemberInfo>) -> bool {
        let n = members.len();
        self.set_view(generation, members, vec![true; n])
    }

    fn bump(&mut self) {
        self.generation += 1;
        self.member_list_version += 1;
        self.partition_list_version += 1;
    }

    /// Alive members as wire tuples (uuid, host, client_port, lite=false, version).
    pub fn member_tuples(&self) -> Vec<MemberTuple> {
        self.members
            .iter()
            .zip(&self.alive)
            .filter(|(_, &a)| a)
            .map(|(m, _)| (m.uuid, m.host.clone(), m.client_port, false, VERSION))
            .collect()
    }

    /// Per-alive-member partition lists: partition `p` listed under `owner(p)`.
    pub fn partition_table(&self) -> Vec<((i64, i64), Vec<i32>)> {
        let mut by_member: Vec<((i64, i64), Vec<i32>)> = self
            .members
            .iter()
            .zip(&self.alive)
            .filter(|(_, &a)| a)
            .map(|(m, _)| (m.uuid, Vec::new()))
            .collect();
        for p in 0..PARTITION_COUNT {
            let owner_uuid = self.members[self.owner(p)].uuid;
            if let Some(e) = by_member.iter_mut().find(|(u, _)| *u == owner_uuid) {
                e.1.push(p);
            }
        }
        by_member
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(i: u64) -> MemberInfo {
        MemberInfo::new((1, i as i64 + 1), "127.0.0.1".into(), 5701 + i as i32, 7701 + i as i32, i)
    }

    #[test]
    fn master_is_oldest_alive() {
        let mut c = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        assert_eq!(c.master(), Some(0));
        assert!(c.is_master(0));
        // owner 0 backed by 1; kill 0 -> owner falls through to backup 1, master->1
        c.remove_member_by_uuid((1, 1));
        assert_eq!(c.owner(0), 1);
        assert_eq!(c.master(), Some(1));
        assert!(c.is_master(1));
        assert_eq!(c.member_tuples().len(), 2);
    }

    #[test]
    fn apply_view_generation_guard() {
        let mut c = Cluster::new(vec![m(0), m(1)], 1, 1);
        let g0 = c.generation;
        assert!(!c.apply_view(g0, vec![m(0)])); // not newer -> ignored
        assert!(c.apply_view(g0 + 5, vec![m(0), m(1), m(2)])); // newer -> applied
        assert_eq!(c.len(), 3);
        assert_eq!(c.generation, g0 + 5);
    }

    #[test]
    fn add_member_appends_join_id() {
        let mut c = Cluster::new(vec![m(0), m(1)], 1, 1);
        let jid = c.add_member(MemberInfo::new((1, 99), "127.0.0.1".into(), 5799, 7799, 0));
        assert_eq!(jid, 2); // max(0,1)+1
        assert_eq!(c.master(), Some(0));
    }

    #[test]
    fn quorum_gate() {
        let mut c = Cluster::new(vec![m(0), m(1), m(2)], 1, 2);
        assert!(c.has_quorum());
        c.remove_member_by_uuid((1, 1));
        assert!(c.has_quorum()); // 2 live == quorum
        c.remove_member_by_uuid((1, 2));
        assert!(!c.has_quorum()); // 1 live < 2
    }

    #[test]
    fn partition_table_covers_all_partitions() {
        let c = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let total: usize = c.partition_table().iter().map(|(_, p)| p.len()).sum();
        assert_eq!(total, PARTITION_COUNT as usize);
    }
}
