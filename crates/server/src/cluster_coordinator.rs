//! Cluster coordination on the member thread: heartbeats, deadline-based failure
//! detection, master election, dynamic join, and migration scheduling.
//!
//! Runs inside the member transport `Handler`. Owns this member's `Cluster` copy.
//! Self is identified by its **uuid** (stable across join-id assignment). Each
//! entry point mutates the cluster, pushes outgoing messages to the transport
//! outbox, and returns a [`Change`] telling the member thread whether the view
//! changed and which partitions this member must now migrate out.

use crate::membership::{Cluster, MemberInfo};
use crate::migration;
use member::wire::{MemberRec, Msg};
use std::collections::HashMap;

const PARTITION_COUNT: i32 = crate::handlers::PARTITION_COUNT;

/// Result of a coordination step.
#[derive(Default)]
pub struct Change {
    pub changed: bool,
    /// `(partition, dest_member_index)` this member must stream out.
    pub migrations: Vec<(i32, usize)>,
}

pub struct Coordinator {
    pub cluster: Cluster,
    self_uuid: (i64, i64),
    /// join_id -> last tick a heartbeat was seen.
    last_seen: HashMap<u64, u64>,
    tick: u64,
    hb_interval: u64,
    hb_timeout: u64,
    last_hb_sent: u64,
    /// Set on a joining member until the master admits it.
    pending_join: Option<MemberInfo>,
    joined: bool,
}

impl Coordinator {
    pub fn new(
        cluster: Cluster,
        self_uuid: (i64, i64),
        hb_interval: u64,
        hb_timeout: u64,
    ) -> Coordinator {
        // last_seen starts empty: a member is only eligible for death once we've
        // actually heard a heartbeat from it. Otherwise a freshly-joined member
        // (whom the others don't heartbeat yet) would falsely declare them dead.
        let last_seen = HashMap::new();
        Coordinator {
            cluster,
            self_uuid,
            last_seen,
            tick: 0,
            hb_interval,
            hb_timeout,
            last_hb_sent: 0,
            pending_join: None,
            joined: true,
        }
    }

    /// Mark this member as a joiner that must request admission from the master.
    pub fn set_pending_join(&mut self, info: MemberInfo) {
        self.pending_join = Some(info);
        self.joined = false;
    }

    fn self_join_id(&self) -> u64 {
        self.cluster
            .index_of_uuid(self.self_uuid)
            .map(|i| self.cluster.members[i].join_id)
            .unwrap_or(0)
    }

    fn alive_peer_indices(&self) -> Vec<usize> {
        (0..self.cluster.len())
            .filter(|&i| self.cluster.alive[i] && self.cluster.members[i].uuid != self.self_uuid)
            .collect()
    }

    fn recs(&self) -> Vec<MemberRec> {
        self.cluster
            .members
            .iter()
            .zip(&self.cluster.alive)
            .map(|(m, &alive)| MemberRec {
                uuid: m.uuid,
                host: m.host.clone(),
                client_port: m.client_port,
                member_port: m.member_port,
                join_id: m.join_id,
                alive,
            })
            .collect()
    }

    pub fn view_recs(&self) -> (u64, Vec<MemberRec>) {
        (self.cluster.generation, self.recs())
    }

    fn is_master(&self) -> bool {
        self.cluster.master().map(|i| self.cluster.members[i].uuid) == Some(self.self_uuid)
    }

    fn broadcast_view(&self, outbox: &mut Vec<(usize, Msg)>) {
        let generation = self.cluster.generation;
        let recs = self.recs();
        for i in self.alive_peer_indices() {
            outbox.push((
                i,
                Msg::MemberView {
                    generation,
                    members: recs.clone(),
                },
            ));
        }
    }

    /// Migrations this member must send after a change from `old` to the current
    /// cluster (covers join, death-rebalance, and restore-K).
    fn outgoing(&self, old: &Cluster) -> Vec<(i32, usize)> {
        migration::plan(old, &self.cluster, PARTITION_COUNT, self.self_uuid)
    }

    pub fn on_heartbeat(&mut self, from_join_id: u64, _generation: u64) {
        self.last_seen.insert(from_join_id, self.tick);
    }

    /// Master handling a JoinRequest: admit the member, broadcast the new view,
    /// and plan the migrations the join requires.
    pub fn on_join(&mut self, info: MemberInfo, outbox: &mut Vec<(usize, Msg)>) -> Change {
        if !self.is_master() {
            return Change::default(); // only the master admits members
        }
        let old = self.cluster.clone();
        let added = self.cluster.add_member(info).is_some();
        // Always (re)broadcast the current view so a joiner recovers a lost view by
        // re-requesting; only an actual admission plans migrations.
        self.broadcast_view(outbox);
        if added {
            Change {
                changed: true,
                migrations: self.outgoing(&old),
            }
        } else {
            Change::default()
        }
    }

    /// Apply a master's view if newer. Non-masters adopt it (no re-broadcast).
    pub fn on_view(&mut self, generation: u64, members: Vec<MemberRec>) -> Change {
        let old = self.cluster.clone();
        let alive: Vec<bool> = members.iter().map(|m| m.alive).collect();
        let infos: Vec<MemberInfo> = members
            .into_iter()
            .map(|m| MemberInfo::new(m.uuid, m.host, m.client_port, m.member_port, m.join_id))
            .collect();
        if !self.cluster.set_view(generation, infos, alive) {
            return Change::default();
        }
        for m in &self.cluster.members {
            if m.uuid != self.self_uuid {
                self.last_seen.insert(m.join_id, self.tick);
            }
        }
        if self.cluster.index_of_uuid(self.self_uuid).is_some() {
            self.joined = true;
            self.pending_join = None;
        }
        Change {
            changed: true,
            migrations: self.outgoing(&old),
        }
    }

    pub fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> Change {
        self.tick += 1;

        if self.tick - self.last_hb_sent >= self.hb_interval {
            self.last_hb_sent = self.tick;
            let generation = self.cluster.generation;
            let from = self.self_join_id();
            for i in self.alive_peer_indices() {
                outbox.push((
                    i,
                    Msg::Heartbeat {
                        from_join_id: from,
                        generation,
                    },
                ));
            }
            // A joiner re-asks the master to admit it until it appears in a view.
            if !self.joined {
                if let (Some(info), Some(mi)) = (self.pending_join.clone(), self.cluster.master()) {
                    outbox.push((
                        mi,
                        Msg::JoinRequest {
                            uuid: info.uuid,
                            host: info.host,
                            client_port: info.client_port,
                            member_port: info.member_port,
                        },
                    ));
                }
            }
        }

        if self.tick <= self.hb_timeout {
            return Change::default();
        }

        let suspects: Vec<(i64, i64)> = self
            .cluster
            .members
            .iter()
            .zip(&self.cluster.alive)
            .filter(|(m, &a)| a && m.uuid != self.self_uuid)
            // Only a member we've actually heard from can be declared dead.
            .filter(|(m, _)| matches!(self.last_seen.get(&m.join_id), Some(&seen) if self.tick - seen > self.hb_timeout))
            .map(|(m, _)| m.uuid)
            .collect();

        if suspects.is_empty() {
            return Change::default();
        }
        if self.master_after_removing(&suspects) != Some(self.self_uuid) {
            return Change::default();
        }
        let old = self.cluster.clone();
        let mut changed = false;
        for uuid in &suspects {
            changed |= self.cluster.remove_member_by_uuid(*uuid);
        }
        if !changed {
            return Change::default();
        }
        self.broadcast_view(outbox);
        // Restore-K: re-replicate so every partition again has its backups (the
        // generalized plan emits owner→fresh-backup sends after a death).
        Change {
            changed: true,
            migrations: self.outgoing(&old),
        }
    }

    /// The uuid that would be master if `dead` were removed.
    fn master_after_removing(&self, dead: &[(i64, i64)]) -> Option<(i64, i64)> {
        self.cluster
            .members
            .iter()
            .zip(&self.cluster.alive)
            .filter(|(m, &a)| a && !dead.contains(&m.uuid))
            .min_by_key(|(m, _)| m.join_id)
            .map(|(m, _)| m.uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(i: u64) -> MemberInfo {
        MemberInfo::new(
            (1, i as i64 + 1),
            "127.0.0.1".into(),
            5701 + i as i32,
            7701 + i as i32,
            i,
        )
    }

    #[test]
    fn next_oldest_elects_itself_when_master_dies() {
        // We are member 1 (uuid (1,2)); master is member 0. Member 2 stays alive.
        let cluster = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let mut co = Coordinator::new(cluster, (1, 2), 5, 20);
        let mut outbox = Vec::new();
        let mut changed = false;
        for t in 1..=60 {
            // Hear from member 0 only early on (it "dies" at t=15); keep hearing
            // from member 2 throughout.
            if t % 5 == 0 {
                co.on_heartbeat(2, 1);
                if t <= 15 {
                    co.on_heartbeat(0, 1);
                }
            }
            changed |= co.on_tick(&mut outbox).changed;
        }
        assert!(changed, "member 1 should finalize member 0's death");
        assert_eq!(
            co.cluster
                .index_of_uuid((1, 1))
                .map(|i| co.cluster.alive[i]),
            Some(false)
        );
        assert_eq!(
            co.cluster.master().map(|i| co.cluster.members[i].uuid),
            Some((1, 2))
        );
        assert!(outbox
            .iter()
            .any(|(_, msg)| matches!(msg, Msg::MemberView { .. })));
    }

    #[test]
    fn no_false_death_while_heartbeats_flow() {
        let cluster = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let mut co = Coordinator::new(cluster, (1, 2), 5, 20);
        let mut outbox = Vec::new();
        for t in 1..=100 {
            if t % 3 == 0 {
                co.on_heartbeat(0, 1);
                co.on_heartbeat(2, 1);
            }
            assert!(!co.on_tick(&mut outbox).changed);
        }
        assert_eq!(co.cluster.live_count(), 3);
    }

    #[test]
    fn master_admits_join_and_plans_migration() {
        // Master is member 0; a 2-member cluster admits member 2.
        let cluster = Cluster::new(vec![m(0), m(1)], 1, 1);
        let mut co = Coordinator::new(cluster, (1, 1), 5, 20);
        let mut outbox = Vec::new();
        let ch = co.on_join(m(2), &mut outbox);
        assert!(ch.changed);
        assert_eq!(co.cluster.len(), 3);
        assert!(outbox
            .iter()
            .any(|(_, msg)| matches!(msg, Msg::MemberView { .. })));
        // Member 0 owned partitions that now move; some go to the new member.
        assert!(
            !ch.migrations.is_empty(),
            "join must plan migrations from the master"
        );
    }
}
