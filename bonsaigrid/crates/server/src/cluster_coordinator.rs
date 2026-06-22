//! Cluster coordination on the member thread: heartbeats, deadline-based failure
//! detection, master election, and (D2) join + migration scheduling.
//!
//! Runs inside the member transport `Handler`. It owns this member's `Cluster`
//! copy. `on_tick`/`on_heartbeat`/`on_view` mutate the cluster and push outgoing
//! messages into the transport outbox; each returns whether the local view
//! changed, so the member thread can forward a snapshot to the reactor.
//!
//! Peers are addressed by **member index** (position in `cluster.members`), which
//! the transport maps to a member port. Tombstones keep indices stable, so this
//! holds across deaths. Heartbeats carry the sender's `join_id` + `generation`.

use crate::membership::{Cluster, MemberInfo};
use member::wire::{MemberRec, Msg};
use std::collections::HashMap;

pub struct Coordinator {
    pub cluster: Cluster,
    self_join_id: u64,
    /// join_id -> last tick a heartbeat (or view) was seen from that member.
    last_seen: HashMap<u64, u64>,
    tick: u64,
    hb_interval: u64,
    hb_timeout: u64,
    last_hb_sent: u64,
}

impl Coordinator {
    pub fn new(cluster: Cluster, self_join_id: u64, hb_interval: u64, hb_timeout: u64) -> Coordinator {
        let mut last_seen = HashMap::new();
        // Seed all current peers as just-seen so the startup grace period holds.
        for m in &cluster.members {
            if m.join_id != self_join_id {
                last_seen.insert(m.join_id, 0);
            }
        }
        Coordinator { cluster, self_join_id, last_seen, tick: 0, hb_interval, hb_timeout, last_hb_sent: 0 }
    }

    fn alive_peer_indices(&self) -> Vec<usize> {
        let me = self.cluster.index_of_join(self.self_join_id);
        (0..self.cluster.len())
            .filter(|&i| self.cluster.alive[i] && Some(i) != me)
            .collect()
    }

    /// The current view (generation + full member records incl. tombstones) for
    /// forwarding to the reactor.
    pub fn view_recs(&self) -> (u64, Vec<MemberRec>) {
        (self.cluster.generation, self.recs())
    }

    /// All members (including tombstones, with their alive flag).
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

    fn broadcast_view(&self, outbox: &mut Vec<(usize, Msg)>) {
        let generation = self.cluster.generation;
        let recs = self.recs();
        for i in self.alive_peer_indices() {
            outbox.push((i, Msg::MemberView { generation, members: recs.clone() }));
        }
    }

    /// Record liveness from a peer; if its generation is newer we'll converge once
    /// its MemberView arrives.
    pub fn on_heartbeat(&mut self, from_join_id: u64, _generation: u64) {
        self.last_seen.insert(from_join_id, self.tick);
    }

    /// Apply a master's view if newer. Returns whether the local view changed.
    pub fn on_view(&mut self, generation: u64, members: Vec<MemberRec>) -> bool {
        let alive: Vec<bool> = members.iter().map(|m| m.alive).collect();
        let infos: Vec<MemberInfo> = members
            .into_iter()
            .map(|m| MemberInfo::new(m.uuid, m.host, m.client_port, m.member_port, m.join_id))
            .collect();
        let applied = self.cluster.set_view(generation, infos, alive);
        if applied {
            // Treat everyone in the new view as freshly seen.
            for m in &self.cluster.members {
                if m.join_id != self.self_join_id {
                    self.last_seen.insert(m.join_id, self.tick);
                }
            }
        }
        applied
    }

    /// Advance one tick: send heartbeats on schedule, detect deaths, and (if we are
    /// or become master) finalize membership. Returns whether the local view
    /// changed (so the caller forwards a snapshot to the reactor).
    pub fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> bool {
        self.tick += 1;

        if self.tick - self.last_hb_sent >= self.hb_interval {
            self.last_hb_sent = self.tick;
            let generation = self.cluster.generation;
            for i in self.alive_peer_indices() {
                outbox.push((i, Msg::Heartbeat { from_join_id: self.self_join_id, generation }));
            }
        }

        // Startup grace: don't declare anyone dead until a full timeout has passed.
        if self.tick <= self.hb_timeout {
            return false;
        }

        // Suspects: alive peers silent past the timeout.
        let suspects: Vec<(i64, i64)> = self
            .cluster
            .members
            .iter()
            .zip(&self.cluster.alive)
            .filter(|(m, &a)| a && m.join_id != self.self_join_id)
            .filter(|(m, _)| {
                let seen = self.last_seen.get(&m.join_id).copied().unwrap_or(0);
                self.tick - seen > self.hb_timeout
            })
            .map(|(m, _)| m.uuid)
            .collect();

        if suspects.is_empty() {
            return false;
        }

        // Only the (current or successor) master finalizes. If the master is among
        // the suspects, recompute the master *as if* the suspects were already gone.
        let i_am_master = self.master_after_removing(&suspects) == Some(self.self_join_id);
        if !i_am_master {
            return false;
        }

        let mut changed = false;
        for uuid in &suspects {
            changed |= self.cluster.remove_member_by_uuid(*uuid);
        }
        if changed {
            self.broadcast_view(outbox);
        }
        changed
    }

    /// The join_id that would be master if `dead` uuids were removed.
    fn master_after_removing(&self, dead: &[(i64, i64)]) -> Option<u64> {
        self.cluster
            .members
            .iter()
            .zip(&self.cluster.alive)
            .filter(|(m, &a)| a && !dead.contains(&m.uuid))
            .min_by_key(|(m, _)| m.join_id)
            .map(|(m, _)| m.join_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(i: u64) -> MemberInfo {
        MemberInfo::new((1, i as i64 + 1), "127.0.0.1".into(), 5701 + i as i32, 7701 + i as i32, i)
    }

    #[test]
    fn next_oldest_elects_itself_when_master_dies() {
        // We are member 1 (join_id 1); master is member 0. Member 2 stays alive.
        let cluster = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let mut co = Coordinator::new(cluster, 1, 5, 20); // interval 5, timeout 20 ticks
        let mut outbox = Vec::new();

        // For 60 ticks, keep hearing from member 2 (join_id 2) but never from 0.
        let mut changed = false;
        for t in 1..=60 {
            if t % 5 == 0 {
                co.on_heartbeat(2, 1);
            }
            changed |= co.on_tick(&mut outbox);
        }

        assert!(changed, "member 1 should have finalized member 0's death");
        assert_eq!(co.cluster.index_of_uuid((1, 1)).map(|i| co.cluster.alive[i]), Some(false));
        assert_eq!(co.cluster.master().map(|i| co.cluster.members[i].join_id), Some(1));
        assert!(co.cluster.generation > 1);
        // It broadcast a MemberView to the surviving peer (member 2).
        assert!(outbox.iter().any(|(_, msg)| matches!(msg, Msg::MemberView { .. })));
    }

    #[test]
    fn no_false_death_while_heartbeats_flow() {
        let cluster = Cluster::new(vec![m(0), m(1), m(2)], 1, 1);
        let mut co = Coordinator::new(cluster, 1, 5, 20);
        let mut outbox = Vec::new();
        for t in 1..=100 {
            if t % 3 == 0 {
                co.on_heartbeat(0, 1);
                co.on_heartbeat(2, 1);
            }
            assert!(!co.on_tick(&mut outbox), "no death while everyone heartbeats");
        }
        assert_eq!(co.cluster.live_count(), 3);
    }
}
