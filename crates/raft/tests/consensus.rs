//! Core Raft safety + liveness under the deterministic simulation harness:
//! single leader, replication + commit, re-election after a crash, minority
//! cannot commit, and committed logs never diverge.

mod common;
use common::{assert_no_two_leaders_same_term, Sim};

#[test]
fn elects_a_single_leader() {
    let mut sim = Sim::new(3);
    sim.run(80);
    assert_no_two_leaders_same_term(&sim);
    assert!(sim.leader().is_some(), "a leader must be elected");
}

#[test]
fn single_node_elects_and_commits_immediately() {
    // A lone node is its own majority: it must elect itself and commit proposals
    // with no peer acknowledgements.
    let mut sim = Sim::new(1);
    sim.run(60);
    assert!(sim.leader() == Some(0), "the sole node leads");
    assert!(sim.propose(b"x=1"), "leader accepts a proposal");
    sim.run(20);
    let cmds: Vec<Vec<u8>> = sim.applied[0].iter().map(|(_, c)| c.clone()).collect();
    assert!(
        cmds.contains(&b"x=1".to_vec()),
        "single node commits + applies"
    );
}

#[test]
fn replicates_and_commits_to_all() {
    let mut sim = Sim::new(3);
    sim.run(80);
    assert!(sim.propose(b"x=1"), "leader accepts a proposal");
    sim.run(80);
    for i in 0..3 {
        let cmds: Vec<Vec<u8>> = sim.applied[i].iter().map(|(_, c)| c.clone()).collect();
        assert!(
            cmds.contains(&b"x=1".to_vec()),
            "node {i} applied the command"
        );
    }
}

#[test]
fn reelects_after_leader_crash() {
    let mut sim = Sim::new(5);
    sim.run(80);
    let old = sim.leader().expect("initial leader");
    sim.kill(old);
    sim.run(120);
    let new = sim.leader().expect("a new leader after crash");
    assert_ne!(new, old, "the dead node is not the leader");
    assert_no_two_leaders_same_term(&sim);
}

#[test]
fn partitioned_minority_cannot_commit() {
    let mut sim = Sim::new(5);
    sim.run(80);
    sim.partition(&[0, 1], &[2, 3, 4]);
    sim.run(150);
    // Propose on EVERY node that currently claims leadership — a partitioned
    // stale leader in the minority may still think it leads, but must never
    // commit; only the majority-side leader can.
    for i in 0..5 {
        if sim.alive[i] && sim.nodes[i].is_leader() {
            sim.nodes[i].propose(b"maj=1".to_vec());
        }
    }
    sim.run(150);
    for i in [0, 1] {
        let cmds: Vec<Vec<u8>> = sim.applied[i].iter().map(|(_, c)| c.clone()).collect();
        assert!(
            !cmds.contains(&b"maj=1".to_vec()),
            "minority node {i} must not commit"
        );
    }
    let maj_has = [2, 3, 4]
        .iter()
        .any(|&i| sim.applied[i].iter().any(|(_, c)| c == b"maj=1"));
    assert!(maj_has, "a majority node commits the entry");
}

#[test]
fn log_is_bounded_by_compaction() {
    // A healthy cluster proposing steadily must keep committing while the live
    // log stays bounded (the Sim compacts with keep=8 each tick).
    let mut sim = Sim::new(3);
    sim.run(60);
    for i in 0..200 {
        sim.propose(format!("v{i}").as_bytes());
        sim.run(3);
    }
    sim.run(120);
    // Every node's live log is bounded well below the 200 committed entries.
    for i in 0..3 {
        assert!(
            sim.nodes[i].log_len() <= 40,
            "node {i} log not compacted: {} entries",
            sim.nodes[i].log_len()
        );
    }
    // ...yet a late command still commits everywhere (progress preserved).
    assert!(sim.propose(b"final"));
    sim.run(60);
    for i in 0..3 {
        let cmds: Vec<Vec<u8>> = sim.applied[i].iter().map(|(_, c)| c.clone()).collect();
        assert!(
            cmds.contains(&b"final".to_vec()),
            "node {i} committed 'final'"
        );
    }
}

#[test]
fn committed_logs_never_diverge_under_chaos() {
    let mut sim = Sim::new(5);
    sim.run(60);
    for round in 0..6 {
        sim.propose(format!("c{round}").as_bytes());
        sim.run(30);
        sim.partition(&[round % 5], &[(round + 1) % 5, (round + 2) % 5]);
        sim.run(30);
        sim.cut.clear(); // heal
        sim.run(30);
    }
    sim.run(120);
    for a in 0..5 {
        for b in (a + 1)..5 {
            let ca: Vec<&Vec<u8>> = sim.applied[a].iter().map(|(_, c)| c).collect();
            let cb: Vec<&Vec<u8>> = sim.applied[b].iter().map(|(_, c)| c).collect();
            let n = ca.len().min(cb.len());
            assert_eq!(
                &ca[..n],
                &cb[..n],
                "committed logs of {a} and {b} diverge on their common prefix"
            );
        }
    }
}
