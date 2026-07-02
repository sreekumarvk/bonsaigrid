//! Linearizability of the AtomicLong replicated state machine over Raft. Because
//! Raft delivers one committed total order and `apply` is deterministic, every
//! replica must converge to identical state, and the state must equal a
//! sequential execution of the committed order — even under partitions and
//! leader changes.

mod common;
use common::Sim;
use raft::atomiclong::{decode, encode, AlOp, AlReply, AtomicLongSm};

/// Apply a node's full committed stream to a fresh state machine.
fn replay(sim: &Sim, node: usize) -> AtomicLongSm {
    let mut sm = AtomicLongSm::new();
    for (_, cmd) in &sim.applied[node] {
        sm.apply(cmd);
    }
    sm
}

#[test]
fn atomiclong_converges_and_counts_under_chaos() {
    let mut sim = Sim::new(5);
    sim.run(60);
    for round in 0..8 {
        // Fire a burst of increments (some fail if mid-election — that's fine).
        for _ in 0..5 {
            sim.propose(&encode("c", &AlOp::AddAndGet(1)));
        }
        sim.run(20);
        // Partition a node off, then heal.
        sim.partition(&[round % 5], &[(round + 1) % 5, (round + 2) % 5]);
        sim.run(25);
        sim.cut.clear();
        sim.run(25);
    }
    // Full heal + long settle: all replicas must catch up to the same commit.
    sim.run(250);

    let lens: Vec<usize> = (0..5).map(|i| sim.applied[i].len()).collect();
    assert!(
        lens.iter().all(|&l| l == lens[0]),
        "replicas did not converge to the same commit length: {lens:?}"
    );

    // Every replica reaches the same AtomicLong value...
    let vals: Vec<i64> = (0..5).map(|i| replay(&sim, i).get("c")).collect();
    assert!(
        vals.iter().all(|&v| v == vals[0]),
        "replicas diverge on the AtomicLong value: {vals:?}"
    );
    // ...and it equals a sequential execution of the committed order (one +1 per
    // committed increment).
    let committed_incrs = sim.applied[0]
        .iter()
        .filter(|(_, c)| {
            decode(c)
                .map(|(n, op)| n == "c" && op == AlOp::AddAndGet(1))
                .unwrap_or(false)
        })
        .count() as i64;
    assert!(committed_incrs > 0, "some increments must commit");
    assert_eq!(
        vals[0], committed_incrs,
        "value must equal the number of committed increments"
    );
}

#[test]
fn compare_and_set_is_exclusive() {
    let mut sim = Sim::new(3);
    sim.run(60);
    assert!(
        sim.propose(&encode("g", &AlOp::Set(0))),
        "set the base value"
    );
    sim.run(30);
    // Two competing compare-and-sets from the same expected value.
    sim.propose(&encode("g", &AlOp::CompareAndSet(0, 100)));
    sim.propose(&encode("g", &AlOp::CompareAndSet(0, 200)));
    sim.run(80);

    // Replaying the canonical committed order, exactly one CAS may succeed.
    let mut sm = AtomicLongSm::new();
    let mut successes = 0;
    for (_, cmd) in &sim.applied[0] {
        if let AlReply::Bool(true) = sm.apply(cmd) {
            if decode(cmd).map(|(_, op)| matches!(op, AlOp::CompareAndSet(..))) == Some(true) {
                successes += 1;
            }
        }
    }
    assert_eq!(successes, 1, "exactly one competing CAS may succeed");

    // All replicas agree on the final value, which is one of the CAS targets.
    let finals: Vec<i64> = (0..3).map(|i| replay(&sim, i).get("g")).collect();
    assert!(
        finals.iter().all(|&v| v == finals[0]),
        "replicas diverge on g: {finals:?}"
    );
    assert!(
        finals[0] == 100 || finals[0] == 200,
        "g must be a committed CAS target, got {}",
        finals[0]
    );
}
