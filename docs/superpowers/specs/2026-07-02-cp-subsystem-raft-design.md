# CP Subsystem (Raft) — Design

**Date:** 2026-07-02
**Status:** Approved scope (minimal-Raft-first); design record for implementation.
**Scope:** A from-scratch Raft consensus core + one linearizable primitive
(`IAtomicLong`) driven end-to-end by a real Hazelcast client. Gap 2 of the
platform-gap roadmap. CP is Enterprise-only in Hazelcast ≥ 5.5, so this is for
functional completeness / the platform-diagram "Consistency" box, not strict OSS
parity.

## Goal

Give BonsaiGrid a **linearizable** consistency tier (distinct from the
AP sync-backup IMap path) via Raft: leader election, log replication, and a
committed, deterministically-ordered command stream applied to a replicated state
machine. First primitive: `AtomicLong`. Honor the guardrails — the Raft RPC path
rides the existing member io_uring transport; the Raft log is durable via the
Gap-3 WAL; the CP state machines run off the IMap hot path.

## Non-Goals (v1)

- Snapshots / log compaction (the Raft log grows unbounded in v1; snapshots are a
  fast follow-up reusing the Gap-3 snapshot machinery).
- FencedLock, ISemaphore, ICountDownLatch, IAtomicReference, CPMap, CP sessions.
- Multiple / named CP groups (one fixed default group).
- Dynamic CP membership changes (the CP group is the static bootstrap member set).
- Read-index / lease-based linearizable reads (v1 routes reads through the log or
  the leader; see Reads).

## Decisions

1. **Raft from scratch** in a new `crates/raft` — pure, state-machine-agnostic,
   driven by a tick + message in/out interface (like the member `Handler`), so it
   is **deterministically simulation-testable** (reuse the `sim.rs` pattern).
2. **One default CP group** = the static bootstrap member set (assumed odd-sized;
   majority = ⌊n/2⌋+1). No METADATA group, no named groups in v1.
3. **AtomicLong** as the first replicated state machine: `HashMap<String, i64>`.
4. **Client-reachable:** wire the Hazelcast `AtomicLong*` client codecs. Any
   member accepting a CP op **forwards it to the current Raft leader** over the
   member transport and relays the committed result — so a stock smart client
   works without CP-aware routing.
5. **Durable Raft log** on the Gap-3 `WalSegment` primitives, with Raft-specific
   indexing/conflict-truncation layered on top.

## Architecture

```
Hazelcast client --AtomicLong codec--> any member (reactor)
    -> CP request forwarded over member transport to the Raft LEADER
        -> Raft: append entry -> replicate (AppendEntries) -> commit (majority)
            -> apply to AtomicLong state machine -> result
        <- committed result relayed back to the accepting member -> client
Raft RPCs (RequestVote/AppendEntries) ride the member io_uring transport.
Raft log entries are persisted (append + fsync) via the WAL primitives.
```

New crate `crates/raft` (pure consensus). CP integration (state machine, client
op forwarding, transport wiring) lives in the server/member crates.

### Component 1 — Raft core (`crates/raft`)

State-machine-agnostic, single-threaded, message-driven (no I/O, no threads —
mirrors the member `Handler` seam so it is deterministically testable):

- **Persistent state:** `current_term: u64`, `voted_for: Option<NodeId>`,
  `log: RaftLog` (entries `{ term, index, command: Vec<u8> }`).
- **Volatile state:** `commit_index`, `last_applied`, role
  (`Follower`/`Candidate`/`Leader`), and per-peer `next_index`/`match_index` on a
  leader.
- **API:** `RaftNode::new(id, peers, log)`,
  `propose(command: Vec<u8>) -> Option<index>` (leader only),
  `step(msg: RaftMsg, out: &mut Vec<(NodeId, RaftMsg)>)`,
  `tick(now_ticks, out)` (election + heartbeat timers),
  `take_committed() -> Vec<(index, Vec<u8>)>` (entries newly safe to apply).
- **RPCs:** `RequestVote{term,candidate,last_log_index,last_log_term}` +
  `RequestVoteResp{term,granted}`; `AppendEntries{term,leader,prev_log_index,
  prev_log_term,entries,leader_commit}` + `AppendEntriesResp{term,success,
  match_index}`. Randomized election timeout (seeded, deterministic in tests).
- **Safety invariants enforced:** election restriction (a candidate's log must be
  at least as up-to-date), log matching (truncate-on-conflict), commit only
  entries of the current term via majority `match_index`.

### Component 2 — Durable Raft log (`crates/raft` + Gap-3 WAL)

- `RaftLog` over a `WalSegment`: append `{term,index,command}` framed records
  (CRC, torn-tail safe — reuse the record-framing approach); `truncate_from(index)`
  on a conflict (rewrites the segment tail); `entries_from(index)`, `last()`;
  `current_term`/`voted_for` persisted in a small companion file, fsync'd before a
  vote is granted or a term advances.
- Recovery: on restart, read the segment to rebuild the in-memory index; the state
  machine is re-derived by replaying committed entries (v1: replay the whole log,
  since there are no snapshots yet).

### Component 3 — AtomicLong state machine (server)

- State: `HashMap<String, i64>`. Command (the Raft entry payload) encodes the op:
  `Get`, `Set(v)`, `GetAndSet(v)`, `AddAndGet(d)`, `GetAndAdd(d)`,
  `CompareAndSet(expected,new)`. `apply(command) -> reply_bytes` is deterministic.
- Reads (`Get`, and the read half of CAS) go **through the log** in v1
  (linearizable, simplest correct choice); a read-index optimization is a
  follow-up.

### Component 4 — Client wiring + leader forwarding (server/member)

- Dispatch the `AtomicLong*` client codecs in `handlers.rs`: decode → build a CP
  command → hand to the CP subsystem.
- The accepting member forwards the command to the **current Raft leader** over
  the member transport (new `Msg::CpPropose{req_id, command}` /
  `Msg::CpResult{req_id, reply}`), and delivers the committed reply to the client
  via the existing deferred-response (`EventBroker`) path — the same mechanism as
  sync-backup replication. If the leader is unknown (election in flight), the op
  waits/retries within a bound.

### Component 5 — CP driver thread wiring

The Raft node for the default group is driven on the member thread (it already
owns a tick and the member transport): route `RaftMsg` variants through the
member `Handler`, call `tick` each member tick, apply committed commands to the
AtomicLong state machine, and complete forwarded client ops.

## Testing Strategy

- **Unit (`crates/raft`):** log matching + truncate-on-conflict; election
  restriction (stale-log candidate denied); term/commit rules; RequestVote /
  AppendEntries handlers.
- **Deterministic simulation (the acid test, reuse `sim.rs` pattern):** N Raft
  nodes in one process over an in-memory bus with virtual time + fault injection:
  - a single leader is elected and re-elected after a leader kill;
  - committed entries never diverge across nodes (log-matching invariant);
  - a partitioned minority cannot commit; the majority makes progress;
  - AtomicLong is **linearizable** under partitions + leader changes (a
    sequential-consistency checker over the committed op history).
- **Integration:** AtomicLong state-machine apply round-trips each op; a forwarded
  client op commits and returns the correct value; a real Hazelcast client
  `getAndAdd`/`compareAndSet` round-trips end-to-end (conformance).
- **Durability:** append + fsync + recover the Raft log; a torn tail is dropped;
  `voted_for`/`term` survive a restart (no double-vote).

## Guardrail Compliance

- Raft RPCs ride the existing member io_uring transport (no new hot-path I/O
  model). Raft log fsync is on the durable path, off the IMap reactor.
- The CP state machines and Raft node run on the member thread (shared-nothing per
  node; no `Mutex`/`RwLock` across cores). The AP IMap hot path is untouched
  (CP is a separate command stream).
- Allocation on the Raft path is at proposal/replication time (not the IMap
  zero-alloc request loop); acceptable and off that hot path.

## Phasing

- **A — Raft core** (`crates/raft`): consensus + durable log, unit + deterministic
  simulation tested (election, replication, partition, leader-kill, log-matching).
  Self-contained; the hardest, highest-value part.
- **B — AtomicLong state machine + linearizability sim** on top of the core.
- **C — Transport wiring + leader forwarding + client codecs**: a real Hazelcast
  client drives AtomicLong end-to-end.

## Open Questions / Risks

- **Read linearizability:** v1 routes reads through the log (correct but adds a
  round-trip); read-index/lease is the follow-up.
- **CP group vs cluster membership:** v1 fixes the CP group to the bootstrap set;
  interaction with the AP membership/failover (Gap D) is decoupled (separate Raft
  election). Dynamic CP membership is deferred.
- **Unbounded log without snapshots:** acceptable for v1 correctness; snapshots
  (reusing Gap-3) are the first follow-up before production.
- **Leader forwarding + client timeouts:** a bounded wait/retry when the leader is
  unknown during an election; surfaced as a retriable error to the client.
