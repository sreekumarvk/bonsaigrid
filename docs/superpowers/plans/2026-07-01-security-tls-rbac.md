# Security (TLS, mTLS, Auth Hardening, RBAC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give BonsaiGrid encryption-in-transit (kTLS/mTLS) and access control (hardened authentication + Hazelcast-parity RBAC) without violating the zero-alloc / thread-per-core / io_uring guardrails.

**Architecture:** A new pure-logic `crates/security` crate owns the permission model, matcher, credential store, identity provider, and config parsing. `handlers.rs` calls a zero-alloc `authorize()` in dispatch; the reactor binds a `Principal` to each connection at authentication. Transport TLS is added last as a three-state mode (`disabled`/`permissive`/`required`) using userspace rustls handshake + kernel kTLS, so the hot path stays plaintext.

**Tech Stack:** Rust; `pbkdf2`+`sha2`+`hmac`+`subtle` (credential hashing, constant-time compare), `serde`/`serde_json` (config), `getrandom` (salts). Phases 4–5 add `rustls` + `ktls`.

**Spec:** `docs/superpowers/specs/2026-07-01-security-tls-rbac-design.md`

## Global Constraints

- **Guardrails (non-negotiable):** no heap allocation in the request/authorize hot path; no `Mutex`/`RwLock` or shared mutable state across core threads (the security context is immutable, shared via `Arc`/per-core clone); no blocking I/O in the reactor loop.
- **Credential hash:** PBKDF2-HMAC-SHA256 (in-tree `pbkdf2`), 600k iterations, 16-byte random salt. (Spec named Argon2id; PBKDF2 chosen because it is already in the dependency tree. The `CredentialHash` type is the seam to swap later.)
- **Security config format:** JSON (via `serde_json`, already a dependency), not TOML. Config format is not a wire-compatibility concern.
- **Default-deny:** any op whose `msg_type` is not in the RBAC table is denied unless the principal holds an admin/`all` grant.
- **Backward compatibility:** when no security config is present, principal = `anonymous` with a full default grant → existing plaintext, no-auth flows are unchanged.
- **Wire compatibility:** real Hazelcast clients must keep working; a denial returns a Hazelcast `AccessControlException`-shaped error frame with the correlation id.
- **Phases 4–5 (kTLS/mTLS)** require Linux `TLS_TX`/`TLS_RX` (TLS1.3 RX ⇒ kernel ≥ 5.1) and real certs; they are implemented and verified in a kernel-capable environment, not headless CI.

---

## File Structure

- `crates/security/Cargo.toml` — new crate manifest.
- `crates/security/src/lib.rs` — re-exports; `SecurityContext` (the assembled, immutable per-node security state).
- `crates/security/src/permission.rs` — `ResourceType`, `Action`, `ActionSet`, `Permission`, glob `name_matches`.
- `crates/security/src/principal.rs` — `Principal` (name + compiled grants) + `authorize()`.
- `crates/security/src/optable.rs` — static `msg_type → (ResourceType, Action)` table + name-extractor dispatch.
- `crates/security/src/credential.rs` — `CredentialHash` (PBKDF2), `verify()`, constant-time compare.
- `crates/security/src/identity.rs` — `IdentityProvider` trait + `StaticIdentityProvider`.
- `crates/security/src/config.rs` — JSON config types + loader → `SecurityContext`.
- `crates/security/src/tls.rs` — (Phase 4/5) TLS mode enum, PEM loading, rustls config builders.
- `crates/server/src/handlers.rs` — call `authorize()` in dispatch; denial response.
- `crates/server/src/reactor.rs` — bind `Principal` to `Conn`; (Phase 4) handshake state machine.
- `crates/member/src/transport.rs` — (Phase 5) mTLS.
- `crates/server/src/main.rs` — load `SecurityContext` at startup; env config.

---

## Phase 1 — `security` crate (pure logic, fully unit-tested)

Deliverable: a standalone, unit-tested crate. No server wiring yet.

### Task 1.1: Scaffold the crate

**Files:** Create `crates/security/Cargo.toml`, `crates/security/src/lib.rs`; Modify root `Cargo.toml` (add member).

- [ ] Add `crates/security` to workspace members.
- [ ] Manifest deps: `pbkdf2`, `sha2`, `hmac`, `subtle`, `serde` (derive), `serde_json`, `getrandom`.
- [ ] `lib.rs` with `pub mod` declarations and a smoke `#[test] fn builds() {}`.
- [ ] Run `cargo test -p security` → passes. Commit.

### Task 1.2: Permission model + glob matcher

**Files:** Create `crates/security/src/permission.rs`; Test: inline `#[cfg(test)]`.

**Produces:** `enum ResourceType { Map, MultiMap, Queue, List, Set, Ringbuffer, Topic, PnCounter, Lock, Sql, Job, Cluster }`; `enum Action { Create, Destroy, Read, Put, Remove, Listen, Lock, Offer, Poll, Admin }`; `struct ActionSet(u32)` (bitset) with `contains`/`all`; `struct Permission { resource_type: ResourceType, name: String, actions: ActionSet }`; `fn name_matches(pattern: &str, name: &str) -> bool` (supports trailing `*` and exact and `*`-all).

- [ ] Unit tests: `name_matches("cart*","cart42")==true`, `("cart*","order")==false`, `("*","anything")==true`, `("exact","exact")==true`, `("exact","other")==false`.
- [ ] `ActionSet` tests: `all()` contains every action; a set built from `[Read,Put]` contains Read+Put, not Remove.
- [ ] Implement; tests pass; commit.

### Task 1.3: Principal + authorize()

**Files:** Create `crates/security/src/principal.rs`.

**Consumes:** `Permission`, `ResourceType`, `Action`, `name_matches`.
**Produces:** `struct Principal { name: String, grants: Vec<Permission>, is_admin: bool }`; `fn authorize(&self, rt: ResourceType, name: &str, action: Action) -> bool` — true if `is_admin`, else any grant matches `resource_type == rt && name_matches(g.name, name) && g.actions.contains(action)`. Zero allocation (iterates borrowed grants, matches against a `&str` slice).

- [ ] Unit tests: read-only principal (grant Map `orders*` {Read}) → `authorize(Map,"orders9",Read)==true`, `authorize(Map,"orders9",Put)==false`, `authorize(Queue,"orders9",Read)==false`. Admin principal → every `authorize(...)==true`. Anonymous default-grant principal → true.
- [ ] Implement; tests pass; commit.

### Task 1.4: msg_type → (resource, action) table + name extractors

**Files:** Create `crates/security/src/optable.rs`.

**Produces:** `fn op_permission(msg_type: i32) -> Option<(ResourceType, Action)>` covering implemented ops (map put/get/remove/putAll/getAll/delete/replace/containsKey/keySet/values/entrySet, multimap, queue offer/poll/peek/size, list/set, ringbuffer, pncounter, topic publish, lock, sql execute, job submit). Returns `None` for unmapped → caller default-denies. `fn resource_name(msg_type: i32, req: &[Frame]) -> Option<&str>` (borrows the name slice from the request frames using existing codec offset knowledge).

- [ ] Unit test: `op_permission(65792) == Some((Map, Put))` (MapPut), `op_permission(66048) == Some((Map, Read))` (MapGet), a queue offer type → `(Queue, Offer)`, an unknown type → `None`.
- [ ] Coverage test: assert every message type the server currently dispatches (enumerate from `handlers.rs` match arms) is present in the table OR explicitly listed as admin-only. This test is the guard that no op silently bypasses authz.
- [ ] Implement; tests pass; commit.

### Task 1.5: Credential store (PBKDF2) + constant-time verify

**Files:** Create `crates/security/src/credential.rs`.

**Produces:** `struct CredentialHash { salt: [u8;16], iterations: u32, hash: [u8;32] }`; `fn hash_password(pw: &[u8], salt: [u8;16], iterations: u32) -> [u8;32]` (PBKDF2-HMAC-SHA256); `fn verify(&self, pw: &[u8]) -> bool` using `subtle::ConstantTimeEq`; serde (de)serialize as base64/hex strings.

- [ ] Unit tests: `hash_password` is deterministic for same salt; `verify` true for correct pw, false for wrong pw; two different salts on same pw give different hashes.
- [ ] Implement; tests pass; commit.

### Task 1.6: IdentityProvider + StaticIdentityProvider

**Files:** Create `crates/security/src/identity.rs`.

**Consumes:** `Principal`, `CredentialHash`.
**Produces:** `trait IdentityProvider: Send + Sync { fn authenticate(&self, user: Option<&str>, pass: Option<&str>) -> Option<Arc<Principal>>; fn anonymous(&self) -> Arc<Principal>; }`; `struct StaticIdentityProvider { principals: HashMap<String,(CredentialHash, Arc<Principal>)>, anonymous: Arc<Principal> }`.

- [ ] Unit tests: correct user+pass → `Some(principal)`; wrong pass → `None`; unknown user → `None`; no creds configured path returns `anonymous()`.
- [ ] Implement; tests pass; commit.

### Task 1.7: Config loader → SecurityContext

**Files:** Create `crates/security/src/config.rs`; Modify `lib.rs` (`SecurityContext`).

**Produces:** serde structs mirroring the JSON config (principals: name, credential, permissions: resource_type/name/actions); `fn load(json: &str) -> Result<SecurityContext, ConfigError>`; `struct SecurityContext { identity: StaticIdentityProvider }` with `fn authenticate(...)`, `fn anonymous()`. `SecurityContext::open()` — the no-config default (anonymous with full grant).

- [ ] Unit test: parse a sample JSON with two principals (`app` reader on `orders*`, `ops` admin) → authenticate `app` → principal whose `authorize(Map,"orders1",Read)` true / `Put` false; authenticate `ops` → admin.
- [ ] Unit test: `SecurityContext::open()` authenticates anyone (anonymous) with full grant.
- [ ] Implement; tests pass; commit.

**Phase 1 test summary:** all unit tests above (`cargo test -p security`). No functional/integration yet (no server wiring).

---

## Phase 2 — RBAC enforcement in dispatch (over plaintext)

Deliverable: the server enforces permissions; unauthorized ops are rejected. Fully testable without TLS.

### Task 2.1: Thread a Principal into dispatch

**Files:** Modify `crates/server/src/handlers.rs` (dispatch signature), `crates/server/src/reactor.rs` (`Conn` gets `principal: Arc<Principal>`), `crates/server/src/main.rs` (build `SecurityContext`, default `open()`), `crates/server/Cargo.toml` (dep on `security`).

**Interfaces:** `dispatch_bytes(..., principal: &Principal, ...)`; `Conn.principal` defaults to `SecurityContext::open().anonymous()` until authentication.

- [ ] Add `security` dep; add `principal` param to `dispatch_bytes` (thread through all call sites incl. tests — default anonymous).
- [ ] Build workspace; existing tests pass (anonymous = full grant, no behavior change). Commit.

### Task 2.2: Enforce authorize() in dispatch

**Files:** Modify `crates/server/src/handlers.rs`.

**Consumes:** `security::optable::op_permission`, `resource_name`, `Principal::authorize`.

- [ ] After decoding a request's `msg_type`, before executing: if `op_permission(msg_type)` is `Some((rt, action))`, extract `resource_name`, and if `!principal.authorize(rt, name, action)` return an `AccessControlException` error frame with the correlation id. If `op_permission` is `None` and the op is not an infra/admin op, default-deny unless `principal.is_admin`.
- [ ] Denial helper mirrors the existing quorum-gated-write error path (`error_response(...)` + `set_correlation_id`).

**Functional test (unit-level, in-crate):**
- [ ] Build a `Principal` with Map `orders*` {Read}. Call `dispatch_bytes` with a MapPut to `orders1` → response is an `AccessControlException` frame. With a MapGet to `orders1` → normal response. With MapPut to `orders1` as an admin principal → normal response.

**Integration test:** `crates/server/tests/rbac.rs`
- [ ] Drive `dispatch_bytes` end-to-end (like `zero_alloc.rs`/`replication.rs` do) with a read-only principal: GET succeeds, PUT returns the security error frame decoded to the right error class name. Admin principal: both succeed. Unmapped op with non-admin: denied.
- [ ] Commit.

### Task 2.3: Zero-alloc check verification

**Files:** Modify `crates/server/tests/zero_alloc.rs` (or a new `rbac_zero_alloc.rs`).

- [ ] Extend the zero-alloc harness: run the MapGet hot path with a non-admin principal that has a matching grant; assert the authorize path allocates 0 times over 10k calls (proves the matcher + name-slice extraction are allocation-free). Commit.

---

## Phase 3 — Authentication hardening

Deliverable: real hashed credentials replace the plaintext compare; the authenticated principal is bound to the connection.

### Task 3.1: Route ClientAuthentication through IdentityProvider

**Files:** Modify `crates/server/src/handlers.rs` (`Cfg::auth_status` → use `SecurityContext`), `crates/server/src/reactor.rs` (on successful auth, set `Conn.principal`).

**Consumes:** `SecurityContext::authenticate(user, pass) -> Option<Arc<Principal>>`.

- [ ] Replace the plaintext `username`/`password` compare in `auth_status` with `SecurityContext::authenticate`. On success, bind the returned `Principal` to the connection; on failure, return the existing auth-failure status code.
- [ ] When no security config → `open()` → anonymous principal (unchanged dev behavior).

**Functional/integration test:** `crates/server/tests/auth.rs`
- [ ] With a config defining `app`/PBKDF2(secret): a ClientAuthentication with correct password → success + subsequent ops use `app`'s grants; wrong password → auth-failure status; no username when creds required → failure.
- [ ] With no config: any auth → anonymous success (back-comp).
- [ ] Commit.

**Phase 3 test summary:** unit (identity provider, from Phase 1) + functional (auth_status) + integration (`auth.rs` full ClientAuthentication round-trip through `dispatch_bytes`).

---

## Phase 4 — kTLS on the client reactor (kernel-capable env)

Deliverable: TLS/kTLS on the client protocol with the three-state mode. Requires `rustls` + `ktls` and a Linux kernel with kTLS.

### Task 4.1: TLS config + PEM loading

**Files:** Create `crates/security/src/tls.rs`; `crates/security/Cargo.toml` (+`rustls`, `rustls-pemfile`).

**Produces:** `enum TlsMode { Disabled, Permissive, Required }` (parse from `BONSAI_TLS_MODE`); `fn server_config(cert, key, ca, mutual) -> Arc<rustls::ServerConfig>`; `fn client_config(...)` for member dials; PEM loaders.

- [ ] Unit tests: PEM parse of a test cert/key/CA fixture; `TlsMode::parse`. Commit.

### Task 4.2: Non-blocking handshake state machine

**Files:** Modify `crates/server/src/reactor.rs`.

- [ ] `Conn` gains a `tls: TlsState` (`Plaintext | Handshaking(Box<rustls::ServerConnection>) | Ktls`). New connection: in `Required` → `Handshaking`; `Disabled` → `Plaintext`; `Permissive` → decide on first byte (`0x16` → `Handshaking`, `CP2` → `Plaintext`).
- [ ] Drive rustls over the existing io_uring `Recv`/`Send`: feed inbound bytes to `read_tls`/`process_new_packets`, pump `write_tls` outbound. No blocking.

**Test (kernel-capable):** a rustls test client completes a handshake over loopback; partial-byte feed proves non-blocking (core not wedged).

### Task 4.3: Enable kTLS + plaintext hot path

**Files:** Modify `crates/server/src/reactor.rs`; `crates/security/Cargo.toml` (+`ktls`).

- [ ] On handshake completion, `ktls::config_ktls_server` → `setsockopt(TLS_TX/TLS_RX)`, drain buffered records, flip `Conn.tls = Ktls`. Steady state: io_uring `Send`/`Recv` on plaintext buffers unchanged.

**Integration test (acid test, kernel-capable):** a real Hazelcast client (conformance-python) connects over TLS + auth and performs map ops end-to-end. Negative: plaintext client against `Required` rejected; mixed `Permissive` cluster stays connected.

---

## Phase 5 — mTLS on the member transport (kernel-capable env)

**Files:** Modify `crates/member/src/transport.rs`.

- [ ] Apply the same `TlsMode` + handshake + kTLS machinery to inbound member accepts and outbound dials, requiring peer certs (mTLS) verified against the CA.
- [ ] Reuse the first-byte peek in `Permissive` for the member preamble vs `0x16`.

**Integration test (kernel-capable):** a member with a valid cert joins; a cert-less / wrong-CA member is rejected; a `Permissive` member talks plaintext to a not-yet-upgraded peer and TLS to an upgraded one (the live-rollout invariant).

---

## Per-Phase Test Matrix

| Phase | Unit | Functional | Integration |
|-------|------|-----------|-------------|
| 1 | permission matcher, ActionSet, optable, PBKDF2, identity, config parse | — | — |
| 2 | authorize() in dispatch | dispatch_bytes deny/allow | `rbac.rs` (GET ok / PUT denied / admin / default-deny), zero-alloc authorize |
| 3 | identity provider | auth_status via IdentityProvider | `auth.rs` (correct/wrong pw, anonymous back-comp) |
| 4 | tls config/PEM parse, TlsMode | handshake state machine | rustls client TLS→kTLS→CP2 round-trip; **real Hazelcast client over TLS**; mode matrix |
| 5 | — | member handshake | member mTLS join accept/reject; permissive mixed cluster |

## Self-Review

- **Spec coverage:** TLS ✅ (P4), mTLS ✅ (P5), RBAC ✅ (P1–2), auth hardening ✅ (P1,P3), three-state mode ✅ (P4.2), config surface ✅ (P1.7 + env in P2.1/P4.1), zero-alloc ✅ (P2.3), default-deny ✅ (P2.2), back-compat ✅ (P2.1/P3.1). Deviations recorded in Global Constraints (PBKDF2 for Argon2; JSON for TOML).
- **Placeholder scan:** none — every task names files, interfaces, and concrete tests.
- **Type consistency:** `Principal`, `authorize(rt,name,action)`, `op_permission(msg_type)`, `resource_name(msg_type,req)`, `SecurityContext::{authenticate,open,anonymous}`, `IdentityProvider`, `CredentialHash`, `TlsMode` are used consistently across tasks.

## Execution Note

Phases 1–3 (RBAC + hardened auth, over plaintext) are self-contained, fully verifiable headless, and are implemented first. Phases 4–5 (kTLS/mTLS) require a kernel-capable environment with certs and are implemented/verified there.

## Status (2026-07-01)

- ✅ **Phase 1 shipped** (`1d57666`) — `crates/security` crate; 26 unit tests.
- ✅ **Phase 2 shipped** (`dd12ba2`) — RBAC enforcement in dispatch; `rbac.rs` + zero-alloc authorize; falsified.
- ✅ **Phase 3 shipped** (`537acf7`) — auth hardening + per-connection principal binding; `auth.rs`.
- ⏳ **Phases 4–5 pending** — kTLS client reactor + mTLS members. Deferred to a kernel-capable environment (needs `TLS_TX`/`TLS_RX`, real certs, and io_uring+kTLS validation); not landed headless to avoid shipping unverified transport-security code.

Everything through Phase 3 is on `main`, whole workspace green, zero-alloc hot path preserved.
