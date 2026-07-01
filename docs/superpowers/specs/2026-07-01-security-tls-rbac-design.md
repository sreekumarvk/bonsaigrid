# Security: TLS, mTLS, Authentication Hardening, and RBAC — Design

**Date:** 2026-07-01
**Status:** Approved design; pending implementation plan.
**Scope:** First Security iteration for BonsaiGrid. Closes the "Security" gap
identified against the Hazelcast platform (encryption in transit + access
control). One of four subsystem gaps (the others — CP Subsystem, Geo-Replication,
Streaming/SQL depth — are separate specs).

## Goal

Give BonsaiGrid a real security posture without abandoning its architectural
guardrails (zero-allocation hot path, thread-per-core shared-nothing, io_uring
kernel-bypass). Four capabilities, all in scope for v1:

1. **TLS in transit (client↔server)** — real Hazelcast clients connect over TLS.
2. **mTLS between members** — nodes mutually authenticate; no rogue joins/snooping.
3. **Authorization / RBAC** — Hazelcast-parity per-resource, per-action permissions.
4. **Authentication hardening** — hashed credentials + a pluggable identity source.

### Reference: how Hazelcast does TLS

Hazelcast implements TLS entirely in **userspace** via Java's JSSE `SSLEngine`
(seam: `com.hazelcast.nio.ssl.SSLEngineFactory#create(clientMode, peerAddress)`),
running `wrap()`/`unwrap()` over `ByteBuffer`s inside its NIO reactor. An optional
higher-throughput OpenSSL provider (`OpenSSLEngineFactory`, netty-tcnative /
BoringSSL) sits behind the same seam. Java has no kernel-TLS, so Hazelcast is
fundamentally userspace TLS. `SSLConfig` exposes protocol, cipher suites,
keystore/truststore, and `mutualAuthentication` = REQUIRED/OPTIONAL.

**BonsaiGrid deliberately diverges to kTLS** (below): the wire is identical
standard TLS — clients cannot tell the difference — but we offload record crypto
to the kernel to preserve the zero-alloc io_uring hot path. This is consistent
with BonsaiGrid's reason for existing (kernel-bypass, bare-metal).

## Non-Goals (v1)

- LDAP/JAAS/Kerberos identity backends (the `IdentityProvider` trait is the seam
  for these later; only a static provider ships now).
- At-rest / persistence encryption (BonsaiGrid is in-memory; no persistence yet).
- Auditing, socket interceptors, client-certificate-as-principal mapping.
- Userspace-rustls TLS data path (documented contingency only; not built).
- JKS/PKCS12 keystore parsing (PEM only; keystore format is a Java-ism that does
  not affect wire compatibility — Hazelcast clients bring their own truststore).

## Architecture

Two independent concerns, cleanly separated so each can be understood, tested,
and shipped on its own:

- **Transport security** (encryption + peer authentication): kTLS on both the
  client reactor and the member↔member transport, controlled by a **three-state
  per-node TLS mode** (`disabled` / `permissive` / `required`). `disabled` is
  plaintext-only (today); `required` is TLS-only; `permissive` accepts *both* per
  connection and exists purely to enable a **zero-downtime rolling migration**
  from plaintext to TLS (see "Enabling TLS on a live cluster"). Steady state is
  always `disabled` or `required`; `permissive` is transitional.
- **Access control** (who + what): authentication (principals + hashed
  credentials) and authorization (RBAC), enforced in the dispatch path,
  independent of how the transport was secured. An authenticated principal is
  bound to a connection regardless of TLS.

### New crate: `crates/security`

Owns the reusable, I/O-free pieces so both the reactor and member transport can
consume them and `handlers.rs` can call the permission check:

- TLS config types + PEM cert/key/CA loading (produces `rustls` server/client
  configs).
- Credential store + `IdentityProvider` trait + `StaticIdentityProvider`.
- Permission model (`Permission`, `ResourceType`, `Action`), the compiled
  per-principal matcher, and the `msg_type → (resource_type, action, name-extractor)`
  static table.
- TOML config parsing for principals and permissions.

The crate is pure logic (no sockets, no io_uring); the reactor/transport own the
I/O and call into it. This keeps the security logic unit-testable in isolation.

## Component 1 — TLS/kTLS data path

The connection is io_uring-driven, so the TLS handshake must not block the core
thread (thread-per-core: one blocking handshake would stall every connection on
that core). Per-connection lifecycle:

1. **Accept** — unchanged (`AcceptMulti`). New connections begin in a
   `Handshaking` state instead of `Established`.
2. **Mode branch on the first bytes.** The action depends on the node's TLS mode:
   - `required` → always TLS handshake (below); a plaintext first byte is rejected.
   - `disabled` → always plaintext (skip to step 4); the handshake state is a
     no-op.
   - `permissive` → **peek the first inbound byte**: `0x16` (TLS handshake record)
     → TLS handshake; the `CP2` preamble (`0x43 0x50 0x32`) → treat as plaintext
     (skip to step 4). The two byte patterns are disjoint, so detection is
     unambiguous. This peek runs *only* in `permissive` mode.
3. **Handshake — userspace rustls, driven over io_uring, non-blocking.**
   Handshake records flow through the existing io_uring `Recv`/`Send`; the reactor
   feeds inbound bytes to a per-connection `rustls::ServerConnection` (member
   dials also use `rustls::ClientConnection`) and pumps its outbound bytes back.
   No blocking; a slow client cannot wedge the core. This is the primary new code.
4. **Enable kTLS** — on handshake completion, export the negotiated session
   secrets and install them on the socket via `setsockopt(TLS_TX/TLS_RX)` using
   the `ktls` crate, which handles the rustls→kernel key handoff and drains any
   buffered records.
5. **Established** — the connection flips to plaintext mode: io_uring `Send`/`Recv`
   move **plaintext** application buffers exactly as today; the kernel performs
   per-record crypto (a plaintext-detected connection in `permissive`/`disabled`
   mode simply skips kTLS and runs on the raw socket). The CP2 preamble detection
   and all dispatch run unchanged. The zero-alloc hot path is untouched.

### Members / mTLS

The member transport (`crates/member/src/transport.rs`) uses the identical
machinery and the same three-state mode with **mutual TLS**: both ends present
certs and verify the peer against the configured CA. A node without a CA-signed
cert cannot complete the handshake, so it can neither join nor snoop. mTLS is
required whenever a member connection *is* TLS; the `permissive` mode still
accepts plaintext member connections during migration (inbound peek `0x16` vs
member preamble), and a member dialing out in `permissive` mode may connect
plaintext to a not-yet-upgraded peer and TLS to an upgraded one.

### Crypto material

All PEM: cert chain, private key, CA bundle — loaded once at startup into
`rustls` configs. rustls consumes PEM/DER natively.

### Allocation

rustls handshake state and TLS buffers are allocated at **connection setup**,
already off the zero-alloc hot path (like today's per-connection `rbuf`/`sendbuf`).
Once kTLS is enabled, steady state is allocation-free.

### Kernel dependency

kTLS requires Linux `TLS_TX`/`TLS_RX`. TLS 1.3 RX offload needs kernel ≥ 5.1
(documented as the minimum). **Contingency (not built in v1):** the same TLS
config seam can fall back to a userspace-rustls data path (Hazelcast's model —
io_uring `Recv` ciphertext → rustls decrypt → parse; encode → rustls encrypt →
`Send`) if kTLS is unavailable.

## Component 2 — Authentication hardening

Replaces the current single optional plaintext username/password compared in
`handlers.rs` (`Cfg::auth_status`, ~line 200).

- **`IdentityProvider` trait** — `authenticate(username, password) -> Option<Principal>`.
  v1 ships `StaticIdentityProvider` (from config); the trait is the seam for
  LDAP/JAAS later without touching the reactor.
- **Credential store** — config maps each principal to a **salted password hash**
  (Argon2id; PBKDF2-HMAC-SHA256 as a lighter fallback), verified with a
  **constant-time compare**. No plaintext secrets at rest.
- **Principal binding** — on successful `ClientAuthentication`, resolve the
  principal *and its compiled permission set* once and store it on the
  connection's `Conn` state. Every later op carries it; no re-lookup per request.
- **Identity vs. transport** — v1 principal = the app-level authenticated
  username; mTLS separately establishes transport trust. (Client-cert-CN as
  principal is a future extension behind the same `Principal` type.)
- **No-auth / dev** — when no credentials are configured, principal = `anonymous`
  with a configurable default grant, so existing dev flows keep working.

## Component 3 — Authorization (RBAC)

- **Model** — `Permission { resource_type: ResourceType, name_pattern, actions:
  ActionSet }`. `ResourceType` ∈ {Map, MultiMap, Queue, List, Set, Ringbuffer,
  Topic, PNCounter, Lock, Sql, Job, Cluster, …}; `Action` mirrors Hazelcast's
  strings (`create, destroy, read, put, remove, listen, lock, offer, poll, …`).
  A principal holds a list of grants.
- **Enforcement seam** — one call in dispatch:
  `authorize(principal, resource_type, name, action) -> bool`. Backed by a static
  `msg_type → (ResourceType, Action, name-extractor)` table, e.g.
  `MapPut(65792) → (Map, Put, extract map-name frame)`. Building this table for
  the implemented ops (map, multimap, queue, list, set, ringbuffer, pncounter,
  topic, lock, SQL, jobs) is the bulk of the RBAC work. Unmapped/admin ops
  **default-deny** unless the principal has an admin grant.
- **Zero-alloc check** — each principal's grants are **compiled at auth time**
  into a matcher; per-op we index the static table by `msg_type`, extract the
  resource-name **slice** from the request frame (no copy), and match the glob
  against that slice. No allocation in the hot path.
- **Denial** — return a Hazelcast `AccessControlException`-shaped error response
  carrying the correlation id (same mechanism as the existing quorum-gated-write
  rejection in `handlers.rs`), so real clients surface a proper security error.

## Configuration

- **Toggles & paths via `BONSAI_*` env** (matches existing style):
  `BONSAI_TLS_MODE` = `disabled|permissive|required` (default `disabled`),
  `BONSAI_TLS_CERT` / `_KEY` / `_CA` (PEM paths; required when mode ≠ `disabled`),
  `BONSAI_TLS_MUTUAL` = `none|optional|required`, `BONSAI_SECURITY_CONFIG` (path
  to the file below).
- **Principals & permissions via TOML** — `[[principal]]` with name + credential
  (Argon2 hash + salt) and nested `[[principal.permission]]` (resource_type, name
  pattern, actions). Read once at startup into the immutable, per-core security
  context.
- Members reuse the same CA for mTLS trust; one cert set per node.

### Enabling TLS on a live cluster (zero-downtime rollout)

The `permissive` mode makes turning TLS on a routine rolling upgrade, no
maintenance window:

1. **`disabled → permissive`**, one node at a time (rolling restart). A
   `permissive` node accepts both TLS and plaintext, so it still talks plaintext
   to not-yet-upgraded peers — the cluster stays fully up throughout.
2. Once **all** nodes are `permissive`, members negotiate TLS between themselves,
   and clients migrate to TLS **at their own pace** (permissive accepts either).
3. When every member and client is on TLS, **`permissive → required`**, one node
   at a time, to lock out plaintext and complete the cutover.

Disabling TLS reverses the sequence (`required → permissive → disabled`). A
direct `disabled ⇄ required` flip is only safe as a coordinated cold cutover
(brief downtime), so operators should route through `permissive`.

Example security config:

```toml
[[principal]]
name = "app"
credential = { alg = "argon2id", hash = "…", salt = "…" }
  [[principal.permission]]
  resource_type = "map"
  name = "cart*"
  actions = ["read", "put", "remove"]
  [[principal.permission]]
  resource_type = "map"
  name = "orders*"
  actions = ["read"]

[[principal]]
name = "ops"
credential = { alg = "argon2id", hash = "…", salt = "…" }
  [[principal.permission]]
  resource_type = "*"
  name = "*"
  actions = ["all"]
```

## Testing Strategy

- **Unit**
  - Credential hash/verify: Argon2id round-trip, constant-time compare, wrong
    password rejected.
  - Permission matcher: glob patterns, resource-type + action matching,
    default-deny, admin/`all` grant.
  - **Coverage test**: every implemented `msg_type` has a table entry (no op
    silently bypasses authz), and each mapped codec's name-extractor returns the
    right slice.
- **Integration**
  - **Acid test**: a real Hazelcast client (conformance-python/java) connects over
    TLS + auth and performs map ops end-to-end.
  - rustls client: handshake → kTLS enable → CP2 request round-trip on loopback.
  - **mTLS**: a member with a valid cert joins; a cert-less / wrong-CA member is
    rejected.
  - **RBAC**: read-only principal → GET ok, PUT → `AccessControlException`; admin
    → all ops; unmapped op → default-deny.
  - Negative: wrong password, wrong CA, expired cert, plaintext client against a
    `required` endpoint — all rejected.
  - **Mode matrix**: `permissive` accepts *both* a TLS client and a plaintext
    client on the same listener (first-byte peek picks the path); `required`
    rejects plaintext; `disabled` rejects/ignores a TLS ClientHello as
    unparseable. A mixed cluster (some `permissive` members plaintext, some TLS)
    stays connected — the live-rollout invariant.
- **Non-blocking handshake**: feeding partial handshake bytes proves a stalled
  client cannot wedge the core.

## Guardrail Compliance

- **Zero-alloc hot path**: kTLS keeps io_uring on plaintext buffers; the RBAC
  check is a compiled matcher + slice lookup; handshake/config allocation is
  setup-only.
- **Thread-per-core shared-nothing**: the handshake is non-blocking per-connection
  state inside the core's reactor; the security context is read-only and shared
  immutably (or copied per core) — no `Mutex`/`RwLock`, no cross-thread mutable
  state.
- **Kernel-bypass I/O**: kTLS *is* the kernel-bypass-consistent choice — record
  crypto offloaded to the kernel, data path stays on io_uring.

## Rollout (incremental; each phase shippable and independently valuable)

1. **`security` crate**: permission model + matcher + credential store + config
   parsing (pure, fully unit-tested).
2. **RBAC enforcement in dispatch, over plaintext** — decouples authz from TLS;
   immediately testable. Includes the `msg_type` table.
3. **Auth hardening** (`IdentityProvider` + hashed creds) replacing the plaintext
   compare.
4. **kTLS on the client reactor** — non-blocking handshake state machine + kTLS
   enable, plus the `disabled`/`permissive`/`required` mode branch and the
   first-byte peek.
5. **mTLS on the member transport** — same three-state mode on the member path,
   completing the zero-downtime rollout story.

TLS is the riskiest work, so it lands last, behind `BONSAI_TLS_MODE`; authz and
authN ship first over plaintext. Real Hazelcast clients are unaffected when the
mode is `disabled`, connect with standard SSL config under `required`, and can be
migrated one at a time under `permissive`.

## Open Questions / Risks

- **kTLS + io_uring interaction**: `setsockopt(TLS_TX/TLS_RX)` on an fd that has
  in-flight io_uring `Recv`/`Send` needs careful sequencing (enable kTLS only when
  no read/write is in flight, right after the handshake drains). Prototype early.
- **`ktls` crate maturity** for the rustls key export path — validate against the
  target kernel in Phase 4 before committing.
- **RBAC table completeness** is ongoing: new ops must be added to the table or
  they default-deny. The coverage test enforces this.
