//! Phase 3 authentication hardening: a ClientAuthentication carrying valid
//! hashed credentials binds the connection's principal (so subsequent ops use
//! its grants); wrong credentials do not; and with no security config, anyone is
//! the full anonymous principal (back-compat).

use protocol::fixed::write_i32_le;
use protocol::frame::{write_message, Frame, UNFRAGMENTED};
use protocol::primitives::{data_frame, null_frame, string_frame};
use security::credential::{bytes_to_hex, hash_password, DEFAULT_ITERATIONS};
use security::{Principal, SecurityContext};
use server::events::EventBroker;
use server::handlers::{dispatch_bytes, Cfg};
use server::membership::{Cluster, MemberInfo};
use std::sync::Arc;
use store::Store;

const ACCESS_CONTROL_CLASS: &[u8] = b"java.security.AccessControlException";

fn cluster() -> Cluster {
    Cluster::new(
        vec![MemberInfo::new((1, 1), "127.0.0.1".into(), 5701, 7701, 0)],
        0,
        1,
    )
}

fn build_auth_msg(cluster: &str, user: Option<&str>, pass: Option<&str>) -> Vec<u8> {
    let mut c = vec![0u8; 40]; // fixed fields incl. serializationVersion@33, routingMode@34
    write_i32_le(&mut c, 0, 256); // ClientAuthentication
    let frames = vec![
        Frame {
            flags: UNFRAGMENTED,
            content: c,
        },
        string_frame(cluster),
        user.map(string_frame).unwrap_or_else(null_frame),
        pass.map(string_frame).unwrap_or_else(null_frame),
        string_frame("rust-test"), // clientType
    ];
    write_message(&frames)
}

fn build_put_msg(name: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut c = vec![0u8; 32];
    write_i32_le(&mut c, 0, 65792); // MapPut
    let frames = vec![
        Frame {
            flags: UNFRAGMENTED,
            content: c,
        },
        string_frame(name),
        data_frame(key),
        data_frame(value),
    ];
    write_message(&frames)
}

/// A security context with one principal `app` (password "s3cret") holding
/// read+put on `orders*`.
fn secured_context() -> SecurityContext {
    let salt = [8u8; 16];
    let hash = hash_password(b"s3cret", &salt, DEFAULT_ITERATIONS);
    let json = format!(
        r#"{{"principals":[{{"name":"app",
            "credential":{{"salt_hex":"{}","hash_hex":"{}","iterations":{}}},
            "permissions":[{{"resource_type":"map","name":"orders*","actions":["read","put"]}}]}}]}}"#,
        bytes_to_hex(&salt),
        bytes_to_hex(&hash),
        DEFAULT_ITERATIONS
    );
    SecurityContext::from_json(&json).unwrap()
}

fn cfg_with(security: SecurityContext) -> Cfg {
    let mut cfg = Cfg::single();
    cfg.cluster_name = "dev".into();
    cfg.security = Arc::new(security);
    cfg
}

/// Drive a sequence of messages on one "connection" (shared principal handle),
/// returning the response bytes of the final message.
fn session(cfg: &Cfg, principal: &mut Arc<Principal>, msgs: &[Vec<u8>]) -> Vec<u8> {
    let store = Store::new();
    let broker = EventBroker::new((1, 1));
    let schemas = serialization::schema::SchemaService::new();
    let cl = cluster();
    let executor = server::executor::ExecutorService::new();
    let txn = server::txn::TransactionService::new();
    let jet = jet::executor::JetService::new();
    let mut out = Vec::new();
    for msg in msgs {
        out.clear();
        dispatch_bytes(
            msg, 1, &store, cfg, &broker, &schemas, &cl, None, &executor, &txn, &jet, principal,
            &mut out,
        );
    }
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn correct_auth_binds_principal_and_grants_apply() {
    let cfg = cfg_with(secured_context());
    let mut principal = cfg.security.anonymous(); // starts unauthenticated (deny-all)
                                                  // Authenticate, then PUT on a granted map.
    let out = session(
        &cfg,
        &mut principal,
        &[
            build_auth_msg("dev", Some("app"), Some("s3cret")),
            build_put_msg("orders1", b"k", b"v"),
        ],
    );
    assert_eq!(principal.name, "app", "principal must be bound after auth");
    assert!(
        !contains(&out, ACCESS_CONTROL_CLASS),
        "granted PUT after auth must succeed"
    );
}

#[test]
fn authenticated_principal_still_denied_outside_grant() {
    let cfg = cfg_with(secured_context());
    let mut principal = cfg.security.anonymous();
    // Authenticated as `app` (orders* only), then PUT on a non-granted map.
    let out = session(
        &cfg,
        &mut principal,
        &[
            build_auth_msg("dev", Some("app"), Some("s3cret")),
            build_put_msg("secrets1", b"k", b"v"),
        ],
    );
    assert_eq!(principal.name, "app");
    assert!(
        contains(&out, ACCESS_CONTROL_CLASS),
        "PUT outside the principal's grant must be denied"
    );
}

#[test]
fn wrong_password_does_not_bind_principal() {
    let cfg = cfg_with(secured_context());
    let mut principal = cfg.security.anonymous();
    let out = session(
        &cfg,
        &mut principal,
        &[
            build_auth_msg("dev", Some("app"), Some("WRONG")),
            build_put_msg("orders1", b"k", b"v"),
        ],
    );
    assert_ne!(principal.name, "app", "wrong password must not bind app");
    assert!(
        contains(&out, ACCESS_CONTROL_CLASS),
        "unauthenticated principal must be denied the PUT"
    );
}

#[test]
fn no_security_config_is_anonymous_full() {
    // open() context: no auth required, anonymous is full-grant (back-compat).
    let cfg = cfg_with(SecurityContext::open());
    let mut principal = cfg.security.anonymous();
    let out = session(
        &cfg,
        &mut principal,
        &[
            build_auth_msg("dev", None, None),
            build_put_msg("anything", b"k", b"v"),
        ],
    );
    assert!(
        !contains(&out, ACCESS_CONTROL_CLASS),
        "with no security config, all ops are allowed"
    );
}
