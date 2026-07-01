//! RBAC enforcement in the dispatch path: a read-only principal may GET but not
//! PUT; an admin may do both; an unmapped op is default-denied for non-admins.
//! Denials come back as a Hazelcast `AccessControlException` frame.

use protocol::fixed::write_i32_le;
use protocol::frame::{write_message, Frame, UNFRAGMENTED};
use protocol::primitives::{data_frame, string_frame};
use security::permission::{Action, ActionSet, Permission, ResourceType};
use security::Principal;
use server::events::EventBroker;
use server::handlers::{dispatch_bytes, Cfg};
use server::membership::{Cluster, MemberInfo};
use store::Store;

const ACCESS_CONTROL_CLASS: &[u8] = b"java.security.AccessControlException";

fn build_msg(msg_type: i32, name: &str, key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let mut c = vec![0u8; 32]; // room for type@0, corr@4, partition@12, threadId@16, ttl@24
    write_i32_le(&mut c, 0, msg_type);
    let mut frames = vec![
        Frame {
            flags: UNFRAGMENTED,
            content: c,
        },
        string_frame(name),
        data_frame(key),
    ];
    if let Some(v) = value {
        frames.push(data_frame(v));
    }
    write_message(&frames)
}

fn cluster() -> Cluster {
    Cluster::new(
        vec![MemberInfo::new((1, 1), "127.0.0.1".into(), 5701, 7701, 0)],
        0,
        1,
    )
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// Run one op through dispatch as `principal`, returning the response bytes.
fn run(principal: Principal, msg: &[u8]) -> Vec<u8> {
    let store = Store::new();
    store.put("orders1", b"k".to_vec(), b"value".to_vec());
    let cfg = Cfg::single();
    let broker = EventBroker::new((1, 1));
    let schemas = serialization::schema::SchemaService::new();
    let cl = cluster();
    let mut principal = std::sync::Arc::new(principal);
    let mut out = Vec::new();
    dispatch_bytes(
        msg,
        1,
        &store,
        &cfg,
        &broker,
        &schemas,
        &cl,
        None,
        &server::executor::ExecutorService::new(),
        &server::txn::TransactionService::new(),
        &jet::executor::JetService::new(),
        &mut principal,
        &mut out,
    );
    out
}

fn read_only_on_orders() -> Principal {
    Principal {
        name: "app".into(),
        grants: vec![Permission {
            resource_type: ResourceType::Map,
            name: "orders*".into(),
            actions: ActionSet::of(Action::Read),
        }],
        is_admin: false,
    }
}

#[test]
fn read_only_principal_can_get() {
    let out = run(
        read_only_on_orders(),
        &build_msg(66048, "orders1", b"k", None),
    );
    assert!(contains(&out, b"value"), "GET should return the value");
    assert!(
        !contains(&out, ACCESS_CONTROL_CLASS),
        "GET must not be denied"
    );
}

#[test]
fn read_only_principal_cannot_put() {
    let out = run(
        read_only_on_orders(),
        &build_msg(65792, "orders1", b"k", Some(b"v2")),
    );
    assert!(
        contains(&out, ACCESS_CONTROL_CLASS),
        "PUT by a read-only principal must be denied with AccessControlException"
    );
}

#[test]
fn read_only_principal_cannot_get_ungranted_map() {
    // "cart1" is not covered by the "orders*" grant → denied even for a read.
    let out = run(
        read_only_on_orders(),
        &build_msg(66048, "cart1", b"k", None),
    );
    assert!(
        contains(&out, ACCESS_CONTROL_CLASS),
        "GET on a non-granted map must be denied"
    );
}

#[test]
fn admin_principal_can_put() {
    let admin = Principal::anonymous_full(); // is_admin = true
    let out = run(admin, &build_msg(65792, "orders1", b"k", Some(b"v2")));
    assert!(
        !contains(&out, ACCESS_CONTROL_CLASS),
        "admin PUT must be allowed"
    );
}

#[test]
fn non_admin_denied_admin_only_op() {
    // MCGetTimedMemberState (2099968) is AdminOnly.
    let out = run(read_only_on_orders(), &build_msg(2099968, "x", b"", None));
    assert!(
        contains(&out, ACCESS_CONTROL_CLASS),
        "AdminOnly op must be denied for a non-admin principal"
    );
}
