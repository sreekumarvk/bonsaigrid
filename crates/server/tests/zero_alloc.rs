//! Proves the MapGet hot path allocates zero times after warmup, using a
//! counting global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}

#[global_allocator]
static A: Counting = Counting;

use protocol::fixed::write_i32_le;
use protocol::frame::{write_message, Frame, UNFRAGMENTED};
use protocol::primitives::{data_frame, string_frame};
use server::events::EventBroker;
use server::handlers::{dispatch_bytes, Cfg};
use server::membership::{Cluster, MemberInfo};
use store::Store;

fn build_get_msg(name: &str, key: &[u8]) -> Vec<u8> {
    let mut c = vec![0u8; 24]; // type@0, corr@4, partitionId@12, threadId@16
    write_i32_le(&mut c, 0, 66048);
    let frames = vec![
        Frame {
            flags: UNFRAGMENTED,
            content: c,
        },
        string_frame(name),
        data_frame(key),
    ];
    write_message(&frames)
}

#[test]
fn map_get_hot_path_is_zero_alloc() {
    let store = Store::new();
    store.put("m", b"k".to_vec(), b"value".to_vec());
    let cfg = Cfg::single();
    let cluster = Cluster::new(
        vec![MemberInfo::new((1, 1), "127.0.0.1".into(), 5701, 7701, 0)],
        0,
        1,
    );
    let broker = EventBroker::new((1, 1));
    let schemas = serialization::schema::SchemaService::new();
    let msg = build_get_msg("m", b"k");
    let mut out: Vec<u8> = Vec::with_capacity(64 * 1024); // pre-reserved, like the reactor

    // These services are constructed once at startup by the reactor (see
    // main.rs), not per request. Build them here — outside the measured loop —
    // so the test faithfully measures the request hot path and not its setup.
    let executor = server::executor::ExecutorService::new();
    let txn = server::txn::TransactionService::new();
    let jet = jet::executor::JetService::new();
    // The authenticated principal is bound once per connection (not per request);
    // build it outside the loop. Admin short-circuits authorize() with no alloc.
    let principal = security::Principal::anonymous_full();

    // Warmup: intern the map name, settle buffers.
    for _ in 0..200 {
        out.clear();
        dispatch_bytes(
            &msg, 1, &store, &cfg, &broker, &schemas, &cluster, None, &executor, &txn, &jet,
            &principal, &mut out,
        );
    }
    assert!(
        out.windows(5).any(|w| w == b"value"),
        "response carries the value"
    );

    let before = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..10_000 {
        out.clear();
        dispatch_bytes(
            &msg, 1, &store, &cfg, &broker, &schemas, &cluster, None, &executor, &txn, &jet,
            &principal, &mut out,
        );
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - before;
    assert_eq!(
        allocs, 0,
        "MapGet hot path allocated {allocs} times over 10k calls"
    );

    // Same hot path, but with a NON-admin principal whose grant must actually be
    // matched (glob + resource + action) — proving the Phase 2 RBAC enforcement
    // is allocation-free. Kept in this one test (not a separate #[test]) because
    // the counting allocator is process-global and parallel tests would
    // contaminate each other's counts.
    let granted = security::Principal {
        name: "app".into(),
        grants: vec![security::permission::Permission {
            resource_type: security::permission::ResourceType::Map,
            name: "m*".into(),
            actions: security::permission::ActionSet::of(security::permission::Action::Read),
        }],
        is_admin: false,
    };
    for _ in 0..200 {
        out.clear();
        dispatch_bytes(
            &msg, 1, &store, &cfg, &broker, &schemas, &cluster, None, &executor, &txn, &jet,
            &granted, &mut out,
        );
    }
    assert!(
        out.windows(5).any(|w| w == b"value"),
        "grant must permit GET"
    );
    let before = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..10_000 {
        out.clear();
        dispatch_bytes(
            &msg, 1, &store, &cfg, &broker, &schemas, &cluster, None, &executor, &txn, &jet,
            &granted, &mut out,
        );
    }
    let allocs = ALLOCS.load(Ordering::Relaxed) - before;
    assert_eq!(
        allocs, 0,
        "RBAC authorize (non-admin matching grant) allocated {allocs} times over 10k gets"
    );
}
