//! Live integration test for the CDC connector. Gated on a PostgreSQL server started
//! with `wal_level = logical` (logical replication). Set `BONSAI_TEST_POSTGRES`; unset
//! it is skipped so CI without a DB stays green.
//!
//!   docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 5432:5432 \
//!     postgres:16-alpine -c wal_level=logical
//!   BONSAI_TEST_POSTGRES='host=127.0.0.1 user=postgres password=pw' \
//!     cargo test -p server --test cdc_postgres

use server::cdc::{CdcSource, ChangeOp};
use server::jdbc::JdbcSource;

#[test]
fn captures_inserts_updates_deletes_in_commit_order() {
    let Ok(conn) = std::env::var("BONSAI_TEST_POSTGRES") else {
        eprintln!(
            "skipping cdc_postgres: needs Postgres with wal_level=logical via BONSAI_TEST_POSTGRES"
        );
        return;
    };
    let mut db = JdbcSource::connect(&conn).expect("connect");
    db.execute("DROP TABLE IF EXISTS bonsai_cdc").unwrap();
    db.execute("CREATE TABLE bonsai_cdc (id INT PRIMARY KEY, name TEXT)")
        .unwrap();

    // Start the slot BEFORE the mutations so they are all captured from the WAL.
    let mut cdc = CdcSource::connect(&conn, "bonsai_cdc_slot")
        .expect("create logical slot (server needs wal_level=logical)");

    db.execute("INSERT INTO bonsai_cdc VALUES (1, 'alice')")
        .unwrap();
    db.execute("UPDATE bonsai_cdc SET name = 'alice2' WHERE id = 1")
        .unwrap();
    db.execute("DELETE FROM bonsai_cdc WHERE id = 1").unwrap();

    let changes = cdc.poll().unwrap();
    let ops: Vec<_> = changes.iter().map(|c| c.op).collect();
    assert_eq!(
        ops,
        vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
        "captured in commit order: {changes:?}"
    );
    assert_eq!(changes[0].table, "public.bonsai_cdc");
    assert_eq!(changes[0].get("id"), Some("1"));
    assert_eq!(changes[0].get("name"), Some("alice"));
    assert_eq!(
        changes[1].get("name"),
        Some("alice2"),
        "update carries new value"
    );
    assert_eq!(changes[2].get("id"), Some("1"), "delete carries the key");

    // A second poll after no further writes is empty (the slot advanced).
    assert!(cdc.poll().unwrap().is_empty(), "slot consumed the changes");

    cdc.drop_slot().ok();
    db.execute("DROP TABLE bonsai_cdc").ok();
}
