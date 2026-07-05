//! Live integration test for the JDBC/PostgreSQL connector. Gated on a real database:
//! set `BONSAI_TEST_POSTGRES` to a connection string (e.g. from a dockerized Postgres)
//! to run it; when unset it is skipped, so CI without a DB stays green.
//!
//!   docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16-alpine
//!   BONSAI_TEST_POSTGRES='host=127.0.0.1 user=postgres password=pw' cargo test -p server --test jdbc_postgres

use server::jdbc::JdbcSource;
use store::Store;

#[test]
fn loads_a_postgres_table_into_an_imap() {
    let Ok(conn) = std::env::var("BONSAI_TEST_POSTGRES") else {
        eprintln!("skipping jdbc_postgres: set BONSAI_TEST_POSTGRES to a Postgres conn string");
        return;
    };
    let mut src = JdbcSource::connect(&conn).expect("connect to postgres");

    // Fresh schema + rows spanning text / bigint / bool.
    src.execute("DROP TABLE IF EXISTS bonsai_users").unwrap();
    src.execute(
        "CREATE TABLE bonsai_users (id INT PRIMARY KEY, name TEXT, score BIGINT, active BOOL)",
    )
    .unwrap();
    src.execute("INSERT INTO bonsai_users VALUES (1,'alice',100,true),(2,'bob',250,false)")
        .unwrap();

    // Load the table into an IMap: first column (id) is the key, the rest a json value.
    let store = Store::new();
    let n = src
        .load_into_map(
            &store,
            "users",
            "SELECT id, name, score, active FROM bonsai_users ORDER BY id",
        )
        .unwrap();
    assert_eq!(n, 2, "two rows loaded");

    let s1 = String::from_utf8(store.get("users", b"1").expect("row 1 in the map")).unwrap();
    assert!(s1.contains("\"name\":\"alice\""), "value: {s1}");
    assert!(s1.contains("\"score\":100"), "bigint bare: {s1}");
    assert!(s1.contains("\"active\":true"), "bool bare: {s1}");

    let s2 = String::from_utf8(store.get("users", b"2").expect("row 2 in the map")).unwrap();
    assert!(
        s2.contains("\"name\":\"bob\"") && s2.contains("\"active\":false"),
        "value: {s2}"
    );

    // query_rows returns the same (key, json) pairs directly.
    let rows = src
        .query_rows("SELECT id, name FROM bonsai_users WHERE id = 1")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "1");
    assert_eq!(
        String::from_utf8(rows[0].1.clone()).unwrap(),
        "{\"name\":\"alice\"}"
    );

    src.execute("DROP TABLE bonsai_users").ok();
}
