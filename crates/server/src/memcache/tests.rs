//! Codec unit tests + execution/component tests (drive `execute` against a real
//! Store), mirroring memcached's t/*.t cases: getset, add/replace, cas, delete,
//! touch, flush_all, flags, expiry, noreply, errors, multi-get, maxsize.

use super::*;

fn have(acc: &[u8]) -> Option<usize> {
    match frame(acc) {
        Frame::Have(n) => Some(n),
        Frame::Need => None,
    }
}

// ---- framing ----

#[test]
fn frame_needs_full_line() {
    assert_eq!(have(b"get foo"), None); // no CRLF yet
    assert_eq!(have(b"get foo\r\n"), Some(9));
}

#[test]
fn frame_storage_waits_for_data_block() {
    assert_eq!(have(b"set k 0 0 5\r\nhel"), None); // data incomplete
    assert_eq!(have(b"set k 0 0 5\r\nhello\r\n"), Some(20)); // 13 (line) + 5 (data) + 2 (CRLF)
                                                             // extra bytes after one command are not consumed
    assert_eq!(have(b"set k 0 0 5\r\nhello\r\nget k\r\n"), Some(20));
}

// ---- parse ----

#[test]
fn parse_get_and_gets() {
    assert_eq!(
        parse(b"get a b c\r\n"),
        Command::Get {
            keys: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            with_cas: false
        }
    );
    assert!(matches!(
        parse(b"gets x\r\n"),
        Command::Get { with_cas: true, .. }
    ));
    assert!(matches!(parse(b"get\r\n"), Command::ClientError(_))); // no keys
}

#[test]
fn parse_set_with_flags_exptime_noreply() {
    let c = parse(b"set key 42 100 5 noreply\r\nhello\r\n");
    assert_eq!(
        c,
        Command::Store {
            op: StoreOp::Set,
            key: b"key".to_vec(),
            flags: 42,
            exptime: 100,
            cas: 0,
            data: b"hello".to_vec(),
            noreply: true
        }
    );
}

#[test]
fn parse_cas_carries_unique() {
    let c = parse(b"cas key 0 0 2 777\r\nhi\r\n");
    assert_eq!(
        c,
        Command::Store {
            op: StoreOp::Cas,
            key: b"key".to_vec(),
            flags: 0,
            exptime: 0,
            cas: 777,
            data: b"hi".to_vec(),
            noreply: false
        }
    );
}

#[test]
fn parse_rejects_bad_keys_and_verbs() {
    assert!(matches!(
        parse(b"set \x01bad 0 0 1\r\nx\r\n"),
        Command::ClientError(_)
    ));
    let long = format!("get {}\r\n", "k".repeat(251));
    assert!(matches!(parse(long.as_bytes()), Command::ClientError(_)));
    assert_eq!(parse(b"frobnicate x\r\n"), Command::Error);
    assert_eq!(parse(b"incr k 1\r\n"), Command::NotImplemented);
}

#[test]
fn parse_delete_touch_admin() {
    assert_eq!(
        parse(b"delete k noreply\r\n"),
        Command::Delete {
            key: b"k".to_vec(),
            noreply: true
        }
    );
    assert_eq!(
        parse(b"touch k 30\r\n"),
        Command::Touch {
            key: b"k".to_vec(),
            exptime: 30,
            noreply: false
        }
    );
    assert_eq!(
        parse(b"flush_all\r\n"),
        Command::FlushAll { noreply: false }
    );
    assert_eq!(parse(b"version\r\n"), Command::Version);
    assert_eq!(parse(b"quit\r\n"), Command::Quit);
}

// ---- header + exptime ----

#[test]
fn header_round_trips() {
    let blob = pack(0xDEADBEEF, 12345, b"payload");
    assert_eq!(unpack(&blob), Some((0xDEADBEEF, 12345, &b"payload"[..])));
    assert_eq!(unpack(b"short"), None);
}

#[test]
fn exptime_semantics() {
    assert_eq!(classify_exptime(0, 1000), Ttl::Never);
    assert_eq!(classify_exptime(60, 1000), Ttl::Ms(60_000));
    assert_eq!(classify_exptime(-1, 1000), Ttl::Past);
    // absolute unix time (> 30 days => 2_592_000)
    assert_eq!(classify_exptime(2_600_060, 2_600_000), Ttl::Ms(60_000)); // 60s in the future
    assert_eq!(classify_exptime(2_600_000, 2_600_060), Ttl::Past); // already elapsed
}

// ---- execution (component) ----

fn run(store: &Store, cas: &AtomicU64, line: &[u8]) -> String {
    let (out, _) = execute(store, &parse(line), 1_000_000, cas, "test");
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn getset_roundtrip_with_flags() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    assert_eq!(run(&s, &cas, b"set k 7 0 5\r\nhello\r\n"), "STORED\r\n");
    assert_eq!(
        run(&s, &cas, b"get k\r\n"),
        "VALUE k 7 5\r\nhello\r\nEND\r\n"
    );
    assert_eq!(run(&s, &cas, b"get missing\r\n"), "END\r\n"); // miss
                                                              // overwrite
    assert_eq!(run(&s, &cas, b"set k 9 0 3\r\nbye\r\n"), "STORED\r\n");
    assert_eq!(run(&s, &cas, b"get k\r\n"), "VALUE k 9 3\r\nbye\r\nEND\r\n");
}

#[test]
fn add_and_replace() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    assert_eq!(run(&s, &cas, b"add k 0 0 1\r\na\r\n"), "STORED\r\n");
    assert_eq!(run(&s, &cas, b"add k 0 0 1\r\nb\r\n"), "NOT_STORED\r\n"); // exists
    assert_eq!(run(&s, &cas, b"replace k 0 0 1\r\nc\r\n"), "STORED\r\n");
    assert_eq!(
        run(&s, &cas, b"replace absent 0 0 1\r\nx\r\n"),
        "NOT_STORED\r\n"
    );
    assert_eq!(run(&s, &cas, b"get k\r\n"), "VALUE k 0 1\r\nc\r\nEND\r\n");
}

#[test]
fn cas_match_stale_and_missing() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    run(&s, &cas, b"set k 0 0 1\r\na\r\n");
    let gets = run(&s, &cas, b"gets k\r\n"); // VALUE k 0 1 <cas>\r\na\r\nEND\r\n
    let unique: u64 = gets.split_whitespace().nth(3).unwrap().parse().unwrap();
    assert_eq!(
        run(
            &s,
            &cas,
            format!("cas k 0 0 1 {}\r\nb\r\n", unique).as_bytes()
        ),
        "STORED\r\n"
    );
    // now stale
    assert_eq!(
        run(
            &s,
            &cas,
            format!("cas k 0 0 1 {}\r\nc\r\n", unique).as_bytes()
        ),
        "EXISTS\r\n"
    );
    assert_eq!(
        run(&s, &cas, b"cas absent 0 0 1 1\r\nx\r\n"),
        "NOT_FOUND\r\n"
    );
}

#[test]
fn delete_hit_and_miss() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    run(&s, &cas, b"set k 0 0 1\r\na\r\n");
    assert_eq!(run(&s, &cas, b"delete k\r\n"), "DELETED\r\n");
    assert_eq!(run(&s, &cas, b"delete k\r\n"), "NOT_FOUND\r\n");
    assert_eq!(run(&s, &cas, b"get k\r\n"), "END\r\n");
}

#[test]
fn touch_updates_and_misses() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    run(&s, &cas, b"set k 0 0 1\r\na\r\n");
    assert_eq!(run(&s, &cas, b"touch k 100\r\n"), "TOUCHED\r\n");
    assert_eq!(run(&s, &cas, b"get k\r\n"), "VALUE k 0 1\r\na\r\nEND\r\n"); // still there
    assert_eq!(run(&s, &cas, b"touch absent 100\r\n"), "NOT_FOUND\r\n");
}

#[test]
fn flush_all_clears() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    run(&s, &cas, b"set a 0 0 1\r\nx\r\n");
    run(&s, &cas, b"set b 0 0 1\r\ny\r\n");
    assert_eq!(run(&s, &cas, b"flush_all\r\n"), "OK\r\n");
    assert_eq!(run(&s, &cas, b"get a\r\n"), "END\r\n");
    assert_eq!(run(&s, &cas, b"get b\r\n"), "END\r\n");
}

#[test]
fn noreply_suppresses_output() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    assert_eq!(run(&s, &cas, b"set k 0 0 1 noreply\r\na\r\n"), "");
    assert_eq!(run(&s, &cas, b"delete k noreply\r\n"), "");
    assert_eq!(run(&s, &cas, b"get k\r\n"), "END\r\n"); // deleted, no reply seen
}

#[test]
fn multiget_returns_present_then_end() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    run(&s, &cas, b"set a 0 0 1\r\n1\r\n");
    run(&s, &cas, b"set c 0 0 1\r\n3\r\n");
    assert_eq!(
        run(&s, &cas, b"get a b c\r\n"),
        "VALUE a 0 1\r\n1\r\nVALUE c 0 1\r\n3\r\nEND\r\n"
    ); // b absent
}

#[test]
fn errors_and_value_cap() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    assert_eq!(run(&s, &cas, b"bogus\r\n"), "ERROR\r\n");
    let big = format!("set k 0 0 {}\r\n", MAX_VALUE + 1);
    let mut line = big.into_bytes();
    line.extend(std::iter::repeat(b'x').take(MAX_VALUE + 1));
    line.extend_from_slice(b"\r\n");
    assert!(run(&s, &cas, &line).starts_with("SERVER_ERROR"));
}

#[test]
fn version_and_quit() {
    let s = Store::new();
    let cas = AtomicU64::new(0);
    assert_eq!(run(&s, &cas, b"version\r\n"), "VERSION bonsaigrid-test\r\n");
    let (_, close) = execute(&s, &Command::Quit, 1_000_000, &cas, "test");
    assert!(close);
}
