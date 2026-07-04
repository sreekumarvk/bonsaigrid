use super::*;

fn have(acc: &[u8]) -> Option<usize> {
    match frame(acc) {
        Frame::Have(n) => Some(n),
        Frame::Need => None,
    }
}
// build a RESP array request from string args
fn req(parts: &[&str]) -> Vec<u8> {
    let mut v = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        v.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    v
}
fn run(store: &Store, parts: &[&str]) -> String {
    let (out, _) = execute(store, &parse(&req(parts)).unwrap());
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn frame_waits_for_full_array() {
    let full = req(&["SET", "k", "v"]);
    assert_eq!(have(&full), Some(full.len()));
    assert_eq!(have(&full[..full.len() - 3]), None); // data incomplete
    assert_eq!(have(b"*2\r\n$3\r\nGET"), None); // missing value+crlf
}

#[test]
fn parse_extracts_bulk_args() {
    assert_eq!(parse(&req(&["SET", "key", "val"])).unwrap(),
        vec![b"SET".to_vec(), b"key".to_vec(), b"val".to_vec()]);
    // binary-safe value with embedded CRLF
    let mut m = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n".to_vec();
    m.extend_from_slice(b"a\r\nb\r\n");
    assert_eq!(parse(&m).unwrap()[2], b"a\r\nb");
}

#[test]
fn set_get_del_exists() {
    let s = Store::new();
    assert_eq!(run(&s, &["SET", "k", "hello"]), "+OK\r\n");
    assert_eq!(run(&s, &["GET", "k"]), "$5\r\nhello\r\n");
    assert_eq!(run(&s, &["GET", "missing"]), "$-1\r\n");
    assert_eq!(run(&s, &["EXISTS", "k", "missing"]), ":1\r\n");
    assert_eq!(run(&s, &["DEL", "k"]), ":1\r\n");
    assert_eq!(run(&s, &["GET", "k"]), "$-1\r\n");
}

#[test]
fn set_nx_xx() {
    let s = Store::new();
    assert_eq!(run(&s, &["SET", "k", "1", "NX"]), "+OK\r\n"); // absent → stored
    assert_eq!(run(&s, &["SET", "k", "2", "NX"]), "$-1\r\n"); // present → nil
    assert_eq!(run(&s, &["SET", "k", "3", "XX"]), "+OK\r\n"); // present → stored
    assert_eq!(run(&s, &["GET", "k"]), "$1\r\n3\r\n");
    assert_eq!(run(&s, &["SET", "absent", "9", "XX"]), "$-1\r\n"); // absent → nil
}

#[test]
fn ping_echo_flush_quit_unknown() {
    let s = Store::new();
    assert_eq!(run(&s, &["PING"]), "+PONG\r\n");
    assert_eq!(run(&s, &["PING", "hi"]), "$2\r\nhi\r\n");
    assert_eq!(run(&s, &["ECHO", "yo"]), "$2\r\nyo\r\n");
    run(&s, &["SET", "a", "1"]);
    assert_eq!(run(&s, &["FLUSHALL"]), "+OK\r\n");
    assert_eq!(run(&s, &["GET", "a"]), "$-1\r\n");
    assert!(run(&s, &["FROBNICATE"]).starts_with("-ERR unknown command"));
    let (_, close) = execute(&s, &parse(&req(&["QUIT"])).unwrap());
    assert!(close);
}

#[test]
fn handshake_noops() {
    let s = Store::new();
    assert_eq!(run(&s, &["SELECT", "0"]), "+OK\r\n");
    assert_eq!(run(&s, &["CONFIG", "GET", "maxmemory"]), "*0\r\n");
    assert_eq!(run(&s, &["COMMAND"]), "*0\r\n");
}
