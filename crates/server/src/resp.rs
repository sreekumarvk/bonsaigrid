//! RESP2 (Redis serialization protocol) — a fourth protocol on the reactor port, so
//! standard Redis clients and memtier_benchmark can drive BonsaiGrid. Requests are
//! arrays of bulk strings (`*N\r\n$len\r\n<data>\r\n…`). See docs/specs/resp-protocol.md.
//!
//! Scope: PING, ECHO, SET (EX/PX/NX/XX/KEEPTTL), GET, DEL, EXISTS, DBSIZE,
//! FLUSHDB/FLUSHALL, SELECT/AUTH/CONFIG/COMMAND/INFO/RESET (handshake no-ops), QUIT.
//! INCR/DECR/EXPIRE/TTL are phase 2 (RMW / TTL-preserving).

use store::{McAction, Store};

/// Keyspace map for RESP keys (isolated from Hazelcast and memcache maps).
pub const MAP: &str = "redis";

pub enum Frame {
    Need,
    Have(usize),
}

fn crlf(b: &[u8], from: usize) -> Option<usize> {
    b.get(from..)?.windows(2).position(|w| w == b"\r\n").map(|p| from + p)
}

/// Length of the next complete RESP command in `acc` (best-effort on malformed
/// framing — returns the header line so parse() can emit an error).
pub fn frame(acc: &[u8]) -> Frame {
    if acc.is_empty() {
        return Frame::Need;
    }
    if acc[0] != b'*' {
        // Inline command (rare; real clients use arrays).
        return match crlf(acc, 0) {
            Some(e) => Frame::Have(e + 2),
            None => Frame::Need,
        };
    }
    let Some(e0) = crlf(acc, 0) else {
        return Frame::Need;
    };
    let count: i64 = match std::str::from_utf8(&acc[1..e0]).ok().and_then(|s| s.parse().ok()) {
        Some(c) => c,
        None => return Frame::Have(e0 + 2),
    };
    if count <= 0 {
        return Frame::Have(e0 + 2);
    }
    let mut pos = e0 + 2;
    for _ in 0..count {
        if pos >= acc.len() {
            return Frame::Need;
        }
        if acc[pos] != b'$' {
            return Frame::Have(e0 + 2);
        }
        let Some(el) = crlf(acc, pos) else {
            return Frame::Need;
        };
        let len: i64 = match std::str::from_utf8(&acc[pos + 1..el]).ok().and_then(|s| s.parse().ok()) {
            Some(l) => l,
            None => return Frame::Have(e0 + 2),
        };
        pos = el + 2;
        if len < 0 {
            continue;
        }
        let need = pos + len as usize + 2;
        if acc.len() < need {
            return Frame::Need;
        }
        pos = need;
    }
    Frame::Have(pos)
}

/// Extract the bulk-string arguments of one complete command, or None if malformed.
pub fn parse(cmd: &[u8]) -> Option<Vec<Vec<u8>>> {
    if cmd.first() != Some(&b'*') {
        let e = crlf(cmd, 0)?;
        return Some(
            cmd[..e]
                .split(|&b| b == b' ')
                .filter(|t| !t.is_empty())
                .map(|t| t.to_vec())
                .collect(),
        );
    }
    let e0 = crlf(cmd, 0)?;
    let count: i64 = std::str::from_utf8(&cmd[1..e0]).ok()?.parse().ok()?;
    if count < 0 {
        return Some(vec![]);
    }
    let mut pos = e0 + 2;
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if cmd.get(pos) != Some(&b'$') {
            return None;
        }
        let el = crlf(cmd, pos)?;
        let len: i64 = std::str::from_utf8(&cmd[pos + 1..el]).ok()?.parse().ok()?;
        pos = el + 2;
        if len < 0 {
            args.push(Vec::new());
            continue;
        }
        let end = pos + len as usize;
        args.push(cmd.get(pos..end)?.to_vec());
        pos = end + 2;
    }
    Some(args)
}

// ---- reply encoders ----
fn bulk(out: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        Some(d) => {
            out.extend_from_slice(format!("${}\r\n", d.len()).as_bytes());
            out.extend_from_slice(d);
            out.extend_from_slice(b"\r\n");
        }
        None => out.extend_from_slice(b"$-1\r\n"),
    }
}
fn integer(out: &mut Vec<u8>, n: i64) {
    out.extend_from_slice(format!(":{n}\r\n").as_bytes());
}
fn simple(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(format!("+{s}\r\n").as_bytes());
}
fn error(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(format!("-ERR {s}\r\n").as_bytes());
}

fn u64_arg(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v).ok()?.parse().ok()
}

/// Execute one parsed command. Returns the RESP reply and whether to close (`QUIT`).
pub fn execute(store: &Store, args: &[Vec<u8>]) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let Some(verb) = args.first() else {
        error(&mut out, "empty command");
        return (out, false);
    };
    match verb.to_ascii_uppercase().as_slice() {
        b"PING" => match args.get(1) {
            Some(m) => bulk(&mut out, Some(m)),
            None => simple(&mut out, "PONG"),
        },
        b"ECHO" => bulk(&mut out, args.get(1).map(|v| v.as_slice())),
        b"GET" => {
            let v = args.get(1).and_then(|k| store.get(MAP, k));
            bulk(&mut out, v.as_deref());
        }
        b"SET" => set(store, args, &mut out),
        b"DEL" => {
            let mut n = 0;
            for k in &args[1..] {
                if store.remove(MAP, k).is_some() {
                    n += 1;
                }
            }
            integer(&mut out, n);
        }
        b"EXISTS" => {
            let mut n = 0;
            for k in &args[1..] {
                if store.get(MAP, k).is_some() {
                    n += 1;
                }
            }
            integer(&mut out, n);
        }
        b"DBSIZE" => integer(&mut out, store.size(MAP) as i64),
        b"FLUSHDB" | b"FLUSHALL" => {
            store.clear(MAP);
            simple(&mut out, "OK");
        }
        b"SELECT" | b"AUTH" => simple(&mut out, "OK"),
        b"CONFIG" | b"COMMAND" => out.extend_from_slice(b"*0\r\n"),
        b"INFO" => bulk(&mut out, Some(b"# Server\r\nserver:bonsaigrid\r\n")),
        b"RESET" => simple(&mut out, "RESET"),
        b"QUIT" => {
            simple(&mut out, "OK");
            return (out, true);
        }
        _ => error(
            &mut out,
            &format!("unknown command '{}'", String::from_utf8_lossy(verb)),
        ),
    }
    (out, false)
}

fn set(store: &Store, args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() < 3 {
        return error(out, "wrong number of arguments for 'set'");
    }
    let (key, val) = (&args[1], &args[2]);
    let mut ttl_ms: u64 = 0;
    let (mut nx, mut xx) = (false, false);
    let mut i = 3;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"EX" => {
                i += 1;
                ttl_ms = args.get(i).and_then(|v| u64_arg(v)).unwrap_or(0) * 1000;
            }
            b"PX" => {
                i += 1;
                ttl_ms = args.get(i).and_then(|v| u64_arg(v)).unwrap_or(0);
            }
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"KEEPTTL" => {}
            _ => return error(out, "syntax error"),
        }
        i += 1;
    }
    if nx || xx {
        let stored = store.mc_update(MAP, key, |cur| {
            let present = cur.is_some();
            if (nx && present) || (xx && !present) {
                (McAction::Keep, false)
            } else {
                (McAction::Store(val.clone(), ttl_ms), true)
            }
        });
        if stored {
            simple(out, "OK");
        } else {
            out.extend_from_slice(b"$-1\r\n"); // nil: condition not met
        }
    } else {
        store.put_ttl(MAP, key.clone(), val.clone(), ttl_ms);
        simple(out, "OK");
    }
}

#[cfg(test)]
mod tests;
