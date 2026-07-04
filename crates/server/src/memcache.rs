//! Memcached ASCII (text) protocol: framing, parsing, and execution against the
//! store. A third protocol on the reactor's shared port alongside the Hazelcast
//! binary client and REST. See docs/specs/memcache-protocol.md.
//!
//! Scope (this change): get, gets, set, add, replace, cas, delete, touch,
//! flush_all, version, verbosity, quit, plus `noreply`, flags, and exptime.
//! incr/decr/append/prepend are phase 2 (they must preserve the existing TTL).

use std::sync::atomic::{AtomicU64, Ordering};
use store::{McAction, Store};

/// The store map memcached keys live in (isolated from Hazelcast maps).
pub const MAP: &str = "memcache";
/// Item value cap, matching memcached's default 1 MiB.
pub const MAX_VALUE: usize = 1 << 20;
const MAX_KEY: usize = 250;
const HDR: usize = 12; // value blob header: [flags: u32 LE][cas: u64 LE]

// ---- value header ----------------------------------------------------------

fn pack(flags: u32, cas: u64, data: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(HDR + data.len());
    v.extend_from_slice(&flags.to_le_bytes());
    v.extend_from_slice(&cas.to_le_bytes());
    v.extend_from_slice(data);
    v
}

/// (flags, cas, data) from a stored blob, or None if it is too short to be one.
pub fn unpack(blob: &[u8]) -> Option<(u32, u64, &[u8])> {
    if blob.len() < HDR {
        return None;
    }
    let flags = u32::from_le_bytes(blob[0..4].try_into().unwrap());
    let cas = u64::from_le_bytes(blob[4..12].try_into().unwrap());
    Some((flags, cas, &blob[HDR..]))
}

// ---- exptime ---------------------------------------------------------------

/// memcached exptime semantics → the store's relative-ms TTL.
#[derive(Debug, PartialEq, Eq)]
pub enum Ttl {
    Never,
    Ms(u64),
    Past, // negative, or an absolute time already elapsed → immediately expired
}

pub fn classify_exptime(exptime: i64, now_unix: u64) -> Ttl {
    if exptime == 0 {
        Ttl::Never
    } else if exptime < 0 {
        Ttl::Past
    } else if exptime <= 2_592_000 {
        Ttl::Ms(exptime as u64 * 1000)
    } else {
        let abs = exptime as u64;
        if abs <= now_unix {
            Ttl::Past
        } else {
            Ttl::Ms((abs - now_unix) * 1000)
        }
    }
}

// ---- framing ---------------------------------------------------------------

pub enum Frame {
    /// Need more bytes before a complete command is present.
    Need,
    /// The next `n` bytes of the buffer are exactly one complete command.
    Have(usize),
}

fn find_crlf(b: &[u8]) -> Option<usize> {
    b.windows(2).position(|w| w == b"\r\n")
}

fn tokens(line: &[u8]) -> Vec<&[u8]> {
    line.split(|&b| b == b' ').filter(|t| !t.is_empty()).collect()
}

fn is_storage(verb: &[u8]) -> bool {
    matches!(
        verb,
        b"set" | b"add" | b"replace" | b"append" | b"prepend" | b"cas"
    )
}

/// How many leading bytes of `acc` form the next complete command (line, plus the
/// data block for storage commands). `Need` if the buffer is short.
pub fn frame(acc: &[u8]) -> Frame {
    let Some(eol) = find_crlf(acc) else {
        return Frame::Need;
    };
    let line_end = eol + 2;
    let toks = tokens(&acc[..eol]);
    if toks.first().map(|v| is_storage(v)).unwrap_or(false) {
        // <bytes> is the 5th token (verb key flags exptime bytes ...).
        match toks
            .get(4)
            .and_then(|t| std::str::from_utf8(t).ok())
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(bytes) => {
                let need = line_end + bytes + 2; // data + trailing CRLF
                if acc.len() >= need {
                    Frame::Have(need)
                } else {
                    Frame::Need
                }
            }
            None => Frame::Have(line_end), // malformed storage line; parse() will error
        }
    } else {
        Frame::Have(line_end)
    }
}

// ---- parsing ---------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum StoreOp {
    Set,
    Add,
    Replace,
    Cas,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Get { keys: Vec<Vec<u8>>, with_cas: bool },
    Store { op: StoreOp, key: Vec<u8>, flags: u32, exptime: i64, cas: u64, data: Vec<u8>, noreply: bool },
    Delete { key: Vec<u8>, noreply: bool },
    Touch { key: Vec<u8>, exptime: i64, noreply: bool },
    FlushAll { noreply: bool },
    Version,
    Verbosity { noreply: bool },
    Quit,
    /// Unknown command → `ERROR`.
    Error,
    /// Malformed command → `CLIENT_ERROR <msg>`.
    ClientError(&'static str),
    /// A recognized storage/RMW verb that isn't implemented yet (phase 2).
    NotImplemented,
}

fn valid_key(k: &[u8]) -> bool {
    !k.is_empty() && k.len() <= MAX_KEY && k.iter().all(|&b| b > 0x20 && b != 0x7f)
}

fn num<T: std::str::FromStr>(t: &[u8]) -> Option<T> {
    std::str::from_utf8(t).ok()?.parse().ok()
}

fn is_noreply(t: Option<&&[u8]>) -> bool {
    t == Some(&&b"noreply"[..])
}

/// Parse one complete command (the slice `frame` returned `Have(n)` for).
pub fn parse(cmd: &[u8]) -> Command {
    let eol = find_crlf(cmd).unwrap_or(cmd.len());
    let toks = tokens(&cmd[..eol]);
    let Some(&verb) = toks.first() else {
        return Command::Error;
    };
    match verb {
        b"get" | b"gets" => {
            let keys: Vec<Vec<u8>> = toks[1..].iter().map(|k| k.to_vec()).collect();
            if keys.is_empty() || keys.iter().any(|k| !valid_key(k)) {
                return Command::ClientError("bad command line format");
            }
            Command::Get { keys, with_cas: verb == b"gets" }
        }
        b"set" | b"add" | b"replace" | b"cas" => {
            let is_cas = verb == b"cas";
            if toks.len() < if is_cas { 6 } else { 5 } {
                return Command::ClientError("bad command line format");
            }
            let key = toks[1].to_vec();
            let (Some(flags), Some(exptime), Some(bytes)) =
                (num::<u32>(toks[2]), num::<i64>(toks[3]), num::<usize>(toks[4]))
            else {
                return Command::ClientError("bad command line format");
            };
            if !valid_key(&key) {
                return Command::ClientError("bad command line format");
            }
            let (cas, nr_idx) = if is_cas {
                match num::<u64>(toks[5]) {
                    Some(c) => (c, 6),
                    None => return Command::ClientError("bad command line format"),
                }
            } else {
                (0, 5)
            };
            let noreply = is_noreply(toks.get(nr_idx));
            let ds = eol + 2;
            let Some(data) = cmd.get(ds..ds + bytes) else {
                return Command::ClientError("bad data chunk");
            };
            if cmd.get(ds + bytes..ds + bytes + 2) != Some(&b"\r\n"[..]) {
                return Command::ClientError("bad data chunk");
            }
            let op = match verb {
                b"set" => StoreOp::Set,
                b"add" => StoreOp::Add,
                b"replace" => StoreOp::Replace,
                _ => StoreOp::Cas,
            };
            Command::Store { op, key, flags, exptime, cas, data: data.to_vec(), noreply }
        }
        b"append" | b"prepend" | b"incr" | b"decr" => Command::NotImplemented,
        b"delete" => {
            if toks.len() < 2 || !valid_key(toks[1]) {
                return Command::ClientError("bad command line format");
            }
            Command::Delete { key: toks[1].to_vec(), noreply: is_noreply(toks.get(2)) }
        }
        b"touch" => {
            if toks.len() < 3 || !valid_key(toks[1]) {
                return Command::ClientError("bad command line format");
            }
            let Some(exptime) = num::<i64>(toks[2]) else {
                return Command::ClientError("invalid exptime argument");
            };
            Command::Touch { key: toks[1].to_vec(), exptime, noreply: is_noreply(toks.get(3)) }
        }
        b"flush_all" => Command::FlushAll { noreply: is_noreply(toks.last()) },
        b"version" => Command::Version,
        b"verbosity" => Command::Verbosity { noreply: is_noreply(toks.last()) },
        b"quit" => Command::Quit,
        _ => Command::Error,
    }
}

// ---- execution -------------------------------------------------------------

/// Execute one parsed command against the store. Returns the reply bytes (empty
/// when suppressed by `noreply`) and whether the connection should close (`quit`).
pub fn execute(store: &Store, cmd: &Command, now_unix: u64, cas_ctr: &AtomicU64, version: &str) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let next_cas = || cas_ctr.fetch_add(1, Ordering::Relaxed) + 1;
    match cmd {
        Command::Get { keys, with_cas } => {
            for k in keys {
                if let Some(blob) = store.get(MAP, k) {
                    if let Some((flags, cas, data)) = unpack(&blob) {
                        out.extend_from_slice(b"VALUE ");
                        out.extend_from_slice(k);
                        out.extend_from_slice(format!(" {} {}", flags, data.len()).as_bytes());
                        if *with_cas {
                            out.extend_from_slice(format!(" {cas}").as_bytes());
                        }
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(data);
                        out.extend_from_slice(b"\r\n");
                    }
                }
            }
            out.extend_from_slice(b"END\r\n");
        }
        Command::Store { op, key, flags, exptime, cas, data, noreply } => {
            let reply: &[u8] = if data.len() > MAX_VALUE {
                b"SERVER_ERROR object too large for cache\r\n"
            } else {
                let ttl = classify_exptime(*exptime, now_unix);
                let ttl_ms = match ttl {
                    Ttl::Never => 0,
                    Ttl::Ms(m) => m,
                    Ttl::Past => 0, // stored-then-expired: we ensure absence below
                };
                let expired = ttl == Ttl::Past;
                match op {
                    StoreOp::Set => {
                        if expired {
                            store.remove(MAP, key);
                        } else {
                            store.put_ttl(MAP, key.clone(), pack(*flags, next_cas(), data), ttl_ms);
                        }
                        b"STORED\r\n"
                    }
                    StoreOp::Add => store.mc_update(MAP, key, |cur| {
                        if cur.is_some() {
                            (McAction::Keep, &b"NOT_STORED\r\n"[..])
                        } else if expired {
                            (McAction::Remove, &b"STORED\r\n"[..])
                        } else {
                            (McAction::Store(pack(*flags, next_cas(), data), ttl_ms), &b"STORED\r\n"[..])
                        }
                    }),
                    StoreOp::Replace => store.mc_update(MAP, key, |cur| {
                        if cur.is_none() {
                            (McAction::Keep, &b"NOT_STORED\r\n"[..])
                        } else if expired {
                            (McAction::Remove, &b"STORED\r\n"[..])
                        } else {
                            (McAction::Store(pack(*flags, next_cas(), data), ttl_ms), &b"STORED\r\n"[..])
                        }
                    }),
                    StoreOp::Cas => store.mc_update(MAP, key, |cur| match cur.and_then(unpack) {
                        None => (McAction::Keep, &b"NOT_FOUND\r\n"[..]),
                        Some((_, cur_cas, _)) if cur_cas != *cas => (McAction::Keep, &b"EXISTS\r\n"[..]),
                        Some(_) if expired => (McAction::Remove, &b"STORED\r\n"[..]),
                        Some(_) => (McAction::Store(pack(*flags, next_cas(), data), ttl_ms), &b"STORED\r\n"[..]),
                    }),
                }
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::Delete { key, noreply } => {
            let reply: &[u8] = if store.remove(MAP, key).is_some() {
                b"DELETED\r\n"
            } else {
                b"NOT_FOUND\r\n"
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::Touch { key, exptime, noreply } => {
            let ttl = classify_exptime(*exptime, now_unix);
            let reply = store.mc_update(MAP, key, |cur| match cur {
                None => (McAction::Keep, &b"NOT_FOUND\r\n"[..]),
                Some(blob) => match ttl {
                    Ttl::Past => (McAction::Remove, &b"TOUCHED\r\n"[..]),
                    Ttl::Never => (McAction::Store(blob.to_vec(), 0), &b"TOUCHED\r\n"[..]),
                    Ttl::Ms(m) => (McAction::Store(blob.to_vec(), m), &b"TOUCHED\r\n"[..]),
                },
            });
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::FlushAll { noreply } => {
            store.clear(MAP);
            if !noreply {
                out.extend_from_slice(b"OK\r\n");
            }
        }
        Command::Version => {
            out.extend_from_slice(format!("VERSION bonsaigrid-{version}\r\n").as_bytes());
        }
        Command::Verbosity { noreply } => {
            if !noreply {
                out.extend_from_slice(b"OK\r\n");
            }
        }
        Command::Quit => return (out, true),
        Command::Error | Command::NotImplemented => out.extend_from_slice(b"ERROR\r\n"),
        Command::ClientError(msg) => {
            out.extend_from_slice(format!("CLIENT_ERROR {msg}\r\n").as_bytes());
        }
    }
    (out, false)
}

#[cfg(test)]
mod tests;
