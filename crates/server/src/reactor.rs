//! Single-core io_uring reactor.
//!
//! Replaces the blocking thread-per-connection model with one event loop driving
//! many connections via io_uring. Per-connection buffers are allocated once at
//! accept and reused across every request — the recv/parse/send hot path makes
//! no per-request socket-buffer allocation. (The codec/dispatch layer still
//! allocates response frames; eliminating that is a later refactor.)
//!
//! Designed to be run once (increment 2) or spawned once per pinned core over a
//! SO_REUSEPORT listener (increment 3).

use io_uring::{opcode, types, IoUring};
use protocol::fixed::read_u16_le;
use protocol::fragment::Reassembler;
use protocol::frame::{message_len, UNFRAGMENTED};
use security::tls::{Handshake, TlsAcceptor, TlsMode};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-connection transport-security state.
enum ConnTls {
    /// Plaintext (TLS disabled, or a permissive connection that spoke plaintext).
    Plain,
    /// Permissive mode: decide TLS-vs-plaintext from the first received byte.
    Detect,
    /// TLS handshake in progress (userspace rustls).
    Handshaking(Box<Handshake>),
    /// kTLS installed — the io_uring data path carries plaintext, kernel crypts.
    Ktls,
}

// Globally-unique connection ids, so a shared event broker can address any
// connection across reactor threads.
static CONN_SEQ: AtomicU64 = AtomicU64::new(1);

// Accepts use the top of the user_data range, one slot per listener.
const ACCEPT_BASE: u64 = u64::MAX - 255;
// Periodic timer used to flush cross-connection events even when a connection
// has no socket activity of its own.
const TIMEOUT_UD: u64 = ACCEPT_BASE - 1;
const RECV_BUF: usize = 64 * 1024;
const OUT_CAP: usize = 4 * 1024 * 1024; // reserved so appends never realloc mid-send

#[derive(PartialEq, Eq, Clone, Copy)]
enum Mode {
    Unknown, // haven't seen the preamble yet
    Binary,  // CP2 client protocol
    Http,    // REST (health endpoints)
}

struct Conn {
    fd: RawFd,
    conn_id: u64,
    rbuf: Box<[u8]>,
    acc: Vec<u8>,
    out: Vec<u8>,
    // Bytes currently being sent. Separate from `out` so appends to `out` can't
    // reallocate the memory an in-flight io_uring Send still references.
    sendbuf: Vec<u8>,
    inflight_send: usize, // bytes currently being sent (0 == no send in flight)
    mode: Mode,
    close_after_flush: bool,
    open: bool,
    frag: Reassembler,
    tls: ConnTls,
}

impl Conn {
    fn new(fd: RawFd, tls: ConnTls) -> Conn {
        let mut out = Vec::new();
        out.reserve_exact(OUT_CAP);
        let mut sendbuf = Vec::new();
        sendbuf.reserve_exact(OUT_CAP);
        Conn {
            fd,
            conn_id: CONN_SEQ.fetch_add(1, Ordering::Relaxed),
            rbuf: vec![0u8; RECV_BUF].into_boxed_slice(),
            acc: Vec::with_capacity(RECV_BUF),
            out,
            sendbuf,
            inflight_send: 0,
            mode: Mode::Unknown,
            close_after_flush: false,
            open: true,
            frag: Reassembler::new(),
            tls,
        }
    }
}

/// The initial per-connection TLS state implied by the node's acceptor.
fn initial_tls(acceptor: &Option<TlsAcceptor>) -> ConnTls {
    match acceptor {
        None => ConnTls::Plain,
        Some(a) => match a.mode() {
            TlsMode::Disabled => ConnTls::Plain,
            TlsMode::Permissive => ConnTls::Detect,
            TlsMode::Required => match a.handshake() {
                Ok(hs) => ConnTls::Handshaking(Box::new(hs)),
                Err(_) => ConnTls::Plain, // config error; fall back (accept will still work)
            },
        },
    }
}

fn recv_ud(id: usize) -> u64 {
    (id as u64) << 1
}
fn send_ud(id: usize) -> u64 {
    ((id as u64) << 1) | 1
}

/// Run the reactor over `listener`, dispatching each parsed request message to
/// `dispatch` (which returns zero or more reply messages). Never returns under
/// normal operation.
/// `dispatch(msg, out)` receives one complete binary request message and appends
/// framed reply bytes to `out`. `http(path, out)` handles a REST request line
/// (target path) and appends a full HTTP response to `out`.
#[allow(clippy::too_many_arguments)]
pub fn run(
    listeners: Vec<std::net::TcpListener>,
    mut dispatch: impl FnMut(&[u8], u64, &mut Vec<u8>),
    http: impl Fn(&str) -> (u16, &'static str, String),
    drain_events: impl Fn(u64, &mut Vec<u8>),
    on_close: impl Fn(u64),
    mut on_tick: impl FnMut(),
    tls: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    let fds: Vec<RawFd> = listeners.iter().map(|l| l.as_raw_fd()).collect();
    let mut ring = IoUring::new(4096)?;
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut pending: Vec<io_uring::squeue::Entry> = Vec::new();

    // Prime one *multishot* accept per listener: a single SQE yields a stream of
    // accept completions, cutting submission churn under connection load.
    for (i, fd) in fds.iter().enumerate() {
        pending.push(
            opcode::AcceptMulti::new(types::Fd(*fd))
                .build()
                .user_data(ACCEPT_BASE + i as u64),
        );
    }

    // Periodic 20ms timer to flush queued cross-connection events.
    let tick = types::Timespec::new().sec(0).nsec(20_000_000);
    pending.push(opcode::Timeout::new(&tick).build().user_data(TIMEOUT_UD));

    let mut flush_events = false;
    loop {
        // Submit everything queued, then wait for at least one completion.
        flush(&mut ring, &mut pending)?;
        ring.submit_and_wait(1)?;

        let cqes: Vec<(u64, i32, u32)> = ring
            .completion()
            .map(|c| (c.user_data(), c.result(), c.flags()))
            .collect();

        for (ud, res, flags) in cqes {
            if ud == TIMEOUT_UD {
                flush_events = true;
                pending.push(opcode::Timeout::new(&tick).build().user_data(TIMEOUT_UD));
                continue;
            }
            if ud >= ACCEPT_BASE {
                let idx = (ud - ACCEPT_BASE) as usize;
                // Multishot: only re-arm if the kernel won't keep delivering.
                if !io_uring::cqueue::more(flags) {
                    pending.push(
                        opcode::AcceptMulti::new(types::Fd(fds[idx]))
                            .build()
                            .user_data(ud),
                    );
                }
                if res >= 0 {
                    let fd = res as RawFd;
                    let id = match free.pop() {
                        Some(i) => {
                            conns[i] = Some(Conn::new(fd, initial_tls(&tls)));
                            i
                        }
                        None => {
                            conns.push(Some(Conn::new(fd, initial_tls(&tls))));
                            conns.len() - 1
                        }
                    };
                    arm_recv(&mut conns, id, &mut pending);
                }
                continue;
            }

            let id = (ud >> 1) as usize;
            let is_send = ud & 1 == 1;
            if conns.get(id).and_then(|c| c.as_ref()).is_none() {
                continue;
            }

            if is_send {
                on_send(&mut conns, id, res, &mut pending);
            } else {
                on_recv(
                    &mut conns,
                    id,
                    res,
                    &mut pending,
                    &mut dispatch,
                    &http,
                    &drain_events,
                    &tls,
                );
            }

            if !conns[id].as_ref().map(|c| c.open).unwrap_or(false) {
                if let Some(c) = conns[id].take() {
                    on_close(c.conn_id);
                    unsafe { libc::close(c.fd) };
                }
                free.push(id);
            }
        }

        // On each timer tick, flush queued events to every binary connection
        // (covers events published by *other* connections).
        if flush_events {
            flush_events = false;
            // Membership maintenance: drain the member→reactor signal, update the
            // authoritative cluster, and enqueue cluster-view events to listeners.
            on_tick();
            for id in 0..conns.len() {
                let ready =
                    matches!(conns.get(id), Some(Some(c)) if c.open && c.mode == Mode::Binary);
                if !ready {
                    continue;
                }
                {
                    let c = conns[id].as_mut().unwrap();
                    drain_events(c.conn_id, &mut c.out);
                }
                maybe_arm_send(&mut conns, id, &mut pending);
            }
        }
    }
}

fn flush(ring: &mut IoUring, pending: &mut Vec<io_uring::squeue::Entry>) -> std::io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut sq = ring.submission();
    for e in pending.drain(..) {
        // Ring is sized (4096) well above our in-flight op count.
        unsafe {
            let _ = sq.push(&e);
        }
    }
    Ok(())
}

fn arm_recv(conns: &mut [Option<Conn>], id: usize, pending: &mut Vec<io_uring::squeue::Entry>) {
    let c = conns[id].as_mut().unwrap();
    let entry = opcode::Recv::new(types::Fd(c.fd), c.rbuf.as_mut_ptr(), c.rbuf.len() as u32)
        .build()
        .user_data(recv_ud(id));
    pending.push(entry);
}

fn maybe_arm_send(
    conns: &mut [Option<Conn>],
    id: usize,
    pending: &mut Vec<io_uring::squeue::Entry>,
) {
    let c = conns[id].as_mut().unwrap();
    if c.inflight_send > 0 {
        return; // a send is already in flight on `sendbuf`
    }
    if c.sendbuf.is_empty() {
        if c.out.is_empty() {
            return;
        }
        // Move queued bytes into the stable send buffer; `out` keeps taking
        // appends without disturbing the in-flight send's memory.
        std::mem::swap(&mut c.sendbuf, &mut c.out);
    }
    c.inflight_send = c.sendbuf.len();
    let entry = opcode::Send::new(types::Fd(c.fd), c.sendbuf.as_ptr(), c.sendbuf.len() as u32)
        .build()
        .user_data(send_ud(id));
    pending.push(entry);
}

#[allow(clippy::too_many_arguments)]
fn on_recv(
    conns: &mut [Option<Conn>],
    id: usize,
    res: i32,
    pending: &mut Vec<io_uring::squeue::Entry>,
    dispatch: &mut impl FnMut(&[u8], u64, &mut Vec<u8>),
    http: &impl Fn(&str) -> (u16, &'static str, String),
    drain_events: &impl Fn(u64, &mut Vec<u8>),
    tls: &Option<TlsAcceptor>,
) {
    if res <= 0 {
        conns[id].as_mut().unwrap().open = false;
        return;
    }
    let n = res as usize;
    // Turn this recv into plaintext in `acc` (driving the TLS handshake when
    // needed). If we're mid-handshake with nothing to dispatch yet, just wait.
    if !tls_ingest(conns, id, n, pending, tls) {
        if conns[id].as_ref().map(|c| c.open).unwrap_or(false) {
            arm_recv(conns, id, pending);
        }
        return;
    }

    // Detect the protocol from the first bytes: "CP2" -> binary client, anything
    // else (an HTTP method) -> REST.
    let c = conns[id].as_mut().unwrap();
    if c.mode == Mode::Unknown {
        if c.acc.len() < 3 {
            arm_recv(conns, id, pending);
            return;
        }
        if &c.acc[..3] == b"CP2" {
            c.mode = Mode::Binary;
            c.acc.drain(0..3);
        } else {
            c.mode = Mode::Http;
        }
    }

    match conns[id].as_ref().unwrap().mode {
        Mode::Http => {
            let c = conns[id].as_mut().unwrap();
            // Wait for the end of the request headers.
            if let Some(pos) = find_subslice(&c.acc, b"\r\n\r\n") {
                let path = request_target(&c.acc[..pos]);
                let (status, ctype, body) = http(&path);
                write_http_response(&mut c.out, status, ctype, &body);
                c.acc.clear();
                c.close_after_flush = true; // HTTP/1.0-style: one request per connection
                maybe_arm_send(conns, id, pending);
            } else {
                arm_recv(conns, id, pending);
            }
        }
        Mode::Binary => {
            loop {
                let c = conns[id].as_mut().unwrap();
                let Some(len) = message_len(&c.acc) else {
                    break;
                };
                // First-frame flags: both fragment bits set == a complete,
                // unfragmented message (the common, fast path).
                if read_u16_le(&c.acc, 4) & UNFRAGMENTED == UNFRAGMENTED {
                    let Conn {
                        acc, out, conn_id, ..
                    } = c;
                    dispatch(&acc[..len], *conn_id, out);
                    acc.drain(0..len);
                } else {
                    // A fragment: reassemble; dispatch once the message completes.
                    let frag: Vec<u8> = c.acc[..len].to_vec();
                    c.acc.drain(0..len);
                    if let Some(assembled) = c.frag.push(&frag) {
                        let Conn { out, conn_id, .. } = c;
                        dispatch(&assembled, *conn_id, out);
                    }
                }
            }
            // Flush any entry-listener events queued for this connection.
            let c = conns[id].as_mut().unwrap();
            drain_events(c.conn_id, &mut c.out);
            maybe_arm_send(conns, id, pending);
            arm_recv(conns, id, pending);
        }
        Mode::Unknown => unreachable!(),
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Extract the request-target from an HTTP request's first line
/// (`GET /path HTTP/1.1`).
fn request_target(headers: &[u8]) -> String {
    let line_end = find_subslice(headers, b"\r\n").unwrap_or(headers.len());
    let line = &headers[..line_end];
    let mut parts = line.split(|&b| b == b' ');
    let _method = parts.next();
    match parts.next() {
        Some(p) => String::from_utf8_lossy(p).into_owned(),
        None => String::new(),
    }
}

fn write_http_response(out: &mut Vec<u8>, status: u16, ctype: &str, body: &str) {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    out.extend_from_slice(head.as_bytes());
    out.extend_from_slice(body.as_bytes());
}

fn on_send(
    conns: &mut [Option<Conn>],
    id: usize,
    res: i32,
    pending: &mut Vec<io_uring::squeue::Entry>,
) {
    if res < 0 {
        conns[id].as_mut().unwrap().open = false;
        return;
    }
    let c = conns[id].as_mut().unwrap();
    let sent = res as usize;
    c.sendbuf.drain(0..sent); // consume the acknowledged prefix (handles partial sends)
    c.inflight_send = 0;
    if c.sendbuf.is_empty() && c.out.is_empty() && c.close_after_flush {
        c.open = false; // HTTP response fully sent -> close
        return;
    }
    // If more queued (or a partial send), arm the next send.
    maybe_arm_send(conns, id, pending);
}

/// Turn a recv into plaintext appended to the connection's `acc`, advancing the
/// TLS handshake when needed. Returns `true` if there is plaintext to try
/// dispatching, `false` if we're mid-handshake (the caller just waits for more).
fn tls_ingest(
    conns: &mut [Option<Conn>],
    id: usize,
    n: usize,
    pending: &mut Vec<io_uring::squeue::Entry>,
    tls: &Option<TlsAcceptor>,
) -> bool {
    let mut start_handshake = false;
    {
        let c = conns[id].as_mut().unwrap();
        match c.tls {
            // Plaintext (or kTLS: the kernel already decrypted the bytes).
            ConnTls::Plain | ConnTls::Ktls => {
                c.acc.extend_from_slice(&c.rbuf[..n]);
                return true;
            }
            // Permissive: a leading 0x16 is a TLS handshake record; else plaintext.
            ConnTls::Detect => {
                if n > 0 && c.rbuf[0] == 0x16 {
                    start_handshake = true;
                } else {
                    c.tls = ConnTls::Plain;
                    c.acc.extend_from_slice(&c.rbuf[..n]);
                    return true;
                }
            }
            ConnTls::Handshaking(_) => {}
        }
    }
    if start_handshake {
        match tls.as_ref().and_then(|a| a.handshake().ok()) {
            Some(hs) => conns[id].as_mut().unwrap().tls = ConnTls::Handshaking(Box::new(hs)),
            None => {
                conns[id].as_mut().unwrap().open = false;
                return false;
            }
        }
    }
    pump_handshake(conns, id, n, pending)
}

/// Feed a recv into the in-progress rustls handshake; send its output; install
/// kTLS at a clean record boundary once the handshake completes. Returns `true`
/// if application plaintext was produced (proceed to dispatch).
fn pump_handshake(
    conns: &mut [Option<Conn>],
    id: usize,
    n: usize,
    pending: &mut Vec<io_uring::squeue::Entry>,
) -> bool {
    {
        let c = conns[id].as_mut().unwrap();
        // Move the handshake out so we can borrow `rbuf`/`out` freely.
        let mut hs = match std::mem::replace(&mut c.tls, ConnTls::Plain) {
            ConnTls::Handshaking(hs) => hs,
            other => {
                c.tls = other;
                return true;
            }
        };
        let (mut send, mut plain) = (Vec::new(), Vec::new());
        match hs.pump(&c.rbuf[..n], &mut send, &mut plain) {
            Ok(ready) => {
                if !send.is_empty() {
                    c.out.extend_from_slice(&send);
                }
                if !plain.is_empty() {
                    c.acc.extend_from_slice(&plain);
                }
                if ready {
                    match hs.into_ktls(c.fd) {
                        Ok(()) => c.tls = ConnTls::Ktls,
                        Err(_) => c.open = false,
                    }
                } else {
                    c.tls = ConnTls::Handshaking(hs);
                }
            }
            Err(_) => c.open = false,
        }
    }
    // Flush any handshake output, then dispatch only if we produced plaintext.
    if conns[id].as_ref().map(|c| c.open).unwrap_or(false) {
        maybe_arm_send(conns, id, pending);
        return conns[id]
            .as_ref()
            .map(|c| !c.acc.is_empty())
            .unwrap_or(false);
    }
    false
}
