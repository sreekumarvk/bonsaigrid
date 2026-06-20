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
use protocol::frame::message_len;
use std::os::fd::{AsRawFd, RawFd};

// Accepts use the top of the user_data range, one slot per listener.
const ACCEPT_BASE: u64 = u64::MAX - 255;
const RECV_BUF: usize = 64 * 1024;
const OUT_CAP: usize = 4 * 1024 * 1024; // reserved so appends never realloc mid-send

struct Conn {
    fd: RawFd,
    rbuf: Box<[u8]>,
    acc: Vec<u8>,
    out: Vec<u8>,
    inflight_send: usize, // bytes currently being sent (0 == no send in flight)
    preamble_left: u8,
    open: bool,
}

impl Conn {
    fn new(fd: RawFd) -> Conn {
        let mut out = Vec::new();
        out.reserve_exact(OUT_CAP);
        Conn {
            fd,
            rbuf: vec![0u8; RECV_BUF].into_boxed_slice(),
            acc: Vec::with_capacity(RECV_BUF),
            out,
            inflight_send: 0,
            preamble_left: 3,
            open: true,
        }
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
/// `dispatch(msg, out)` receives one complete request message as a byte slice and
/// appends framed reply bytes (responses and pushed events) to `out`.
pub fn run(
    listeners: Vec<std::net::TcpListener>,
    mut dispatch: impl FnMut(&[u8], &mut Vec<u8>),
) -> std::io::Result<()> {
    let fds: Vec<RawFd> = listeners.iter().map(|l| l.as_raw_fd()).collect();
    let mut ring = IoUring::new(4096)?;
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut pending: Vec<io_uring::squeue::Entry> = Vec::new();

    // Prime one accept per listener.
    for (i, fd) in fds.iter().enumerate() {
        pending.push(
            opcode::Accept::new(types::Fd(*fd), std::ptr::null_mut(), std::ptr::null_mut())
                .build()
                .user_data(ACCEPT_BASE + i as u64),
        );
    }

    loop {
        // Submit everything queued, then wait for at least one completion.
        flush(&mut ring, &mut pending)?;
        ring.submit_and_wait(1)?;

        let cqes: Vec<(u64, i32)> = ring
            .completion()
            .map(|c| (c.user_data(), c.result()))
            .collect();

        for (ud, res) in cqes {
            if ud >= ACCEPT_BASE {
                let idx = (ud - ACCEPT_BASE) as usize;
                // Re-arm this listener's accept regardless.
                pending.push(
                    opcode::Accept::new(types::Fd(fds[idx]), std::ptr::null_mut(), std::ptr::null_mut())
                        .build()
                        .user_data(ud),
                );
                if res >= 0 {
                    let fd = res as RawFd;
                    let id = match free.pop() {
                        Some(i) => {
                            conns[i] = Some(Conn::new(fd));
                            i
                        }
                        None => {
                            conns.push(Some(Conn::new(fd)));
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
                on_recv(&mut conns, id, res, &mut pending, &mut dispatch);
            }

            if !conns[id].as_ref().map(|c| c.open).unwrap_or(false) {
                if let Some(c) = conns[id].take() {
                    unsafe { libc::close(c.fd) };
                }
                free.push(id);
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

fn maybe_arm_send(conns: &mut [Option<Conn>], id: usize, pending: &mut Vec<io_uring::squeue::Entry>) {
    let c = conns[id].as_mut().unwrap();
    if c.inflight_send == 0 && !c.out.is_empty() {
        c.inflight_send = c.out.len();
        let entry = opcode::Send::new(types::Fd(c.fd), c.out.as_ptr(), c.out.len() as u32)
            .build()
            .user_data(send_ud(id));
        pending.push(entry);
    }
}

fn on_recv(
    conns: &mut [Option<Conn>],
    id: usize,
    res: i32,
    pending: &mut Vec<io_uring::squeue::Entry>,
    dispatch: &mut impl FnMut(&[u8], &mut Vec<u8>),
) {
    if res <= 0 {
        conns[id].as_mut().unwrap().open = false;
        return;
    }
    let n = res as usize;
    {
        let c = conns[id].as_mut().unwrap();
        c.acc.extend_from_slice(&c.rbuf[..n]);
        // Consume the CP2 preamble.
        while c.preamble_left > 0 && !c.acc.is_empty() {
            c.acc.remove(0);
            c.preamble_left -= 1;
        }
    }
    // Hand each complete message (as a byte slice) to the dispatcher, which
    // appends reply bytes straight into the reused `out` buffer.
    loop {
        let c = conns[id].as_mut().unwrap();
        if c.preamble_left > 0 {
            break;
        }
        let Some(len) = message_len(&c.acc) else { break };
        let Conn { acc, out, .. } = c;
        dispatch(&acc[..len], out);
        acc.drain(0..len);
    }
    maybe_arm_send(conns, id, pending);
    arm_recv(conns, id, pending);
}

fn on_send(conns: &mut [Option<Conn>], id: usize, res: i32, pending: &mut Vec<io_uring::squeue::Entry>) {
    if res < 0 {
        conns[id].as_mut().unwrap().open = false;
        return;
    }
    let c = conns[id].as_mut().unwrap();
    let sent = res as usize;
    c.out.drain(0..sent);
    c.inflight_send = 0;
    // If more queued (or a partial send), arm the next send.
    maybe_arm_send(conns, id, pending);
}
