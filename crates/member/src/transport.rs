//! io_uring full-mesh member transport.
//!
//! One io_uring loop per member thread. Inbound peer connections are accepted and
//! their frames decoded (data path entirely on io_uring, mirroring the client
//! reactor). Outbound connections are opened lazily with a one-time blocking
//! `TcpStream::connect` (off any hot path — done once per peer as the mesh forms),
//! after which their send/recv runs on io_uring too.
//!
//! The transport is generic over a [`Handler`]: it calls `on_msg` for each decoded
//! inbound message and `on_tick` every ~1 ms (so the owner can drain its SPSC ring
//! and run the ack-timeout sweep). Both push outgoing `(dest_index, Msg)` pairs
//! into an outbox the transport then routes by peer index — establishing the
//! outbound connection on demand. Replies therefore travel on the sender's own
//! outbound connection to the destination, not on the inbound socket.

use crate::wire::{decode, encode, Msg};
use io_uring::{opcode, types, IoUring};
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::rc::Rc;

/// Shared peer-address table (member index → member-port address). Updated by the
/// handler as it learns members, read by the transport when dialing.
pub type Peers = Rc<RefCell<HashMap<usize, SocketAddr>>>;

const ACCEPT_UD: u64 = u64::MAX - 1;
const TIMEOUT_UD: u64 = u64::MAX - 2;
const RECV_BUF: usize = 64 * 1024;

/// Member-thread logic driven by the transport.
pub trait Handler {
    /// An inbound message arrived from member `src`. Push replies to `outbox`.
    fn on_msg(&mut self, src: usize, msg: Msg, outbox: &mut Vec<(usize, Msg)>);
    /// Periodic ~1 ms tick. Push outgoing messages to `outbox`. Return `false` to
    /// stop the transport loop.
    fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> bool;
}

struct MConn {
    fd: RawFd,
    peer: Option<usize>,
    rbuf: Box<[u8]>,
    acc: Vec<u8>,
    /// Pending bytes to send (appended to freely).
    out: Vec<u8>,
    /// Bytes currently being sent. A separate buffer so appends to `out` can't
    /// reallocate the memory an in-flight io_uring Send still references.
    sendbuf: Vec<u8>,
    inflight: usize,
    open: bool,
}

impl MConn {
    fn new(fd: RawFd) -> MConn {
        MConn {
            fd,
            peer: None,
            rbuf: vec![0u8; RECV_BUF].into_boxed_slice(),
            acc: Vec::with_capacity(RECV_BUF),
            out: Vec::with_capacity(4096),
            sendbuf: Vec::with_capacity(4096),
            inflight: 0,
            open: true,
        }
    }
}

pub struct Transport {
    self_index: usize,
    peers: Peers,
    listener: TcpListener,
}

impl Transport {
    /// Bind this member's inbound listener on `ports[self_index]`; seed the peer
    /// table from `ports` (index → 127.0.0.1:port).
    pub fn bind(self_index: usize, ports: &[i32]) -> std::io::Result<Transport> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", ports[self_index]))?;
        let mut map = HashMap::new();
        for (i, p) in ports.iter().enumerate() {
            map.insert(i, format!("127.0.0.1:{p}").parse().unwrap());
        }
        Ok(Transport {
            self_index,
            peers: Rc::new(RefCell::new(map)),
            listener,
        })
    }

    /// Handle to the shared peer table so the handler can register new members'
    /// addresses (e.g. after a runtime join).
    pub fn peers(&self) -> Peers {
        self.peers.clone()
    }

    /// Run the loop, driving `handler`. Returns when `on_tick` returns `false`.
    pub fn run(self, mut handler: impl Handler) -> std::io::Result<()> {
        let Transport {
            self_index,
            peers,
            listener,
        } = self;
        let lfd = listener.as_raw_fd();
        let mut ring = IoUring::new(256)?;
        let mut conns: Vec<Option<MConn>> = Vec::new();
        let mut free: Vec<usize> = Vec::new();
        let mut outbound: HashMap<usize, usize> = HashMap::new(); // peer index -> conn slot
        let mut pending: Vec<io_uring::squeue::Entry> = Vec::new();
        let mut outbox: Vec<(usize, Msg)> = Vec::new();

        pending.push(
            opcode::AcceptMulti::new(types::Fd(lfd))
                .build()
                .user_data(ACCEPT_UD),
        );
        let tick = types::Timespec::new().sec(0).nsec(1_000_000); // 1 ms
        pending.push(opcode::Timeout::new(&tick).build().user_data(TIMEOUT_UD));

        loop {
            flush(&mut ring, &mut pending);
            ring.submit_and_wait(1)?;
            let cqes: Vec<(u64, i32, u32)> = ring
                .completion()
                .map(|c| (c.user_data(), c.result(), c.flags()))
                .collect();

            let mut do_tick = false;
            for (ud, res, flags) in cqes {
                if ud == TIMEOUT_UD {
                    do_tick = true;
                    pending.push(opcode::Timeout::new(&tick).build().user_data(TIMEOUT_UD));
                    continue;
                }
                if ud == ACCEPT_UD {
                    if !io_uring::cqueue::more(flags) {
                        pending.push(
                            opcode::AcceptMulti::new(types::Fd(lfd))
                                .build()
                                .user_data(ACCEPT_UD),
                        );
                    }
                    if res >= 0 {
                        let slot = alloc(&mut conns, &mut free, res as RawFd);
                        arm_recv(&mut conns, slot, &mut pending);
                    }
                    continue;
                }
                let slot = (ud >> 1) as usize;
                let is_send = ud & 1 == 1;
                if conns.get(slot).and_then(|c| c.as_ref()).is_none() {
                    continue;
                }
                if is_send {
                    on_send(&mut conns, slot, res, &mut pending);
                } else {
                    on_recv(
                        &mut conns,
                        slot,
                        res,
                        &mut handler,
                        &mut outbox,
                        &mut pending,
                    );
                    deliver(
                        &mut outbox,
                        &mut conns,
                        &mut free,
                        &mut outbound,
                        self_index,
                        &peers,
                        &mut pending,
                    );
                }
                if !conns[slot].as_ref().map(|c| c.open).unwrap_or(false) {
                    if let Some(c) = conns[slot].take() {
                        unsafe { libc::close(c.fd) };
                        if let Some(p) = c.peer {
                            if outbound.get(&p) == Some(&slot) {
                                outbound.remove(&p);
                            }
                        }
                    }
                    free.push(slot);
                }
            }

            if do_tick {
                let cont = handler.on_tick(&mut outbox);
                deliver(
                    &mut outbox,
                    &mut conns,
                    &mut free,
                    &mut outbound,
                    self_index,
                    &peers,
                    &mut pending,
                );
                if !cont {
                    return Ok(());
                }
            }
        }
    }
}

fn alloc(conns: &mut Vec<Option<MConn>>, free: &mut Vec<usize>, fd: RawFd) -> usize {
    match free.pop() {
        Some(i) => {
            conns[i] = Some(MConn::new(fd));
            i
        }
        None => {
            conns.push(Some(MConn::new(fd)));
            conns.len() - 1
        }
    }
}

fn flush(ring: &mut IoUring, pending: &mut Vec<io_uring::squeue::Entry>) {
    if pending.is_empty() {
        return;
    }
    let mut sq = ring.submission();
    for e in pending.drain(..) {
        unsafe {
            let _ = sq.push(&e);
        }
    }
}

fn arm_recv(conns: &mut [Option<MConn>], slot: usize, pending: &mut Vec<io_uring::squeue::Entry>) {
    let c = conns[slot].as_mut().unwrap();
    pending.push(
        opcode::Recv::new(types::Fd(c.fd), c.rbuf.as_mut_ptr(), c.rbuf.len() as u32)
            .build()
            .user_data((slot as u64) << 1),
    );
}

fn arm_send(conns: &mut [Option<MConn>], slot: usize, pending: &mut Vec<io_uring::squeue::Entry>) {
    let c = conns[slot].as_mut().unwrap();
    if c.inflight > 0 {
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
    c.inflight = c.sendbuf.len();
    pending.push(
        opcode::Send::new(types::Fd(c.fd), c.sendbuf.as_ptr(), c.sendbuf.len() as u32)
            .build()
            .user_data(((slot as u64) << 1) | 1),
    );
}

fn on_send(
    conns: &mut [Option<MConn>],
    slot: usize,
    res: i32,
    pending: &mut Vec<io_uring::squeue::Entry>,
) {
    if res < 0 {
        conns[slot].as_mut().unwrap().open = false;
        return;
    }
    let c = conns[slot].as_mut().unwrap();
    c.sendbuf.drain(0..res as usize); // consume the acknowledged prefix (handles partial sends)
    c.inflight = 0;
    arm_send(conns, slot, pending); // finish sendbuf, then swap in any newly-queued `out`
}

fn on_recv(
    conns: &mut [Option<MConn>],
    slot: usize,
    res: i32,
    handler: &mut impl Handler,
    outbox: &mut Vec<(usize, Msg)>,
    pending: &mut Vec<io_uring::squeue::Entry>,
) {
    if res <= 0 {
        conns[slot].as_mut().unwrap().open = false;
        return;
    }
    {
        let c = conns[slot].as_mut().unwrap();
        c.acc.extend_from_slice(&c.rbuf[..res as usize]);
    }
    loop {
        let c = conns[slot].as_mut().unwrap();
        let Some((msg, n)) = decode(&c.acc) else {
            break;
        };
        c.acc.drain(0..n);
        match msg {
            Msg::Hello { index } => c.peer = Some(index as usize),
            other => {
                let src = c.peer.unwrap_or(usize::MAX);
                handler.on_msg(src, other, outbox);
            }
        }
    }
    arm_recv(conns, slot, pending);
}

/// Route every queued `(dest, msg)` to the destination's outbound connection,
/// opening it if needed, then arm sends.
fn deliver(
    outbox: &mut Vec<(usize, Msg)>,
    conns: &mut Vec<Option<MConn>>,
    free: &mut Vec<usize>,
    outbound: &mut HashMap<usize, usize>,
    self_index: usize,
    peers: &Peers,
    pending: &mut Vec<io_uring::squeue::Entry>,
) {
    for (dest, msg) in outbox.drain(..) {
        let Some(slot) = ensure_outbound(dest, conns, free, outbound, self_index, peers, pending)
        else {
            continue; // peer not reachable yet; ack-timeout will cover the write
        };
        let bytes = encode(&msg);
        conns[slot].as_mut().unwrap().out.extend_from_slice(&bytes);
        arm_send(conns, slot, pending);
    }
}

fn ensure_outbound(
    dest: usize,
    conns: &mut Vec<Option<MConn>>,
    free: &mut Vec<usize>,
    outbound: &mut HashMap<usize, usize>,
    self_index: usize,
    peers: &Peers,
    pending: &mut Vec<io_uring::squeue::Entry>,
) -> Option<usize> {
    if let Some(&slot) = outbound.get(&dest) {
        if conns
            .get(slot)
            .and_then(|c| c.as_ref())
            .map(|c| c.open)
            .unwrap_or(false)
        {
            return Some(slot);
        }
        outbound.remove(&dest);
    }
    let addr = *peers.borrow().get(&dest)?;
    let stream = TcpStream::connect(addr).ok()?;
    let _ = stream.set_nodelay(true);
    let fd = stream.into_raw_fd();
    let slot = alloc(conns, free, fd);
    {
        let c = conns[slot].as_mut().unwrap();
        c.peer = Some(dest);
        c.out.extend_from_slice(&encode(&Msg::Hello {
            index: self_index as u32,
        }));
    }
    outbound.insert(dest, slot);
    arm_recv(conns, slot, pending);
    arm_send(conns, slot, pending);
    Some(slot)
}
