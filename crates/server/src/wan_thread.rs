//! WAN thread: the only WAN disk+socket writer. It drains captured mutations from
//! the reactor cores over an SPSC ring into a durable outbound queue, ships unacked
//! batches to each remote cluster over plain TCP, and applies inbound batches via
//! the HLC merge. WAN is latency-dominated and off the hot path, so a dedicated
//! blocking-TCP thread (not io_uring) is the right fit and keeps the reactor clean.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use store::Store;
use wan::{apply_batch, decode_msg, encode_msg, WanMsg, WanQueue, WanRecord};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    Throw,
    DropOldest,
}

#[derive(Clone, Debug)]
pub struct WanConfig {
    pub targets: Vec<String>, // remote WAN endpoints, host:port
    pub listen: u16,          // this cluster's inbound WAN port
    pub batch: usize,         // max records per shipped batch
    pub queue_bytes: u64,     // outbound queue byte bound
    pub backpressure: Backpressure,
    pub poll_ms: u64,
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Parse WAN config from the environment. Returns `None` (WAN disabled) unless
/// `BONSAI_WAN_TARGETS` is set.
pub fn wan_config() -> Option<WanConfig> {
    let targets: Vec<String> = std::env::var("BONSAI_WAN_TARGETS")
        .ok()?
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if targets.is_empty() {
        return None;
    }
    Some(WanConfig {
        targets,
        listen: env_u64("BONSAI_WAN_PORT", 7701) as u16,
        batch: env_u64("BONSAI_WAN_BATCH", 256) as usize,
        queue_bytes: env_u64("BONSAI_WAN_QUEUE_MB", 256) * 1024 * 1024,
        backpressure: match std::env::var("BONSAI_WAN_BACKPRESSURE").as_deref() {
            Ok("drop-oldest") => Backpressure::DropOldest,
            _ => Backpressure::Throw,
        },
        poll_ms: env_u64("BONSAI_WAN_POLL_MS", 50),
    })
}

// ---- length-prefixed framing over TCP: [len:u32-le][encode_msg bytes] ----
fn write_frame(w: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)?;
    w.flush()
}
fn read_frame(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Ship one batch to a target and return the sequence it acked.
fn ship_batch(target: &str, up_to: u64, recs: &[WanRecord]) -> std::io::Result<u64> {
    let mut s = TcpStream::connect(target)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.set_write_timeout(Some(Duration::from_secs(5)))?;
    write_frame(&mut s, &encode_msg(&WanMsg::Batch { up_to_seq: up_to, records: recs.to_vec() }))?;
    match decode_msg(&read_frame(&mut s)?) {
        Some(WanMsg::Ack { up_to_seq }) => Ok(up_to_seq),
        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad WAN ack")),
    }
}

/// Inbound listener: apply each received batch via the HLC merge and ack it.
fn inbound_loop(listen: u16, store: Arc<Store>) {
    let l = match TcpListener::bind(("0.0.0.0", listen)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("WAN inbound bind :{listen} failed: {e}");
            return;
        }
    };
    for conn in l.incoming().flatten() {
        let store = store.clone();
        std::thread::spawn(move || handle_inbound(conn, store));
    }
}

fn handle_inbound(mut s: TcpStream, store: Arc<Store>) {
    loop {
        let frame = match read_frame(&mut s) {
            Ok(f) => f,
            Err(_) => return, // peer closed
        };
        if let Some(WanMsg::Batch { up_to_seq, records }) = decode_msg(&frame) {
            apply_batch(&store, &records);
            let _ = write_frame(&mut s, &encode_msg(&WanMsg::Ack { up_to_seq }));
        }
    }
}

/// Spawn the WAN threads (inbound listener + outbound drain/ship). Owns all WAN
/// disk under `dir` and the WAN sockets. Returns the outbound thread's handle.
pub fn spawn_wan(
    dir: PathBuf,
    store: Arc<Store>,
    rx: spsc::Consumer<WanRecord>,
    cfg: WanConfig,
) -> std::thread::JoinHandle<()> {
    // Inbound listener (its own thread).
    let in_store = store.clone();
    let listen = cfg.listen;
    std::thread::spawn(move || inbound_loop(listen, in_store));

    // Outbound drain + ship loop.
    std::thread::spawn(move || {
        std::fs::create_dir_all(&dir).ok();
        let mut q = match WanQueue::open(&dir) {
            Ok(q) => q,
            Err(e) => {
                eprintln!("WAN queue open {dir:?} failed: {e}");
                return;
            }
        };
        loop {
            // Drain captured records into the durable queue, honoring the bound.
            while let Some(r) = rx.pop() {
                if q.would_exceed(cfg.queue_bytes) {
                    match cfg.backpressure {
                        // Stop draining: the SPSC ring fills and the reactor's push
                        // returns Err, parking/dropping the producing write.
                        Backpressure::Throw => break,
                        // Advance past the oldest unacked to make room, then append.
                        Backpressure::DropOldest => {
                            let _ = q.ack(q.acked() + 1);
                        }
                    }
                }
                let _ = q.append(&r);
            }
            // Ship unacked records, batched; ack only what ALL targets confirm.
            let un = q.unacked();
            for chunk in un.chunks(cfg.batch.max(1)) {
                let up_to = chunk.last().unwrap().0;
                let recs: Vec<WanRecord> = chunk.iter().map(|(_, r)| r.clone()).collect();
                let mut min_ack = up_to;
                for t in &cfg.targets {
                    match ship_batch(t, up_to, &recs) {
                        Ok(a) => min_ack = min_ack.min(a),
                        Err(_) => min_ack = q.acked(), // target down → don't advance
                    }
                }
                let _ = q.ack(min_ack);
            }
            std::thread::sleep(Duration::from_millis(cfg.poll_ms));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wan_config_parses_targets_and_defaults() {
        std::env::set_var("BONSAI_WAN_TARGETS", "host-a:7701, host-b:7702");
        std::env::set_var("BONSAI_WAN_PORT", "7799");
        std::env::remove_var("BONSAI_WAN_BATCH");
        std::env::remove_var("BONSAI_WAN_BACKPRESSURE");
        let c = wan_config().unwrap();
        assert_eq!(c.targets, vec!["host-a:7701", "host-b:7702"]);
        assert_eq!(c.listen, 7799);
        assert_eq!(c.batch, 256);
        assert_eq!(c.backpressure, Backpressure::Throw);
        std::env::remove_var("BONSAI_WAN_TARGETS");
        assert!(wan_config().is_none(), "WAN disabled without targets");
    }
}
