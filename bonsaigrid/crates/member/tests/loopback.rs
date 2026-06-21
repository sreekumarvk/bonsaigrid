//! Two transports over loopback exchange a BackupPut → Ack, proving the
//! io_uring mesh connects, frames, and routes replies by peer index.

use member::transport::{Handler, Transport};
use member::wire::Msg;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PORTS: [i32; 2] = [17801, 17802];

/// Member 0: sends one BackupPut to member 1, records the returning Ack.
struct Sender {
    sent: bool,
    got_ack: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}
impl Handler for Sender {
    fn on_msg(&mut self, _src: usize, msg: Msg, _outbox: &mut Vec<(usize, Msg)>) {
        if msg == (Msg::Ack { op_id: 5 }) {
            self.got_ack.store(true, SeqCst);
        }
    }
    fn on_tick(&mut self, outbox: &mut Vec<(usize, Msg)>) -> bool {
        if !self.sent {
            outbox.push((
                1,
                Msg::BackupPut {
                    op_id: 5,
                    name: "m".into(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ttl_ms: 0,
                },
            ));
            self.sent = true;
        }
        !self.stop.load(SeqCst)
    }
}

/// Member 1: acks every BackupPut back to its source.
struct Backup {
    stop: Arc<AtomicBool>,
}
impl Handler for Backup {
    fn on_msg(&mut self, src: usize, msg: Msg, outbox: &mut Vec<(usize, Msg)>) {
        if let Msg::BackupPut { op_id, .. } = msg {
            outbox.push((src, Msg::Ack { op_id }));
        }
    }
    fn on_tick(&mut self, _outbox: &mut Vec<(usize, Msg)>) -> bool {
        !self.stop.load(SeqCst)
    }
}

#[test]
fn put_then_ack_over_loopback() {
    let got_ack = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let b_stop = stop.clone();
    let b = std::thread::spawn(move || {
        Transport::bind(1, &PORTS).unwrap().run(Backup { stop: b_stop }).unwrap();
    });
    // Let member 1's listener come up before member 0 connects.
    std::thread::sleep(Duration::from_millis(200));

    let a_ack = got_ack.clone();
    let a_stop = stop.clone();
    let a = std::thread::spawn(move || {
        Transport::bind(0, &PORTS)
            .unwrap()
            .run(Sender { sent: false, got_ack: a_ack, stop: a_stop })
            .unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !got_ack.load(SeqCst) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(got_ack.load(SeqCst), "member 0 did not receive the Ack within 5s");

    stop.store(true, SeqCst);
    a.join().unwrap();
    b.join().unwrap();
}
