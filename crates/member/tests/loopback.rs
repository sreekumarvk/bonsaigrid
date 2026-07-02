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

/// One shared self-signed member identity (cert = its own CA), with the
/// required member-mesh SAN. Used by both members for mutual TLS.
fn shared_member_tls() -> security::tls::MemberTls {
    let ck =
        rcgen::generate_simple_self_signed(vec![security::tls::MEMBER_SERVER_NAME.into()]).unwrap();
    security::tls::MemberTls::new(
        security::tls::TlsMode::Required,
        security::tls::load_certs(ck.cert.pem().as_bytes()).unwrap(),
        security::tls::load_private_key(ck.key_pair.serialize_pem().as_bytes()).unwrap(),
        security::tls::load_ca(ck.cert.pem().as_bytes()).unwrap(),
    )
    .unwrap()
}

/// Same BackupPut → Ack exchange, but over the member mesh with mutual TLS: the
/// io_uring transport drives the handshake, installs kTLS, and routes the
/// encrypted frames end-to-end.
#[test]
fn put_then_ack_over_loopback_mtls() {
    const TPORTS: [i32; 2] = [17821, 17822];
    let mtls = shared_member_tls();
    let got_ack = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let (b_stop, b_tls) = (stop.clone(), mtls.clone());
    let b = std::thread::spawn(move || {
        Transport::bind(1, &TPORTS)
            .unwrap()
            .with_tls(Some(b_tls))
            .run(Backup { stop: b_stop })
            .unwrap();
    });
    std::thread::sleep(Duration::from_millis(200));

    let (a_ack, a_stop, a_tls) = (got_ack.clone(), stop.clone(), mtls);
    let a = std::thread::spawn(move || {
        Transport::bind(0, &TPORTS)
            .unwrap()
            .with_tls(Some(a_tls))
            .run(Sender {
                sent: false,
                got_ack: a_ack,
                stop: a_stop,
            })
            .unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !got_ack.load(SeqCst) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        got_ack.load(SeqCst),
        "member 0 did not receive the Ack over mutual TLS within 5s"
    );
    stop.store(true, SeqCst);
    a.join().unwrap();
    b.join().unwrap();
}

#[test]
fn put_then_ack_over_loopback() {
    let got_ack = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let b_stop = stop.clone();
    let b = std::thread::spawn(move || {
        Transport::bind(1, &PORTS)
            .unwrap()
            .run(Backup { stop: b_stop })
            .unwrap();
    });
    // Let member 1's listener come up before member 0 connects.
    std::thread::sleep(Duration::from_millis(200));

    let a_ack = got_ack.clone();
    let a_stop = stop.clone();
    let a = std::thread::spawn(move || {
        Transport::bind(0, &PORTS)
            .unwrap()
            .run(Sender {
                sent: false,
                got_ack: a_ack,
                stop: a_stop,
            })
            .unwrap();
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !got_ack.load(SeqCst) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        got_ack.load(SeqCst),
        "member 0 did not receive the Ack within 5s"
    );

    stop.store(true, SeqCst);
    a.join().unwrap();
    b.join().unwrap();
}
