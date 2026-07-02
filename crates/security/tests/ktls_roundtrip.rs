//! End-to-end proof that the kTLS key handoff is correct on this kernel: a real
//! rustls TLS 1.3 handshake over a loopback TCP pair, then `enable_ktls` on both
//! sockets, then a PLAINTEXT write on one side is read back correctly on the
//! other — meaning the kernel encrypted it on the wire and decrypted it on read.
//! A wrong key/IV/sequence handoff would surface as a decryption error (EBADMSG)
//! instead of the expected bytes.

use rustls::pki_types::ServerName;
use rustls::{ClientConnection, ServerConnection};
use security::tls;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;

#[test]
fn ktls_encrypts_on_the_wire_and_decrypts_on_read() {
    // Self-signed cert for "localhost"; the client trusts it directly.
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.key_pair.serialize_pem();

    let server_cfg = tls::server_config(
        tls::load_certs(cert_pem.as_bytes()).unwrap(),
        tls::load_private_key(key_pem.as_bytes()).unwrap(),
        None,
    )
    .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for c in tls::load_certs(cert_pem.as_bytes()).unwrap() {
        roots.add(c).unwrap();
    }
    let client_cfg = tls::client_config(roots, None).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Server thread: accept, handshake, enable kTLS, send a plaintext greeting.
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut conn = ServerConnection::new(server_cfg).unwrap();
        drive(&mut conn, &mut sock);
        let fd = sock.as_raw_fd();
        let secrets = conn.dangerous_extract_secrets().unwrap();
        tls::enable_ktls(fd, secrets).expect("server kTLS");
        // Plaintext to the app; the kernel encrypts it into a TLS record.
        sock.write_all(b"hello over ktls").unwrap();
        sock.flush().unwrap();
        // Keep the socket open briefly so the client can read.
        std::thread::sleep(std::time::Duration::from_millis(100));
    });

    let mut sock = TcpStream::connect(addr).unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = ClientConnection::new(client_cfg, name).unwrap();
    drive(&mut conn, &mut sock);
    let fd = sock.as_raw_fd();
    let secrets = conn.dangerous_extract_secrets().unwrap();
    tls::enable_ktls(fd, secrets).expect("client kTLS");

    // The kernel decrypts the TLS record back to plaintext for the app.
    let mut buf = [0u8; 64];
    let n = sock.read(&mut buf).unwrap();
    assert_eq!(
        &buf[..n],
        b"hello over ktls",
        "kTLS round-trip must return the exact plaintext (proves correct key/IV/seq handoff)"
    );

    server.join().unwrap();
}

/// Exercises the exact reactor-facing API: `ServerHandshake::pump` drives the
/// handshake over a byte transport, then `into_ktls`; the client's application
/// data arrives either as pump-decrypted plaintext or via the kernel after kTLS.
#[test]
fn server_handshake_pump_then_ktls_reads_app_data() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.key_pair.serialize_pem();
    let server_cfg = tls::server_config(
        tls::load_certs(cert_pem.as_bytes()).unwrap(),
        tls::load_private_key(key_pem.as_bytes()).unwrap(),
        None,
    )
    .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for c in tls::load_certs(cert_pem.as_bytes()).unwrap() {
        roots.add(c).unwrap();
    }
    let client_cfg = tls::client_config(roots, None).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let fd = sock.as_raw_fd();
        let mut hs = Some(tls::Handshake::server(server_cfg).unwrap());
        let mut ktls_on = false;
        let mut got: Vec<u8> = Vec::new();
        let mut rbuf = [0u8; 4096];
        while got != b"CP2hello" {
            let n = sock.read(&mut rbuf).unwrap();
            if n == 0 {
                break;
            }
            if ktls_on {
                got.extend_from_slice(&rbuf[..n]); // kernel-decrypted plaintext
                continue;
            }
            let (mut send, mut plain) = (Vec::new(), Vec::new());
            let ready = hs
                .as_mut()
                .unwrap()
                .pump(&rbuf[..n], &mut send, &mut plain)
                .unwrap();
            if !send.is_empty() {
                sock.write_all(&send).unwrap();
            }
            got.extend_from_slice(&plain);
            if ready {
                hs.take().unwrap().into_ktls(fd).unwrap();
                ktls_on = true;
            }
        }
        assert_eq!(got, b"CP2hello", "server must recover the app plaintext");
    });

    let mut sock = TcpStream::connect(addr).unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = ClientConnection::new(client_cfg, name).unwrap();
    drive(&mut conn, &mut sock);
    // Send application data (encrypted by the client's rustls).
    std::io::Write::write_all(&mut conn.writer(), b"CP2hello").unwrap();
    while conn.wants_write() {
        conn.write_tls(&mut sock).unwrap();
    }
    sock.flush().unwrap();
    server.join().unwrap();
}

/// Drive one side of a member handshake to kTLS over a blocking socket.
/// `prime` = emit the ClientHello first (the dialing/client side).
fn negotiate(mut hs: tls::Handshake, sock: &mut TcpStream, prime: bool) {
    let fd = sock.as_raw_fd();
    if prime {
        let (mut s, mut p) = (Vec::new(), Vec::new());
        hs.pump(&[], &mut s, &mut p).unwrap();
        if !s.is_empty() {
            sock.write_all(&s).unwrap();
        }
    }
    let mut rbuf = [0u8; 8192];
    loop {
        let n = sock.read(&mut rbuf).unwrap();
        assert!(n > 0, "unexpected EOF during member handshake");
        let (mut s, mut p) = (Vec::new(), Vec::new());
        let ready = hs.pump(&rbuf[..n], &mut s, &mut p).unwrap();
        if !s.is_empty() {
            sock.write_all(&s).unwrap();
        }
        if ready {
            hs.into_ktls(fd).unwrap();
            return;
        }
    }
}

/// One self-signed identity, used as both the member cert and its own CA.
fn member_identity() -> tls::MemberTls {
    let ck = rcgen::generate_simple_self_signed(vec![tls::MEMBER_SERVER_NAME.into()]).unwrap();
    tls::MemberTls::new(
        tls::TlsMode::Required,
        tls::load_certs(ck.cert.pem().as_bytes()).unwrap(),
        tls::load_private_key(ck.key_pair.serialize_pem().as_bytes()).unwrap(),
        tls::load_ca(ck.cert.pem().as_bytes()).unwrap(),
    )
    .unwrap()
}

/// Two members sharing a CA complete a mutual-TLS handshake and exchange an
/// application message over kTLS.
#[test]
fn member_mtls_handshake_and_exchange() {
    let mtls = member_identity();
    let (a, b) = (mtls.clone(), mtls);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        negotiate(a.accept().unwrap(), &mut sock, false); // we are the TLS server
        let mut buf = [0u8; 64];
        let n = sock.read(&mut buf).unwrap();
        assert_eq!(
            &buf[..n],
            b"PEER-MSG",
            "server must decrypt the peer message"
        );
    });

    let mut sock = TcpStream::connect(addr).unwrap();
    negotiate(b.dial().unwrap(), &mut sock, true); // we dialed: TLS client, prime ClientHello
    sock.write_all(b"PEER-MSG").unwrap();
    sock.flush().unwrap();
    server.join().unwrap();
}

/// Best-effort handshake driver: returns whether kTLS was established. Never
/// blocks indefinitely (the caller sets a read timeout) and never panics on a
/// rejected/failed handshake.
fn try_negotiate(mut hs: tls::Handshake, sock: &mut TcpStream, prime: bool) -> bool {
    let fd = sock.as_raw_fd();
    if prime {
        let (mut s, mut p) = (Vec::new(), Vec::new());
        if hs.pump(&[], &mut s, &mut p).is_err() || sock.write_all(&s).is_err() {
            return false;
        }
    }
    let mut rbuf = [0u8; 8192];
    loop {
        let n = match sock.read(&mut rbuf) {
            Ok(0) | Err(_) => return false,
            Ok(n) => n,
        };
        let (mut s, mut p) = (Vec::new(), Vec::new());
        match hs.pump(&rbuf[..n], &mut s, &mut p) {
            Ok(ready) => {
                if sock.write_all(&s).is_err() {
                    return false;
                }
                if ready {
                    return hs.into_ktls(fd).is_ok();
                }
            }
            Err(_) => return false,
        }
    }
}

/// A member whose certificate is NOT signed by the cluster CA cannot complete
/// mutual TLS (no rogue node can join / send data).
#[test]
fn member_without_trusted_cert_is_rejected() {
    let good = member_identity(); // server trusts only its own CA
    let rogue = member_identity(); // a DIFFERENT self-signed identity/CA

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        if try_negotiate(good.accept().unwrap(), &mut sock, false) {
            let mut buf = [0u8; 64];
            let got = sock.read(&mut buf).unwrap_or(0);
            assert_eq!(
                got, 0,
                "rogue member must not deliver an application message"
            );
        }
    });

    let mut sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .unwrap();
    let established = try_negotiate(rogue.dial().unwrap(), &mut sock, true);
    assert!(
        !established,
        "a member without a CA-signed cert must not complete mutual TLS"
    );
    let _ = sock.write_all(b"PEER-MSG"); // best-effort; server must not accept it
    server.join().unwrap();
}

/// Blocking handshake driver. Flushes outgoing flights with `write_tls` (never
/// blocking on a read once the handshake is done) so the two threads can't
/// deadlock reading each other after the final Finished.
fn drive<S: rustls::SideData>(conn: &mut rustls::ConnectionCommon<S>, stream: &mut TcpStream) {
    loop {
        while conn.wants_write() {
            conn.write_tls(stream).expect("write_tls");
        }
        if !conn.is_handshaking() {
            break;
        }
        conn.read_tls(stream).expect("read_tls");
        conn.process_new_packets().expect("process handshake");
    }
}
