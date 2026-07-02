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
        let mut hs = Some(tls::ServerHandshake::new(server_cfg).unwrap());
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
