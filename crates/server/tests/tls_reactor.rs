//! End-to-end proof that the io_uring reactor speaks TLS: a real rustls client
//! completes a TLS 1.3 handshake against the reactor (which drives the handshake
//! over io_uring and installs kTLS), then sends the `CP2` preamble + a framed
//! request inside the TLS session and reads back the dispatcher's response —
//! decrypted by the client. This exercises handshake-over-io_uring, the
//! kTLS transition, and the plaintext data path on a kTLS socket.

use protocol::fixed::write_i32_le;
use protocol::frame::{write_message, Frame, UNFRAGMENTED};
use protocol::primitives::{data_frame, string_frame};
use rustls::pki_types::ServerName;
use rustls::ClientConnection;
use security::tls;
use std::io::{Read, Write};
use std::net::TcpStream;

const MARKER: &[u8] = b"REACTOR-TLS-OK";

fn framed_request() -> Vec<u8> {
    let mut c = vec![0u8; 24];
    write_i32_le(&mut c, 0, 66048); // MapGet (any valid type; dispatcher ignores it here)
    let frames = vec![
        Frame {
            flags: UNFRAGMENTED,
            content: c,
        },
        string_frame("m"),
        data_frame(b"k"),
    ];
    write_message(&frames)
}

/// Drive a rustls client handshake to completion over a blocking stream.
fn client_handshake(conn: &mut ClientConnection, sock: &mut TcpStream) {
    loop {
        while conn.wants_write() {
            conn.write_tls(sock).expect("write_tls");
        }
        if !conn.is_handshaking() {
            break;
        }
        conn.read_tls(sock).expect("read_tls");
        conn.process_new_packets().expect("process");
    }
    while conn.wants_write() {
        conn.write_tls(sock).expect("write_tls");
    }
}

/// In `permissive` mode a PLAINTEXT client (first byte `C` of `CP2`, not the TLS
/// `0x16`) must still be served — the zero-downtime-rollout invariant.
#[test]
fn permissive_mode_serves_a_plaintext_client() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_cfg = tls::server_config(
        tls::load_certs(ck.cert.pem().as_bytes()).unwrap(),
        tls::load_private_key(ck.key_pair.serialize_pem().as_bytes()).unwrap(),
        None,
    )
    .unwrap();
    let acceptor = tls::TlsAcceptor::new(tls::TlsMode::Permissive, server_cfg);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let _ = server::reactor::run(
            vec![listener],
            move |_msg, _conn_id, out: &mut Vec<u8>| out.extend_from_slice(MARKER),
            |_p| (404, "text/plain", "no".to_string()),
            |_cmd: &[u8], _out: &mut Vec<u8>| false,
            |_c, _o| {},
            |_c| {},
            || {},
            Some(acceptor),
        );
    });

    // Plain TCP (no TLS): send CP2 + request, expect the marker back in clear.
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    sock.write_all(b"CP2").unwrap();
    sock.write_all(&framed_request()).unwrap();
    sock.flush().unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 256];
    while !got.windows(MARKER.len()).any(|w| w == MARKER) {
        match sock.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
    assert!(
        got.windows(MARKER.len()).any(|w| w == MARKER),
        "permissive mode must serve a plaintext client"
    );
}

#[test]
fn tls_client_round_trips_a_request_through_the_reactor() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = ck.cert.pem();
    let key_pem = ck.key_pair.serialize_pem();

    // Server-side acceptor in `required` mode.
    let server_cfg = tls::server_config(
        tls::load_certs(cert_pem.as_bytes()).unwrap(),
        tls::load_private_key(key_pem.as_bytes()).unwrap(),
        None,
    )
    .unwrap();
    let acceptor = tls::TlsAcceptor::new(tls::TlsMode::Required, server_cfg);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Run the reactor on its own thread; it never returns, so we detach it.
    std::thread::spawn(move || {
        let _ = server::reactor::run(
            vec![listener],
            move |_msg, _conn_id, out: &mut Vec<u8>| {
                // Any dispatched binary request gets the marker response.
                out.extend_from_slice(MARKER);
            },
            |_path| (404, "text/plain", "no".to_string()),
            |_cmd: &[u8], _out: &mut Vec<u8>| false,
            |_conn_id, _out| {},
            |_conn_id| {},
            || {},
            Some(acceptor),
        );
    });

    // Client: trust the self-signed cert, connect, handshake, send inside TLS.
    let mut roots = rustls::RootCertStore::empty();
    for c in tls::load_certs(cert_pem.as_bytes()).unwrap() {
        roots.add(c).unwrap();
    }
    let client_cfg = tls::client_config(roots, None).unwrap();

    let mut sock = TcpStream::connect(addr).unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = ClientConnection::new(client_cfg, name).unwrap();
    client_handshake(&mut conn, &mut sock);

    // Send the CP2 preamble + a framed request as TLS application data.
    conn.writer().write_all(b"CP2").unwrap();
    conn.writer().write_all(&framed_request()).unwrap();
    while conn.wants_write() {
        conn.write_tls(&mut sock).unwrap();
    }
    sock.flush().unwrap();

    // Read the decrypted response and look for the marker.
    let mut got: Vec<u8> = Vec::new();
    sock.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    while !got.windows(MARKER.len()).any(|w| w == MARKER) {
        if conn.read_tls(&mut sock).unwrap() == 0 {
            break;
        }
        conn.process_new_packets().unwrap();
        let mut buf = [0u8; 256];
        if let Ok(n) = conn.reader().read(&mut buf) {
            if n > 0 {
                got.extend_from_slice(&buf[..n]);
            }
        }
    }
    assert!(
        got.windows(MARKER.len()).any(|w| w == MARKER),
        "TLS client did not receive the reactor's response through the encrypted channel"
    );
}
