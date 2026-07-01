//! Drives `connection::handle` over a real loopback socket (no Hazelcast client):
//! sends the CP2 preamble + an auth-typed request, and asserts the dispatched
//! reply comes back framed with the echoed correlation id.

use protocol::fixed::write_i32_le;
use protocol::frame::{read_message, write_message, Frame, UNFRAGMENTED};
use protocol::message::{correlation_id, msg_type, set_correlation_id};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

#[test]
fn cp2_preamble_then_echo_correlation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        server::connection::handle(stream, |req| {
            let mut c = vec![0u8; 13];
            write_i32_le(&mut c, 0, 257); // pretend auth response
            let mut reply = vec![Frame {
                flags: UNFRAGMENTED,
                content: c,
            }];
            set_correlation_id(&mut reply, correlation_id(&req));
            vec![reply]
        })
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"CP2").unwrap();

    let mut c = vec![0u8; 36];
    write_i32_le(&mut c, 0, 256); // auth request type
    let mut req = vec![Frame {
        flags: UNFRAGMENTED,
        content: c,
    }];
    set_correlation_id(&mut req, 7);
    client.write_all(&write_message(&req)).unwrap();

    let mut buf = vec![0u8; 1024];
    let n = client.read(&mut buf).unwrap();
    let (frames, _) = read_message(&buf[..n]).unwrap();
    assert_eq!(msg_type(&frames), 257);
    assert_eq!(correlation_id(&frames), 7);

    drop(client);
    let _ = server.join();
}
