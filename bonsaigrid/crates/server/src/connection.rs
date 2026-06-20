//! Per-connection handling: consume the `CP2` preamble, then loop reading
//! complete messages, dispatching each, and writing the resulting reply
//! messages (responses and pushed events).

use protocol::frame::{read_message, write_message, Frame};
use std::io::{Read, Write};
use std::net::TcpStream;

/// `dispatch` maps one request message to zero or more reply messages.
pub fn handle(
    mut stream: TcpStream,
    mut dispatch: impl FnMut(Vec<Frame>) -> Vec<Vec<Frame>>,
) -> std::io::Result<()> {
    let mut preamble = [0u8; 3];
    stream.read_exact(&mut preamble)?;
    if &preamble != b"CP2" {
        return Ok(()); // unknown protocol; close.
    }

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        // Drain every complete message currently buffered.
        while let Some((frames, used)) = read_message(&buf) {
            for reply in dispatch(frames) {
                stream.write_all(&write_message(&reply))?;
            }
            buf.drain(0..used);
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(()); // peer closed.
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}
