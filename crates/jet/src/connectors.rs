//! Streaming source connectors beyond Kafka/MapStore. v1 adds a file source: read
//! a text file line by line and emit each line as an `Item::Data`, then `Done`.
//! A source ignores its inbox and produces from the external resource.

use crate::processor::{Item, Processor};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Reads a text file and emits one `Item::Data(line_bytes)` per line, then `Done`.
/// Lines are buffered at construction so `process` is pure (no I/O in the loop).
pub struct FileSource {
    lines: VecDeque<Vec<u8>>,
    done: bool,
}

impl FileSource {
    /// Open `path` and buffer its lines. A read error yields an empty source
    /// (immediately `Done`) rather than panicking.
    pub fn open(path: &std::path::Path) -> FileSource {
        let lines = std::fs::read_to_string(path)
            .map(|s| s.lines().map(|l| l.as_bytes().to_vec()).collect())
            .unwrap_or_default();
        FileSource { lines, done: false }
    }

    /// A source over already-collected lines (tests / in-memory).
    pub fn from_lines(lines: Vec<Vec<u8>>) -> FileSource {
        FileSource {
            lines: lines.into(),
            done: false,
        }
    }
}

impl Processor for FileSource {
    /// Emits all buffered lines then `Done`. Subsequent calls are no-ops. The
    /// inbox is ignored (a source has no upstream).
    fn process(&mut self, _inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        if self.done {
            return false;
        }
        while let Some(line) = self.lines.pop_front() {
            outbox.push_back(Item::Data(line));
        }
        outbox.push_back(Item::Done);
        self.done = true;
        true
    }
}

/// Socket source: reads newline-delimited records from a TCP connection and emits
/// one `Item::Data(line)` per line, `Item::Done` when the peer closes. The stream is
/// non-blocking so `process` drains whatever has arrived and returns without waiting
/// (the DAG scheduler calls it again). A trailing partial line at EOF is flushed.
pub struct SocketSource {
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    done: bool,
}

impl SocketSource {
    pub fn connect(addr: &str) -> std::io::Result<SocketSource> {
        SocketSource::from_stream(TcpStream::connect(addr)?)
    }
    pub fn from_stream(stream: TcpStream) -> std::io::Result<SocketSource> {
        stream.set_nonblocking(true)?;
        Ok(SocketSource {
            stream: Some(stream),
            buf: Vec::new(),
            done: false,
        })
    }
    fn emit_lines(&mut self, outbox: &mut VecDeque<Item>) {
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
            line.pop(); // drop the '\n'
            if line.last() == Some(&b'\r') {
                line.pop(); // CRLF
            }
            outbox.push_back(Item::Data(line));
        }
    }
}

impl Processor for SocketSource {
    fn process(&mut self, _inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        if self.done {
            return false;
        }
        let mut progressed = false;
        let mut eof = false;
        if let Some(s) = self.stream.as_mut() {
            let mut tmp = [0u8; 8192];
            loop {
                match s.read(&mut tmp) {
                    Ok(0) => {
                        eof = true;
                        progressed = true;
                        break;
                    }
                    Ok(n) => {
                        self.buf.extend_from_slice(&tmp[..n]);
                        progressed = true;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        eof = true;
                        progressed = true;
                        break;
                    }
                }
            }
        }
        self.emit_lines(outbox);
        if eof {
            if !self.buf.is_empty() {
                outbox.push_back(Item::Data(std::mem::take(&mut self.buf)));
            }
            outbox.push_back(Item::Done);
            self.done = true;
        }
        progressed
    }
}

/// Socket sink: writes each inbound `Item::Data` as a newline-terminated record to a
/// TCP connection.
pub struct SocketSink {
    stream: TcpStream,
}

impl SocketSink {
    pub fn connect(addr: &str) -> std::io::Result<SocketSink> {
        Ok(SocketSink {
            stream: TcpStream::connect(addr)?,
        })
    }
    pub fn from_stream(stream: TcpStream) -> SocketSink {
        SocketSink { stream }
    }
}

impl Processor for SocketSink {
    fn process(&mut self, inbox: &mut VecDeque<Item>, _outbox: &mut VecDeque<Item>) -> bool {
        let mut progressed = false;
        while let Some(item) = inbox.pop_front() {
            if let Item::Data(mut line) = item {
                line.push(b'\n');
                let _ = self.stream.write_all(&line);
                progressed = true;
            }
        }
        let _ = self.stream.flush();
        progressed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(src: &mut FileSource) -> (Vec<Vec<u8>>, bool) {
        let mut inbox = VecDeque::new();
        let mut outbox = VecDeque::new();
        src.process(&mut inbox, &mut outbox);
        let mut data = Vec::new();
        let mut done = false;
        for i in outbox {
            match i {
                Item::Data(b) => data.push(b),
                Item::Done => done = true,
                _ => {}
            }
        }
        (data, done)
    }

    #[test]
    fn emits_each_line_then_done() {
        let mut src = FileSource::from_lines(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let (data, done) = run(&mut src);
        assert_eq!(data, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert!(done, "source terminates with Done");
        // Idempotent: a second pass produces nothing.
        assert_eq!(run(&mut src), (vec![], false));
    }

    #[test]
    fn reads_a_real_file() {
        let mut path = std::env::temp_dir();
        path.push(format!("bonsai-filesource-{}.txt", std::process::id()));
        std::fs::write(&path, "line1\nline2\n").unwrap();
        let mut src = FileSource::open(&path);
        let (data, done) = run(&mut src);
        assert_eq!(data, vec![b"line1".to_vec(), b"line2".to_vec()]);
        assert!(done);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn missing_file_is_empty_source() {
        let mut src = FileSource::open(std::path::Path::new("/no/such/file/xyz"));
        assert_eq!(run(&mut src), (vec![], true)); // just Done
    }

    #[test]
    fn socket_source_reads_lines_then_done_on_eof() {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            conn.write_all(b"alpha\nbeta\ngamma\r\n").unwrap(); // incl. a CRLF line
            conn.write_all(b"delta").unwrap(); // trailing partial line, flushed at EOF
                                               // drop conn -> EOF
        });
        let mut src = SocketSource::connect(&addr.to_string()).unwrap();
        let (mut lines, mut done) = (Vec::new(), false);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !done && std::time::Instant::now() < deadline {
            let (mut inbox, mut outbox) = (VecDeque::new(), VecDeque::new());
            src.process(&mut inbox, &mut outbox);
            for i in outbox {
                match i {
                    Item::Data(b) => lines.push(b),
                    Item::Done => done = true,
                    _ => {}
                }
            }
            if !done {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        server.join().unwrap();
        assert!(done, "source terminates on peer EOF");
        assert_eq!(
            lines,
            vec![
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"gamma".to_vec(),
                b"delta".to_vec()
            ]
        );
    }

    #[test]
    fn socket_sink_writes_newline_records() {
        use std::io::{BufRead, BufReader};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let mut r = BufReader::new(conn);
            let mut got = Vec::new();
            for _ in 0..2 {
                let mut line = String::new();
                r.read_line(&mut line).unwrap();
                got.push(line.trim_end().to_string());
            }
            got
        });
        let mut sink = SocketSink::connect(&addr.to_string()).unwrap();
        let mut inbox: VecDeque<Item> = VecDeque::new();
        inbox.push_back(Item::Data(b"one".to_vec()));
        inbox.push_back(Item::Data(b"two".to_vec()));
        sink.process(&mut inbox, &mut VecDeque::new());
        assert_eq!(
            server.join().unwrap(),
            vec!["one".to_string(), "two".to_string()]
        );
    }
}
