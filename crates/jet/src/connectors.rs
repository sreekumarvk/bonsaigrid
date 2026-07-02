//! Streaming source connectors beyond Kafka/MapStore. v1 adds a file source: read
//! a text file line by line and emit each line as an `Item::Data`, then `Done`.
//! A source ignores its inbox and produces from the external resource.

use crate::processor::{Item, Processor};
use std::collections::VecDeque;

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
}
