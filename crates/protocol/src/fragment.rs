//! Inbound message fragment reassembly.
//!
//! A large client message is split into fragments. Each fragment is a frame
//! stream whose first frame is a "fragmentation frame" — content holds an 8-byte
//! fragmentation id (little-endian @0), flags carry BEGIN_FRAGMENT / END_FRAGMENT
//! (or neither for middle fragments). The receiver drops the fragmentation frame
//! and concatenates the remaining frames across fragments (keyed by id) until
//! END_FRAGMENT, then has the complete message.
//!
//! An *unfragmented* message has both fragment bits set (UNFRAGMENTED) on its
//! first frame and is handled directly — the reactor only calls this for
//! fragmented messages.

use crate::fixed::read_i64_le;
use crate::frame::{read_message, write_message, Frame, END_FRAGMENT, IS_FINAL};
use std::collections::HashMap;

#[derive(Default)]
pub struct Reassembler {
    partial: HashMap<u64, Vec<Frame>>,
}

impl Reassembler {
    pub fn new() -> Reassembler {
        Reassembler::default()
    }

    /// Feed one complete fragment's wire bytes. Returns the assembled message's
    /// wire bytes once END_FRAGMENT arrives, else None.
    pub fn push(&mut self, fragment: &[u8]) -> Option<Vec<u8>> {
        let (frames, _) = read_message(fragment)?;
        if frames.is_empty() {
            return None;
        }
        let frag_flags = frames[0].flags;
        let frag_id = read_i64_le(&frames[0].content, 0) as u64;

        // Drop the fragmentation frame; accumulate the real frames, clearing the
        // per-fragment IS_FINAL terminator (write_message re-adds it once).
        let entry = self.partial.entry(frag_id).or_default();
        for f in &frames[1..] {
            entry.push(Frame {
                flags: f.flags & !IS_FINAL,
                content: f.content.clone(),
            });
        }

        if frag_flags & END_FRAGMENT != 0 {
            let full = self.partial.remove(&frag_id).unwrap_or_default();
            Some(write_message(&full))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::{write_i32_le, write_i64_le};
    use crate::frame::{BEGIN_FRAGMENT, UNFRAGMENTED};

    fn frag_frame(flags: u16, frag_id: u64) -> Frame {
        let mut content = vec![0u8; 8];
        write_i64_le(&mut content, 0, frag_id as i64);
        Frame { flags, content }
    }

    #[test]
    fn reassembles_a_two_fragment_message() {
        // Build a real message of 3 frames (an initial frame + two data frames).
        let mut initial = vec![0u8; 16];
        write_i32_le(&mut initial, 0, 65792); // a MapPut-ish header
        let real = vec![
            Frame {
                flags: UNFRAGMENTED,
                content: initial,
            },
            Frame {
                flags: 0,
                content: vec![1, 2, 3],
            },
            Frame {
                flags: 0,
                content: vec![4, 5, 6, 7],
            },
        ];
        let expected = write_message(&real);

        // Split into two fragments (frag id 99): [fragFrame(BEGIN), real[0], real[1]]
        // and [fragFrame(END), real[2]]. Each fragment is its own wire message.
        let frag1 = write_message(&[
            frag_frame(BEGIN_FRAGMENT, 99),
            real[0].clone(),
            real[1].clone(),
        ]);
        let frag2 = write_message(&[frag_frame(END_FRAGMENT, 99), real[2].clone()]);

        let mut r = Reassembler::new();
        assert!(r.push(&frag1).is_none(), "first fragment is incomplete");
        let assembled = r.push(&frag2).expect("END fragment completes the message");
        assert_eq!(
            assembled, expected,
            "reassembled bytes equal the original message"
        );
    }

    #[test]
    fn three_fragment_message_with_middle() {
        let real: Vec<Frame> = (0..3)
            .map(|i| Frame {
                flags: if i == 0 { UNFRAGMENTED } else { 0 },
                content: vec![i as u8; 5],
            })
            .collect();
        let expected = write_message(&real);
        let f1 = write_message(&[frag_frame(BEGIN_FRAGMENT, 7), real[0].clone()]);
        let f2 = write_message(&[frag_frame(0, 7), real[1].clone()]); // middle
        let f3 = write_message(&[frag_frame(END_FRAGMENT, 7), real[2].clone()]);
        let mut r = Reassembler::new();
        assert!(r.push(&f1).is_none());
        assert!(r.push(&f2).is_none());
        assert_eq!(r.push(&f3).unwrap(), expected);
    }
}
