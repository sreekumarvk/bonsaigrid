//! Stateful stream-stream inner join on a key. Each side's records are buffered
//! by key; an arriving record emits one joined output per already-seen record on
//! the opposite side (both arrival orders covered). Input records are tagged with
//! their side so the join fits the single-inbox [`Processor`] model.
//!
//! State is per key and unbounded in v1; a windowed/TTL eviction variant (drop
//! buffered records once a watermark passes their timestamp) is a follow-up.

use crate::processor::{Item, Processor};
use std::collections::{HashMap, VecDeque};

/// Which input a record arrived on.
pub const LEFT: u8 = 0;
pub const RIGHT: u8 = 1;

/// Encode a join input record: `[side:u8][key_len:u32][key][payload]`.
pub fn encode_input(side: u8, key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(5 + key.len() + payload.len());
    b.push(side);
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(payload);
    b
}

fn decode_input(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if b.len() < 5 {
        return None;
    }
    let side = b[0];
    let kl = u32::from_le_bytes(b[1..5].try_into().ok()?) as usize;
    if b.len() < 5 + kl {
        return None;
    }
    Some((side, &b[5..5 + kl], &b[5 + kl..]))
}

/// Encode a joined output: `[key_len:u32][key][left_len:u32][left][right]`.
pub fn encode_joined(key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(8 + key.len() + left.len() + right.len());
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(&(left.len() as u32).to_le_bytes());
    b.extend_from_slice(left);
    b.extend_from_slice(right);
    b
}

/// Decode a joined output (tests / downstream).
pub fn decode_joined(b: &[u8]) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let kl = u32::from_le_bytes(b.get(0..4)?.try_into().ok()?) as usize;
    let key = b.get(4..4 + kl)?.to_vec();
    let o = 4 + kl;
    let ll = u32::from_le_bytes(b.get(o..o + 4)?.try_into().ok()?) as usize;
    let left = b.get(o + 4..o + 4 + ll)?.to_vec();
    let right = b.get(o + 4 + ll..)?.to_vec();
    Some((key, left, right))
}

/// A stateful keyed inner-join processor.
#[derive(Default)]
pub struct JoinProcessor {
    left: HashMap<Vec<u8>, Vec<Vec<u8>>>,
    right: HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl JoinProcessor {
    pub fn new() -> JoinProcessor {
        JoinProcessor::default()
    }
}

impl Processor for JoinProcessor {
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        let mut processed = false;
        while let Some(item) = inbox.pop_front() {
            processed = true;
            match item {
                Item::Data(bytes) => {
                    let Some((side, key, payload)) = decode_input(&bytes) else {
                        continue;
                    };
                    let (key, payload) = (key.to_vec(), payload.to_vec());
                    if side == LEFT {
                        for r in self.right.get(&key).into_iter().flatten() {
                            outbox.push_back(Item::Data(encode_joined(&key, &payload, r)));
                        }
                        self.left.entry(key).or_default().push(payload);
                    } else {
                        for l in self.left.get(&key).into_iter().flatten() {
                            outbox.push_back(Item::Data(encode_joined(&key, l, &payload)));
                        }
                        self.right.entry(key).or_default().push(payload);
                    }
                }
                Item::Watermark(w) => outbox.push_back(Item::Watermark(w)),
                Item::Done => outbox.push_back(Item::Done),
            }
        }
        processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(p: &mut JoinProcessor, items: Vec<Item>) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let mut inbox: VecDeque<Item> = items.into();
        let mut outbox = VecDeque::new();
        p.process(&mut inbox, &mut outbox);
        outbox
            .into_iter()
            .filter_map(|i| match i {
                Item::Data(b) => decode_joined(&b),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn matches_across_arrival_orders() {
        let mut p = JoinProcessor::new();
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_input(LEFT, b"k", b"L1")),
                // right arrives after left -> emits (L1, R1)
                Item::Data(encode_input(RIGHT, b"k", b"R1")),
                // second left after right -> emits (L2, R1)
                Item::Data(encode_input(LEFT, b"k", b"L2")),
                // no match for a different key
                Item::Data(encode_input(RIGHT, b"other", b"X")),
            ],
        );
        assert_eq!(
            out,
            vec![
                (b"k".to_vec(), b"L1".to_vec(), b"R1".to_vec()),
                (b"k".to_vec(), b"L2".to_vec(), b"R1".to_vec()),
            ]
        );
    }

    #[test]
    fn fans_out_to_all_matches_on_a_key() {
        let mut p = JoinProcessor::new();
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_input(LEFT, b"k", b"L1")),
                Item::Data(encode_input(LEFT, b"k", b"L2")),
                // one right joins both buffered lefts
                Item::Data(encode_input(RIGHT, b"k", b"R1")),
            ],
        );
        assert_eq!(
            out,
            vec![
                (b"k".to_vec(), b"L1".to_vec(), b"R1".to_vec()),
                (b"k".to_vec(), b"L2".to_vec(), b"R1".to_vec()),
            ]
        );
    }
}
