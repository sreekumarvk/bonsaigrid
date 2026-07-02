//! Stateful stream-stream inner join on a key, with **watermark-driven state
//! eviction** so buffered state stays bounded. Each side's records are buffered
//! by key with an event timestamp; an arriving record emits one joined output per
//! buffered record on the opposite side (both arrival orders), and a watermark
//! `w` evicts records whose timestamp is older than `w - retention`. Input
//! records are tagged with their side so the join fits the single-inbox
//! [`Processor`] model.

use crate::processor::{Item, Processor};
use std::collections::{HashMap, VecDeque};

/// Which input a record arrived on.
pub const LEFT: u8 = 0;
pub const RIGHT: u8 = 1;

/// Encode a join input record: `[side:u8][ts:i64][key_len:u32][key][payload]`.
pub fn encode_input(side: u8, ts: i64, key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(13 + key.len() + payload.len());
    b.push(side);
    b.extend_from_slice(&ts.to_le_bytes());
    b.extend_from_slice(&(key.len() as u32).to_le_bytes());
    b.extend_from_slice(key);
    b.extend_from_slice(payload);
    b
}

fn decode_input(b: &[u8]) -> Option<(u8, i64, &[u8], &[u8])> {
    if b.len() < 13 {
        return None;
    }
    let side = b[0];
    let ts = i64::from_le_bytes(b[1..9].try_into().ok()?);
    let kl = u32::from_le_bytes(b[9..13].try_into().ok()?) as usize;
    if b.len() < 13 + kl {
        return None;
    }
    Some((side, ts, &b[13..13 + kl], &b[13 + kl..]))
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

type Buffer = HashMap<Vec<u8>, Vec<(i64, Vec<u8>)>>;

/// A stateful keyed inner-join processor with watermark eviction.
pub struct JoinProcessor {
    left: Buffer,
    right: Buffer,
    /// A record is retained until a watermark passes `ts + retention`.
    retention: i64,
}

impl JoinProcessor {
    /// A join that never evicts (unbounded state).
    pub fn unbounded() -> JoinProcessor {
        JoinProcessor::new(i64::MAX)
    }
    /// A join that evicts a record once a watermark passes `ts + retention`.
    pub fn new(retention: i64) -> JoinProcessor {
        JoinProcessor {
            left: HashMap::new(),
            right: HashMap::new(),
            retention,
        }
    }

    fn evict(buf: &mut Buffer, cutoff: i64) {
        for recs in buf.values_mut() {
            recs.retain(|(ts, _)| *ts >= cutoff);
        }
        buf.retain(|_, recs| !recs.is_empty());
    }
}

impl Processor for JoinProcessor {
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        let mut processed = false;
        while let Some(item) = inbox.pop_front() {
            processed = true;
            match item {
                Item::Data(bytes) => {
                    let Some((side, ts, key, payload)) = decode_input(&bytes) else {
                        continue;
                    };
                    let (key, payload) = (key.to_vec(), payload.to_vec());
                    if side == LEFT {
                        for (_, r) in self.right.get(&key).into_iter().flatten() {
                            outbox.push_back(Item::Data(encode_joined(&key, &payload, r)));
                        }
                        self.left.entry(key).or_default().push((ts, payload));
                    } else {
                        for (_, l) in self.left.get(&key).into_iter().flatten() {
                            outbox.push_back(Item::Data(encode_joined(&key, l, &payload)));
                        }
                        self.right.entry(key).or_default().push((ts, payload));
                    }
                }
                Item::Watermark(w) => {
                    if self.retention != i64::MAX {
                        let cutoff = w - self.retention;
                        Self::evict(&mut self.left, cutoff);
                        Self::evict(&mut self.right, cutoff);
                    }
                    outbox.push_back(Item::Watermark(w));
                }
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
        let mut p = JoinProcessor::unbounded();
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_input(LEFT, 1, b"k", b"L1")),
                Item::Data(encode_input(RIGHT, 2, b"k", b"R1")), // -> (L1, R1)
                Item::Data(encode_input(LEFT, 3, b"k", b"L2")),  // -> (L2, R1)
                Item::Data(encode_input(RIGHT, 4, b"other", b"X")), // no match
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
        let mut p = JoinProcessor::unbounded();
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_input(LEFT, 1, b"k", b"L1")),
                Item::Data(encode_input(LEFT, 2, b"k", b"L2")),
                Item::Data(encode_input(RIGHT, 3, b"k", b"R1")),
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
    fn watermark_evicts_stale_state() {
        let mut p = JoinProcessor::new(5);
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_input(LEFT, 1, b"k", b"L1")),
                Item::Watermark(100), // evicts L1 (1 < 100 - 5)
                Item::Data(encode_input(RIGHT, 101, b"k", b"R1")), // no L1 to join -> buffered
                Item::Data(encode_input(LEFT, 102, b"k", b"L2")), // joins the live R1
            ],
        );
        // L1 was evicted, so (L1, R1) never emits; only the fresh L2 joins R1.
        assert_eq!(out, vec![(b"k".to_vec(), b"L2".to_vec(), b"R1".to_vec())]);
    }
}
