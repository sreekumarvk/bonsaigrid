//! Event-time windowing processors: tumbling, sliding, and session windows with
//! a sum/count aggregate, driven by watermarks.
//!
//! Input events are `Item::Data([ts:i64][value:i64][key...])`. A window's result
//! is emitted as `Item::Data([win_start:i64][win_end:i64][aggregate:i64][key...])`
//! once a watermark proves the window complete (no later event can fall in it):
//! tumbling/sliding close when `watermark >= window_end`; a session closes when
//! `watermark >= last_event_ts + gap`. `Item::Done` flushes everything.

use crate::processor::{Item, Processor};
use std::collections::{BTreeMap, HashMap, VecDeque};

/// The windowing strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowKind {
    /// Fixed, non-overlapping windows of `size`.
    Tumbling { size: i64 },
    /// Overlapping windows of `size` advancing by `slide`.
    Sliding { size: i64, slide: i64 },
    /// Per-key windows that extend while events arrive within `gap`.
    Session { gap: i64 },
}

/// The aggregate computed per window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Agg {
    Sum,
    Count,
    Min,
    Max,
    /// Integer mean (`sum / count`).
    Avg,
}

/// A per-window accumulator supporting every [`Agg`] with one pass.
#[derive(Clone, Copy, Default)]
struct Acc {
    sum: i64,
    count: i64,
    min: i64,
    max: i64,
}

impl Acc {
    fn add(&mut self, value: i64) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.sum += value;
        self.count += 1;
    }
    fn get(&self, agg: Agg) -> i64 {
        match agg {
            Agg::Sum => self.sum,
            Agg::Count => self.count,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Avg if self.count > 0 => self.sum / self.count,
            Agg::Avg => 0,
        }
    }
}

/// Encode a windowing input event.
pub fn encode_event(ts: i64, value: i64, key: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + key.len());
    b.extend_from_slice(&ts.to_le_bytes());
    b.extend_from_slice(&value.to_le_bytes());
    b.extend_from_slice(key);
    b
}

fn decode_event(b: &[u8]) -> Option<(i64, i64, &[u8])> {
    if b.len() < 16 {
        return None;
    }
    let ts = i64::from_le_bytes(b[0..8].try_into().ok()?);
    let value = i64::from_le_bytes(b[8..16].try_into().ok()?);
    Some((ts, value, &b[16..]))
}

/// Encode a window result.
pub fn encode_result(start: i64, end: i64, agg: i64, key: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(24 + key.len());
    b.extend_from_slice(&start.to_le_bytes());
    b.extend_from_slice(&end.to_le_bytes());
    b.extend_from_slice(&agg.to_le_bytes());
    b.extend_from_slice(key);
    b
}

/// Decode a window result (for tests / downstream).
pub fn decode_result(b: &[u8]) -> Option<(i64, i64, i64, Vec<u8>)> {
    if b.len() < 24 {
        return None;
    }
    let start = i64::from_le_bytes(b[0..8].try_into().ok()?);
    let end = i64::from_le_bytes(b[8..16].try_into().ok()?);
    let agg = i64::from_le_bytes(b[16..24].try_into().ok()?);
    Some((start, end, agg, b[24..].to_vec()))
}

#[derive(Clone)]
struct Session {
    start: i64,
    last: i64,
    acc: Acc,
}

/// A stateful windowing processor.
pub struct WindowProcessor {
    kind: WindowKind,
    agg: Agg,
    /// Tumbling/sliding accumulators keyed by `(window_start, key)`.
    fixed: BTreeMap<(i64, Vec<u8>), Acc>,
    /// Open session windows per key (kept sorted by start).
    sessions: HashMap<Vec<u8>, Vec<Session>>,
}

impl WindowProcessor {
    pub fn new(kind: WindowKind, agg: Agg) -> WindowProcessor {
        WindowProcessor {
            kind,
            agg,
            fixed: BTreeMap::new(),
            sessions: HashMap::new(),
        }
    }

    fn window_ends(&self) -> i64 {
        match self.kind {
            WindowKind::Tumbling { size } | WindowKind::Sliding { size, .. } => size,
            WindowKind::Session { .. } => 0,
        }
    }

    /// Assign an event to its window(s) and fold in the value.
    fn add(&mut self, ts: i64, value: i64, key: &[u8]) {
        match self.kind {
            WindowKind::Tumbling { size } => {
                let start = ts.div_euclid(size) * size;
                self.fixed
                    .entry((start, key.to_vec()))
                    .or_default()
                    .add(value);
            }
            WindowKind::Sliding { size, slide } => {
                // Every window start that is a multiple of `slide` and covers ts.
                let last = ts.div_euclid(slide) * slide;
                let mut start = last;
                while start > ts - size {
                    self.fixed
                        .entry((start, key.to_vec()))
                        .or_default()
                        .add(value);
                    start -= slide;
                }
            }
            WindowKind::Session { gap } => {
                let list = self.sessions.entry(key.to_vec()).or_default();
                // Find a session this event extends (within gap on either side).
                if let Some(s) = list
                    .iter_mut()
                    .find(|s| ts >= s.start - gap && ts <= s.last + gap)
                {
                    s.start = s.start.min(ts);
                    s.last = s.last.max(ts);
                    s.acc.add(value);
                } else {
                    let mut acc = Acc::default();
                    acc.add(value);
                    list.push(Session {
                        start: ts,
                        last: ts,
                        acc,
                    });
                }
            }
        }
    }

    /// Emit (and drop) every window proven complete by watermark `w`, in
    /// `(start, key)` order.
    fn close(&mut self, w: i64, outbox: &mut VecDeque<Item>) {
        match self.kind {
            WindowKind::Tumbling { .. } | WindowKind::Sliding { .. } => {
                let size = self.window_ends();
                let ready: Vec<(i64, Vec<u8>)> = self
                    .fixed
                    .range(..)
                    .filter(|((start, _), _)| start + size <= w)
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in ready {
                    let acc = self.fixed.remove(&k).unwrap();
                    outbox.push_back(Item::Data(encode_result(
                        k.0,
                        k.0 + size,
                        acc.get(self.agg),
                        &k.1,
                    )));
                }
            }
            WindowKind::Session { gap } => {
                let mut out: Vec<(i64, Vec<u8>, i64, Acc)> = Vec::new();
                for (key, list) in self.sessions.iter_mut() {
                    list.retain(|s| {
                        if s.last + gap <= w {
                            out.push((s.start, key.clone(), s.last, s.acc));
                            false
                        } else {
                            true
                        }
                    });
                }
                self.sessions.retain(|_, l| !l.is_empty());
                out.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
                for (start, key, last, acc) in out {
                    outbox.push_back(Item::Data(encode_result(
                        start,
                        last + 1,
                        acc.get(self.agg),
                        &key,
                    )));
                }
            }
        }
    }

    /// Emit all remaining windows (on Done), in order.
    fn flush(&mut self, outbox: &mut VecDeque<Item>) {
        self.close(i64::MAX, outbox);
    }
}

impl Processor for WindowProcessor {
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        let mut processed = false;
        while let Some(item) = inbox.pop_front() {
            processed = true;
            match item {
                Item::Data(bytes) => {
                    if let Some((ts, value, key)) = decode_event(&bytes) {
                        let key = key.to_vec();
                        self.add(ts, value, &key);
                    }
                }
                Item::Watermark(w) => {
                    self.close(w, outbox);
                    outbox.push_back(Item::Watermark(w));
                }
                Item::Done => {
                    self.flush(outbox);
                    outbox.push_back(Item::Done);
                }
            }
        }
        processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(p: &mut WindowProcessor, items: Vec<Item>) -> Vec<(i64, i64, i64, Vec<u8>)> {
        let mut inbox: VecDeque<Item> = items.into();
        let mut outbox = VecDeque::new();
        p.process(&mut inbox, &mut outbox);
        outbox
            .into_iter()
            .filter_map(|i| match i {
                Item::Data(b) => decode_result(&b),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn tumbling_sums_per_window_on_watermark() {
        let mut p = WindowProcessor::new(WindowKind::Tumbling { size: 10 }, Agg::Sum);
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_event(1, 5, b"k")),
                Item::Data(encode_event(3, 7, b"k")), // window [0,10) -> 12
                Item::Data(encode_event(11, 4, b"k")), // window [10,20)
                Item::Watermark(10),                  // closes [0,10) only
            ],
        );
        assert_eq!(out, vec![(0, 10, 12, b"k".to_vec())]);
    }

    #[test]
    fn tumbling_separates_keys() {
        let mut p = WindowProcessor::new(WindowKind::Tumbling { size: 10 }, Agg::Count);
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_event(1, 0, b"a")),
                Item::Data(encode_event(2, 0, b"a")),
                Item::Data(encode_event(2, 0, b"b")),
                Item::Watermark(10),
            ],
        );
        assert_eq!(
            out,
            vec![(0, 10, 2, b"a".to_vec()), (0, 10, 1, b"b".to_vec())]
        );
    }

    #[test]
    fn sliding_assigns_event_to_overlapping_windows() {
        // size 10, slide 5: ts=7 -> windows [0,10) and [5,15).
        let mut p = WindowProcessor::new(WindowKind::Sliding { size: 10, slide: 5 }, Agg::Sum);
        let out = drive(
            &mut p,
            vec![Item::Data(encode_event(7, 3, b"k")), Item::Watermark(20)],
        );
        assert_eq!(
            out,
            vec![(0, 10, 3, b"k".to_vec()), (5, 15, 3, b"k".to_vec())]
        );
    }

    #[test]
    fn session_merges_within_gap_and_closes_after() {
        let mut p = WindowProcessor::new(WindowKind::Session { gap: 5 }, Agg::Sum);
        // 1 and 4 merge (gap 5); 100 is a separate session.
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_event(1, 2, b"k")),
                Item::Data(encode_event(4, 3, b"k")),
                Item::Data(encode_event(100, 9, b"k")),
                Item::Watermark(50), // closes the [1,4] session (4+5<=50), not the 100 one
            ],
        );
        assert_eq!(out, vec![(1, 5, 5, b"k".to_vec())]);
    }

    #[test]
    fn min_max_avg_aggregates() {
        for (agg, expect) in [(Agg::Min, 3), (Agg::Max, 10), (Agg::Avg, 6)] {
            let mut p = WindowProcessor::new(WindowKind::Tumbling { size: 100 }, agg);
            let out = drive(
                &mut p,
                vec![
                    Item::Data(encode_event(1, 5, b"k")),
                    Item::Data(encode_event(2, 10, b"k")),
                    Item::Data(encode_event(3, 3, b"k")), // {5,10,3}: min 3, max 10, avg 6
                    Item::Watermark(100),
                ],
            );
            assert_eq!(out, vec![(0, 100, expect, b"k".to_vec())], "agg {agg:?}");
        }
    }

    #[test]
    fn done_flushes_all_open_windows() {
        let mut p = WindowProcessor::new(WindowKind::Tumbling { size: 10 }, Agg::Sum);
        let out = drive(
            &mut p,
            vec![
                Item::Data(encode_event(1, 5, b"k")),
                Item::Data(encode_event(25, 8, b"k")),
                Item::Done,
            ],
        );
        assert_eq!(
            out,
            vec![(0, 10, 5, b"k".to_vec()), (20, 30, 8, b"k".to_vec())]
        );
    }
}
