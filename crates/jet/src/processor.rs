use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub enum Item {
    Data(Vec<u8>),
    Watermark(i64),
    Done,
}

pub trait Processor: Send + Sync {
    /// Process incoming items and return output items.
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool;
}

pub struct MapProcessor {
    // In a real implementation this would hold a serialized closure or expression.
    // For now, we mock it.
}

impl Processor for MapProcessor {
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        let mut processed = false;
        while let Some(item) = inbox.pop_front() {
            processed = true;
            match item {
                Item::Data(data) => {
                    // Mock map: just append some bytes or pass through
                    let mut new_data = data.clone();
                    new_data.push(0);
                    outbox.push_back(Item::Data(new_data));
                }
                Item::Watermark(w) => outbox.push_back(Item::Watermark(w)),
                Item::Done => outbox.push_back(Item::Done),
            }
        }
        processed
    }
}

pub struct FilterProcessor {
    // Mock filter condition
}

impl Processor for FilterProcessor {
    fn process(&mut self, inbox: &mut VecDeque<Item>, outbox: &mut VecDeque<Item>) -> bool {
        let mut processed = false;
        while let Some(item) = inbox.pop_front() {
            processed = true;
            match item {
                Item::Data(data) => {
                    // Mock filter: drop empty data
                    if !data.is_empty() {
                        outbox.push_back(Item::Data(data));
                    }
                }
                Item::Watermark(w) => outbox.push_back(Item::Watermark(w)),
                Item::Done => outbox.push_back(Item::Done),
            }
        }
        processed
    }
}
