//! Capture sink: a `store::WalSink` that turns each local IMap mutation into a
//! `WanRecord` pushed to the WAN thread over an SPSC ring. A full ring drops the
//! record (surfaced by metrics); the hot path never blocks on WAN.

use crate::record::{WanOp, WanRecord};
use store::WalSink;

pub struct WanPublisher {
    tx: spsc::Producer<WanRecord>,
}

impl WanPublisher {
    pub fn new(tx: spsc::Producer<WanRecord>) -> WanPublisher {
        WanPublisher { tx }
    }
}

impl WalSink for WanPublisher {
    fn map_put(&self, stamp: u64, ttl_ms: u64, map: &str, key: &[u8], value: &[u8]) {
        let _ = self.tx.push(WanRecord {
            op: WanOp::Put,
            stamp,
            ttl_ms,
            map: map.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }
    fn map_remove(&self, stamp: u64, map: &str, key: &[u8]) {
        let _ = self.tx.push(WanRecord {
            op: WanOp::Remove,
            stamp,
            ttl_ms: 0,
            map: map.to_string(),
            key: key.to_vec(),
            value: Vec::new(),
        });
    }
    fn aux_state(&self, kind: u8, name: &str, state: &[u8]) {
        let _ = self.tx.push(WanRecord {
            op: WanOp::Aux(kind),
            stamp: 0,
            ttl_ms: 0,
            map: name.to_string(),
            key: Vec::new(),
            value: state.to_vec(),
        });
    }
}
