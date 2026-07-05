//! WAN consumer: apply an inbound batch to the local store via `apply_wan`
//! (HLC merge, not re-published — loop prevention lives in the store).

use crate::record::{WanOp, WanRecord};
use store::Store;

pub fn apply_batch(store: &Store, records: &[WanRecord]) {
    for r in records {
        match r.op {
            // A non-map structure's full state — install directly (install_aux does
            // not re-emit to any sink, so it is loop-free by construction).
            WanOp::Aux(kind) => store.install_aux(kind, &r.map, &r.value),
            WanOp::Put => store.apply_wan(true, &r.map, &r.key, &r.value, r.ttl_ms, r.stamp),
            WanOp::Remove => store.apply_wan(false, &r.map, &r.key, &r.value, r.ttl_ms, r.stamp),
        }
    }
}
