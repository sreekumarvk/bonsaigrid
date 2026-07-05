//! WAN consumer: apply an inbound batch to the local store via `apply_wan`
//! (HLC merge, not re-published — loop prevention lives in the store).

use crate::record::{WanOp, WanRecord};
use store::Store;

pub fn apply_batch(store: &Store, records: &[WanRecord]) {
    for r in records {
        store.apply_wan(
            matches!(r.op, WanOp::Put),
            &r.map,
            &r.key,
            &r.value,
            r.ttl_ms,
            r.stamp,
        );
    }
}
