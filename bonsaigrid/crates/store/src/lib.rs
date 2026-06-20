//! Single-node, opaque-blob in-memory map store.
//!
//! Keys and values are stored as raw serialized `Data` blobs and never
//! deserialized. Keyed by `(map_name, key_blob)`.
//!
//! The `Mutex` here exists only because the increment-0 server uses
//! thread-per-connection. The shared-nothing, per-core store replaces this in
//! increment 3 (see the cross-core routing spec). Do not optimize now.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct Store {
    inner: Mutex<HashMap<(String, Vec<u8>), Vec<u8>>>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Insert; returns the previous value if the key existed.
    pub fn put(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().insert((map.to_string(), key), val)
    }

    /// Look up; returns the stored blob verbatim.
    pub fn get(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(&(map.to_string(), key.to_vec()))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_returns_prior_value() {
        let s = Store::new();
        assert_eq!(s.put("m", vec![1, 2], vec![9]), None);
        assert_eq!(s.put("m", vec![1, 2], vec![8]), Some(vec![9]));
    }

    #[test]
    fn get_returns_stored_blob_verbatim() {
        let s = Store::new();
        s.put("m", vec![1, 2], vec![0xAB, 0xCD]);
        assert_eq!(s.get("m", &[1, 2]), Some(vec![0xAB, 0xCD]));
        assert_eq!(s.get("m", &[9, 9]), None);
    }

    #[test]
    fn maps_are_isolated_by_name() {
        let s = Store::new();
        s.put("a", vec![1], vec![10]);
        assert_eq!(s.get("b", &[1]), None);
    }
}
