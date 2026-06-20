//! Single-node, opaque-blob in-memory map.
//!
//! Increment 1: a slab allocator (contiguous, size-classed, O(1) free list)
//! holds each entry's `key ++ value` bytes, and an open-addressing (linear
//! probing) table holds fixed-size entry records inline. This removes the
//! per-entry `HashMap` node, `String` key, and separate `Vec` allocations the
//! baseline paid — cutting bytes/entry substantially.
//!
//! Keys/values remain opaque serialized `Data` blobs, never deserialized.
//!
//! The `Mutex` exists only because the increment-1 server is still
//! thread-per-connection. Increment 3 replaces it with a shared-nothing,
//! per-core store (see the cross-core routing spec).

mod slab;

use slab::{Handle, Slab};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy)]
struct Entry {
    hash: u64, // 0 == empty slot
    map_id: u32,
    handle: Handle,
    key_len: u32,
    val_len: u32,
}

impl Entry {
    const EMPTY: Entry = Entry {
        hash: 0,
        map_id: 0,
        handle: Handle { class: 0, slot: 0 },
        key_len: 0,
        val_len: 0,
    };
    fn is_empty(&self) -> bool {
        self.hash == 0
    }
}

struct Inner {
    slab: Slab,
    table: Vec<Entry>,
    mask: usize,
    len: usize,
    map_ids: HashMap<String, u32>,
    map_names: Vec<String>,
}

/// FNV-1a over (map_id, key); forced non-zero so 0 can mark empty slots.
fn hash(map_id: u32, key: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in map_id.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h | 1
}

impl Inner {
    fn new() -> Inner {
        let cap = 1024;
        Inner {
            slab: Slab::new(),
            table: vec![Entry::EMPTY; cap],
            mask: cap - 1,
            len: 0,
            map_ids: HashMap::new(),
            map_names: Vec::new(),
        }
    }

    fn intern(&mut self, map: &str) -> u32 {
        if let Some(&id) = self.map_ids.get(map) {
            return id;
        }
        let id = self.map_names.len() as u32;
        self.map_names.push(map.to_string());
        self.map_ids.insert(map.to_string(), id);
        id
    }

    fn key_bytes(&self, e: &Entry) -> &[u8] {
        &self.slab.get(e.handle, (e.key_len + e.val_len) as usize)[..e.key_len as usize]
    }

    fn val_bytes(&self, e: &Entry) -> Vec<u8> {
        let total = (e.key_len + e.val_len) as usize;
        self.slab.get(e.handle, total)[e.key_len as usize..].to_vec()
    }

    fn maybe_grow(&mut self) {
        // Resize at 7/8 load (matches hashbrown, for fair density comparison).
        if (self.len + 1) * 8 < (self.mask + 1) * 7 {
            return;
        }
        let new_cap = (self.mask + 1) * 2;
        let mut new_table = vec![Entry::EMPTY; new_cap];
        let new_mask = new_cap - 1;
        for e in self.table.iter().filter(|e| !e.is_empty()) {
            let mut i = e.hash as usize & new_mask;
            while !new_table[i].is_empty() {
                i = (i + 1) & new_mask;
            }
            new_table[i] = *e;
        }
        self.table = new_table;
        self.mask = new_mask;
    }

    fn put(&mut self, map: &str, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
        let map_id = self.intern(map);
        let h = hash(map_id, key);
        self.maybe_grow();
        let mut i = h as usize & self.mask;
        loop {
            let e = self.table[i];
            if e.is_empty() {
                let handle = self.slab.put_two(key, val);
                self.table[i] = Entry {
                    hash: h,
                    map_id,
                    handle,
                    key_len: key.len() as u32,
                    val_len: val.len() as u32,
                };
                self.len += 1;
                return None;
            }
            if e.hash == h && e.map_id == map_id && self.key_bytes(&e) == key {
                let old = self.val_bytes(&e);
                self.slab.free(e.handle);
                let handle = self.slab.put_two(key, val);
                self.table[i] = Entry {
                    hash: h,
                    map_id,
                    handle,
                    key_len: key.len() as u32,
                    val_len: val.len() as u32,
                };
                return Some(old);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn get(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        let map_id = *self.map_ids.get(map)?;
        let h = hash(map_id, key);
        let mut i = h as usize & self.mask;
        loop {
            let e = self.table[i];
            if e.is_empty() {
                return None;
            }
            if e.hash == h && e.map_id == map_id && self.key_bytes(&e) == key {
                return Some(self.val_bytes(&e));
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Standalone shard selector — independent of per-shard map interning, so the
/// same (map, key) always lands on the same shard regardless of which core or
/// connection serves the request. This makes a request servable by *any* core
/// correctly (the spec's per-core ownership realized as per-shard locks; under
/// TPC each core touches only its own shard, so the lock is uncontended).
fn shard_of(map: &str, key: &[u8], n: usize) -> usize {
    let mut h = 0xcbf29ce484222325u64;
    for &b in map.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) % n
}

pub struct Store {
    shards: Vec<Mutex<Inner>>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Store {
        Self::with_shards(1)
    }

    /// Create a store partitioned into `n` independently-locked shards.
    pub fn with_shards(n: usize) -> Store {
        assert!(n >= 1);
        Store {
            shards: (0..n).map(|_| Mutex::new(Inner::new())).collect(),
        }
    }

    /// Insert; returns the previous value if the key existed.
    pub fn put(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        let i = shard_of(map, &key, self.shards.len());
        self.shards[i].lock().unwrap().put(map, &key, &val)
    }

    /// Look up; returns the stored blob verbatim.
    pub fn get(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        let i = shard_of(map, key, self.shards.len());
        self.shards[i].lock().unwrap().get(map, key)
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

    #[test]
    fn survives_growth_and_overwrites() {
        let s = Store::new();
        for i in 0..5000u32 {
            assert_eq!(s.put("m", i.to_le_bytes().to_vec(), vec![i as u8; 40]), None);
        }
        for i in 0..5000u32 {
            assert_eq!(s.get("m", &i.to_le_bytes()), Some(vec![i as u8; 40]));
        }
        // overwrite returns prior and reclaims slab
        assert_eq!(s.put("m", 7u32.to_le_bytes().to_vec(), vec![1]), Some(vec![7u8; 40]));
        assert_eq!(s.get("m", &7u32.to_le_bytes()), Some(vec![1]));
    }

    #[test]
    fn sharded_store_is_correct_for_all_keys() {
        let s = Store::with_shards(8);
        for i in 0..10000u32 {
            s.put("m", i.to_le_bytes().to_vec(), vec![i as u8; 30]);
        }
        for i in 0..10000u32 {
            assert_eq!(s.get("m", &i.to_le_bytes()), Some(vec![i as u8; 30]));
        }
        // same key always resolves to the same shard -> overwrite works
        assert_eq!(s.put("m", 42u32.to_le_bytes().to_vec(), vec![1]), Some(vec![42u8; 30]));
    }

    #[test]
    fn large_overflow_values_roundtrip() {
        let s = Store::new();
        let big = vec![3u8; 20_000];
        s.put("m", vec![1], big.clone());
        assert_eq!(s.get("m", &[1]), Some(big));
    }
}
