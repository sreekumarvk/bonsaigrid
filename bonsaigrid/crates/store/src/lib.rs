//! Single-node, opaque-blob in-memory map.
//!
//! A size-classed slab allocator (contiguous, O(1) free list) holds each entry's
//! `key ++ value` bytes; an open-addressing (linear-probing, tombstoned) table
//! holds fixed-size entry records inline. Keys/values stay opaque serialized
//! `Data` blobs, never deserialized. Optional per-entry TTL with lazy expiry.
//!
//! Partitioned into N independently-locked shards (`with_shards`); single-key
//! ops route to one shard, aggregate ops fold over all. The per-shard `Mutex`
//! is the increment-3 realization of per-core ownership.

mod slab;

use slab::{Handle, Slab};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

const EMPTY: u64 = 0;
const TOMBSTONE: u64 = 1;

/// Monotonic milliseconds since first use (for TTL).
fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[derive(Clone, Copy)]
struct Entry {
    hash: u64, // EMPTY / TOMBSTONE / real (MSB set)
    map_id: u32,
    handle: Handle,
    key_len: u32,
    val_len: u32,
    expire_at: u64, // 0 == never
}

impl Entry {
    const EMPTY: Entry = Entry {
        hash: EMPTY,
        map_id: 0,
        handle: Handle { class: 0, slot: 0 },
        key_len: 0,
        val_len: 0,
        expire_at: 0,
    };
    fn occupied(&self) -> bool {
        self.hash > TOMBSTONE
    }
    fn expired(&self, now: u64) -> bool {
        self.expire_at != 0 && now >= self.expire_at
    }
}

struct Inner {
    slab: Slab,
    table: Vec<Entry>,
    mask: usize,
    len: usize,
    tombstones: usize,
    map_ids: HashMap<String, u32>,
    map_names: Vec<String>,
    counts: Vec<usize>, // live entries per map_id
}

/// FNV-1a over (map_id, key) with the MSB forced set, so real hashes are
/// distinct from the EMPTY(0) and TOMBSTONE(1) sentinels.
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
    h | (1u64 << 63)
}

impl Inner {
    fn new() -> Inner {
        let cap = 1024;
        Inner {
            slab: Slab::new(),
            table: vec![Entry::EMPTY; cap],
            mask: cap - 1,
            len: 0,
            tombstones: 0,
            map_ids: HashMap::new(),
            map_names: Vec::new(),
            counts: Vec::new(),
        }
    }

    fn intern(&mut self, map: &str) -> u32 {
        if let Some(&id) = self.map_ids.get(map) {
            return id;
        }
        let id = self.map_names.len() as u32;
        self.map_names.push(map.to_string());
        self.map_ids.insert(map.to_string(), id);
        self.counts.push(0);
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
        // Grow at 7/8 of (live + tombstones); rehash drops tombstones.
        if (self.len + self.tombstones + 1) * 8 < (self.mask + 1) * 7 {
            return;
        }
        let new_cap = (self.mask + 1) * 2;
        let mut new_table = vec![Entry::EMPTY; new_cap];
        let new_mask = new_cap - 1;
        for e in self.table.iter().filter(|e| e.occupied()) {
            let mut i = e.hash as usize & new_mask;
            while new_table[i].occupied() {
                i = (i + 1) & new_mask;
            }
            new_table[i] = *e;
        }
        self.table = new_table;
        self.mask = new_mask;
        self.tombstones = 0;
    }

    fn put(&mut self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64) -> Option<Vec<u8>> {
        let map_id = self.intern(map);
        let h = hash(map_id, key);
        let expire_at = if ttl_ms > 0 { now_ms() + ttl_ms } else { 0 };
        self.maybe_grow();
        let mut i = h as usize & self.mask;
        let mut first_free: Option<usize> = None;
        loop {
            let e = self.table[i];
            if e.hash == EMPTY {
                let slot = first_free.unwrap_or(i);
                if self.table[slot].hash == TOMBSTONE {
                    self.tombstones -= 1;
                }
                let handle = self.slab.put_two(key, val);
                self.table[slot] = Entry {
                    hash: h,
                    map_id,
                    handle,
                    key_len: key.len() as u32,
                    val_len: val.len() as u32,
                    expire_at,
                };
                self.len += 1;
                self.counts[map_id as usize] += 1;
                return None;
            }
            if e.hash == TOMBSTONE {
                if first_free.is_none() {
                    first_free = Some(i);
                }
                i = (i + 1) & self.mask;
                continue;
            }
            if e.hash == h && e.map_id == map_id && self.key_bytes(&e) == key {
                let old = if e.expired(now_ms()) { None } else { Some(self.val_bytes(&e)) };
                self.slab.free(e.handle);
                let handle = self.slab.put_two(key, val);
                self.table[i] = Entry {
                    hash: h,
                    map_id,
                    handle,
                    key_len: key.len() as u32,
                    val_len: val.len() as u32,
                    expire_at,
                };
                if old.is_none() && e.expired(now_ms()) {
                    // previously-expired slot becomes live again
                }
                return old;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns the slot index of a live (non-expired) entry for (map,key), lazily
    /// reaping an expired one.
    fn find(&mut self, map: &str, key: &[u8]) -> Option<usize> {
        let map_id = *self.map_ids.get(map)?;
        let h = hash(map_id, key);
        let now = now_ms();
        let mut i = h as usize & self.mask;
        loop {
            let e = self.table[i];
            if e.hash == EMPTY {
                return None;
            }
            if e.hash == TOMBSTONE {
                i = (i + 1) & self.mask;
                continue;
            }
            if e.hash == h && e.map_id == map_id && self.key_bytes(&e) == key {
                if e.expired(now) {
                    self.slab.free(e.handle);
                    self.table[i].hash = TOMBSTONE;
                    self.len -= 1;
                    self.tombstones += 1;
                    self.counts[map_id as usize] -= 1;
                    return None;
                }
                return Some(i);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn get(&mut self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        let i = self.find(map, key)?;
        Some(self.val_bytes(&self.table[i]))
    }

    fn contains_key(&mut self, map: &str, key: &[u8]) -> bool {
        self.find(map, key).is_some()
    }

    fn remove(&mut self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        let i = self.find(map, key)?;
        let e = self.table[i];
        let old = self.val_bytes(&e);
        self.slab.free(e.handle);
        self.table[i].hash = TOMBSTONE;
        self.len -= 1;
        self.tombstones += 1;
        self.counts[e.map_id as usize] -= 1;
        Some(old)
    }

    fn put_if_absent(&mut self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64) -> Option<Vec<u8>> {
        if let Some(i) = self.find(map, key) {
            return Some(self.val_bytes(&self.table[i]));
        }
        self.put(map, key, val, ttl_ms);
        None
    }

    fn replace(&mut self, map: &str, key: &[u8], val: &[u8]) -> Option<Vec<u8>> {
        let i = self.find(map, key)?;
        let e = self.table[i];
        let old = self.val_bytes(&e);
        self.slab.free(e.handle);
        let handle = self.slab.put_two(key, val);
        self.table[i] = Entry {
            handle,
            key_len: key.len() as u32,
            val_len: val.len() as u32,
            ..e
        };
        Some(old)
    }

    fn size(&self, map: &str) -> usize {
        match self.map_ids.get(map) {
            Some(&id) => self.counts[id as usize],
            None => 0,
        }
    }

    fn clear(&mut self, map: &str) {
        let Some(&map_id) = self.map_ids.get(map) else { return };
        for i in 0..self.table.len() {
            let e = self.table[i];
            if e.occupied() && e.map_id == map_id {
                self.slab.free(e.handle);
                self.table[i].hash = TOMBSTONE;
                self.tombstones += 1;
                self.len -= 1;
            }
        }
        self.counts[map_id as usize] = 0;
    }

    fn collect_entries(&self, map: &str, out: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        let Some(&map_id) = self.map_ids.get(map) else { return };
        let now = now_ms();
        for e in self.table.iter() {
            if e.occupied() && e.map_id == map_id && !e.expired(now) {
                let total = (e.key_len + e.val_len) as usize;
                let bytes = self.slab.get(e.handle, total);
                out.push((bytes[..e.key_len as usize].to_vec(), bytes[e.key_len as usize..].to_vec()));
            }
        }
    }

    fn contains_value(&self, map: &str, val: &[u8]) -> bool {
        let Some(&map_id) = self.map_ids.get(map) else { return false };
        let now = now_ms();
        self.table.iter().any(|e| {
            e.occupied()
                && e.map_id == map_id
                && !e.expired(now)
                && &self.slab.get(e.handle, (e.key_len + e.val_len) as usize)[e.key_len as usize..]
                    == val
        })
    }
}

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
    // Distributed Queue backing: each queue is single-partition, so the owning
    // member holds it locally. Keyed by queue name.
    queues: Mutex<HashMap<String, VecDeque<Vec<u8>>>>,
    sets: Mutex<HashMap<String, HashSet<Vec<u8>>>>,
    // MultiMap with Set semantics (no duplicate values per key).
    multimaps: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<Vec<u8>>>>>,
    lists: Mutex<HashMap<String, Vec<Vec<u8>>>>,
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

    pub fn with_shards(n: usize) -> Store {
        assert!(n >= 1);
        Store {
            shards: (0..n).map(|_| Mutex::new(Inner::new())).collect(),
            queues: Mutex::new(HashMap::new()),
            sets: Mutex::new(HashMap::new()),
            multimaps: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
        }
    }

    // ---- Distributed List ----
    pub fn list_add(&self, name: &str, v: Vec<u8>) -> bool {
        self.lists.lock().unwrap().entry(name.to_string()).or_default().push(v);
        true
    }
    pub fn list_get(&self, name: &str, index: i32) -> Option<Vec<u8>> {
        if index < 0 {
            return None;
        }
        self.lists.lock().unwrap().get(name).and_then(|l| l.get(index as usize).cloned())
    }
    pub fn list_size(&self, name: &str) -> usize {
        self.lists.lock().unwrap().get(name).map_or(0, |l| l.len())
    }
    pub fn list_contains(&self, name: &str, v: &[u8]) -> bool {
        self.lists.lock().unwrap().get(name).is_some_and(|l| l.iter().any(|x| x.as_slice() == v))
    }
    pub fn list_remove(&self, name: &str, v: &[u8]) -> bool {
        if let Some(l) = self.lists.lock().unwrap().get_mut(name) {
            if let Some(i) = l.iter().position(|x| x.as_slice() == v) {
                l.remove(i);
                return true;
            }
        }
        false
    }
    pub fn list_get_all(&self, name: &str) -> Vec<Vec<u8>> {
        self.lists.lock().unwrap().get(name).cloned().unwrap_or_default()
    }
    pub fn list_clear(&self, name: &str) {
        if let Some(l) = self.lists.lock().unwrap().get_mut(name) {
            l.clear();
        }
    }
    pub fn list_is_empty(&self, name: &str) -> bool {
        self.list_size(name) == 0
    }

    // ---- Distributed Set ----
    pub fn set_add(&self, name: &str, v: Vec<u8>) -> bool {
        self.sets.lock().unwrap().entry(name.to_string()).or_default().insert(v)
    }
    pub fn set_remove(&self, name: &str, v: &[u8]) -> bool {
        self.sets.lock().unwrap().get_mut(name).is_some_and(|s| s.remove(v))
    }
    pub fn set_contains(&self, name: &str, v: &[u8]) -> bool {
        self.sets.lock().unwrap().get(name).is_some_and(|s| s.contains(v))
    }
    pub fn set_size(&self, name: &str) -> usize {
        self.sets.lock().unwrap().get(name).map_or(0, |s| s.len())
    }
    pub fn set_get_all(&self, name: &str) -> Vec<Vec<u8>> {
        self.sets.lock().unwrap().get(name).map_or_else(Vec::new, |s| s.iter().cloned().collect())
    }
    pub fn set_clear(&self, name: &str) {
        if let Some(s) = self.sets.lock().unwrap().get_mut(name) {
            s.clear();
        }
    }

    // ---- MultiMap (Set semantics) ----
    pub fn mm_put(&self, name: &str, key: Vec<u8>, value: Vec<u8>) -> bool {
        let mut g = self.multimaps.lock().unwrap();
        let values = g.entry(name.to_string()).or_default().entry(key).or_default();
        if values.iter().any(|v| *v == value) {
            false
        } else {
            values.push(value);
            true
        }
    }
    pub fn mm_get(&self, name: &str, key: &[u8]) -> Vec<Vec<u8>> {
        self.multimaps
            .lock()
            .unwrap()
            .get(name)
            .and_then(|m| m.get(key))
            .cloned()
            .unwrap_or_default()
    }
    pub fn mm_remove(&self, name: &str, key: &[u8]) -> Vec<Vec<u8>> {
        self.multimaps.lock().unwrap().get_mut(name).and_then(|m| m.remove(key)).unwrap_or_default()
    }
    pub fn mm_value_count(&self, name: &str, key: &[u8]) -> usize {
        self.multimaps.lock().unwrap().get(name).and_then(|m| m.get(key)).map_or(0, |v| v.len())
    }
    pub fn mm_size(&self, name: &str) -> usize {
        self.multimaps.lock().unwrap().get(name).map_or(0, |m| m.values().map(|v| v.len()).sum())
    }

    // ---- Distributed Queue ----
    pub fn queue_offer(&self, q: &str, v: Vec<u8>) -> bool {
        self.queues.lock().unwrap().entry(q.to_string()).or_default().push_back(v);
        true
    }
    pub fn queue_poll(&self, q: &str) -> Option<Vec<u8>> {
        self.queues.lock().unwrap().get_mut(q)?.pop_front()
    }
    pub fn queue_peek(&self, q: &str) -> Option<Vec<u8>> {
        self.queues.lock().unwrap().get(q)?.front().cloned()
    }
    pub fn queue_size(&self, q: &str) -> usize {
        self.queues.lock().unwrap().get(q).map_or(0, |d| d.len())
    }
    pub fn queue_remove(&self, q: &str, v: &[u8]) -> bool {
        if let Some(d) = self.queues.lock().unwrap().get_mut(q) {
            if let Some(i) = d.iter().position(|x| x.as_slice() == v) {
                d.remove(i);
                return true;
            }
        }
        false
    }
    pub fn queue_contains(&self, q: &str, v: &[u8]) -> bool {
        self.queues.lock().unwrap().get(q).is_some_and(|d| d.iter().any(|x| x.as_slice() == v))
    }
    pub fn queue_clear(&self, q: &str) {
        if let Some(d) = self.queues.lock().unwrap().get_mut(q) {
            d.clear();
        }
    }
    pub fn queue_is_empty(&self, q: &str) -> bool {
        self.queue_size(q) == 0
    }

    fn shard(&self, map: &str, key: &[u8]) -> &Mutex<Inner> {
        &self.shards[shard_of(map, key, self.shards.len())]
    }

    pub fn put(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        self.shard(map, &key).lock().unwrap().put(map, &key, &val, 0)
    }

    /// Put with TTL in milliseconds (0 == no expiry).
    pub fn put_ttl(&self, map: &str, key: Vec<u8>, val: Vec<u8>, ttl_ms: u64) -> Option<Vec<u8>> {
        self.shard(map, &key).lock().unwrap().put(map, &key, &val, ttl_ms)
    }

    pub fn get(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.shard(map, key).lock().unwrap().get(map, key)
    }

    /// Zero-copy read: runs `f` with the value slice (or None) while holding the
    /// shard lock, so the value is never copied into an intermediate `Vec`.
    pub fn get_with<R>(&self, map: &str, key: &[u8], f: impl FnOnce(Option<&[u8]>) -> R) -> R {
        let mut inner = self.shard(map, key).lock().unwrap();
        match inner.find(map, key) {
            Some(i) => {
                let e = inner.table[i];
                let total = (e.key_len + e.val_len) as usize;
                let val = &inner.slab.get(e.handle, total)[e.key_len as usize..];
                f(Some(val))
            }
            None => f(None),
        }
    }

    pub fn remove(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.shard(map, key).lock().unwrap().remove(map, key)
    }

    /// Insert only if absent; returns the existing value if present.
    pub fn put_if_absent(&self, map: &str, key: Vec<u8>, val: Vec<u8>, ttl_ms: u64) -> Option<Vec<u8>> {
        self.shard(map, &key).lock().unwrap().put_if_absent(map, &key, &val, ttl_ms)
    }

    /// Replace only if present; returns the old value, or None if absent.
    pub fn replace(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        self.shard(map, &key).lock().unwrap().replace(map, &key, &val)
    }

    pub fn contains_key(&self, map: &str, key: &[u8]) -> bool {
        self.shard(map, key).lock().unwrap().contains_key(map, key)
    }

    pub fn size(&self, map: &str) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().size(map)).sum()
    }

    pub fn is_empty(&self, map: &str) -> bool {
        self.size(map) == 0
    }

    pub fn clear(&self, map: &str) {
        for s in &self.shards {
            s.lock().unwrap().clear(map);
        }
    }

    pub fn contains_value(&self, map: &str, val: &[u8]) -> bool {
        self.shards.iter().any(|s| s.lock().unwrap().contains_value(map, val))
    }

    /// All live (key, value) pairs for a map, across shards.
    pub fn entries(&self, map: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        for s in &self.shards {
            s.lock().unwrap().collect_entries(map, &mut out);
        }
        out
    }

    /// Bulk get: returns the present (key, value) pairs for the requested keys.
    pub fn get_all(&self, map: &str, keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
        keys.iter()
            .filter_map(|k| self.get(map, k).map(|v| (k.clone(), v)))
            .collect()
    }

    /// Bulk put.
    pub fn put_all(&self, map: &str, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        for (k, v) in entries {
            self.put(map, k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_remove_roundtrip() {
        let s = Store::new();
        assert_eq!(s.put("m", vec![1, 2], vec![9]), None);
        assert_eq!(s.get("m", &[1, 2]), Some(vec![9]));
        assert_eq!(s.remove("m", &[1, 2]), Some(vec![9]));
        assert_eq!(s.get("m", &[1, 2]), None);
        assert_eq!(s.remove("m", &[1, 2]), None);
    }

    #[test]
    fn contains_size_clear() {
        let s = Store::with_shards(4);
        for i in 0..100u32 {
            s.put("m", i.to_le_bytes().to_vec(), vec![i as u8]);
        }
        assert_eq!(s.size("m"), 100);
        assert!(s.contains_key("m", &7u32.to_le_bytes()));
        assert!(!s.contains_key("m", &999u32.to_le_bytes()));
        assert!(s.contains_value("m", &[7]));
        assert!(!s.contains_value("m", &[200]));
        assert!(!s.is_empty("m"));
        s.clear("m");
        assert_eq!(s.size("m"), 0);
        assert!(s.is_empty("m"));
        assert_eq!(s.get("m", &7u32.to_le_bytes()), None);
    }

    #[test]
    fn tombstone_reuse_keeps_probe_chains_correct() {
        let s = Store::new();
        // insert, remove, re-insert many times to churn tombstones
        for round in 0..50 {
            for i in 0..200u32 {
                s.put("m", i.to_le_bytes().to_vec(), vec![round as u8]);
            }
            for i in 0..200u32 {
                assert_eq!(s.get("m", &i.to_le_bytes()), Some(vec![round as u8]));
            }
            if round < 49 {
                for i in 0..200u32 {
                    s.remove("m", &i.to_le_bytes());
                }
            }
        }
        assert_eq!(s.size("m"), 200);
    }

    #[test]
    fn ttl_entries_expire() {
        let s = Store::new();
        s.put_ttl("m", vec![1], vec![9], 1); // 1 ms
        std::thread::sleep(std::time::Duration::from_millis(15));
        assert_eq!(s.get("m", &[1]), None, "expired entry is gone");
        assert!(!s.contains_key("m", &[1]));
        assert_eq!(s.size("m"), 0);
        // non-expiring put still works
        s.put("m", vec![2], vec![8]);
        std::thread::sleep(std::time::Duration::from_millis(15));
        assert_eq!(s.get("m", &[2]), Some(vec![8]));
    }

    #[test]
    fn maps_are_isolated_by_name() {
        let s = Store::new();
        s.put("a", vec![1], vec![10]);
        assert_eq!(s.get("b", &[1]), None);
        assert_eq!(s.size("b"), 0);
    }

    #[test]
    fn large_overflow_values_roundtrip() {
        let s = Store::new();
        let big = vec![3u8; 20_000];
        s.put("m", vec![1], big.clone());
        assert_eq!(s.get("m", &[1]), Some(big));
    }
}
