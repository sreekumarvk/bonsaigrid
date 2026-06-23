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
    stamp: u64,     // monotonic update stamp (for split-brain LatestUpdate merge)
}

impl Entry {
    const EMPTY: Entry = Entry {
        hash: EMPTY,
        map_id: 0,
        handle: Handle { class: 0, slot: 0 },
        key_len: 0,
        val_len: 0,
        expire_at: 0,
        stamp: 0,
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

    fn put(&mut self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64, stamp: u64) -> Option<Vec<u8>> {
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
                    stamp,
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
                    stamp,
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

    fn put_if_absent(&mut self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64, stamp: u64) -> Option<Vec<u8>> {
        if let Some(i) = self.find(map, key) {
            return Some(self.val_bytes(&self.table[i]));
        }
        self.put(map, key, val, ttl_ms, stamp);
        None
    }

    /// Merge an inbound (key,value,stamp): absent → insert; present → keep the
    /// higher stamp when `latest_update`, else keep the existing (PutIfAbsent).
    fn put_merge(&mut self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64, stamp: u64, latest_update: bool) {
        match self.find(map, key) {
            None => {
                self.put(map, key, val, ttl_ms, stamp);
            }
            Some(i) => {
                if latest_update && stamp > self.table[i].stamp {
                    self.put(map, key, val, ttl_ms, stamp);
                }
                // PutIfAbsent (or lower stamp): keep the existing entry.
            }
        }
    }

    /// All live entries across every map as (map, key, value, stamp).
    fn collect_all_stamped(&self, out: &mut Vec<(String, Vec<u8>, Vec<u8>, u64)>) {
        let now = now_ms();
        for e in self.table.iter() {
            if e.occupied() && !e.expired(now) {
                let total = (e.key_len + e.val_len) as usize;
                let bytes = self.slab.get(e.handle, total);
                out.push((
                    self.map_names[e.map_id as usize].clone(),
                    bytes[..e.key_len as usize].to_vec(),
                    bytes[e.key_len as usize..].to_vec(),
                    e.stamp,
                ));
            }
        }
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
    /// Monotonic per-member update-stamp source (seeded so different members'
    /// stamps differ); used for split-brain LatestUpdate merge.
    stamp_seq: std::sync::atomic::AtomicU64,
    shards: Vec<Mutex<Inner>>,
    // Distributed Queue backing: each queue is single-partition, so the owning
    // member holds it locally. Keyed by queue name.
    queues: Mutex<HashMap<String, VecDeque<Vec<u8>>>>,
    sets: Mutex<HashMap<String, HashSet<Vec<u8>>>>,
    // MultiMap with Set semantics (no duplicate values per key).
    multimaps: Mutex<HashMap<String, HashMap<Vec<u8>, Vec<Vec<u8>>>>>,
    lists: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    // Per-(map,key) reentrant lock with a FIFO waiter queue for blocking lock().
    locks: Mutex<HashMap<(String, Vec<u8>), LockState>>,
    ringbuffers: Mutex<HashMap<String, Ring>>,
    pncounters: Mutex<HashMap<String, i64>>,
    flake: Mutex<HashMap<String, i64>>,
}

struct Ring {
    items: VecDeque<Vec<u8>>,
    head: i64,
    tail: i64,
    cap: i64,
}

struct LockState {
    owner: i64,
    count: u32,
    waiters: VecDeque<(u64, i64, i64)>, // (conn_id, correlation_id, thread_id)
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
        Self::with_shards_seed(n, 0)
    }

    /// Like `with_shards`, but seed the stamp counter (member-specific) so stamps
    /// from different members are distinguishable for merge.
    pub fn with_shards_seed(n: usize, stamp_seed: u64) -> Store {
        assert!(n >= 1);
        Store {
            stamp_seq: std::sync::atomic::AtomicU64::new(stamp_seed),
            shards: (0..n).map(|_| Mutex::new(Inner::new())).collect(),
            queues: Mutex::new(HashMap::new()),
            sets: Mutex::new(HashMap::new()),
            multimaps: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            ringbuffers: Mutex::new(HashMap::new()),
            pncounters: Mutex::new(HashMap::new()),
            flake: Mutex::new(HashMap::new()),
        }
    }

    // ---- Ringbuffer ----
    pub fn rb_add(&self, name: &str, v: Vec<u8>) -> i64 {
        let mut g = self.ringbuffers.lock().unwrap();
        let r = g.entry(name.to_string()).or_insert_with(|| Ring {
            items: VecDeque::new(),
            head: 0,
            tail: -1,
            cap: 10_000,
        });
        r.tail += 1;
        r.items.push_back(v);
        if r.items.len() as i64 > r.cap {
            r.items.pop_front();
            r.head += 1;
        }
        r.tail
    }
    pub fn rb_read_one(&self, name: &str, seq: i64) -> Option<Vec<u8>> {
        let g = self.ringbuffers.lock().unwrap();
        let r = g.get(name)?;
        if seq < r.head || seq > r.tail {
            return None;
        }
        r.items.get((seq - r.head) as usize).cloned()
    }
    pub fn rb_size(&self, name: &str) -> i64 {
        self.ringbuffers.lock().unwrap().get(name).map_or(0, |r| r.items.len() as i64)
    }
    pub fn rb_capacity(&self, name: &str) -> i64 {
        self.ringbuffers.lock().unwrap().get(name).map_or(10_000, |r| r.cap)
    }
    pub fn rb_tail(&self, name: &str) -> i64 {
        self.ringbuffers.lock().unwrap().get(name).map_or(-1, |r| r.tail)
    }
    pub fn rb_head(&self, name: &str) -> i64 {
        self.ringbuffers.lock().unwrap().get(name).map_or(0, |r| r.head)
    }

    // ---- PNCounter (single-node: a plain counter + a logical clock) ----
    /// Monotonic per-process clock for this replica's CRDT timestamp.
    pub fn pn_tick(&self) -> i64 {
        use std::sync::atomic::{AtomicI64, Ordering};
        static CLOCK: AtomicI64 = AtomicI64::new(1);
        CLOCK.fetch_add(1, Ordering::Relaxed)
    }
    pub fn pn_get(&self, name: &str) -> i64 {
        *self.pncounters.lock().unwrap().get(name).unwrap_or(&0)
    }
    pub fn pn_add(&self, name: &str, delta: i64, get_before: bool) -> i64 {
        let mut g = self.pncounters.lock().unwrap();
        let c = g.entry(name.to_string()).or_insert(0);
        let old = *c;
        *c += delta;
        if get_before {
            old
        } else {
            *c
        }
    }

    // ---- FlakeIdGenerator: hand out monotonic id batches (base, increment=1, size) ----
    pub fn flake_batch(&self, name: &str, batch: i32) -> (i64, i64, i32) {
        let batch = batch.max(1);
        let mut g = self.flake.lock().unwrap();
        let next = g.entry(name.to_string()).or_insert(1);
        let base = *next;
        *next += batch as i64;
        (base, 1, batch)
    }

    // ---- Per-key locking ----
    pub fn try_lock(&self, map: &str, key: &[u8], tid: i64) -> bool {
        let mut g = self.locks.lock().unwrap();
        let k = (map.to_string(), key.to_vec());
        match g.get_mut(&k) {
            Some(s) => {
                if s.owner == tid {
                    s.count += 1; // reentrant
                    true
                } else {
                    false
                }
            }
            None => {
                g.insert(k, LockState { owner: tid, count: 1, waiters: VecDeque::new() });
                true
            }
        }
    }
    /// Blocking lock: grant immediately if free/reentrant (returns true), else
    /// queue (conn_id, corr) and return false — the caller defers its response
    /// until granted on a later unlock.
    pub fn lock_or_wait(&self, map: &str, key: &[u8], tid: i64, conn_id: u64, corr: i64) -> bool {
        let mut g = self.locks.lock().unwrap();
        let k = (map.to_string(), key.to_vec());
        match g.get_mut(&k) {
            Some(s) => {
                if s.owner == tid {
                    s.count += 1;
                    true
                } else {
                    s.waiters.push_back((conn_id, corr, tid));
                    false
                }
            }
            None => {
                g.insert(k, LockState { owner: tid, count: 1, waiters: VecDeque::new() });
                true
            }
        }
    }
    /// Release; if a waiter is granted, returns (conn_id, corr) to wake.
    pub fn unlock(&self, map: &str, key: &[u8], tid: i64) -> Option<(u64, i64)> {
        let mut g = self.locks.lock().unwrap();
        let k = (map.to_string(), key.to_vec());
        if let Some(s) = g.get_mut(&k) {
            if s.owner == tid {
                s.count -= 1;
                if s.count == 0 {
                    if let Some((c, corr, wtid)) = s.waiters.pop_front() {
                        s.owner = wtid;
                        s.count = 1;
                        return Some((c, corr));
                    }
                    g.remove(&k);
                }
            }
        }
        None
    }
    pub fn is_locked(&self, map: &str, key: &[u8]) -> bool {
        self.locks.lock().unwrap().contains_key(&(map.to_string(), key.to_vec()))
    }
    pub fn force_unlock(&self, map: &str, key: &[u8]) -> Option<(u64, i64)> {
        let mut g = self.locks.lock().unwrap();
        let k = (map.to_string(), key.to_vec());
        if let Some(s) = g.get_mut(&k) {
            if let Some((c, corr, wtid)) = s.waiters.pop_front() {
                s.owner = wtid;
                s.count = 1;
                return Some((c, corr));
            }
            g.remove(&k);
        }
        None
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

    /// Install a `(name, key)` value-set (HA replication/migration). An empty set
    /// removes the key (and the map if it becomes empty).
    pub fn mm_install(&self, name: &str, key: Vec<u8>, values: Vec<Vec<u8>>) {
        let mut g = self.multimaps.lock().unwrap();
        if values.is_empty() {
            if let Some(m) = g.get_mut(name) {
                m.remove(&key);
                if m.is_empty() {
                    g.remove(name);
                }
            }
        } else {
            g.entry(name.to_string()).or_default().insert(key, values);
        }
    }

    /// Every `(name, key, values)` whose key maps to `partition` — the migration
    /// source for MultiMap (which is key-partitioned like IMap).
    pub fn mm_entries_for_partition(&self, partition: i32, count: i32) -> Vec<(String, Vec<u8>, Vec<Vec<u8>>)> {
        let mut out = Vec::new();
        for (name, m) in self.multimaps.lock().unwrap().iter() {
            for (key, values) in m.iter() {
                if serialization::partition_id(key, count) == partition {
                    out.push((name.clone(), key.clone(), values.clone()));
                }
            }
        }
        out
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

    /// A fresh monotonic update stamp.
    pub fn next_stamp(&self) -> u64 {
        self.stamp_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
    }

    pub fn put(&self, map: &str, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        let stamp = self.next_stamp();
        self.shard(map, &key).lock().unwrap().put(map, &key, &val, 0, stamp)
    }

    /// Put with TTL in milliseconds (0 == no expiry).
    pub fn put_ttl(&self, map: &str, key: Vec<u8>, val: Vec<u8>, ttl_ms: u64) -> Option<Vec<u8>> {
        let stamp = self.next_stamp();
        self.shard(map, &key).lock().unwrap().put(map, &key, &val, ttl_ms, stamp)
    }

    /// Put with an explicit stamp (used when applying a migrated/replicated entry
    /// to preserve the originating member's stamp).
    pub fn put_stamped(&self, map: &str, key: Vec<u8>, val: Vec<u8>, ttl_ms: u64, stamp: u64) -> Option<Vec<u8>> {
        self.shard(map, &key).lock().unwrap().put(map, &key, &val, ttl_ms, stamp)
    }

    /// Merge an inbound entry under the given policy (`latest_update` keeps the
    /// higher stamp; otherwise PutIfAbsent keeps any existing entry).
    pub fn put_merge(&self, map: &str, key: &[u8], val: &[u8], ttl_ms: u64, stamp: u64, latest_update: bool) {
        self.shard(map, key).lock().unwrap().put_merge(map, key, val, ttl_ms, stamp, latest_update);
    }

    /// Every live entry across all maps as (map, key, value, stamp) — the source
    /// side of a partition migration filters this by partition.
    pub fn all_entries_stamped(&self) -> Vec<(String, Vec<u8>, Vec<u8>, u64)> {
        let mut out = Vec::new();
        for s in &self.shards {
            s.lock().unwrap().collect_all_stamped(&mut out);
        }
        out
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
        let stamp = self.next_stamp();
        self.shard(map, &key).lock().unwrap().put_if_absent(map, &key, &val, ttl_ms, stamp)
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

    // ---- Auxiliary-structure HA: per-partition state snapshot / install ----

    /// Serialize every auxiliary structure whose name maps to `partition` into a
    /// single self-describing blob (for synchronous backup or migration).
    pub fn aux_state_for_partition(&self, partition: i32, count: i32) -> Vec<u8> {
        let mut secs: Vec<u8> = Vec::new();
        let mut n: u32 = 0;
        let on = |name: &str| partition_for_name(name, count) == partition;

        for (name, items) in self.lists.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            sec_seq(&mut secs, AUX_LIST, name, items.iter());
            n += 1;
        }
        for (name, items) in self.sets.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            sec_seq(&mut secs, AUX_SET, name, items.iter());
            n += 1;
        }
        for (name, items) in self.queues.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            sec_seq(&mut secs, AUX_QUEUE, name, items.iter());
            n += 1;
        }
        // NOTE: MultiMap is key-partitioned (like IMap), not name-partitioned, so
        // it is intentionally excluded from this name-based snapshot.
        for (name, r) in self.ringbuffers.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            secs.push(AUX_RINGBUFFER);
            put_blob(&mut secs, name.as_bytes());
            secs.extend_from_slice(&r.head.to_le_bytes());
            secs.extend_from_slice(&r.tail.to_le_bytes());
            secs.extend_from_slice(&r.cap.to_le_bytes());
            put_u32(&mut secs, r.items.len() as u32);
            for v in &r.items {
                put_blob(&mut secs, v);
            }
            n += 1;
        }
        for (name, v) in self.pncounters.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            secs.push(AUX_PNCOUNTER);
            put_blob(&mut secs, name.as_bytes());
            secs.extend_from_slice(&v.to_le_bytes());
            n += 1;
        }
        for (name, v) in self.flake.lock().unwrap().iter().filter(|(k, _)| on(k)) {
            secs.push(AUX_FLAKE);
            put_blob(&mut secs, name.as_bytes());
            secs.extend_from_slice(&v.to_le_bytes());
            n += 1;
        }

        let mut out = Vec::with_capacity(4 + secs.len());
        put_u32(&mut out, n);
        out.extend_from_slice(&secs);
        out
    }

    /// Install auxiliary structures from a blob produced by `aux_state_for_partition`,
    /// replacing each named structure.
    pub fn install_aux_state(&self, bytes: &[u8]) {
        let mut r = AuxReader { b: bytes, pos: 0 };
        let Some(n) = r.u32() else { return };
        for _ in 0..n {
            let Some(kind) = r.u8() else { return };
            let Some(name) = r.string() else { return };
            match kind {
                AUX_LIST => {
                    let items = r.seq().unwrap_or_default();
                    self.lists.lock().unwrap().insert(name, items);
                }
                AUX_SET => {
                    let items = r.seq().unwrap_or_default();
                    self.sets.lock().unwrap().insert(name, items.into_iter().collect());
                }
                AUX_QUEUE => {
                    let items = r.seq().unwrap_or_default();
                    self.queues.lock().unwrap().insert(name, items.into_iter().collect());
                }
                AUX_MULTIMAP => {
                    let Some(kc) = r.u32() else { return };
                    let mut mm: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
                    for _ in 0..kc {
                        let Some(key) = r.blob() else { return };
                        let Some(vc) = r.u32() else { return };
                        let mut vs = Vec::with_capacity(vc as usize);
                        for _ in 0..vc {
                            let Some(v) = r.blob() else { return };
                            vs.push(v);
                        }
                        mm.insert(key, vs);
                    }
                    self.multimaps.lock().unwrap().insert(name, mm);
                }
                AUX_RINGBUFFER => {
                    let (Some(head), Some(tail), Some(cap)) = (r.i64(), r.i64(), r.i64()) else { return };
                    let items = r.seq().map(|v| v.into_iter().collect()).unwrap_or_default();
                    self.ringbuffers.lock().unwrap().insert(name, Ring { items, head, tail, cap });
                }
                AUX_PNCOUNTER => {
                    let Some(v) = r.i64() else { return };
                    self.pncounters.lock().unwrap().insert(name, v);
                }
                AUX_FLAKE => {
                    let Some(v) = r.i64() else { return };
                    self.flake.lock().unwrap().insert(name, v);
                }
                _ => return,
            }
        }
    }
}

// ---- Auxiliary-state blob codec ----
const AUX_LIST: u8 = 1;
const AUX_SET: u8 = 2;
const AUX_QUEUE: u8 = 3;
const AUX_MULTIMAP: u8 = 4;
const AUX_RINGBUFFER: u8 = 5;
const AUX_PNCOUNTER: u8 = 6;
const AUX_FLAKE: u8 = 7;

/// The partition a distributed object's name maps to — matching the client, which
/// hashes the name's String `Data` (`[partitionHash=0][type=-11][len][utf8]`).
pub fn partition_for_name(name: &str, count: i32) -> i32 {
    let mut data = Vec::with_capacity(12 + name.len());
    data.extend_from_slice(&0i32.to_be_bytes()); // partitionHash (0 -> murmur of payload)
    data.extend_from_slice(&(-11i32).to_be_bytes()); // STRING serializer type
    data.extend_from_slice(&(name.len() as i32).to_be_bytes());
    data.extend_from_slice(name.as_bytes());
    serialization::partition_id(&data, count)
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_blob(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}
fn sec_seq<'a>(out: &mut Vec<u8>, kind: u8, name: &str, items: impl Iterator<Item = &'a Vec<u8>>) {
    out.push(kind);
    put_blob(out, name.as_bytes());
    let start = out.len();
    put_u32(out, 0); // count placeholder
    let mut n = 0u32;
    for it in items {
        put_blob(out, it);
        n += 1;
    }
    out[start..start + 4].copy_from_slice(&n.to_le_bytes());
}

struct AuxReader<'a> {
    b: &'a [u8],
    pos: usize,
}
impl AuxReader<'_> {
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }
    fn u32(&mut self) -> Option<u32> {
        let s = self.b.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_le_bytes(s.try_into().unwrap()))
    }
    fn i64(&mut self) -> Option<i64> {
        let s = self.b.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(i64::from_le_bytes(s.try_into().unwrap()))
    }
    fn blob(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        let s = self.b.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(s.to_vec())
    }
    fn string(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(&self.blob()?).into_owned())
    }
    fn seq(&mut self) -> Option<Vec<Vec<u8>>> {
        let c = self.u32()?;
        let mut out = Vec::with_capacity(c as usize);
        for _ in 0..c {
            out.push(self.blob()?);
        }
        Some(out)
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

    #[test]
    fn put_merge_latest_update_keeps_higher_stamp() {
        let s = Store::new();
        s.put_stamped("m", vec![1], b"v5".to_vec(), 0, 5);
        // lower stamp loses
        s.put_merge("m", &[1], b"v3", 0, 3, true);
        assert_eq!(s.get("m", &[1]), Some(b"v5".to_vec()));
        // higher stamp wins
        s.put_merge("m", &[1], b"v9", 0, 9, true);
        assert_eq!(s.get("m", &[1]), Some(b"v9".to_vec()));
        // absent key inserts
        s.put_merge("m", &[2], b"new", 0, 1, true);
        assert_eq!(s.get("m", &[2]), Some(b"new".to_vec()));
    }

    #[test]
    fn put_merge_put_if_absent_keeps_existing() {
        let s = Store::new();
        s.put_stamped("m", vec![1], b"old".to_vec(), 0, 1);
        s.put_merge("m", &[1], b"new", 0, 99, false); // PutIfAbsent: keep existing
        assert_eq!(s.get("m", &[1]), Some(b"old".to_vec()));
        s.put_merge("m", &[2], b"fresh", 0, 1, false); // absent -> insert
        assert_eq!(s.get("m", &[2]), Some(b"fresh".to_vec()));
    }

    #[test]
    fn partition_for_name_matches_client() {
        // Captured from a stock client: "q"->229, "mylist"->62, "myset"->170 (271 partitions).
        assert_eq!(partition_for_name("q", 271), 229);
        assert_eq!(partition_for_name("mylist", 271), 62);
        assert_eq!(partition_for_name("myset", 271), 170);
    }

    #[test]
    fn aux_state_roundtrips_all_structures() {
        let s = Store::new();
        // Place a few structures; compute the partition each lands on.
        s.list_add("L", b"a".to_vec());
        s.list_add("L", b"b".to_vec());
        s.set_add("S", b"x".to_vec());
        s.queue_offer("Q", b"q1".to_vec());
        s.queue_offer("Q", b"q2".to_vec());
        s.rb_add("R", b"r1".to_vec());
        s.pn_add("P", 7, false);

        // Snapshot every partition, install into a fresh store, compare.
        let dst = Store::new();
        for p in 0..271 {
            let blob = s.aux_state_for_partition(p, 271);
            dst.install_aux_state(&blob);
        }
        assert_eq!(dst.list_get_all("L"), vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(dst.set_contains("S", b"x"));
        assert_eq!(dst.queue_poll("Q"), Some(b"q1".to_vec()));
        assert_eq!(dst.queue_poll("Q"), Some(b"q2".to_vec()));
        assert_eq!(dst.rb_read_one("R", 0), Some(b"r1".to_vec()));
        assert_eq!(dst.pn_get("P"), 7);
    }

    #[test]
    fn merge_converges_regardless_of_order() {
        // Two members' versions of the same key heal: whichever order they merge,
        // LatestUpdate converges on the higher-stamp value (split-brain heal).
        let a = Store::new();
        a.put_merge("m", &[1], b"from-A", 0, 10, true);
        a.put_merge("m", &[1], b"from-B", 0, 20, true); // B's write is newer
        let b = Store::new();
        b.put_merge("m", &[1], b"from-B", 0, 20, true);
        b.put_merge("m", &[1], b"from-A", 0, 10, true); // reverse order
        assert_eq!(a.get("m", &[1]), Some(b"from-B".to_vec()));
        assert_eq!(b.get("m", &[1]), b.get("m", &[1]));
        assert_eq!(a.get("m", &[1]), b.get("m", &[1]), "convergent");
    }

    #[test]
    fn all_entries_stamped_spans_maps() {
        let s = Store::new();
        s.put("a", vec![1], b"x".to_vec());
        s.put("b", vec![2], b"y".to_vec());
        let mut all = s.all_entries_stamped();
        all.sort_by(|p, q| p.0.cmp(&q.0));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, "a");
        assert!(all[0].3 >= 1 && all[1].3 >= 1); // monotonic stamps assigned
    }
}
