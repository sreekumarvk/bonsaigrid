//! Entry-listener event broker.
//!
//! Clients register entry listeners per map; mutations publish entry events that
//! are queued per connection and flushed to that connection by the reactor.
//!
//! The broker is shared across reactor threads, so it supports cross-connection
//! delivery (client B's mutation → client A's listener). Increment scope wires
//! *same-connection* flushing (events drained right after a connection is
//! processed); periodic cross-connection flushing needs a reactor timer and is
//! the documented next step. The queue/registry here already supports it.

use codecs::map::{encode_entry_event, encode_invalidation_event, encode_topic_event};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

struct Listener {
    conn_id: u64,
    corr: i64,
    flags: i32,
    include_value: bool,
    predicate: Option<query::Predicate>,
}

struct TopicSub {
    conn_id: u64,
    corr: i64,
}

struct Inner {
    listeners: HashMap<String, Vec<Listener>>, // map name -> entry listeners
    topics: HashMap<String, Vec<TopicSub>>,     // topic name -> subscribers
    near_caches: HashMap<String, Vec<TopicSub>>, // map name -> near-cache invalidation listeners
    queues: HashMap<u64, Vec<Vec<u8>>>,         // conn id -> pending event messages
    cluster_view: HashMap<u64, i64>,            // conn id -> cluster-view listener correlation id
}

pub struct EventBroker {
    inner: Mutex<Inner>,
    member_uuid: (i64, i64),
    nc_seq: AtomicI64, // monotonic invalidation sequence
}

impl EventBroker {
    pub fn new(member_uuid: (i64, i64)) -> EventBroker {
        EventBroker {
            inner: Mutex::new(Inner {
                listeners: HashMap::new(),
                topics: HashMap::new(),
                near_caches: HashMap::new(),
                queues: HashMap::new(),
                cluster_view: HashMap::new(),
            }),
            member_uuid,
            nc_seq: AtomicI64::new(1),
        }
    }

    // ---- Near-cache invalidation ----
    pub fn register_near_cache(&self, map: &str, conn_id: u64, corr: i64) {
        self.inner
            .lock()
            .unwrap()
            .near_caches
            .entry(map.to_string())
            .or_default()
            .push(TopicSub { conn_id, corr });
    }

    pub fn has_near_cache(&self, map: &str) -> bool {
        self.inner.lock().unwrap().near_caches.get(map).map(|v| !v.is_empty()).unwrap_or(false)
    }

    pub fn invalidate(&self, map: &str, key: &[u8]) {
        let seq = self.nc_seq.fetch_add(1, Ordering::Relaxed);
        let uuid = self.member_uuid;
        let mut g = self.inner.lock().unwrap();
        let Some(subs) = g.near_caches.get(map) else { return };
        let to_queue: Vec<(u64, Vec<u8>)> = subs
            .iter()
            .map(|s| (s.conn_id, encode_invalidation_event(s.corr, uuid, uuid, seq, key)))
            .collect();
        for (conn_id, bytes) in to_queue {
            g.queues.entry(conn_id).or_default().push(bytes);
        }
    }

    pub fn register(&self, map: &str, conn_id: u64, corr: i64, flags: i32, include_value: bool) {
        self.inner
            .lock()
            .unwrap()
            .listeners
            .entry(map.to_string())
            .or_default()
            .push(Listener { conn_id, corr, flags, include_value, predicate: None });
    }

    pub fn register_with_predicate(&self, map: &str, conn_id: u64, corr: i64, flags: i32, include_value: bool, predicate_data: &[u8]) {
        let predicate = query::decode(predicate_data);
        self.inner
            .lock()
            .unwrap()
            .listeners
            .entry(map.to_string())
            .or_default()
            .push(Listener { conn_id, corr, flags, include_value, predicate: Some(predicate) });
    }

    pub fn has_listeners(&self, map: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .listeners
            .get(map)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    }

    /// Publish an entry event to every matching listener's connection queue.
    pub fn publish(&self, map: &str, event_type: i32, key: &[u8], value: Option<&[u8]>, old: Option<&[u8]>, schemas: &serialization::schema::SchemaService) {
        let mut g = self.inner.lock().unwrap();
        let uuid = self.member_uuid;
        let Some(listeners) = g.listeners.get(map) else { return };
        // Encode per listener (each needs its own correlation id), then queue.
        let mut to_queue: Vec<(u64, Vec<u8>)> = Vec::new();
        for l in listeners {
            if event_type & l.flags == 0 {
                continue;
            }
            if let Some(pred) = &l.predicate {
                // If the predicate doesn't match the new value (or old for remove), skip.
                let val_to_check = if event_type == codecs::map::REMOVED { old } else { value };
                if let Some(v) = val_to_check {
                    let ex = serialization::compact::AutoExtractor;
                    if !query::eval(pred, v, schemas, &ex) {
                        continue;
                    }
                } else {
                    continue; // can't evaluate without value
                }
            }
            let (v, o) = if l.include_value { (value, old) } else { (None, None) };
            let bytes = encode_entry_event(l.corr, event_type, uuid, Some(key), v, o);
            to_queue.push((l.conn_id, bytes));
        }
        for (conn_id, bytes) in to_queue {
            g.queues.entry(conn_id).or_default().push(bytes);
        }
    }

    // ---- Topic pub/sub ----
    pub fn register_topic(&self, name: &str, conn_id: u64, corr: i64) {
        self.inner
            .lock()
            .unwrap()
            .topics
            .entry(name.to_string())
            .or_default()
            .push(TopicSub { conn_id, corr });
    }

    pub fn publish_topic(&self, name: &str, item: &[u8]) {
        let mut g = self.inner.lock().unwrap();
        let uuid = self.member_uuid;
        let Some(subs) = g.topics.get(name) else { return };
        let to_queue: Vec<(u64, Vec<u8>)> = subs
            .iter()
            .map(|s| (s.conn_id, encode_topic_event(s.corr, 0, uuid, item)))
            .collect();
        for (conn_id, bytes) in to_queue {
            g.queues.entry(conn_id).or_default().push(bytes);
        }
    }

    /// Queue an already-encoded message for a connection (e.g. a deferred
    /// blocking-lock grant). Delivered by the reactor like any other event.
    pub fn enqueue(&self, conn_id: u64, bytes: Vec<u8>) {
        self.inner.lock().unwrap().queues.entry(conn_id).or_default().push(bytes);
    }

    // ---- Cluster-view listeners (membership-change push) ----
    /// Register a connection's cluster-view listener (its `corr` is echoed on
    /// every pushed members/partitions view event).
    pub fn register_cluster_view(&self, conn_id: u64, corr: i64) {
        self.inner.lock().unwrap().cluster_view.insert(conn_id, corr);
    }

    /// All current cluster-view listeners as `(conn_id, corr)`.
    pub fn cluster_view_listeners(&self) -> Vec<(u64, i64)> {
        self.inner.lock().unwrap().cluster_view.iter().map(|(&c, &k)| (c, k)).collect()
    }

    /// Take all pending event-message bytes for a connection.
    pub fn drain(&self, conn_id: u64) -> Vec<Vec<u8>> {
        self.inner.lock().unwrap().queues.remove(&conn_id).unwrap_or_default()
    }

    pub fn drop_conn(&self, conn_id: u64) {
        let mut g = self.inner.lock().unwrap();
        g.queues.remove(&conn_id);
        for v in g.listeners.values_mut() {
            v.retain(|l| l.conn_id != conn_id);
        }
        for v in g.topics.values_mut() {
            v.retain(|s| s.conn_id != conn_id);
        }
        for v in g.near_caches.values_mut() {
            v.retain(|s| s.conn_id != conn_id);
        }
        g.cluster_view.remove(&conn_id);
    }
}
