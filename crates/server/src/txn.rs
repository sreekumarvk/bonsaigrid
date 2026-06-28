use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};

pub enum Mutation {
    Put(String, Vec<u8>, Vec<u8>), // map_name, key, value
    Remove(String, Vec<u8>),       // map_name, key
}

pub struct TransactionContext {
    pub uuid: (i64, i64),
    pub mutations: Vec<Mutation>,
}

pub struct TransactionService {
    contexts: Mutex<HashMap<(i64, i64), TransactionContext>>,
    next_id: AtomicI64,
}

impl TransactionService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            contexts: Mutex::new(HashMap::new()),
            next_id: AtomicI64::new(1),
        })
    }

    pub fn begin(&self) -> (i64, i64) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let uuid = (id, id);
        self.contexts.lock().unwrap().insert(
            uuid,
            TransactionContext {
                uuid,
                mutations: Vec::new(),
            },
        );
        uuid
    }

    pub fn buffer_put(&self, txn_id: (i64, i64), map: String, key: Vec<u8>, value: Vec<u8>) {
        if let Some(ctx) = self.contexts.lock().unwrap().get_mut(&txn_id) {
            ctx.mutations.push(Mutation::Put(map, key, value));
        }
    }

    pub fn buffer_remove(&self, txn_id: (i64, i64), map: String, key: Vec<u8>) {
        if let Some(ctx) = self.contexts.lock().unwrap().get_mut(&txn_id) {
            ctx.mutations.push(Mutation::Remove(map, key));
        }
    }

    pub fn commit(&self, txn_id: (i64, i64), store: &store::Store) -> bool {
        if let Some(ctx) = self.contexts.lock().unwrap().remove(&txn_id) {
            // Very rudimentary "2PC": in reality we would lock all keys, then apply, then unlock.
            // Here we just apply them sequentially as part of the commit phase.
            for m in ctx.mutations {
                match m {
                    Mutation::Put(map, key, value) => {
                        store.put(&map, key, value);
                    }
                    Mutation::Remove(map, key) => {
                        store.remove(&map, &key);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn rollback(&self, txn_id: (i64, i64)) {
        self.contexts.lock().unwrap().remove(&txn_id);
    }
}
