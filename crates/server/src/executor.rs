use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A rudimentary IExecutorService implementation.
/// Real execution of Hazelcast Callables (like Java classes) requires
/// a runtime (JVM or WASM) or predefined IDS tasks. We simulate execution
/// by just keeping a durable ring of submitted tasks and returning null responses.
pub struct ExecutorService {
    tasks: Mutex<HashMap<String, Vec<Vec<u8>>>>,
}

impl ExecutorService {
    pub fn new() -> Arc<ExecutorService> {
        Arc::new(ExecutorService {
            tasks: Mutex::new(HashMap::new()),
        })
    }

    /// Submit a task to a specific partition owner (us).
    pub fn submit_to_partition(&self, name: &str, _uuid: (i64, i64), callable: Vec<u8>) -> Option<Vec<u8>> {
        self.tasks.lock().unwrap().entry(name.to_string()).or_default().push(callable);
        // For now, we don't have built-in task evaluation logic implemented, so return None.
        None
    }

    /// Submit a task to a specific member (us).
    pub fn submit_to_member(&self, name: &str, _uuid: (i64, i64), callable: Vec<u8>) -> Option<Vec<u8>> {
        self.tasks.lock().unwrap().entry(name.to_string()).or_default().push(callable);
        None
    }
}
