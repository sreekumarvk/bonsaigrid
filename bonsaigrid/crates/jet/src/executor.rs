use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};


pub struct Job {
    pub id: i64,
    pub status: i32, // 0 = NOT_RUNNING, 1 = RUNNING, 2 = COMPLETED, 3 = FAILED
}

pub struct JetService {
    jobs: Mutex<HashMap<i64, Job>>,
    next_id: AtomicI64,
}

impl JetService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Mutex::new(HashMap::new()),
            next_id: AtomicI64::new(1),
        })
    }

    pub fn submit(&self, dag_bytes: Vec<u8>) -> i64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let job = Job {
            id,
            status: 1, // RUNNING
        };
        self.jobs.lock().unwrap().insert(id, job);
        

        id
    }

    pub fn get_status(&self, job_id: i64) -> i32 {
        if let Some(job) = self.jobs.lock().unwrap().get(&job_id) {
            job.status
        } else {
            0
        }
    }
}
