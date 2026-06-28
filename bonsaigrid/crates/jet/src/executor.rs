use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;

use crate::processor::{Item, Processor, MapProcessor, FilterProcessor};

pub struct Tasklet {
    pub processor: Box<dyn Processor + Send + Sync>,
    pub inbox: VecDeque<Item>,
    pub outbox: VecDeque<Item>,
}
pub struct Job {
    pub id: i64,
    pub status: i32, // 0 = NOT_RUNNING, 1 = RUNNING, 2 = COMPLETED, 3 = FAILED
}

pub struct JetService {
    jobs: Arc<Mutex<HashMap<i64, Job>>>,
    next_id: AtomicI64,
}

impl JetService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicI64::new(1),
        })
    }

    pub fn submit(&self, _dag_bytes: Vec<u8>) -> i64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let job = Job {
            id,
            status: 1, // RUNNING
        };
        self.jobs.lock().unwrap().insert(id, job);
        
        let jobs_arc = self.jobs.clone();
        
        // Spawn a background execution thread for this job
        thread::spawn(move || {
            // Mock a simple pipeline: Source -> Map -> Filter -> Sink
            // We just instantiate some tasklets and loop them
            let mut tasklets = vec![
                Tasklet {
                    processor: Box::new(MapProcessor {}),
                    inbox: {
                        let mut q = VecDeque::new();
                        q.push_back(Item::Data(vec![1, 2, 3]));
                        q.push_back(Item::Data(vec![4, 5, 6]));
                        q.push_back(Item::Done);
                        q
                    },
                    outbox: VecDeque::new(),
                },
                Tasklet {
                    processor: Box::new(FilterProcessor {}),
                    inbox: VecDeque::new(),
                    outbox: VecDeque::new(),
                },
            ];

            let mut all_done = false;
            while !all_done {
                all_done = true;
                let mut any_progress = false;

                // Process each tasklet
                for i in 0..tasklets.len() {
                    let task = &mut tasklets[i];
                    if task.processor.process(&mut task.inbox, &mut task.outbox) {
                        any_progress = true;
                    }
                    if !task.inbox.is_empty() || !task.outbox.is_empty() {
                        all_done = false;
                    }
                }

                // Move items from outbox of i to inbox of i+1
                for i in 0..tasklets.len() - 1 {
                    let (left, right) = tasklets.split_at_mut(i + 1);
                    let outbox = &mut left[i].outbox;
                    let inbox = &mut right[0].inbox;
                    while let Some(item) = outbox.pop_front() {
                        inbox.push_back(item);
                        any_progress = true;
                    }
                }

                // Drain the sink (last tasklet's outbox)
                if let Some(last) = tasklets.last_mut() {
                    while let Some(_item) = last.outbox.pop_front() {
                        any_progress = true;
                    }
                }

                if !any_progress && !all_done {
                    thread::yield_now();
                }
            }
            
            // Mark as completed
            if let Some(job) = jobs_arc.lock().unwrap().get_mut(&id) {
                job.status = 2; // COMPLETED
            }
        });

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
