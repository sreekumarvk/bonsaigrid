//! Streaming SQL jobs (`CREATE JOB ... AS SINK INTO <kafka> SELECT ... JOIN ...`).
//!
//! Each job runs on its own thread: poll the source Kafka topic, parse each JSON
//! record, look the join key up in the IMap (stream⋈table enrichment), apply the
//! WHERE filter, project the output columns, and produce the merged JSON row to
//! the sink Kafka topic. Single-node (the member that received CREATE JOB runs
//! it, against its local store).

use crate::kafka::{KafkaSink, KafkaSource};
use query::sql::Job;
use std::sync::{Arc, Mutex, OnceLock};
use store::Store;

/// The server's store, published at startup so spawned job threads can look up
/// the IMap they join against.
fn store_handle() -> &'static Mutex<Option<Arc<Store>>> {
    static S: OnceLock<Mutex<Option<Arc<Store>>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(None))
}

pub fn set_store(store: Arc<Store>) {
    *store_handle().lock().unwrap() = Some(store);
}

fn store() -> Option<Arc<Store>> {
    store_handle().lock().unwrap().clone()
}

/// Track running job names so a duplicate CREATE JOB is a no-op.
fn running() -> &'static Mutex<std::collections::HashSet<String>> {
    static R: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(std::collections::HashSet::new()))
}

/// Spawn the job (no-op if a job of the same name already runs).
pub fn spawn(job: Job) {
    if !running().lock().unwrap().insert(job.name.clone()) {
        return;
    }
    let Some(store) = store() else {
        eprintln!("JOB {}: no store available", job.name);
        return;
    };
    std::thread::spawn(move || run(job, store));
}

fn run(job: Job, store: Arc<Store>) {
    let select = &job.select;
    let Some(join) = select.join.clone() else {
        eprintln!("JOB {}: needs a JOIN", job.name);
        return;
    };
    let (Some(src), Some(sink_m), Some(right)) = (
        crate::catalog::get_mapping(&select.map),
        crate::catalog::get_mapping(&job.sink),
        crate::catalog::get_mapping(&join.right),
    ) else {
        eprintln!("JOB {}: missing source/sink/right mapping", job.name);
        return;
    };
    let src_brokers = src
        .option("bootstrap.servers")
        .unwrap_or("127.0.0.1:9092")
        .to_string();
    let sink_brokers = sink_m
        .option("bootstrap.servers")
        .unwrap_or("127.0.0.1:9092")
        .to_string();
    let right_key = right
        .columns
        .first()
        .map(|(n, _)| n.clone())
        .unwrap_or_else(|| "__key".into());

    let mut source = match KafkaSource::new(&src_brokers, &select.map) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("JOB {}: source {}: {e}", job.name, select.map);
            return;
        }
    };
    let sink = match KafkaSink::new(&sink_brokers, &job.sink) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("JOB {}: sink {}: {e}", job.name, job.sink);
            return;
        }
    };
    eprintln!(
        "JOB {} started: {} JOIN {} -> {}",
        job.name, select.map, join.right, job.sink
    );

    loop {
        for rec in source.poll(500) {
            let json = String::from_utf8_lossy(&rec);
            let left = query::json::json_record_fields(&json);
            // Join key value from the streamed record.
            let Some(jkey) = left
                .iter()
                .find(|(c, _)| *c == join.left_col)
                .and_then(|(_, v)| query::sql::fmt_value(v))
            else {
                continue;
            };
            // Look it up in the IMap (the right side is keyed by the join column).
            let Some(rval) = store.get(&join.right, &query::json::string_data(&jkey)) else {
                continue; // no recommendation for this user
            };
            let right_fields =
                query::json::jsonflat_fields(&query::json::string_data(&jkey), &rval, &right_key);
            let mut combined = left.clone();
            combined.extend(right_fields);
            if let Some(row) = query::sql::project_row(select, &combined) {
                let out = query::json::json_object(&row);
                if let Err(e) = sink.send(out.into_bytes()) {
                    eprintln!("JOB {}: sink send: {e}", job.name);
                }
            }
        }
    }
}
