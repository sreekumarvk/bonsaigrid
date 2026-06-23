//! Kafka/Redpanda connector for the streaming job (off the hot path), built on
//! `rskafka` (pure-Rust, understands modern v2 record batches). Single-partition
//! source/sink with synchronous facades over a per-connector current-thread Tokio
//! runtime.

use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use std::collections::BTreeMap;
use tokio::runtime::Runtime;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime")
}

fn hosts(brokers: &str) -> Vec<String> {
    brokers.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

async fn partition(brokers: &str, topic: &str) -> Result<PartitionClient, String> {
    let client = ClientBuilder::new(hosts(brokers)).build().await.map_err(|e| e.to_string())?;
    client
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .map_err(|e| e.to_string())
}

/// Reads `topic` partition 0 sequentially from offset 0.
pub struct KafkaSource {
    rt: Runtime,
    pc: PartitionClient,
    offset: i64,
}

impl KafkaSource {
    pub fn new(brokers: &str, topic: &str) -> Result<KafkaSource, String> {
        let rt = runtime();
        let pc = rt.block_on(partition(brokers, topic))?;
        Ok(KafkaSource { rt, pc, offset: 0 })
    }

    /// Fetch the next batch of record values (advances the offset). Waits up to
    /// `max_wait_ms` for data; returns empty on timeout.
    pub fn poll(&mut self, max_wait_ms: i32) -> Vec<Vec<u8>> {
        let off = self.offset;
        match self.rt.block_on(self.pc.fetch_records(off, 1..1_000_000, max_wait_ms)) {
            Ok((recs, _high_watermark)) => recs
                .into_iter()
                .map(|r| {
                    self.offset = r.offset + 1;
                    r.record.value.unwrap_or_default()
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

/// Produces to `topic` partition 0.
pub struct KafkaSink {
    rt: Runtime,
    pc: PartitionClient,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> Result<KafkaSink, String> {
        let rt = runtime();
        let pc = rt.block_on(partition(brokers, topic))?;
        Ok(KafkaSink { rt, pc })
    }

    pub fn send(&self, value: Vec<u8>) -> Result<(), String> {
        let record = Record {
            key: None,
            value: Some(value),
            headers: BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        self.rt
            .block_on(self.pc.produce(vec![record], Compression::NoCompression))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
