//! Requires a Redpanda/Kafka broker on 127.0.0.1:9092 (run with `--ignored`).

#[test]
#[ignore]
fn produce_then_consume() {
    use server::kafka::{KafkaSink, KafkaSource};
    let brokers = "127.0.0.1:9092";
    let topic = "bonsai_kafka_test";

    let mut source = KafkaSource::new(brokers, topic).expect("source");
    let sink = KafkaSink::new(brokers, topic).expect("sink");
    sink.send(b"hello-rskafka".to_vec()).expect("send");

    let mut found = false;
    for _ in 0..20 {
        if source.poll(500).iter().any(|r| r == b"hello-rskafka") {
            found = true;
            break;
        }
    }
    assert!(found, "did not consume the produced message");
}
