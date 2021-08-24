use std::sync::Arc;
use async_lock::RwLock;
use std::sync::mpsc::Sender;
use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::{TestDriver, TestProducer};

use super::ConcurrentTestCase;
use super::util::*;

pub async fn producer(
    test_driver: Arc<RwLock<TestDriver>>,
    option: ConcurrentTestCase,
    digests: Sender<String>,
) {
    let mut lock = test_driver.write().await;

    let producer = lock
        .create_producer(option.environment.topic_name.as_str())
        .await;

    let TestProducer::Fluvio(fluvio_producer) = producer;
    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {} records", iterations);
    for _ in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        fluvio_producer.send(RecordKey::NULL, record).await.unwrap();
    }
}
