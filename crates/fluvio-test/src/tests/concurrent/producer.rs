use std::sync::mpsc::Sender;

use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;

use super::MyTestCase;
use super::util::*;

pub async fn producer(test_driver: &TestDriver, option: MyTestCase, digests: Sender<String>) {
    let producer = test_driver
        .create_producer(&option.environment.base_topic_name())
        .await;

    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {iterations} records");
    for _ in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        producer.send(RecordKey::NULL, record).await.unwrap();
    }
    producer.flush().await.unwrap();
}
