use std::sync::mpsc::Sender;
use fluvio::RecordKey;
use fluvio_test_util::test_runner::FluvioTestDriver;

use super::ConcurrentTestCase;
use super::util::*;

pub async fn producer(
    test_driver: FluvioTestDriver,
    option: ConcurrentTestCase,
    digests: Sender<String>,
) {
    let producer = test_driver
        .client
        .topic_producer(option.environment.topic_name.clone())
        .await
        .unwrap();

    // Iterations ranging approx. 5000 - 20_000
    let iterations: u16 = (rand::random::<u16>() / 2) + 20000;
    println!("Producing {} records", iterations);
    for _ in 0..iterations {
        let record = rand_record();
        let record_digest = hash_record(&record);
        digests.send(record_digest).unwrap();
        producer.send(RecordKey::NULL, record).await.unwrap();
    }
}
