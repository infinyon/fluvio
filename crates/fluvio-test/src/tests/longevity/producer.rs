use std::sync::Arc;
use async_lock::RwLock;
use async_channel::Sender;
use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use std::time::SystemTime;
use tracing::debug;

use super::LongevityTestCase;
use super::util::*;

pub async fn producer(
    test_driver: Arc<RwLock<TestDriver>>,
    option: LongevityTestCase,
    digests: Sender<String>,
) {
    let mut lock = test_driver.write().await;

    let producer = lock
        .create_producer(option.environment.topic_name.as_str())
        .await;

    drop(lock);

    // Read in the timer value we want to run for

    // Iterations ranging approx. 5000 - 20_000
    //let iterations: u16 = (rand::random::<u16>() / 2) + 20000;

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    while test_start.elapsed().unwrap() <= option.option.runtime_seconds {
        //println!("Producing {} records", iterations);
        //for _ in 0..iterations {

        let record = rand_printable_record();
        let record_digest = hash_record(&record);

        // FIXME: Change send_count to take in a Vec<u8>
        let record_str = std::str::from_utf8(&record).unwrap().to_owned();

        // Change this to send_all
        debug!("{:?}", &record_str);

        // Record the latency
        let mut lock = test_driver.write().await;
        lock.send_count(&producer, RecordKey::NULL, record_str)
            .await
            .expect("Producer Send failed");
        drop(lock);
        //producer.send(RecordKey::NULL, record).await.unwrap();

        // Send the consumer the expected checksum for the record it just sent
        // Note: This might be a brittle test if we have the consumer testing from a different starting offset than the producer

        debug!("{:?}", record_digest);
        digests.send(record_digest).await.unwrap();

        records_sent += 1;
    }
    //}

    println!(
        "Producer stopped. Time's up!\nRecords sent: {:?}",
        records_sent
    )
}
