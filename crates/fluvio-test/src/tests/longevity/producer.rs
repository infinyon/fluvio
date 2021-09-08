use std::sync::Arc;
use async_lock::RwLock;
use async_channel::Sender;
use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
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

    let producer = lock.create_producer(&option.environment.topic_name()).await;

    drop(lock);

    // Read in the timer value we want to run for

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    while test_start.elapsed().unwrap() <= option.option.runtime_seconds {
        let record = rand_printable_record(option.option.record_size);
        let record_digest = hash_record(&record);

        debug!("{:?}", &record);

        // Record the latency
        let mut lock = test_driver.write().await;
        lock.send_count(&producer, RecordKey::NULL, record)
            .await
            .expect("Producer Send failed");
        drop(lock);

        // Send the consumer the expected checksum for the record it just sent
        // Note: We don't support consumer testing from a different starting offset than the producer (i.e., a catch-up read test)
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
