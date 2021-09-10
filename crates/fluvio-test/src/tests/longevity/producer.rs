use std::sync::Arc;
use async_lock::RwLock;
use fluvio::RecordKey;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use std::time::SystemTime;
use tracing::debug;

use super::LongevityTestCase;
use super::util::*;

pub async fn producer(test_driver: Arc<RwLock<TestDriver>>, option: LongevityTestCase) {
    let mut lock = test_driver.write().await;

    let producer = lock.create_producer(&option.environment.topic_name()).await;

    drop(lock);

    // Read in the timer value we want to run for
    // Note, we're going to give the consumer a couple extra seconds since it starts its timer first

    let mut records_sent = 0;
    let test_start = SystemTime::now();

    while test_start.elapsed().unwrap() <= option.option.runtime_seconds {
        let record = LongevityRecordBuilder::new()
            .with_offset(records_sent)
            .with_random_data(option.option.record_size)
            .build();
        let record_json = serde_json::to_string(&record)
            .expect("Convert record to json string failed")
            .as_bytes()
            .to_vec();

        debug!("{:?}", &record);

        if option.option.verbose {
            println!(
                "Producing {:<7} (size {:<5}): produced CRC: {:<10}",
                records_sent,
                record.data.len(),
                record.crc,
            );
        }

        // Record the latency
        let mut lock = test_driver.write().await;
        lock.send_count(&producer, RecordKey::NULL, record_json)
            .await
            .expect("Producer Send failed");
        drop(lock);

        records_sent += 1;
    }
    //}

    println!(
        "Producer stopped. Time's up!\nRecords sent: {:?}",
        records_sent
    )
}
