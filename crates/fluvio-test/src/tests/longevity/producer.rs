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
        let record = LongevityRecordBuilder::new().with_offset(records_sent).with_random_data(option.option.record_size).build();
        let record_json = serde_json::to_string(&record).expect("Convert record to json string failed").as_bytes().to_vec();

        // TODO: Get rid of the channel and remove this var
        let record_digest = hash_record(&record_json);

        debug!("{:?}", &record);

            if option.option.verbose {
                println!(
                    "Producing {:<5} (size {:<5}): produced CRC: {}",
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
