use std::sync::Arc;
use std::time::SystemTime;
use async_lock::RwLock;
use async_channel::Receiver;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use futures_lite::StreamExt;
use fluvio::Offset;

use super::LongevityTestCase;
use super::util::*;

pub async fn consumer_stream(
    test_driver: Arc<RwLock<TestDriver>>,
    option: LongevityTestCase,
    heartbeat: Receiver<()>,
) {
    let mut lock = test_driver.write().await;

    let consumer = lock.get_consumer(&option.environment.topic_name()).await;

    drop(lock);

    // TODO: Support starting stream from consumer offset
    let mut stream = consumer.stream(Offset::from_end(0)).await.unwrap();

    let mut index: i32 = 0;

    // Run consumer while the producer is running
    while heartbeat.recv().await.is_ok() {
        // Take a timestamp before record consumed
        let now = SystemTime::now();
        if let Some(Ok(record_json)) = stream.next().await {
            let record: LongevityRecord =
                serde_json::from_str(std::str::from_utf8(record_json.as_ref()).unwrap())
                    .expect("Deserialize record failed");

            let consume_latency = now.elapsed().clone().unwrap().as_nanos();

            if option.option.verbose {
                println!(
                    "Consuming {:<5} (size {:<5}): consumed CRC: {}",
                    index,
                    record.data.len(),
                    record.crc,
                );
            }

            assert!(record.validate_crc());

            let mut lock = test_driver.write().await;

            // record latency
            lock.consume_latency_record(consume_latency as u64).await;
            lock.consume_bytes_record(record.data.len()).await;

            drop(lock);

            index += 1;
        } else {
            panic!("Stream ended unexpectedly")
        }
    }
}
