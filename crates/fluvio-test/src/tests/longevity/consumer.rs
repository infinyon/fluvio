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
    digests: Receiver<String>,
) {
    let mut lock = test_driver.write().await;

    let consumer = lock.get_consumer(&option.environment.topic_name()).await;

    drop(lock);

    // TODO: Support starting stream from consumer offset
    let mut stream = consumer.stream(Offset::from_end(0)).await.unwrap();

    let mut index: i32 = 0;

    // Run consumer while the producer is running
    while let Ok(existing_record_digest) = digests.recv().await {
        // Take a timestamp before record consumed
        let now = SystemTime::now();
        if let Some(Ok(record)) = stream.next().await {
            let consume_latency = now.elapsed().clone().unwrap().as_nanos();
            let current_record_digest = hash_record(record.as_ref());

            if option.option.verbose {
                println!(
                    "Consuming {:<5} (size {:<5}): was produced: {}, was consumed: {}",
                    index,
                    record.as_ref().len(),
                    existing_record_digest,
                    current_record_digest
                );
            }

            assert_eq!(existing_record_digest, current_record_digest);

            let mut lock = test_driver.write().await;

            // record latency
            lock.consume_latency_record(consume_latency as u64).await;
            lock.consume_bytes_record(record.as_ref().len()).await;

            drop(lock);

            index += 1;
        } else {
            panic!("Stream ended unexpectedly")
        }
    }
}
