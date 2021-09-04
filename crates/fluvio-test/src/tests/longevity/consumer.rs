use std::sync::Arc;
use std::time::SystemTime;
use async_lock::RwLock;
use async_channel::Receiver;
use fluvio_test_util::test_runner::test_driver::TestDriver;
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

    let consumer = lock
        .get_consumer(option.environment.topic_name.as_str())
        .await;

    drop(lock);

    // TODO: Support starting stream from consumer offset
    //let mut stream = consumer.stream(Offset::from_end(0)).await.unwrap();
    let mut stream = consumer.stream(Offset::end()).await.unwrap();

    let mut index: i32 = 0;

    // While the producer is still running
    //while let Ok(ref existing_record_digest) = digests.recv().clone() {

    //while let Some(Ok(record)) = stream.next().await {
    while let Ok(existing_record_digest) = digests.recv().await {
        let now = SystemTime::now();
        //let existing_record_digest = digests.recv().unwrap();
        if let Some(Ok(record)) = stream.next().await {
            let current_record_digest = hash_record(record.as_ref());

            //
            println!(
                "Consuming {:<5} (size {:<5}): was produced: {}, was consumed: {}",
                index,
                record.as_ref().len(),
                existing_record_digest,
                current_record_digest
            );
            assert_eq!(existing_record_digest, current_record_digest);

            let mut lock = test_driver.write().await;

            // record latency
            let consume_time = now.elapsed().clone().unwrap().as_nanos();
            lock.consume_latency_record(consume_time as u64).await;
            lock.consume_bytes_record(record.as_ref().len()).await;

            // debug!("Consume stat updates: {:?} {:?}", lock.consumer_latency_histogram, lock.consumer_bytes);
            //debug!(consumer_bytes = lock.consumer_bytes, "Consume stat updates");

            drop(lock);

            index += 1;
        } else {
            panic!("Stream ended unexpectedly")
        }
    }
    //}
}
