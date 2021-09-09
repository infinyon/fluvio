use std::sync::Arc;
use async_lock::RwLock;
use std::sync::mpsc::Receiver;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_meta::environment::EnvDetail;
use futures_lite::StreamExt;
use fluvio::Offset;

use super::ConcurrentTestCase;
use super::util::*;

pub async fn consumer_stream(
    test_driver: Arc<RwLock<TestDriver>>,
    option: ConcurrentTestCase,
    digests: Receiver<String>,
) {
    let mut lock = test_driver.write().await;

    let consumer = lock.get_consumer(&option.environment.topic_name()).await;
    let mut stream = consumer.stream(Offset::beginning()).await.unwrap();

    let mut index: i32 = 0;
    while let Some(Ok(record)) = stream.next().await {
        let existing_record_digest = digests.recv().unwrap();
        let current_record_digest = hash_record(record.as_ref());
        println!(
            "Consuming {:<5} (size {:<5}): was produced: {}, was consumed: {}",
            index,
            record.as_ref().len(),
            existing_record_digest,
            current_record_digest
        );
        assert_eq!(existing_record_digest, current_record_digest);
        index += 1;
    }
}
