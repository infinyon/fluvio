use std::sync::mpsc::Receiver;
use fluvio_test_util::test_runner::FluvioTestDriver;
use futures_lite::StreamExt;
use fluvio::Offset;

use super::{ConcurrentTestCase, PARTITION};
use super::util::*;

pub async fn consumer_stream(
    test_driver: FluvioTestDriver,
    option: ConcurrentTestCase,
    digests: Receiver<String>,
) {
    let consumer = test_driver
        .client
        .partition_consumer(option.environment.topic_name.clone(), PARTITION)
        .await
        .unwrap();
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
