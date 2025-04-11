use std::time::Duration;

use anyhow::{ensure, Result};
use fluvio::{consumer::ConsumerStream, Fluvio};
use fluvio_future::future::timeout;
use futures_lite::StreamExt;

use crate::tests::consumer_offsets::utils::{
    self, create_consumer_config, delete_consumer, find_consumer, now, RECORDS_COUNT,
};

pub async fn test_strategy_manual_from_end(
    client: &Fluvio,
    topic: &str,
    partitions: usize,
) -> Result<()> {
    if partitions > 1 {
        panic!("Manual tests not implemented for multiple partitions");
    }

    // produce records
    utils::produce_records(client, topic, partitions).await?;
    utils::wait_for_offsets_topic_provisined(client).await?;

    let consumer_id = format!("test_strategy_manual_end_{}", now());
    let config = create_consumer_config(
        topic,
        &consumer_id,
        partitions,
        fluvio::consumer::OffsetManagementStrategy::Manual,
        fluvio::Offset::from_end(4),
        false,
    )?;

    let mut stream = client.consumer_with_config(config.clone()).await?;

    // read 4 records (from end)
    for _ in 0..4 {
        ensure!(stream.next().await.is_some());
    }

    // no more records for this consumer, timeout
    {
        let result = timeout(Duration::from_secs(2), stream.next()).await;
        ensure!(result.is_err());
    }

    utils::produce_records(client, topic, partitions).await?;
    utils::wait_for_offsets_topic_provisined(client).await?;

    for _ in 0..4 {
        ensure!(stream.next().await.is_some());
    }
    stream.offset_commit().await?;
    stream.offset_flush().await?;

    let consumer = find_consumer(client, &consumer_id, 0).await?;
    ensure!(consumer.is_some());
    ensure!(consumer.unwrap().offset == ((RECORDS_COUNT / partitions - 1) + 4) as i64);

    // this stream should be at the end (4) of consumer offset not the topic.
    let mut stream = client.consumer_with_config(config.clone()).await?;

    for _ in 0..(RECORDS_COUNT) {
        let record = stream.next().await;
        record.unwrap().unwrap();
    }

    stream.offset_commit().await?;
    stream.offset_flush().await?;

    let consumer = find_consumer(client, &consumer_id, 0).await?;
    ensure!(consumer.is_some());
    ensure!(consumer.unwrap().offset == (((RECORDS_COUNT / partitions) * 2) - 1) as i64);

    // no more records for this consumer, timeout
    {
        let result = timeout(Duration::from_secs(2), stream.next()).await;
        ensure!(result.is_err());
    }

    for partition in 0..partitions {
        delete_consumer(client, topic, &consumer_id, partition).await?;
    }

    for partition in 0..partitions {
        ensure!(find_consumer(client, &consumer_id, partition)
            .await?
            .is_none());
    }

    Ok(())
}
