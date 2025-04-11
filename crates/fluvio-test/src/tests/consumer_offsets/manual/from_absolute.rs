use std::time::Duration;

use anyhow::{ensure, Result};
use fluvio::{consumer::ConsumerStream, Fluvio};
use fluvio_future::future::timeout;
use futures_lite::StreamExt;

use crate::tests::consumer_offsets::utils::{self, create_consumer_config, now};

pub async fn test_strategy_manual_from_absolute(
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

    let consumer_id = format!("test_strategy_manual_absolute_{}", now());
    let config = create_consumer_config(
        topic,
        &consumer_id,
        partitions,
        fluvio::consumer::OffsetManagementStrategy::Manual,
        fluvio::Offset::absolute(5).expect("absolute offset"),
        false,
    )?;

    let mut stream = client.consumer_with_config(config.clone()).await?;

    // read 95 records missing from beginning
    for _ in 0..95 {
        ensure!(stream.next().await.is_some());
    }
    stream.offset_commit().await?;
    stream.offset_flush().await?;

    // no more records for this consumer, timeout
    {
        let result = timeout(Duration::from_secs(2), stream.next()).await;
        ensure!(result.is_err());
    }

    Ok(())
}
