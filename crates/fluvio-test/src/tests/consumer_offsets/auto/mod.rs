pub mod flush;

use std::{iter::repeat_n, time::Duration};

use anyhow::{ensure, Result};
use fluvio::{consumer::OffsetManagementStrategy, Fluvio, Offset};
use fluvio_future::timer::sleep;
use futures_lite::StreamExt;

use crate::tests::consumer_offsets::utils::{
    create_consumer_config, delete_consumer, ensure_read, find_consumer, now, RECORDS_COUNT,
};

use super::utils;

pub async fn test_strategy_auto(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    utils::produce_records(client, topic, partitions)
        .await
        .expect("produced records");
    utils::wait_for_offsets_topic_provisined(client)
        .await
        .expect("offsets topic");
    let consumer_id = format!("test_strategy_auto_{}", now());
    let offset_start = Offset::beginning();

    let config = create_consumer_config(
        topic,
        &consumer_id,
        partitions,
        OffsetManagementStrategy::Auto,
        offset_start,
        true,
    )?;
    let mut counts = repeat_n(-1, partitions).collect::<Vec<_>>();
    for chunk in (0..RECORDS_COUNT).collect::<Vec<_>>().chunks(20) {
        // reading 20 times by 5 records each
        let mut stream = client.consumer_with_config(config.clone()).await?;
        for _ in chunk {
            ensure_read(&mut stream, &mut counts).await?;
        }
        drop(stream);
        sleep(Duration::from_secs(1)).await; // yeild after stream dropped to drive auto flush flow
    }
    // no more records for this consumer
    {
        let mut stream = client.consumer_with_config(config.clone()).await?;

        ensure!(stream.next().await.is_none());
    }
    for partition in 0..partitions {
        let consumer = find_consumer(client, &consumer_id, partition).await?;
        ensure!(consumer.is_some());
        ensure!(consumer.unwrap().offset == (RECORDS_COUNT / partitions - 1) as i64);
    }

    for partition in 0..partitions {
        delete_consumer(client, topic, &consumer_id, partition).await?;
    }

    for partition in 0..partitions {
        ensure!(
            find_consumer(client, &consumer_id, partition)
                .await?
                .is_none()
        );
    }
    // consumer deleted, start from the beginning
    {
        let mut stream = client.consumer_with_config(config).await?;

        let mut counts = repeat_n(-1, partitions).collect::<Vec<_>>();
        for _ in 0..partitions {
            ensure_read(&mut stream, &mut counts).await?;
        }
    }
    Ok(())
}
