use std::{iter::repeat_n, time::Duration};

use anyhow::{ensure, Result};
use fluvio::{consumer::OffsetManagementStrategy, Fluvio, Offset};
use fluvio_future::timer::sleep;

use crate::tests::consumer_offsets::utils::{
    self, create_consumer_config, ensure_read, find_consumer, now,
};

pub async fn test_strategy_auto_periodic_flush(
    client: &Fluvio,
    topic: &str,
    partitions: usize,
    flush_period: Duration,
) -> Result<()> {
    utils::produce_records(client, topic, partitions)
        .await
        .expect("produced records");
    utils::wait_for_offsets_topic_provisined(client)
        .await
        .expect("offsets topic");
    let consumer_id = format!("test_strategy_auto_periodic_flush_{}", now());
    let offset_start = Offset::beginning();
    let mut config = create_consumer_config(
        topic,
        &consumer_id,
        partitions,
        OffsetManagementStrategy::Auto,
        offset_start,
        true,
    )?;
    config.offset_flush = flush_period;
    let mut stream = client.consumer_with_config(config.clone()).await?;
    let mut counts = repeat_n(-1, partitions).collect::<Vec<_>>();
    // read some records
    for _ in 0..30 {
        ensure_read(&mut stream, &mut counts).await?;
    }

    // wait for periodic flush
    sleep(Duration::from_millis(3100)).await;
    for _ in 0..10 {
        ensure_read(&mut stream, &mut counts).await?;
    }
    sleep(Duration::from_secs(3)).await; // yeild  to drive auto flush flow

    for (partition, offset) in counts.iter().enumerate().take(partitions) {
        let consumer = find_consumer(client, &consumer_id, partition).await?;
        ensure!(consumer.is_some());
        ensure!(consumer.unwrap().offset == *offset);
    }

    drop(stream); //we keep the stream alive to prevent flush on drop occuring

    Ok(())
}
