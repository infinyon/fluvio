use std::iter::repeat_n;

use anyhow::{ensure, Result};
use fluvio::{
    consumer::{ConsumerConfigExtBuilder, ConsumerStream, OffsetManagementStrategy},
    Fluvio, Offset,
};

use crate::tests::consumer_offsets::utils::{ensure_read, RECORDS_COUNT};

pub async fn test_strategy_none(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    let mut builder = ConsumerConfigExtBuilder::default();
    for partition in 0..partitions {
        builder.partition(partition as u32);
    }
    let mut stream = client
        .consumer_with_config(
            builder
                .topic(topic.to_string())
                .offset_strategy(OffsetManagementStrategy::None)
                .offset_start(Offset::beginning())
                .build()?,
        )
        .await?;
    let mut counts = repeat_n(-1, partitions).collect::<Vec<_>>();
    for _ in 0..RECORDS_COUNT {
        ensure_read(&mut stream, &mut counts).await?;
    }
    ensure!(stream.offset_commit().await.is_err());
    ensure!(stream.offset_flush().await.is_err());
    Ok(())
}
