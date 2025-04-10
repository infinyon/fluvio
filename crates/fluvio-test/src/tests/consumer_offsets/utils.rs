use std::{iter::repeat_n, ops::AddAssign, time::{Duration, SystemTime, UNIX_EPOCH}};

use anyhow::{bail, ensure, Result};

use fluvio::{
    consumer::{
        ConsumerConfigExt, ConsumerConfigExtBuilder, ConsumerOffset, ConsumerStream,
        OffsetManagementStrategy,
    }, metadata::objects::ListRequest, Fluvio, Offset, RecordKey, TopicProducerConfigBuilder, TopicProducerPool
};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_future::timer::sleep;
use fluvio_protocol::{link::ErrorCode, record::ConsumerRecord};
use futures_lite::StreamExt;

pub(crate) const RECORDS_COUNT: usize = 100;

pub(crate) async fn produce_records(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    let producer: TopicProducerPool = client
        .topic_producer_with_config(
            topic,
            TopicProducerConfigBuilder::default()
                .linger(std::time::Duration::from_millis(10))
                .build()
                .expect("producer config created"),
        )
        .await?;
    let mut results = Vec::new();
    for i in 0..RECORDS_COUNT {
        let result = producer.send(RecordKey::NULL, i.to_string()).await?;
        results.push(result);
    }
    let mut counts = repeat_n(-1, partitions).collect::<Vec<_>>();
    for result in results.into_iter() {
        let record = result.wait().await?;
        let index = &mut counts[record.partition_id() as usize];
        index.add_assign(1);
        ensure!(record.offset() == *index);
    }
    for i in counts {
        ensure!(i == (RECORDS_COUNT / partitions - 1) as i64);
    }
    println!("Send {RECORDS_COUNT}");
    Ok(())
}

pub(crate) async fn wait_for_offsets_topic_provisined(client: &Fluvio) -> Result<()> {
    for _ in 0..5 {
        let system_topics = client
            .admin()
            .await
            .list_with_config::<TopicSpec, String>(ListRequest::default().system(true))
            .await?;
        if system_topics
            .iter()
            .any(|t| t.name.eq(fluvio_types::defaults::CONSUMER_STORAGE_TOPIC))
        {
            return Ok(());
        }
        sleep(Duration::from_secs(5)).await;
    }
    bail!("offsets topic timeout")
}

pub(crate) async fn find_consumer(
    client: &Fluvio,
    consumer_id: &str,
    partition: usize,
) -> Result<Option<ConsumerOffset>> {
    Ok(client
        .consumer_offsets()
        .await?
        .into_iter()
        .find(|c| c.consumer_id.eq(consumer_id) && c.partition as usize == partition))
}

pub(crate) async fn delete_consumer(
    client: &Fluvio,
    topic: &str,
    consumer_id: &str,
    partition: usize,
) -> Result<()> {
    client
        .delete_consumer_offset(consumer_id, (topic.to_string(), partition as u32))
        .await
}

pub(crate) async fn ensure_read<
    S: ConsumerStream<Item = std::result::Result<ConsumerRecord, ErrorCode>> + Unpin,
>(
    stream: &mut S,
    counts: &mut [i64],
) -> Result<()> {
    let (offset, partition) = match stream.next().await {
        Some(Ok(record)) => (record.offset, record.partition),
        Some(Err(err)) => bail!("got Err({err:?}) from stream"),
        None => bail!("got none from stream "),
    };
    let prev = &mut counts[partition as usize];
    ensure!(
        offset > *prev,
        "prev: {prev}, got: {offset}, partition: {partition}"
    );
    *prev = offset;
    Ok(())
}

pub(crate) fn create_consumer_config(
    topic: &str,
    consumer_id: &str,
    partitions: usize,
    strategy: OffsetManagementStrategy,
    offset_start: Offset,
) -> Result<ConsumerConfigExt> {
    let mut builder = ConsumerConfigExtBuilder::default();
    if partitions == 1 {
        builder.partition(0);
    }
    builder
        .topic(topic.to_string())
        .disable_continuous(true)
        .offset_consumer(consumer_id.to_string())
        .offset_strategy(strategy)
        .offset_start(offset_start)
        .build()
}

pub(crate) fn now() -> u64 {
    SystemTime::elapsed(&UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
