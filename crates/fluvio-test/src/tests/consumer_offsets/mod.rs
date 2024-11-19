use std::{
    iter::repeat,
    ops::AddAssign,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use anyhow::{bail, ensure, Result};
use fluvio::{
    consumer::{ConsumerConfigExt, ConsumerConfigExtBuilder, ConsumerOffset, ConsumerStream},
    metadata::objects::ListRequest,
    TopicProducerPool, Fluvio, Offset, RecordKey, TopicProducerConfigBuilder,
};
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_future::timer::sleep;
use fluvio_protocol::{link::ErrorCode, record::ConsumerRecord};
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;
use futures_lite::StreamExt;

const RECORDS_COUNT: usize = 100;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[command(name = "Fluvio Producer Batch Test")]
pub struct ConsumerOffsetsTestOption {}

#[fluvio_test(async)]
pub async fn consumer_offsets(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting offset_management test");

    let topic_name = test_case.environment.base_topic_name();
    let partitions = test_case.environment.partition as usize;
    if partitions == 1 {
        println!("running on single partition");
    } else {
        println!("running on multiple partitions");
    };
    test_driver.connect().await.expect("connected");
    let client = test_driver.client();
    produce_records(client, &topic_name, partitions)
        .await
        .expect("produced records");
    wait_for_offsets_topic_provisined(client)
        .await
        .expect("offsets topic");

    if let Err(e) = test_strategy_none(client, &topic_name, partitions).await {
        panic!("test_strategy_none on {partitions} partitions failed with {e:#?}");
    }
    if let Err(e) = test_strategy_manual(client, &topic_name, partitions).await {
        panic!("test_strategy_manual on {partitions} partitions failed with {e:#?}");
    }
    if let Err(e) = test_strategy_auto(client, &topic_name, partitions).await {
        panic!("test_strategy_auto on {partitions} partitions failed with {e:#?}");
    }
    if let Err(e) = test_strategy_auto_periodic_flush(client, &topic_name, partitions).await {
        panic!("test_strategy_auto on {partitions} partitions failed with {e:#?}");
    }
}

async fn produce_records(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
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
    let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
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

async fn test_strategy_none(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    let mut builder = ConsumerConfigExtBuilder::default();
    for partition in 0..partitions {
        builder.partition(partition as u32);
    }
    let mut stream = client
        .consumer_with_config(
            builder
                .topic(topic.to_string())
                .offset_strategy(fluvio::consumer::OffsetManagementStrategy::None)
                .offset_start(Offset::beginning())
                .build()?,
        )
        .await?;
    let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
    for _ in 0..RECORDS_COUNT {
        ensure_read(&mut stream, &mut counts).await?;
    }
    ensure!(stream.offset_commit().is_err());
    ensure!(stream.offset_flush().await.is_err());
    Ok(())
}

async fn test_strategy_manual(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    let consumer_id = format!("test_strategy_manual_{}", now());
    let config = manual_startegy_config(topic, &consumer_id, partitions)?;
    let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();

    for chunk in (0..RECORDS_COUNT).collect::<Vec<_>>().chunks(5) {
        // reading 20 times by 5 records each
        let mut stream = client.consumer_with_config(config.clone()).await?;
        for _ in chunk {
            ensure_read(&mut stream, &mut counts).await?;
        }
        stream.offset_commit()?;
        stream.offset_flush().await?;
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
        ensure!(find_consumer(client, &consumer_id, partition)
            .await?
            .is_none());
    }
    // consumer deleted, start from the beginning
    {
        let mut stream = client.consumer_with_config(config).await?;

        let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
        for _ in 0..partitions {
            ensure_read(&mut stream, &mut counts).await?;
        }
    }
    Ok(())
}

async fn test_strategy_auto(client: &Fluvio, topic: &str, partitions: usize) -> Result<()> {
    let consumer_id = format!("test_strategy_auto_{}", now());
    let config = auto_startegy_config(topic, &consumer_id, partitions)?;
    let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
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
        ensure!(find_consumer(client, &consumer_id, partition)
            .await?
            .is_none());
    }
    // consumer deleted, start from the beginning
    {
        let mut stream = client.consumer_with_config(config).await?;

        let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
        for _ in 0..partitions {
            ensure_read(&mut stream, &mut counts).await?;
        }
    }
    Ok(())
}

async fn test_strategy_auto_periodic_flush(
    client: &Fluvio,
    topic: &str,
    partitions: usize,
) -> Result<()> {
    let consumer_id = format!("test_strategy_auto_periodic_flush_{}", now());
    let mut config = auto_startegy_config(topic, &consumer_id, partitions)?;
    config.offset_flush = Duration::from_secs(2);
    let mut stream = client.consumer_with_config(config.clone()).await?;
    let mut counts = repeat(-1).take(partitions).collect::<Vec<_>>();
    // read some records
    for _ in 0..30 {
        ensure_read(&mut stream, &mut counts).await?;
    }

    // wait for periodic flush
    sleep(Duration::from_millis(3100)).await;
    for _ in 0..10 {
        ensure_read(&mut stream, &mut counts).await?;
    }
    sleep(Duration::from_secs(2)).await; // yeild  to drive auto flush flow

    let consumer = find_consumer(client, &consumer_id, 0).await?;
    ensure!(consumer.is_some());
    ensure!(consumer.unwrap().offset > 0i64);

    drop(stream); //we keep the stream alive to prevent flush on drop occuring

    Ok(())
}

async fn wait_for_offsets_topic_provisined(client: &Fluvio) -> Result<()> {
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

async fn find_consumer(
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

async fn delete_consumer(
    client: &Fluvio,
    topic: &str,
    consumer_id: &str,
    partition: usize,
) -> Result<()> {
    client
        .delete_consumer_offset(consumer_id, (topic.to_string(), partition as u32))
        .await
}

async fn ensure_read<
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

fn manual_startegy_config(
    topic: &str,
    consumer_id: &str,
    partitions: usize,
) -> Result<ConsumerConfigExt> {
    let mut builder = ConsumerConfigExtBuilder::default();
    if partitions == 1 {
        builder.partition(0);
    }
    builder
        .topic(topic.to_string())
        .disable_continuous(true)
        .offset_consumer(consumer_id.to_string())
        .offset_strategy(fluvio::consumer::OffsetManagementStrategy::Manual)
        .offset_start(Offset::beginning())
        .build()
}

fn auto_startegy_config(
    topic: &str,
    consumer_id: &str,
    partitions: usize,
) -> Result<ConsumerConfigExt> {
    let mut builder = ConsumerConfigExtBuilder::default();
    if partitions == 1 {
        builder.partition(0);
    }
    builder
        .topic(topic.to_string())
        .disable_continuous(true)
        .offset_consumer(consumer_id.to_string())
        .offset_strategy(fluvio::consumer::OffsetManagementStrategy::Auto)
        .offset_start(Offset::beginning())
        .build()
}

fn now() -> u64 {
    SystemTime::elapsed(&UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
