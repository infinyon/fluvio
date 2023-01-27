use std::time::Duration;

use futures_lite::StreamExt;
use tracing::debug;
use clap::Parser;

use fluvio::{Offset, TopicProducer, TopicProducerConfigBuilder, FluvioAdmin};
use fluvio_protocol::record::Batch;
use fluvio_protocol::record::RawRecords;
use fluvio_protocol::Encoder;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[clap(name = "Fluvio Batching Test")]
pub struct BatchingTestOption {}

#[fluvio_test(topic = "batching", async)]
pub async fn batching(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting produce_batch test");

    let topic_name = test_case.environment.base_topic_name();

    let leader = {
        let admin: FluvioAdmin = test_driver.client().admin().await;
        let partitions = admin.all::<PartitionSpec>().await.expect("partitions");
        let test_topic = &partitions[0];
        test_topic.spec.leader
    };

    println!("Found leader {leader}");

    let consumer = test_driver.get_consumer(&topic_name, 0).await;
    let mut stream = consumer
        .stream(Offset::end())
        .await
        .expect("Failed to create consumer stream");

    for _ in 0..150 {
        // Ensure record is sent after the linger time even if we dont call flush()
        let config = TopicProducerConfigBuilder::default()
            .linger(Duration::from_millis(0))
            .build()
            .expect("failed to build config");

        let producer: TopicProducer = test_driver
            .create_producer_with_config(&topic_name, config)
            .await;
        debug!("Created producer with linger time");

        producer.send("key", "value").await.expect("Failed produce");
        let record = stream
            .next()
            .await
            .expect("Failed consume")
            .expect("Record");
        assert_eq!(record.value(), "value".as_bytes());

        // Ensure record is sent when we call flush() (we make linger_time large to test that)

        let config = TopicProducerConfigBuilder::default()
            .linger(Duration::from_millis(600000))
            .build()
            .expect("failed to build config");
        let producer: TopicProducer = test_driver
            .create_producer_with_config(&topic_name, config)
            .await;
        debug!("Created producer with large linger time");

        producer
            .send("key", "value2")
            .await
            .expect("Failed produce");
        producer.flush().await.expect("Failed flush");
        let record = stream
            .next()
            .await
            .expect("Failed consume")
            .expect("Record");
        assert_eq!(record.value(), "value2".as_bytes());

        // Ensure record is sent when batch is full (we make batch_size smaller))

        let config = TopicProducerConfigBuilder::default()
            .linger(Duration::from_millis(600000))
            .batch_size(
                17 + Vec::<RawRecords>::default().write_size(0)
                    + Batch::<RawRecords>::default().write_size(0),
            )
            .build()
            .expect("failed to build config");

        let producer: TopicProducer = test_driver
            .create_producer_with_config(&topic_name, config)
            .await;
        debug!("Created producer with small batch size");

        // The size of this record is equal to batch_size so it will be sent without calling flush and before the linger time
        producer
            .send("key", "value3")
            .await
            .expect("Failed produce");

        let record = stream
            .next()
            .await
            .expect("Failed consume")
            .expect("Record");
        assert_eq!(record.value(), "value3".as_bytes());
        drop(producer);
    }
}
