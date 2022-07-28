use std::any::Any;
use std::time::Duration;
use futures_lite::StreamExt;

use tracing::{debug, debug_span};
use tracing_futures::Instrument;

use fluvio::{Offset, TopicProducer, TopicProducerConfigBuilder, FluvioAdmin};
use fluvio::dataplane::batch::Batch;
use fluvio::dataplane::batch::RawRecords;

use dataplane::core::Encoder;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};

#[derive(Debug, Clone)]
pub struct BatchingTestCase {
    pub environment: EnvironmentSetup,
    pub option: BatchingTestOption,
}

impl From<TestCase> for BatchingTestCase {
    fn from(test_case: TestCase) -> Self {
        let producer_option = test_case
            .option
            .as_any()
            .downcast_ref::<BatchingTestOption>()
            .expect("BatchingTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: producer_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Batching Test")]
pub struct BatchingTestOption {}

impl TestOption for BatchingTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "batching", async)]
pub async fn batching(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting produce_batch test");

    let topic_name = test_case.environment.base_topic_name();

    let leader = {
        let admin: FluvioAdmin = test_driver.client().admin().await;
        let partitions = admin
            .list::<PartitionSpec, _>(vec![])
            .instrument(debug_span!("list_partitions"))
            .await
            .expect("partitions");
        let test_topic = &partitions[0];
        test_topic.spec.leader
    };

    println!("Found leader {}", leader);

    let consumer = test_driver
        .get_consumer(&topic_name, 0)
        .instrument(debug_span!("get_consumer"))
        .await;

    let mut stream = consumer
        .stream(Offset::end())
        .instrument(debug_span!("create_stream"))
        .await
        .expect("Failed to create consumer stream");

    for i in 0..150 {
        // Ensure record is sent after the linger time even if we dont call flush()
        let config = TopicProducerConfigBuilder::default()
            .linger(Duration::from_millis(0))
            .build()
            .expect("failed to build config");

        let producer: TopicProducer = test_driver
            .create_producer_with_config(&topic_name, config)
            .instrument(debug_span!("producer_create_no_linger", i = i))
            .await;
        debug!("Created producer with linger time");

        producer
            .send("key", "value")
            .instrument(debug_span!("producer_send_no_linger", i = i))
            .await
            .expect("Failed produce");
        let record = stream
            .next()
            .instrument(debug_span!("consumer_recv_no_linger", i = i))
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
            .instrument(debug_span!("producer_send_linger", i = i))
            .await;
        debug!("Created producer with large linger time");

        producer
            .send("key", "value2")
            .instrument(debug_span!("producer_send_linger", i = i))
            .await
            .expect("Failed produce");
        producer
            .flush()
            .instrument(debug_span!("producer_flush_linger", i = i))
            .await
            .expect("Failed flush");
        let record = stream
            .next()
            .instrument(debug_span!("consumer_recv_linger", i = i))
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
            .instrument(debug_span!("producer_create_linger_batch", i = i))
            .await;
        debug!("Created producer with small batch size");

        // The size of this record is equal to batch_size so it will be sent without calling flush and before the linger time
        producer
            .send("key", "value3")
            .instrument(debug_span!("producer_send_linger_batch", i = i))
            .await
            .expect("Failed produce");

        let record = stream
            .next()
            .instrument(debug_span!("consumer_recv_linger_batch", i = i))
            .await
            .expect("Failed consume")
            .expect("Record");
        assert_eq!(record.value(), "value3".as_bytes());
        drop(producer);
    }
}
