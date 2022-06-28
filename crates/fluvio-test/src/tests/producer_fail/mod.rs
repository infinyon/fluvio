use std::any::Any;

use fluvio::{RecordKey, TopicProducer, TopicProducerConfigBuilder, FluvioAdmin, FluvioError};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};

#[derive(Debug, Clone)]
pub struct ProducerBatchTestCase {
    pub environment: EnvironmentSetup,
    pub option: ProduceBatchTestOption,
}

impl From<TestCase> for ProducerBatchTestCase {
    fn from(test_case: TestCase) -> Self {
        let producer_option = test_case
            .option
            .as_any()
            .downcast_ref::<ProduceBatchTestOption>()
            .expect("ProducerBatchTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: producer_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Producer Batch Test")]
pub struct ProduceBatchTestOption {}

impl TestOption for ProduceBatchTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "batch", async)]
pub async fn produce_batch(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting produce_batch test");

    let topic_name = test_case.environment.base_topic_name();
    let config = TopicProducerConfigBuilder::default()
        .linger(std::time::Duration::from_millis(10))
        .build()
        .expect("failed to build config");

    let producer: TopicProducer = test_driver
        .create_producer_with_config(&topic_name, config)
        .await;

    println!("Created producer");

    let leader = {
        let admin: FluvioAdmin = test_driver.client().admin().await;
        let partitions = admin
            .list::<PartitionSpec, _>(vec![])
            .await
            .expect("partitions");
        let test_topic = &partitions[0];
        test_topic.spec.leader
    };

    println!("Found leader {}", leader);

    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();

    println!("Got cluster manager");

    let value = "a".repeat(5000);
    let result: Result<_, FluvioError> = (|| async move {
        let mut results = Vec::new();
        for i in 0..1000 {
            let result = producer.send(RecordKey::NULL, value.clone()).await?;
            results.push(result);
        }
        println!("Send 1000");
        let mut i = 0;
        for result in results.into_iter() {
            let record = result.wait().await.expect("result is not ok");

            // Check that offset is correct for each record sent.
            assert_eq!(record.offset(), i);

            i += 1;
        }

        assert_eq!(i, 1000);

        fluvio_future::timer::sleep(std::time::Duration::from_secs(3)).await;

        cluster_manager.terminate_spu(leader).expect("terminate");
        println!("Terminate SPU");

        println!("Sending one record");

        let _result = producer
            .send(RecordKey::NULL, i.to_string())
            .await
            .expect("Failed send");

        println!("flushing");

        // This should fail because the SPU is terminated.
        producer.flush().await?;

        Ok(())
    })()
    .await;

    // Ensure that one of the calls returned a failure
    assert!(result.is_err());
}
