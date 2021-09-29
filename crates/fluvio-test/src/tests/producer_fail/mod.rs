use std::any::Any;
use std::sync::Arc;

use fluvio::{RecordKey, TopicProducer, FluvioAdmin, FluvioError};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::{TestDriver};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;

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

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Producer Batch Test")]
pub struct ProduceBatchTestOption {}

impl TestOption for ProduceBatchTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test")]
pub async fn produce_batch(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting produce_batch test");

    let topic_name = test_case.environment.topic_name();
    let producer: TopicProducer = test_driver.create_producer(&topic_name).await;

    println!("Created producer");

    let leader = {
        let admin: FluvioAdmin = test_driver.client().admin().await;
        let partitions = admin
            .list::<PartitionSpec, _>(vec![])
            .await
            .expect("partitions");
        let test_topic = &partitions[0];
        let leader = test_topic.spec.leader;
        leader
    };

    println!("Found leader {}", leader);

    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();

    println!("Got cluster manager");

    let result: Result<_, FluvioError> = (|| async move {
        for i in 0..1_000 {
            producer.send(RecordKey::NULL, i.to_string()).await?;
        }
        println!("Send 1000");

        cluster_manager.terminate_spu(leader).expect("terminate");
        println!("Terminate SPU");

        for i in 0..1_000 {
            producer.send(RecordKey::NULL, i.to_string()).await?;
        }
        println!("Send 1000");
        producer.flush().await?;
        println!("Flushed");
        Ok(())
    })()
    .await;

    // Ensure that one of the calls returned a failure
    assert!(result.is_err());
}
