use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use fluvio::RecordKey;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_future::timer::sleep;
use structopt::StructOpt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::{TestDriver, TestDriverType};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use async_lock::RwLock;

#[derive(Debug, Clone)]
pub struct ElectionTestCase {
    pub environment: EnvironmentSetup,
    pub option: ElectionTestOption,
}

impl From<TestCase> for ElectionTestCase {
    fn from(test_case: TestCase) -> Self {
        let election_option = test_case
            .option
            .as_any()
            .downcast_ref::<ElectionTestOption>()
            .expect("SmokeTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: election_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio ELECTION Test")]
pub struct ElectionTestOption {}

impl TestOption for ElectionTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

//inventory::submit! {
//    FluvioTest {
//        name: "smoke".to_string(),
//        test_fn: smoke,
//        validate_fn: validate_subcommand,
//    }
//}

//pub fn validate_subcommand(subcmd: Vec<String>) -> Box<dyn TestOption> {
//    Box::new(SmokeTestOption::from_iter(subcmd))
//}

#[fluvio_test(topic = "test")]
pub async fn election(
    mut test_driver: Arc<RwLock<FluvioTestDriver>>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting election test");

    // first a create simple message
    let topic_name = test_case.environment.topic_name();
    let mut lock = test_driver.write().await;
    let producer = lock.create_producer(&topic_name).await;
    drop(lock);

    producer
        .send(RecordKey::NULL, "Hello World")
        .await
        .expect("sending");

    // this is hack now, because we don't have ack
    sleep(Duration::from_secs(200)).await;

    let lock = test_driver.write().await;
    let TestDriverType::Fluvio(fluvio_client) = lock.admin_client.as_ref();
    let admin = fluvio_client.admin().await;
    let partitions = admin
        .list::<PartitionSpec, _>(vec![])
        .await
        .expect("partitions");
    drop(lock);

    assert_eq!(partitions.len(), 1);
    let test_topic = &partitions[0];
    let status = &test_topic.status;
    let leader = &status.leader;
    assert_eq!(leader.leo, 1);
    assert_eq!(leader.hw, 1);
    let follower_status = &status.replicas[0];
    assert_eq!(follower_status.hw, 1);
    assert_eq!(follower_status.leo, 1);

    println!("election test ok")
}
