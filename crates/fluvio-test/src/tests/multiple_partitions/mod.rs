pub mod producer;
pub mod consumer;

use std::any::Any;
use std::sync::Arc;
use structopt::StructOpt;

use fluvio_future::task::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_runner::test_driver::{TestDriver, SharedTestDriver};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;

#[derive(Debug, Clone)]
pub struct MultiplePartitionTestCase {
    pub environment: EnvironmentSetup,
    pub option: MultiplePartitionTestOption,
}

impl From<TestCase> for MultiplePartitionTestCase {
    fn from(test_case: TestCase) -> Self {
        let concurrent_option = test_case
            .option
            .as_any()
            .downcast_ref::<MultiplePartitionTestOption>()
            .expect("MultiplePartitionTestOption")
            .to_owned();
        MultiplePartitionTestCase {
            environment: test_case.environment,
            option: concurrent_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio MultiplePartition Test")]
pub struct MultiplePartitionTestOption {}

impl TestOption for MultiplePartitionTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test-multiple-partition")]
pub async fn multiple_partition(
    mut test_driver: SharedTestDriver,
    mut test_case: TestCase,
) -> TestResult {
    test_multiple_partition_consume(test_driver.clone(), test_case.into()).await
}

pub async fn test_multiple_partition_consume(
    test_driver: SharedTestDriver,
    option: MultiplePartitionTestCase,
) {
    println!("Testing multiple partition consumer");
    spawn(producer::producer(test_driver.clone(), option.clone()));

    consumer::consumer_stream(&test_driver, option).await;
}
