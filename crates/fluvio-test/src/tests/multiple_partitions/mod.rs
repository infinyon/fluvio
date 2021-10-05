pub mod producer;
pub mod consumer;

use std::any::Any;
use structopt::StructOpt;

use fluvio_future::task::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_future::task::run_block_on;

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
pub fn multiple_partition(mut test_driver: TestDriver, mut test_case: TestCase) -> TestResult {
    println!("Testing multiple partition consumer");

    let option: MultiplePartitionTestCase = test_case.into();

    run_block_on(async {
        spawn(producer::producer(test_driver.clone(), option.clone()));

        consumer::consumer_stream(&test_driver, option).await;
    });
}
