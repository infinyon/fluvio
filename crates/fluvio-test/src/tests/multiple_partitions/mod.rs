pub mod producer;
pub mod consumer;

use clap::Parser;

use fluvio_future::task::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;
use fluvio_future::task::run_block_on;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[clap(name = "Fluvio MultiplePartition Test")]
pub struct MultiplePartitionTestOption {}

#[fluvio_test(topic = "test-multiple-partition")]
pub fn multiple_partition(mut test_driver: TestDriver, mut test_case: TestCase) -> TestResult {
    println!("Testing multiple partition consumer");

    let option: MyTestCase = test_case.into();

    run_block_on(async {
        spawn(producer::producer(test_driver.clone(), option.clone()));

        consumer::consumer_stream(&test_driver, option).await;
    });
}
