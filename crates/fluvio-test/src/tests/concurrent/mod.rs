pub mod producer;
pub mod consumer;
pub mod util;

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
pub struct ConcurrentTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConcurrentTestOption,
}

impl From<TestCase> for ConcurrentTestCase {
    fn from(test_case: TestCase) -> Self {
        let concurrent_option = test_case
            .option
            .as_any()
            .downcast_ref::<ConcurrentTestOption>()
            .expect("ConcurrentTestOption")
            .to_owned();
        ConcurrentTestCase {
            environment: test_case.environment,
            option: concurrent_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Concurrent Test")]
pub struct ConcurrentTestOption {}

impl TestOption for ConcurrentTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test-bug")]
pub async fn concurrent(mut test_driver: SharedTestDriver, mut test_case: TestCase) -> TestResult {
    test_concurrent_consume_produce(test_driver.clone(), test_case.into()).await
}

pub async fn test_concurrent_consume_produce(
    test_driver: SharedTestDriver,
    option: ConcurrentTestCase,
) {
    println!("Testing concurrent consumer and producer");
    let (sender, receiver) = std::sync::mpsc::channel();
    spawn(consumer::consumer_stream(
        test_driver.clone(),
        option.clone(),
        receiver,
    ));
    producer::producer(&test_driver, option, sender).await;
}
