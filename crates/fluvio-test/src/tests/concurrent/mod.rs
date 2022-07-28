pub mod producer;
pub mod consumer;
pub mod util;

use std::any::Any;
use clap::Parser;

use fluvio_future::task::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_future::task::run_block_on;
use tracing::{Instrument, debug_span};

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

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Concurrent Test")]
pub struct ConcurrentTestOption {}

impl TestOption for ConcurrentTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test-bug")]
pub fn concurrent(mut test_driver: TestDriver, mut test_case: TestCase) {
    println!("Testing concurrent consumer and producer");
    let option: ConcurrentTestCase = test_case.into();

    run_block_on(async {
        let (sender, receiver) = std::sync::mpsc::channel();
        spawn(
            consumer::consumer_stream(test_driver.clone(), option.clone(), receiver)
                .instrument(debug_span!("consumer")),
        );
        producer::producer(&test_driver, option, sender)
            .instrument(debug_span!("producer"))
            .await;
    });
}
