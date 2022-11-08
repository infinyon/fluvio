use std::any::Any;
use std::time::Duration;

use clap::Parser;
use fluvio_future::timer::sleep;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

#[derive(Debug, Clone)]
pub struct ExpectedTimeoutTestCase {}

impl From<TestCase> for ExpectedTimeoutTestCase {
    fn from(_test_case: TestCase) -> Self {
        ExpectedTimeoutTestCase {}
    }
}

#[derive(Debug, Parser, Clone)]
#[clap(name = "Fluvio Expected timeout Test")]
pub struct ExpectedTimeoutTestOption {}
impl TestOption for ExpectedTimeoutTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = "expected_timeout", topic = "unused")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    println!("\nStarting example test that timeouts");

    let infinite_loop = async_process!(
        async {
            loop {
                sleep(Duration::from_secs(1)).await
            }
            // Do nothing and exit
        },
        "infinite loop"
    );
    infinite_loop.join().unwrap();
}
