use std::any::Any;
use std::time::Duration;

use clap::Parser;
use fluvio_future::timer::sleep;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

#[derive(Debug, Clone)]
pub struct ExpectedFailTestCase {}

impl From<TestCase> for ExpectedFailTestCase {
    fn from(_test_case: TestCase) -> Self {
        ExpectedFailTestCase {}
    }
}

#[derive(Debug, Parser, Clone)]
#[clap(name = "Fluvio Expected Fail Test")]
pub struct ExpectedFailTestOption {}
impl TestOption for ExpectedFailTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = "expected_fail", topic = "unused")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    println!("\nStarting example test that fails");

    let fast_fail = async_process!(
        async {
            sleep(Duration::from_millis(2000)).await;
            panic!("This test should fail");
        },
        "fast-fail"
    );
    fast_fail.join().unwrap();
}
