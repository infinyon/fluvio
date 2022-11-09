use std::any::Any;
use std::time::Duration;

use clap::Parser;
use fluvio_future::timer::sleep;
use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

#[derive(Debug, Clone)]
pub struct ExpectedFailJoinFailFirstTestCase {}

impl From<TestCase> for ExpectedFailJoinFailFirstTestCase {
    fn from(_test_case: TestCase) -> Self {
        ExpectedFailJoinFailFirstTestCase {}
    }
}

#[derive(Debug, Parser, Clone)]
#[clap(name = "Fluvio Expected FailJoinFailFirst Test")]
pub struct ExpectedFailJoinFailFirstTestOption {}
impl TestOption for ExpectedFailJoinFailFirstTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(name = r#"expected_fail_join_fail_first"#, topic = "unused")]
pub fn run(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    println!("\nStarting example test that fails");

    let success = async_process!(
        async {
            sleep(Duration::from_millis(2000)).await;
        },
        "success"
    );

    let fail = async_process!(
        async {
            sleep(Duration::from_millis(200)).await;
            panic!("This test should fail");
        },
        "fail"
    );
    fail.join().unwrap();
    success.join().unwrap();
}
