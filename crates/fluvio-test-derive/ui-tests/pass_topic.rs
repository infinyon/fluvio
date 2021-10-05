use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;
use structopt::StructOpt;
use std::any::Any;
use fluvio_test_util::test_meta::TestOption;

//use fluvio_test_util::test_meta::test_result::TestResult;
//use fluvio_test_util::test_runner::test_driver::TestDriver;
//use fluvio_test_util::test_meta::derive_attr::TestRequirements;
//use fluvio_test_util::test_meta::TestOption;
//use fluvio_future::task::run_block_on;
//use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Test Example")]
pub struct RunTestOption {}

impl TestOption for RunTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test")]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {

}