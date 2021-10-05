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

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Test Example One")]
pub struct TestOneTestOption {}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Test Example Two")]
pub struct TestTwoTestOption {}

impl TestOption for TestOneTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TestOption for TestTwoTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(cluster_type="k8")]
pub async fn test_one(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(cluster_type="local")]
pub async fn test_two(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}

