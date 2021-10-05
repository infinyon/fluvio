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
#[structopt(name = "Fluvio Test Example")]
pub struct RunTestOption {}

impl TestOption for RunTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(min_spu = 2)]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}
