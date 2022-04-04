
use fluvio_test_derive::fluvio_test;
#[warn(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
use clap::Parser;
use std::any::Any;
use fluvio_test_util::test_meta::TestOption;

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Test Example")]
pub struct BareCaseTestOption {}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Test Example")]
pub struct TrueCaseTestOption {}

#[derive(Debug, Clone, Parser, Default, PartialEq)]
#[clap(name = "Fluvio Test Example")]
pub struct FalseCaseTestOption {}

impl TestOption for BareCaseTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TestOption for TrueCaseTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TestOption for FalseCaseTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(async)]
pub async fn bare_case(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = true)]
pub async fn true_case(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = false)]
pub fn false_case(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {

}