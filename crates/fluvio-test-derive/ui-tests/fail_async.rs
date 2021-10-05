use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;

#[fluvio_test(async = 1)]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = a)]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = "true")]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = "false")]
pub async fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}
