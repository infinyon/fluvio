use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;

#[fluvio_test(name = 1)]
pub async fn test1(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(name = unquoted)]
pub async fn test2(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}

