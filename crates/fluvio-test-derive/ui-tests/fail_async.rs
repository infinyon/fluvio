use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;

#[fluvio_test(async = 1)]
pub fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = a)]
pub fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = "true")]
pub fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(async = "false")]
pub fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}
