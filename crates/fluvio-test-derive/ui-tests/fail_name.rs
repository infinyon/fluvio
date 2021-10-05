use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;

#[fluvio_test(name = 1)]
pub fn test1(mut test_driver: TestDriver, test_case: TestCase) {
}

#[fluvio_test(name = unquoted)]
pub fn test2(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}

