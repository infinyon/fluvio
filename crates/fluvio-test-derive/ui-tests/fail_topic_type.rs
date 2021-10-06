use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;

#[fluvio_test(topic = 1)]
pub fn run(mut test_driver: TestDriver, test_case: TestCase) {
}

fn main() {
}
