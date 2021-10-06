use fluvio_test_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;

#[fluvio_test(cluster_type="not-a-type")]
pub fn test1(mut test_driver: TestDriver, test_case: TestCase) {
}

// Wrong attr type (MetaList), but values would have been valid
#[fluvio_test(cluster_type=["k8", "local"])]
pub fn test2(mut test_driver: TestDriver, test_case: TestCase) {
}


fn main() {
}
