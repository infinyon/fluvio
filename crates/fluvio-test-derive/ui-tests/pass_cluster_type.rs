use fluvio_integration_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;

#[fluvio_test(cluster_type="k8")]
pub async fn test1(client: Arc<Fluvio>, mut test_case: TestCase) {
}

#[fluvio_test(cluster_type="local")]
pub async fn test2(client: Arc<Fluvio>, mut test_case: TestCase) {
}

fn main() {
}

