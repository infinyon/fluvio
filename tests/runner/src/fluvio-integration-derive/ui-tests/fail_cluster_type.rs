use fluvio_integration_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;

#[fluvio_test(cluster_type="not-a-type")]
pub async fn test1(client: Arc<Fluvio>, mut test_case: TestCase) {
}

// Wrong attr type (MetaList), but values would have been valid
#[fluvio_test(cluster_type=["k8", "local"])]
pub async fn test2(client: Arc<Fluvio>, mut test_case: TestCase) {
}


fn main() {
}
