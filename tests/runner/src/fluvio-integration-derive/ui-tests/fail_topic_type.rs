use fluvio_integration_derive::fluvio_test;
#[allow(unused_imports)]
use fluvio::Fluvio;
#[allow(unused_imports)]
use fluvio_test_util::test_meta::TestCase;
#[allow(unused_imports)]
use std::sync::Arc;

#[fluvio_test(topic = 1)]
pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) {
}

fn main() {
}
