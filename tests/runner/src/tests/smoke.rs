#[allow(unused_imports)]
use fluvio_command::CommandExt;

use fluvio_integration_derive::fluvio_test;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::TestOption;
use std::sync::Arc;

#[fluvio_test]
pub async fn run(client: Arc<Fluvio>, option: TestOption) {
    let start_offsets =
        fluvio_test_util::smoke::produce::produce_message(client.clone(), &option).await;
    fluvio_test_util::smoke::consume::validate_consume_message(client, &option, start_offsets)
        .await;
}
