#[allow(unused_imports)]
use fluvio_command::CommandExt;

use fluvio_integration_derive::fluvio_test;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::TestCase;
use std::sync::Arc;

// Custom vars:
// use_cli (default false)
// producer.iteration (default 1)
// producer.record_size (default 100)
// consumer.wait (default false)

// Add logic for this in future
// Disable consumer
// Disable producer
// Producer count

#[fluvio_test]
pub async fn run(client: Arc<Fluvio>, option: TestCase) {
    // Convert TestCase to SmokeTestCase to parse custom test vars
    let smoke_option = option.into();

    let start_offsets =
        fluvio_test_util::smoke::produce::produce_message(client.clone(), &smoke_option).await;
    fluvio_test_util::smoke::consume::validate_consume_message(
        client,
        &smoke_option,
        start_offsets,
    )
    .await;
}
