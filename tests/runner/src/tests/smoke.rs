use std::sync::Arc;
use fluvio::Fluvio;
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::TestCase;

#[fluvio_test(topic = "test")]
pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) {
    let smoke_test_case = test_case.into();

    let start_offsets =
        fluvio_test_util::smoke::produce::produce_message(client.clone(), &smoke_test_case).await;
    fluvio_test_util::smoke::consume::validate_consume_message(
        client,
        &smoke_test_case,
        start_offsets,
    )
    .await;
}
