use std::sync::Arc;

use tracing::debug;
use futures::future::BoxFuture;
use futures::FutureExt;

use flv_future_core::test_async;
use fluvio_socket::KfSocketError;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use kf_protocol::api::DefaultRecord;

use crate::tests::fixture::TestGenerator;
use crate::tests::fixture::SpuTest;
use crate::tests::fixture::SpuTestRunner;

const TOPIC_ID: &str = "topic1";
const PARTITION_ID: i32 = 0;

fn test_repl_id() -> ReplicaKey {
    ReplicaKey::new("topic1", 0)
}

struct SimpleFetchTest {}

async fn test_fetch(runner: Arc<SpuTestRunner<SimpleFetchTest>>) -> Result<(), KfSocketError> {
    let _replica = test_repl_id();
    //   runner.send_metadata_to_all(&replica).await.expect("send metadata");

    let produce_req_msg = runner.create_producer_msg("message", TOPIC_ID, PARTITION_ID);
    let produce_resp = runner
        .leader()
        .send_to_public_server(&produce_req_msg)
        .await?;
    let response = produce_resp.response;
    let topic_responses = response.responses;
    assert_eq!(topic_responses.len(), 1);
    let topic_response = &topic_responses[0];
    assert_eq!(topic_response.name, TOPIC_ID);
    let partition_responses = &topic_response.partitions;
    assert_eq!(partition_responses.len(), 1);
    assert_eq!(partition_responses[0].base_offset, 0);

    // do fetch

    // verify thru follower replica thru fetch request
    let fetch_req_msg = runner.create_fetch_request(0, TOPIC_ID, PARTITION_ID);
    let fetch_response = runner
        .leader()
        .send_to_public_server(&fetch_req_msg)
        .await?;
    debug!("fetch response: {:#?}", fetch_response);
    let response = fetch_response.response;
    assert_eq!(response.topics.len(), 1);
    let topic_response = &response.topics[0];
    assert_eq!(topic_response.name, TOPIC_ID);
    assert_eq!(topic_response.partitions.len(), 1);
    let partition_response = &topic_response.partitions[0];
    assert_eq!(partition_response.high_watermark, 1);
    let batches = &partition_response.records.batches;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.records.len(), 1);
    let record = &batch.records[0];
    let test_record: DefaultRecord = "message".to_owned().into();
    assert_eq!(
        record.value.inner_value_ref(),
        test_record.value.inner_value_ref()
    );

    Ok(())
}

impl SpuTest for SimpleFetchTest {
    type ResponseFuture = BoxFuture<'static, Result<(), KfSocketError>>;

    fn env_configuration(&self) -> TestGenerator {
        TestGenerator::default()
            .set_base_id(6000)
            .set_base_port(9800)
            .set_base_dir("fetch_test")
            .init()
    }

    fn followers(&self) -> usize {
        0
    }

    fn replicas(&self) -> Vec<ReplicaKey> {
        vec![test_repl_id()]
    }

    fn main_test(&self, runner: Arc<SpuTestRunner<SimpleFetchTest>>) -> Self::ResponseFuture {
        async move { test_fetch(runner).await }.boxed()
    }
}

#[test_async]
async fn simple_fetch_test() -> Result<(), KfSocketError> {
    let test = SimpleFetchTest {};
    SpuTestRunner::run("fetch test".to_owned(), test)
        .await
        .expect("test runner should not failer");
    Ok(())
}
