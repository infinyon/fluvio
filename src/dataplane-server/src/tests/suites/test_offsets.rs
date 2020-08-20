use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;

use flv_future_core::test_async;
use kf_socket::KfSocketError;
use flv_metadata_cluster::partition::ReplicaKey;
use kf_protocol::api::RequestMessage;
use spu_api::offsets::FlvFetchOffsetsRequest;
use spu_api::offsets::FetchOffsetTopic;
use spu_api::offsets::FetchOffsetPartition;
use kf_protocol::api::FlvErrorCode;

use crate::tests::fixture::TestGenerator;
use crate::tests::fixture::SpuTest;
use crate::tests::fixture::SpuTestRunner;

const TOPIC_ID: &str = "topic1";
const PARTITION_ID: i32 = 0;

fn test_repl_id() -> ReplicaKey {
    ReplicaKey::new("topic1", 0)
}

struct OffsetsFetchTest {}

async fn test_fetch(runner: Arc<SpuTestRunner<OffsetsFetchTest>>) -> Result<(), KfSocketError> {
    // verify invalid request
    let mut request = FlvFetchOffsetsRequest::default();
    let mut topic = FetchOffsetTopic::default();
    topic.name = "dummy".into();
    let mut partition = FetchOffsetPartition::default();
    partition.partition_index = PARTITION_ID;
    topic.partitions.push(partition);
    request.topics.push(topic);
    let req_msg = RequestMessage::new_request(request).set_client_id("offset fetch tester");
    let produce_resp = runner
        .leader()
        .send_to_public_server(&req_msg)
        .await
        .expect("offset fetch");
    let topic_responses = produce_resp.response.topics;
    assert_eq!(topic_responses.len(), 1);
    let topic_response = &topic_responses[0];
    assert_eq!(topic_response.name, "dummy");
    let partition_responses = &topic_response.partitions;
    assert_eq!(partition_responses.len(), 1);
    let partition = &partition_responses[0];
    assert_eq!(partition.error_code, FlvErrorCode::PartitionNotLeader);

    let produce_req_msg = runner.create_producer_msg("message", TOPIC_ID, PARTITION_ID);
    let _response = runner
        .leader()
        .send_to_public_server(&produce_req_msg)
        .await
        .expect("producer must not fail");

    let mut request = FlvFetchOffsetsRequest::default();
    let mut topic = FetchOffsetTopic::default();
    topic.name = TOPIC_ID.into();
    let mut partition = FetchOffsetPartition::default();
    partition.partition_index = PARTITION_ID;
    topic.partitions.push(partition);
    request.topics.push(topic);
    let req_msg = RequestMessage::new_request(request).set_client_id("offset fetch tester");
    let produce_resp = runner
        .leader()
        .send_to_public_server(&req_msg)
        .await
        .expect("offset fetch");
    let topic_responses = produce_resp.response.topics;
    assert_eq!(topic_responses.len(), 1);
    let topic_response = &topic_responses[0];
    assert_eq!(topic_response.name, TOPIC_ID);
    let partition_responses = &topic_response.partitions;
    assert_eq!(partition_responses.len(), 1);
    let partition = &partition_responses[0];
    assert_eq!(partition.error_code, FlvErrorCode::None);
    assert_eq!(partition.last_stable_offset, 1);
    assert_eq!(partition.start_offset, 0);

    Ok(())
}

impl SpuTest for OffsetsFetchTest {
    type ResponseFuture = BoxFuture<'static, Result<(), KfSocketError>>;

    fn env_configuration(&self) -> TestGenerator {
        TestGenerator::default()
            .set_base_id(6100)
            .set_base_port(9900)
            .set_base_dir("offset_fetch_test")
            .init()
    }

    fn followers(&self) -> usize {
        0
    }

    fn replicas(&self) -> Vec<ReplicaKey> {
        vec![test_repl_id()]
    }

    fn main_test(&self, runner: Arc<SpuTestRunner<OffsetsFetchTest>>) -> Self::ResponseFuture {
        async move { test_fetch(runner).await }.boxed()
    }
}

#[test_async]
async fn flv_offset_fetch_test() -> Result<(), KfSocketError> {
    let test = OffsetsFetchTest {};
    SpuTestRunner::run("offset fetch test".to_owned(), test)
        .await
        .expect("test runner should not failer");
    Ok(())
}
