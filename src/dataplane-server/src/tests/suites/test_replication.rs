use std::time::Duration;
use std::sync::Arc;

use tracing::debug;
use futures::future::BoxFuture;
use futures::FutureExt;

use flv_future_core::test_async;
use flv_future_core::sleep;
use kf_socket::KfSocketError;
use fluvio_metadata::partition::ReplicaKey;
use fluvio_storage::ReplicaStorage;

use crate::tests::fixture::TestGenerator;
use crate::tests::fixture::SpuTest;
use crate::tests::fixture::SpuTestRunner;

const TOPIC_ID: &str = "topic1";
const PARTITION_ID: i32 = 0;

fn test_repl_id() -> ReplicaKey {
    ReplicaKey::new("topic1", 0)
}

struct FollowReplicationTest {
    replicas: usize,
    base: u16,
}

impl FollowReplicationTest {
    fn new(replicas: usize, base: u16) -> Self {
        Self { replicas, base }
    }
}

// simple replication to a sigle follower
async fn inner_test(
    runner: Arc<SpuTestRunner<FollowReplicationTest>>,
) -> Result<(), KfSocketError> {
    let leader_spu = runner.leader_spec();
    let leader_gtx = runner.leader_gtx();
    let _replica = test_repl_id();

    //  wait until all follower has sync up
    sleep(Duration::from_millis(150)).await.expect("panic");

    // verify leader has created replica and received fetch stream from follower
    let leaders_state = leader_gtx.leaders_state();
    assert!(leader_gtx.spus().spu(&leader_spu.name()).is_some());
    assert!(leaders_state.get_replica(&test_repl_id()).is_some());

    // check it has established socket sinks to all followers
    for i in 0..runner.followers_count() {
        let follower_id = runner.follower_spec(i).id;
        assert!(leader_gtx.follower_sinks().get_sink(&follower_id).is_some());
    }
    // verify that follower has created connection controller and created follower replica
    for i in 0..runner.followers_count() {
        let follower_gtx = runner.follower_gtx(i);
        let followers_state = follower_gtx.followers_state();
        assert!(followers_state.mailbox(&leader_spu.id).is_some());
        assert!(followers_state.has_controller(&leader_spu.id));
        assert!(followers_state.get_replica(&test_repl_id()).is_some());

        let follower_replica = followers_state
            .get_replica(&test_repl_id())
            .expect("replica should exists");
        assert_eq!(follower_replica.storage().get_end_offset(), 0);
        drop(follower_replica); // unlock so rest of test can follow it.
    }

    // send records to leader. this will propogate replica to follower

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

    debug!("sleep for 50ms to finish replication");
    sleep(Duration::from_millis(150)).await.expect("panic");

    for i in 0..runner.followers_count() {
        let follower_gtx = runner.follower_gtx(i);
        let followers_state = follower_gtx.followers_state();
        // verify that follower replica has received replica from leader
        let follower_replica = followers_state
            .get_replica(&test_repl_id())
            .expect("follower replica should exists");
        assert_eq!(follower_replica.storage().get_end_offset(), 1);
        assert_eq!(follower_replica.storage().get_high_watermark(), 1);

        drop(follower_replica);
    }

    // verify that leader has update followers offset
    let leader_replica = leaders_state
        .get_replica(&test_repl_id())
        .expect("leader replica should exists");
    assert_eq!(leader_replica.storage().get_end_offset(), 1);
    assert_eq!(leader_replica.storage().get_high_watermark(), 1);

    // verify that leader has it's updated it's followr offset and highwatermark
    for i in 0..runner.followers_count() {
        let follower_id = runner.follower_spec(i).id;
        let follower_info_slot = leader_replica
            .followers(&follower_id)
            .expect("followers info");
        let follower_info = follower_info_slot.expect("value");
        assert_eq!(follower_info.end_offset(), 1);
        assert_eq!(follower_info.high_watermark(), 1);
    }
    drop(leader_replica);

    Ok(())
}

impl SpuTest for FollowReplicationTest {
    type ResponseFuture = BoxFuture<'static, Result<(), KfSocketError>>;

    fn env_configuration(&self) -> TestGenerator {
        TestGenerator::default()
            .set_base_id(self.base as i32)
            .set_base_port(self.base)
            .set_base_dir(format!("replication_base_{}", self.base))
            .init()
    }

    fn followers(&self) -> usize {
        self.replicas
    }

    fn replicas(&self) -> Vec<ReplicaKey> {
        vec![test_repl_id()]
    }

    fn main_test(&self, runner: Arc<SpuTestRunner<FollowReplicationTest>>) -> Self::ResponseFuture {
        async move { inner_test(runner).await }.boxed()
    }
}

#[test_async]
async fn follower_replication_test_2() -> Result<(), KfSocketError> {
    // Todo: fix the intermittent failures (over 50%)
    //    SpuTestRunner::run("replication with 2 followers".to_owned(),FollowReplicationTest::new(2,6000)).await.expect("test runner should not failer");

    Ok(())
}

#[test_async]
async fn follower_replication_test_3() -> Result<(), KfSocketError> {
    // Todo: fix the intermittent failures (over 50%)
    //    SpuTestRunner::run("replication with 3 followers".to_owned(),FollowReplicationTest::new(3,6100)).await.expect("test runner should not failer");
    Ok(())
}
