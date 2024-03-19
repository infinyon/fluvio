use std::io::Error as IoError;

use anyhow::Result;
use fluvio_controlplane::CONSUMER_STORAGE_TOPIC;
use fluvio_protocol::{
    api::{RequestMessage, ResponseMessage},
    record::ReplicaKey,
    link::ErrorCode,
};
use fluvio_storage::FileReplica;
use fluvio_types::PartitionId;
use tracing::{instrument, debug};

use crate::{
    core::DefaultSharedGlobalContext, replication::leader::LeaderReplicaState,
    kv::consumer::Consumer,
};

use super::fetch_consumer_request::{FetchConsumerRequest, FetchConsumerResponse};

#[instrument(skip(req_msg, ctx))]
pub async fn handle_fetch_consumer_request(
    req_msg: RequestMessage<FetchConsumerRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FetchConsumerResponse>, IoError> {
    let FetchConsumerRequest {
        topic,
        partition,
        consumer_id,
    } = req_msg.request;

    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());

    let (consumer, error_code) =
        if let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await {
            match get_offset(ctx, replica, topic, partition, consumer_id).await {
                Ok(offset) => (offset, ErrorCode::None),
                Err(e) => (None, ErrorCode::Other(e.to_string())),
            }
        } else {
            (None, ErrorCode::PartitionNotLeader)
        };
    debug!(?consumer, ?error_code, "consumer fetch result");
    let consumer = consumer.map(|c| super::fetch_consumer_request::Consumer::new(c.offset, c.ttl));
    let response = FetchConsumerResponse::new(error_code, consumer);
    Ok(RequestMessage::<FetchConsumerRequest>::response_with_header(&req_msg.header, response))
}

async fn get_offset(
    ctx: DefaultSharedGlobalContext,
    replica: &LeaderReplicaState<FileReplica>,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
) -> Result<Option<Consumer>> {
    let consumers = ctx
        .consumers()
        .get_or_insert(replica, ctx.follower_notifier())
        .await?;

    consumers.get_by(topic, partition, consumer_id).await
}
