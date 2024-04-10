use std::io::Error as IoError;

use anyhow::Result;
use fluvio_protocol::{
    api::{RequestMessage, ResponseMessage},
    record::ReplicaKey,
    link::ErrorCode,
};
use fluvio_storage::FileReplica;
use fluvio_types::{PartitionId, defaults::CONSUMER_STORAGE_TOPIC};
use tracing::{instrument, debug};

use crate::{
    core::DefaultSharedGlobalContext,
    replication::leader::LeaderReplicaState,
    kv::consumer::{ConsumerOffset, ConsumerOffsetKey},
};

use super::fetch_consumer_offset_request::{FetchConsumerOffsetRequest, FetchConsumerOffsetResponse};

#[instrument(skip(req_msg, ctx))]
pub(crate) async fn handle_fetch_consumer_offset_request(
    req_msg: RequestMessage<FetchConsumerOffsetRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FetchConsumerOffsetResponse>, IoError> {
    let FetchConsumerOffsetRequest {
        consumer_id,
        replica_id,
    } = req_msg.request;

    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());

    let (consumer, error_code) =
        if let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await {
            match get_offset(ctx, replica, replica_id, consumer_id).await {
                Ok(offset) => (offset, ErrorCode::None),
                Err(e) => (None, ErrorCode::Other(e.to_string())),
            }
        } else {
            (None, ErrorCode::PartitionNotLeader)
        };
    debug!(?consumer, ?error_code, "consumer offset fetch result");
    let consumer = consumer.map(|c| super::fetch_consumer_offset_request::Consumer::new(c.offset));
    let response = FetchConsumerOffsetResponse::new(error_code, consumer);
    Ok(
        RequestMessage::<FetchConsumerOffsetRequest>::response_with_header(
            &req_msg.header,
            response,
        ),
    )
}

async fn get_offset(
    ctx: DefaultSharedGlobalContext,
    replica: &LeaderReplicaState<FileReplica>,
    target_replica: ReplicaKey,
    consumer_id: String,
) -> Result<Option<ConsumerOffset>> {
    let consumers = ctx
        .consumer_offset()
        .get_or_insert(replica, ctx.follower_notifier())
        .await?;
    let key = ConsumerOffsetKey::new(target_replica, consumer_id);
    consumers.get(&key).await
}
