use std::{io::Error as IoError, time::Duration};

use fluvio_controlplane::CONSUMER_STORAGE_TOPIC;
use fluvio_protocol::{
    api::{RequestMessage, ResponseMessage},
    link::ErrorCode,
    record::ReplicaKey,
    record::Offset,
};
use fluvio_storage::FileReplica;
use fluvio_types::PartitionId;
use tracing::{instrument, trace};

use crate::{
    core::DefaultSharedGlobalContext,
    replication::leader::LeaderReplicaState,
    kv::consumer::{Consumer, Key},
};

use super::update_consumer_request::{UpdateConsumerRequest, UpdateConsumerResponse};

#[instrument(skip(req_msg, ctx))]
pub async fn handle_update_consumer_request(
    req_msg: RequestMessage<UpdateConsumerRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<UpdateConsumerResponse>, IoError> {
    let UpdateConsumerRequest {
        topic,
        partition,
        consumer_id,
        offset,
        ttl,
    } = req_msg.request;

    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());

    let error_code = if let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await
    {
        match update_offset(ctx, replica, topic, partition, consumer_id, offset, ttl).await {
            Ok(_) => ErrorCode::None,
            Err(e) => ErrorCode::Other(e.to_string()),
        }
    } else {
        ErrorCode::PartitionNotLeader
    };
    trace!(offset, ?error_code, "consumer update result");
    let response = UpdateConsumerResponse { error_code };
    Ok(RequestMessage::<UpdateConsumerRequest>::response_with_header(&req_msg.header, response))
}

async fn update_offset(
    ctx: DefaultSharedGlobalContext,
    replica: &LeaderReplicaState<FileReplica>,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
    offset: Offset,
    ttl: Duration,
) -> anyhow::Result<()> {
    let consumers = ctx
        .consumers()
        .get_or_insert(replica, ctx.follower_notifier())
        .await?;
    let target_replica: ReplicaKey = (topic, partition).into();
    let key = Key::new(target_replica, consumer_id);
    let consumer = Consumer::new(offset, ttl);
    consumers.put(key, consumer).await
}
