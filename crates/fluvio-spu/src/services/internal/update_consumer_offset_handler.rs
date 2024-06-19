use std::io::Error as IoError;

use fluvio_protocol::{
    api::{RequestMessage, ResponseMessage},
    link::ErrorCode,
    record::ReplicaKey,
    record::Offset,
};
use fluvio_storage::FileReplica;
use fluvio_types::{PartitionId, defaults::CONSUMER_STORAGE_TOPIC};
use tracing::{instrument, trace};

use crate::{
    core::DefaultSharedGlobalContext,
    replication::leader::LeaderReplicaState,
    kv::consumer::{ConsumerOffset, ConsumerOffsetKey},
};

use super::update_consumer_offset_request::{UpdateConsumerOffsetRequest, UpdateConsumerOffsetResponse};

#[instrument(skip(req_msg, ctx))]
pub(crate) async fn handle_update_consumer_offset_request(
    req_msg: RequestMessage<UpdateConsumerOffsetRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<UpdateConsumerOffsetResponse>, IoError> {
    let UpdateConsumerOffsetRequest {
        consumer_id,
        offset,
        replica_id,
    } = req_msg.request;

    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());

    let error_code = if let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await
    {
        match update_offset(ctx, replica, replica_id, consumer_id, offset).await {
            Ok(_) => ErrorCode::None,
            Err(e) => ErrorCode::Other(e.to_string()),
        }
    } else {
        ErrorCode::PartitionNotLeader
    };
    trace!(offset, ?error_code, "consumer offset update result");
    let response = UpdateConsumerOffsetResponse { error_code };
    Ok(
        RequestMessage::<UpdateConsumerOffsetRequest>::response_with_header(
            &req_msg.header,
            response,
        ),
    )
}

async fn update_offset(
    ctx: DefaultSharedGlobalContext,
    replica: &LeaderReplicaState<FileReplica>,
    target_replica: ReplicaKey,
    consumer_id: String,
    offset: Offset,
) -> anyhow::Result<()> {
    let consumers = ctx
        .consumer_offset()
        .get_or_insert(replica, ctx.follower_notifier())
        .await?;
    let key = ConsumerOffsetKey::new(target_replica, consumer_id);
    let consumer = ConsumerOffset::new(offset);
    consumers.put(key, consumer).await
}
