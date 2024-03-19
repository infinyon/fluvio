use std::io::Error as IoError;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use fluvio_controlplane::CONSUMER_STORAGE_TOPIC;

use fluvio_spu_schema::server::consumer::DeleteConsumerRequest;
use fluvio_spu_schema::server::consumer::DeleteConsumerResponse;
use fluvio_spu_schema::server::consumer::FetchConsumersRequest;
use fluvio_spu_schema::server::consumer::FetchConsumersResponse;
use fluvio_spu_schema::server::consumer::UpdateConsumerRequest;
use fluvio_spu_schema::server::consumer::UpdateConsumerResponse;
use fluvio_storage::FileReplica;
use fluvio_types::PartitionId;
use tracing::debug;
use tracing::error;
use tracing::instrument;

use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_protocol::link::ErrorCode;
use tracing::trace;
use tracing::warn;

use crate::core::DefaultSharedGlobalContext;
use crate::kv::consumer::Consumer;
use crate::kv::consumer::Key;
use crate::replication::leader::LeaderReplicaState;

use super::conn_context::ConnectionContext;
use super::send_private_request_to_leader;

#[instrument(skip(req_msg, ctx, conn_ctx))]
pub async fn handle_update_consumer_request(
    req_msg: RequestMessage<UpdateConsumerRequest>,
    ctx: DefaultSharedGlobalContext,
    conn_ctx: &mut ConnectionContext,
) -> Result<ResponseMessage<UpdateConsumerResponse>, IoError> {
    let UpdateConsumerRequest { offset, session_id } = req_msg.request;

    let (offset, error_code) = match handle_update(ctx, conn_ctx, offset, session_id).await {
        Ok(offset) => (offset, ErrorCode::None),
        Err(error) => (i64::default(), error),
    };

    trace!(offset, ?error_code, "update consumer result");

    let response = UpdateConsumerResponse { error_code, offset };
    Ok(RequestMessage::<UpdateConsumerRequest>::response_with_header(&req_msg.header, response))
}

#[instrument(skip(req_msg, ctx))]
pub async fn handle_delete_consumer_request(
    req_msg: RequestMessage<DeleteConsumerRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<DeleteConsumerResponse>, IoError> {
    let DeleteConsumerRequest {
        topic,
        partition,
        consumer_id,
    } = req_msg.request;

    let error_code = match handle_delete(ctx, topic, partition, consumer_id).await {
        Ok(_) => ErrorCode::None,
        Err(error_code) => error_code,
    };

    debug!(?error_code, "delete consumer result");

    let response = DeleteConsumerResponse { error_code };
    Ok(RequestMessage::<DeleteConsumerRequest>::response_with_header(&req_msg.header, response))
}

#[instrument(skip(req_msg, ctx))]
pub async fn handle_fetch_consumers_request(
    req_msg: RequestMessage<FetchConsumersRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FetchConsumersResponse>, IoError> {
    let (consumers, error_code) = match handle_fetch_consumers(ctx).await {
        Ok(consumers) => (consumers, ErrorCode::None),
        Err(error_code) => (Vec::new(), error_code),
    };

    trace!(?error_code, ?consumers, "fetch consumers result");

    let response = FetchConsumersResponse {
        error_code,
        consumers,
    };
    Ok(RequestMessage::<FetchConsumersRequest>::response_with_header(&req_msg.header, response))
}

async fn handle_update(
    ctx: DefaultSharedGlobalContext,
    conn_ctx: &mut ConnectionContext,
    offset: i64,
    session_id: u32,
) -> std::result::Result<i64, ErrorCode> {
    let Some(publisher) = conn_ctx.stream_publishers().get_publisher(session_id).await else {
        return Err(ErrorCode::FetchSessionNotFoud);
    };
    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());

    let Some(consumer) = publisher.consumer else {
        return Err(ErrorCode::Other("stream without consumer id".to_string()));
    };

    if let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await {
        trace!(
            consumer.consumer_id,
            offset,
            ?consumer.ttl,
            "update consumer offset locally"
        );
        if let Err(err) = update_offset_for_leader(
            ctx,
            replica,
            publisher.topic.clone(),
            publisher.partition,
            consumer.consumer_id.clone(),
            consumer.ttl,
            offset,
        )
        .await
        {
            error!("update consumer offset locally failed: {err:?}");
            return Err(ErrorCode::Other(err.to_string()));
        }
    } else {
        trace!(consumer.consumer_id, offset, ?consumer.ttl, "update consumer offset remote");
        update_offset_in_peer(
            ctx,
            &consumers_replica_id,
            publisher.topic,
            publisher.partition,
            consumer.consumer_id,
            consumer.ttl,
            offset,
        )
        .await?;
    };

    Ok(offset)
}

async fn handle_delete(
    ctx: DefaultSharedGlobalContext,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
) -> std::result::Result<(), ErrorCode> {
    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());
    let Some(ref replica) = ctx.leaders_state().get(&consumers_replica_id).await else {
        return Err(ErrorCode::PartitionNotLeader);
    };

    let consumers = ctx
        .consumers()
        .get_or_insert(replica, ctx.follower_notifier())
        .await
        .map_err(|e| ErrorCode::Other(e.to_string()))?;

    let target_replica: ReplicaKey = (topic, partition).into();
    let key = Key::new(target_replica, consumer_id);
    consumers
        .delete(&key)
        .await
        .map_err(|e| ErrorCode::Other(format!("unable to delete consumer: {e:?}")))
}

async fn handle_fetch_consumers(
    _ctx: DefaultSharedGlobalContext,
) -> std::result::Result<Vec<fluvio_spu_schema::server::consumer::Consumer>, ErrorCode> {
    unimplemented!()
}

async fn update_offset_for_leader(
    ctx: DefaultSharedGlobalContext,
    replica: &LeaderReplicaState<FileReplica>,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
    ttl: Duration,
    offset: i64,
) -> Result<()> {
    let consumers = ctx
        .consumers()
        .get_or_insert(replica, ctx.follower_notifier())
        .await?;

    let target_replica: ReplicaKey = (topic, partition).into();
    let key = Key::new(target_replica, consumer_id);
    let consumer = Consumer::new(offset, ttl);
    consumers.put(key, consumer).await
}

async fn update_offset_in_peer(
    ctx: DefaultSharedGlobalContext,
    consumers_replica_id: &ReplicaKey,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
    ttl: Duration,
    offset: i64,
) -> Result<(), ErrorCode> {
    let update_req = crate::services::internal::UpdateConsumerRequest::new(
        topic,
        partition,
        consumer_id,
        offset,
        ttl,
    );

    let response = send_private_request_to_leader(&ctx, consumers_replica_id, update_req)
        .await
        .context("update offset in peer")
        .map_err(|e| ErrorCode::Other(e.to_string()))?;

    if response.error_code != ErrorCode::None {
        warn!(%response.error_code, "update offset in peer");
        return Err(response.error_code);
    }
    Ok(())
}
