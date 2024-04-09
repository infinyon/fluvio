use std::io::Error as IoError;

use tracing::{debug, error};
use tracing::{trace, instrument};

use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetTopicResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_protocol::link::ErrorCode;
use fluvio_types::{PartitionId, defaults::CONSUMER_STORAGE_TOPIC};

use crate::core::DefaultSharedGlobalContext;
use crate::kv::consumer::ConsumerOffsetKey;
use crate::services::internal::FetchConsumerOffsetRequest;
use crate::services::public::send_private_request_to_leader;

#[instrument(skip(req_msg, ctx))]
pub async fn handle_offset_request(
    req_msg: RequestMessage<FetchOffsetsRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FetchOffsetsResponse>, IoError> {
    let request = req_msg.request();
    trace!("handling flv fetch request: {:#?}", request);

    let mut response = FetchOffsetsResponse::default();

    for topic_request in &request.topics {
        let topic = &topic_request.name;

        let mut topic_response = FetchOffsetTopicResponse {
            name: topic.clone(),
            ..Default::default()
        };

        for partition_req in &topic_request.partitions {
            let partition = &partition_req.partition_index;
            let mut partition_response = FetchOffsetPartitionResponse {
                partition_index: *partition,
                ..Default::default()
            };
            let rep_id = ReplicaKey::new(topic.clone(), *partition);
            if let Some(ref replica) = ctx.leaders_state().get(&rep_id).await {
                trace!("offset fetch request for replica found: {}", rep_id);
                let (start_offset, hw) = replica.start_offset_info().await;
                partition_response.error_code = ErrorCode::None;
                partition_response.start_offset = start_offset;
                partition_response.last_stable_offset = hw;

                if let Some(ref consumer_id) = request.consumer_id {
                    debug!(consumer_id, "fetch consumer offset");
                    match fetch_consumer_offset(&ctx, topic, *partition, consumer_id).await {
                        Ok(Some(consumer_offset)) => {
                            debug!(consumer_id, consumer_offset, "consumer offset");
                            partition_response.start_offset = consumer_offset + 1;
                        }
                        Ok(None) => {
                            debug!(consumer_id, "no consumer offset");
                        }
                        Err(e) => {
                            error!(consumer_id, "fetch consumer offset failed: {e:?}");
                            partition_response.error_code = e;
                        }
                    }
                }
            } else {
                trace!("offset fetch request is not found: {}", rep_id);
                partition_response.error_code = ErrorCode::PartitionNotLeader;
            }

            topic_response.partitions.push(partition_response);
        }

        response.topics.push(topic_response);
    }

    Ok(req_msg.new_response(response))
}

async fn fetch_consumer_offset(
    ctx: &DefaultSharedGlobalContext,
    topic: &str,
    partition: PartitionId,
    consumer_id: &str,
) -> Result<Option<i64>, ErrorCode> {
    let consumers_replica_id =
        ReplicaKey::new(CONSUMER_STORAGE_TOPIC, <PartitionId as Default>::default());
    if let Some(leader) = ctx.leaders_state().get(&consumers_replica_id).await {
        let consumers = ctx
            .consumer_offset()
            .get_or_insert(&leader, ctx.follower_notifier())
            .await
            .map_err(|e| ErrorCode::Other(e.to_string()))?;
        let key =
            ConsumerOffsetKey::new(ReplicaKey::new(topic.to_string(), partition), consumer_id);
        Ok(consumers
            .get(&key)
            .await
            .map_err(|e| ErrorCode::Other(e.to_string()))?
            .map(|c| c.offset))
    } else {
        fetch_consumer_offset_from_peer(
            ctx,
            &consumers_replica_id,
            topic.to_string(),
            partition,
            consumer_id.to_string(),
        )
        .await
    }
}

async fn fetch_consumer_offset_from_peer(
    ctx: &DefaultSharedGlobalContext,
    consumers_replica_id: &ReplicaKey,
    topic: String,
    partition: PartitionId,
    consumer_id: String,
) -> Result<Option<i64>, ErrorCode> {
    debug!(consumer_id, "fetch consumer from peer");

    let fetch_req = FetchConsumerOffsetRequest::new(topic, partition, consumer_id);
    let response = send_private_request_to_leader(ctx, consumers_replica_id, fetch_req)
        .await
        .map_err(|e| ErrorCode::Other(e.to_string()))?;

    if response.error_code != ErrorCode::None {
        return Err(response.error_code);
    }
    Ok(response.consumer.map(|c| c.offset))
}
