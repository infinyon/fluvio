use std::io::Error;
use std::sync::Arc;

use fluvio_storage::StorageError;
use tracing::{debug, trace, error};
use tracing::instrument;

use dataplane::ErrorCode;
use dataplane::produce::{
    DefaultProduceRequest, ProduceResponse, TopicProduceResponse, PartitionProduceResponse,
    PartitionProduceData, TopicProduceData,
};
use dataplane::api::RequestMessage;
use dataplane::api::ResponseMessage;
use dataplane::record::RecordSet;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::GlobalContext;

#[instrument(
    skip(request,ctx),
    fields(
        id = request.header.correlation_id(),
        client = %request.header.client_id()
    )
)]
pub async fn handle_produce_request(
    request: RequestMessage<DefaultProduceRequest>,
    ctx: Arc<GlobalContext>,
) -> Result<ResponseMessage<ProduceResponse>, Error> {
    let (header, produce_request) = request.get_header_request();
    trace!("Handling ProduceRequest: {:#?}", produce_request);

    let mut response = ProduceResponse::default();
    for topic_request in produce_request.topics.into_iter() {
        let topic_response = handle_produce_topic(&*ctx, topic_request).await?;
        response.responses.push(topic_response);
    }

    trace!("Returning ProduceResponse: {:#?}", &response);
    Ok(RequestMessage::<DefaultProduceRequest>::response_with_header(&header, response))
}

#[instrument(
    skip(ctx, topic_request),
    fields(topic = %topic_request.name),
)]
async fn handle_produce_topic(
    ctx: &GlobalContext,
    topic_request: TopicProduceData<RecordSet>,
) -> Result<TopicProduceResponse, Error> {
    trace!("Handling produce request for topic:");
    let topic = &topic_request.name;

    let mut topic_response = TopicProduceResponse {
        name: topic.to_owned(),
        ..Default::default()
    };

    for partition_request in topic_request.partitions.into_iter() {
        let replica_id = ReplicaKey::new(topic.to_string(), partition_request.partition_index);
        let partition_response =
            handle_produce_partition(ctx, replica_id, partition_request).await?;
        topic_response.partitions.push(partition_response);
    }

    Ok(topic_response)
}

#[instrument(
    skip(ctx, replica_id, partition_request),
    fields(%replica_id),
)]
async fn handle_produce_partition(
    ctx: &GlobalContext,
    replica_id: ReplicaKey,
    mut partition_request: PartitionProduceData<RecordSet>,
) -> Result<PartitionProduceResponse, Error> {
    trace!("Handling produce request for partition:");

    let mut partition_response = PartitionProduceResponse {
        partition_index: replica_id.partition,
        ..Default::default()
    };

    let leader_state = match ctx.leaders_state().get(&replica_id) {
        Some(leader_state) => leader_state,
        None => {
            debug!(%replica_id, "Replica not found");
            partition_response.error_code = ErrorCode::NotLeaderForPartition;
            return Ok(partition_response);
        }
    };

    let write_result = leader_state
        .write_record_set(&mut partition_request.records, ctx.follower_notifier())
        .await;

    match write_result {
        Ok(_) => {
            partition_response.error_code = ErrorCode::None;
        }
        Err(err @ StorageError::BatchTooBig(_)) => {
            error!(%replica_id, "Batch is too big: {:#?}", err);
            partition_response.error_code = ErrorCode::MessageTooLarge
        }
        Err(err) => {
            error!(%replica_id, "Error writing to replica: {:#?}", err);
            partition_response.error_code = ErrorCode::StorageError;
        }
    }

    Ok(partition_response)
}
