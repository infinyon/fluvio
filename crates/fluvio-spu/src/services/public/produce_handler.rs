use std::io::{Error, ErrorKind};

use dataplane::batch::BatchRecords;
use fluvio::Compression;
use fluvio_controlplane_metadata::topic::CompressionAlgorithm;
use fluvio_storage::StorageError;
use tracing::{debug, trace, error};
use tracing::instrument;

use dataplane::ErrorCode;
use dataplane::produce::{
    ProduceResponse, TopicProduceResponse, PartitionProduceResponse, PartitionProduceData,
    DefaultProduceRequest, DefaultTopicRequest,
};
use dataplane::api::RequestMessage;
use dataplane::api::ResponseMessage;
use dataplane::record::RecordSet;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;

#[instrument(
    skip(request,ctx),
    fields(
        id = request.header.correlation_id(),
        client = %request.header.client_id()
    )
)]
pub async fn handle_produce_request(
    request: RequestMessage<DefaultProduceRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<ProduceResponse>, Error> {
    let (header, produce_request) = request.get_header_request();
    trace!("Handling ProduceRequest: {:#?}", produce_request);

    let mut response = ProduceResponse::default();
    for topic_request in produce_request.topics.into_iter() {
        let topic_response = handle_produce_topic(&ctx, topic_request).await?;
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
    ctx: &DefaultSharedGlobalContext,
    topic_request: DefaultTopicRequest,
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
async fn handle_produce_partition<R: BatchRecords>(
    ctx: &DefaultSharedGlobalContext,
    replica_id: ReplicaKey,
    partition_request: PartitionProduceData<RecordSet<R>>,
) -> Result<PartitionProduceResponse, Error> {
    trace!("Handling produce request for partition:");

    let mut partition_response = PartitionProduceResponse {
        partition_index: replica_id.partition,
        ..Default::default()
    };

    let leader_state = match ctx.leaders_state().get(&replica_id).await {
        Some(leader_state) => leader_state,
        None => {
            debug!(%replica_id, "Replica not found");
            partition_response.error_code = ErrorCode::NotLeaderForPartition;
            return Ok(partition_response);
        }
    };

    let replica_metadata = match ctx.replica_localstore().spec(&replica_id) {
        Some(replica_metadata) => replica_metadata,
        None => {
            error!(%replica_id, "Replica not found");
            partition_response.error_code = ErrorCode::TopicNotFound;
            return Ok(partition_response);
        }
    };

    let mut records = partition_request.records;
    if validate_records(
        &records,
        replica_metadata.compression_type.unwrap_or_default(),
    )
    .is_err()
    {
        error!(%replica_id, "Compression in batch not supported by this topic");
        partition_response.error_code = ErrorCode::CompressionError;
        return Ok(partition_response);
    }

    let write_result = leader_state
        .write_record_set(&mut records, ctx.follower_notifier())
        .await;

    match write_result {
        Ok(base_offset) => {
            partition_response.error_code = ErrorCode::None;
            partition_response.base_offset = base_offset;
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

fn validate_records<R: BatchRecords>(
    records: &RecordSet<R>,
    compression: CompressionAlgorithm,
) -> Result<(), Error> {
    if records.batches.iter().all(|batch| {
        let batch_compression = if let Ok(compression) = batch.get_compression() {
            compression
        } else {
            return false;
        };
        match compression {
            CompressionAlgorithm::Any => true,
            CompressionAlgorithm::None => batch_compression == Compression::None,
            CompressionAlgorithm::Gzip => batch_compression == Compression::Gzip,
            CompressionAlgorithm::Snappy => batch_compression == Compression::Snappy,
            CompressionAlgorithm::Lz4 => batch_compression == Compression::Lz4,
        }
    }) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::Other,
            "Compression not supported by topic",
        ))
    }
}
