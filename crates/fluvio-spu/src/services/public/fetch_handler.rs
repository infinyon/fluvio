use tracing::{debug, trace, instrument};

use fluvio_spu_schema::file::FileRecordSet;
use fluvio_socket::ExclusiveFlvSink;
use fluvio_socket::SocketError;
use fluvio_protocol::{link::ErrorCode, api::RequestMessage};
use fluvio_spu_schema::fetch::{
    FileFetchResponse, FileFetchRequest, FilePartitionResponse, FileTopicResponse,
    FetchablePartitionResponse, FetchPartition, FetchableTopic, FetchableTopicResponse,
};
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;

use super::metrics::StorageMetrics;
use super::metrics::StorageMetricsTopicPartition;

/// perform log fetch request using zero copy write
#[instrument(
    skip(request, ctx, sink),
    fields(
        max_bytes = request.request.max_bytes,
    ),
)]
pub async fn handle_fetch_request(
    request: RequestMessage<FileFetchRequest>,
    ctx: DefaultSharedGlobalContext,
    sink: ExclusiveFlvSink,
) -> Result<(), SocketError> {
    let (header, fetch_request) = request.get_header_request();
    trace!("Handling FileFetchRequest: {:#?}", fetch_request);
    let mut fetch_response = FileFetchResponse::default();

    let metrics = super::metrics::StorageMetrics::new();

    for topic_request in &fetch_request.topics {
        let topic_response =
            handle_fetch_topic(&ctx, &fetch_request, topic_request, &metrics).await?;
        fetch_response.topics.push(topic_response);
    }

    let response =
        RequestMessage::<FileFetchRequest>::response_with_header(&header, fetch_response);
    trace!("Sending FileFetchResponse: {:#?}", response);

    let mut inner = sink.lock().await;
    inner
        .encode_file_slices(&response, header.api_version())
        .await?;

    drop(inner);

    trace!("Finished sending FileFetchResponse");
    Ok(())
}

#[instrument(
    skip(ctx, fetch_request, topic_request, metrics),
    fields(topic = %topic_request.name),
)]
async fn handle_fetch_topic(
    ctx: &DefaultSharedGlobalContext,
    fetch_request: &FileFetchRequest,
    topic_request: &FetchableTopic,
    metrics: &StorageMetrics,
) -> Result<FetchableTopicResponse<FileRecordSet>, SocketError> {
    let topic = &topic_request.name;

    let mut topic_response = FileTopicResponse {
        name: topic.clone(),
        ..Default::default()
    };

    let topic_metrics = metrics.with_topic(topic);

    for partition_request in &topic_request.fetch_partitions {
        let partition_metrics = topic_metrics.with_partition(partition_request.partition_index);

        let replica_id = ReplicaKey::new(topic.clone(), partition_request.partition_index);
        let partition_response = handle_fetch_partition(
            ctx,
            replica_id,
            fetch_request,
            partition_request,
            partition_metrics,
        )
        .await?;
        topic_response.partitions.push(partition_response);
    }

    Ok(topic_response)
}

#[instrument(
skip(ctx, replica_id, partition_request, partition_metrics),
    fields(%replica_id)
)]
async fn handle_fetch_partition(
    ctx: &DefaultSharedGlobalContext,
    replica_id: ReplicaKey,
    fetch_request: &FileFetchRequest,
    partition_request: &FetchPartition,
    partition_metrics: StorageMetricsTopicPartition<'_>,
) -> Result<FetchablePartitionResponse<FileRecordSet>, SocketError> {
    trace!("Fetching partition:");
    let fetch_offset = partition_request.fetch_offset;

    let mut partition_response = FilePartitionResponse {
        partition_index: partition_request.partition_index,
        ..Default::default()
    };

    let leader_state = match ctx.leaders_state().get(&replica_id).await {
        Some(leader_state) => leader_state,
        None => {
            debug!("Not leader for partition:");
            partition_response.error_code = ErrorCode::NotLeaderForPartition;
            return Ok(partition_response);
        }
    };

    match leader_state
        .read_records(
            fetch_offset,
            fetch_request.max_bytes as u32,
            fetch_request.isolation_level,
        )
        .await
    {
        Ok(slice) => {
            partition_response.high_watermark = slice.end.hw;
            partition_response.log_start_offset = slice.start;

            partition_metrics.add_records_read((slice.end.hw - slice.start) as u64);

            if let Some(file_slice) = slice.file_slice {
                partition_metrics.add_bytes_read(file_slice.len());
                partition_response.records = file_slice.into();
            }
        }
        Err(err) => {
            debug!(%err,"Failed to read records for partition");
            partition_response.error_code = err;
        }
    }

    Ok(partition_response)
}
