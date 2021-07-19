use tracing::{debug, trace, instrument};

use fluvio_socket::ExclusiveFlvSink;
use fluvio_socket::FlvSocketError;
use fluvio_protocol::api::RequestMessage;
use dataplane::ErrorCode;
use dataplane::fetch::{FileFetchResponse, FileFetchRequest, FilePartitionResponse, FileTopicResponse};
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;

/// perform log fetch request using zero copy write
#[instrument(skip(request, ctx, sink))]
pub async fn handle_fetch_request(
    request: RequestMessage<FileFetchRequest>,
    ctx: DefaultSharedGlobalContext,
    sink: ExclusiveFlvSink,
) -> Result<(), FlvSocketError> {
    let (header, fetch_request) = request.get_header_request();
    let mut fetch_response = FileFetchResponse::default();

    for topic_request in &fetch_request.topics {
        let topic = &topic_request.name;

        let mut topic_response = FileTopicResponse {
            name: topic.clone(),
            ..Default::default()
        };

        for partition_req in &topic_request.fetch_partitions {
            let partition = &partition_req.partition_index;
            debug!(
                "fetch log: {}-{}, max_bytes: {}",
                topic, partition, fetch_request.max_bytes
            );
            let fetch_offset = partition_req.fetch_offset;
            let rep_id = ReplicaKey::new(topic.clone(), *partition);
            let mut partition_response = FilePartitionResponse {
                partition_index: *partition,
                ..Default::default()
            };

            if let Some(leader) = ctx.leaders_state().get(&rep_id) {
                leader
                    .read_records(
                        fetch_offset,
                        fetch_request.max_bytes as u32,
                        fetch_request.isolation_level.clone(),
                        &mut partition_response,
                    )
                    .await;
            } else {
                partition_response.error_code = ErrorCode::NotLeaderForPartition;
            }

            topic_response.partitions.push(partition_response);
        }

        fetch_response.topics.push(topic_response);
    }

    let response =
        RequestMessage::<FileFetchRequest>::response_with_header(&header, fetch_response);
    trace!("sending back file fetch response: {:#?}", response);
    let mut inner = sink.lock().await;
    inner
        .encode_file_slices(&response, header.api_version())
        .await?;
    drop(inner);
    trace!("finish sending fetch response");

    Ok(())
}
