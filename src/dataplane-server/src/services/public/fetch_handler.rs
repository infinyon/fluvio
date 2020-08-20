use tracing::trace;
use tracing::debug;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use kf_socket::InnerKfSink;
use kf_socket::KfSocketError;
use kf_protocol::api::RequestMessage;
use flv_metadata_cluster::partition::ReplicaKey;
use kf_protocol::fs::FileFetchResponse;
use kf_protocol::fs::KfFileFetchRequest;
use kf_protocol::fs::FilePartitionResponse;
use kf_protocol::fs::FileTopicResponse;
use flv_future_aio::zero_copy::ZeroCopyWrite;

use crate::core::DefaultSharedGlobalContext;

/// perform log fetch request using zero copy write
pub async fn handle_fetch_request<S>(
    request: RequestMessage<KfFileFetchRequest>,
    ctx: DefaultSharedGlobalContext,
    sink: &mut InnerKfSink<S>,
) -> Result<(), KfSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    InnerKfSink<S>: ZeroCopyWrite,
{
    let (header, fetch_request) = request.get_header_request();
    let mut fetch_response = FileFetchResponse::default();

    for topic_request in &fetch_request.topics {
        let topic = &topic_request.name;

        let mut topic_response = FileTopicResponse::default();
        topic_response.name = topic.clone();

        for partition_req in &topic_request.fetch_partitions {
            let partition = &partition_req.partition_index;
            debug!(
                "fetch log: {}-{}, max_bytes: {}",
                topic, partition, fetch_request.max_bytes
            );
            let fetch_offset = partition_req.fetch_offset;
            let rep_id = ReplicaKey::new(topic.clone(), *partition);
            let mut partition_response = FilePartitionResponse::default();
            partition_response.partition_index = *partition;

            ctx.leaders_state()
                .read_records(
                    &rep_id,
                    fetch_offset,
                    fetch_request.max_bytes as u32,
                    fetch_request.isolation_level.clone(),
                    &mut partition_response,
                )
                .await;

            topic_response.partitions.push(partition_response);
        }

        fetch_response.topics.push(topic_response);
    }

    let response =
        RequestMessage::<KfFileFetchRequest>::response_with_header(&header, fetch_response);
    trace!("sending back file fetch response: {:#?}", response);
    sink.encode_file_slices(&response, header.api_version())
        .await?;
    trace!("finish sending fetch response");

    Ok(())
}
