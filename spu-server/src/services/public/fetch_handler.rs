use log::trace;

use kf_socket::KfSink;
use kf_socket::KfSocketError;
use kf_protocol::api::RequestMessage;
use metadata::partition::ReplicaKey;
use kf_socket::FileFetchResponse;
use kf_socket::KfFileFetchRequest;
use kf_socket::FilePartitionResponse;
use kf_socket::FileTopicResponse;

use crate::core::DefaultSharedGlobalContext;


pub async fn handle_fetch_request(
    request: RequestMessage<KfFileFetchRequest>,
    ctx: DefaultSharedGlobalContext,
    sink: &mut KfSink,
) -> Result<(), KfSocketError> {
    let (header, fetch_request) = request.get_header_request();
 
    let mut fetch_response = FileFetchResponse::default();

    for topic_request in &fetch_request.topics {
        let topic = &topic_request.name;

        let mut topic_response = FileTopicResponse::default();
        topic_response.name = topic.clone();

        for partition_req in &topic_request.fetch_partitions {
            let partition = &partition_req.partition_index;
            let fetch_offset = partition_req.fetch_offset;
            let rep_id = ReplicaKey::new(topic.clone(), *partition);
            let mut partition_response = FilePartitionResponse::default();
            partition_response.partition_index = *partition;

            ctx.leaders_state().read_records(
                &rep_id,
                fetch_offset,
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
    trace!("sending back file fetch response: {:#?}",response);
    sink.encode_file_slices(&response, header.api_version())
        .await?;
    trace!("finish sending fetch response");

    Ok(())
}
