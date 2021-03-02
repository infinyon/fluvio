use std::io::Error as IoError;

use fluvio_storage::ReplicaStorage;
use tracing::trace;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetTopicResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use dataplane::ErrorCode;


use crate::core::DefaultSharedGlobalContext;

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
            if let Some(replica) = ctx.leaders_state().get(&rep_id) {
                trace!("offset fetch request for replica found: {}", rep_id);
                let reader = replica.read().await;
                let storage = reader.storage();
                partition_response.error_code = ErrorCode::None;
                partition_response.start_offset = storage.get_log_start_offset();
                partition_response.last_stable_offset = storage.get_hw();
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
