use std::io::Error as IoError;

use log::trace;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use spu_api::offsets::FlvFetchOffsetsRequest;
use spu_api::offsets::FetchOffsetTopicResponse;
use spu_api::offsets::FlvFetchOffsetsResponse;
use spu_api::offsets::FetchOffsetPartitionResponse;
use flv_metadata::partition::ReplicaKey;
use kf_protocol::api::FlvErrorCode;
use flv_storage::ReplicaStorage;

use crate::core::DefaultSharedGlobalContext;

pub async fn handle_offset_request(
    req_msg: RequestMessage<FlvFetchOffsetsRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FlvFetchOffsetsResponse>, IoError> {
    let request = req_msg.request();
    trace!("handling flv fetch request: {:#?}", request);

    let mut response = FlvFetchOffsetsResponse::default();

    for topic_request in &request.topics {
        let topic = &topic_request.name;

        let mut topic_response = FetchOffsetTopicResponse::default();
        topic_response.name = topic.clone();

        for partition_req in &topic_request.partitions {
            let partition = &partition_req.partition_index;
            let mut partition_response = FetchOffsetPartitionResponse::default();
            partition_response.partition_index = *partition;
            let rep_id = ReplicaKey::new(topic.clone(), *partition);
            if let Some(replica) = ctx.leaders_state().get_replica(&rep_id) {
                trace!("offset fetch request for replica found: {}", rep_id);
                let storage = replica.storage();
                partition_response.error_code = FlvErrorCode::None;
                partition_response.start_offset = storage.get_log_start_offset();
                partition_response.last_stable_offset = storage.get_hw();
            } else {
                trace!("offset fetch request is not found: {}", rep_id);
                partition_response.error_code = FlvErrorCode::PartitionNotLeader;
            }

            topic_response.partitions.push(partition_response);
        }

        response.topics.push(topic_response);
    }

    Ok(req_msg.new_response(response))
}
