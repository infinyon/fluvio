use std::io::Error as IoError;

use tracing::{trace, instrument};

use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetTopicResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_protocol::link::ErrorCode;

use crate::core::DefaultSharedGlobalContext;

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
