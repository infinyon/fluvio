use std::io::Error;

use tracing::warn;
use tracing::trace;
use tracing::error;

use dataplane::ErrorCode;
use dataplane::produce::{
    DefaultProduceRequest, ProduceResponse, TopicProduceResponse, PartitionProduceResponse,
};
use dataplane::api::RequestMessage;
use dataplane::api::ResponseMessage;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;


pub async fn handle_produce_request(
    request: RequestMessage<DefaultProduceRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<ProduceResponse>, Error> {
    let (header, produce_request) = request.get_header_request();
    trace!("handling produce request: {:#?}", produce_request);

    let mut response = ProduceResponse::default();

    //let ack = produce_request.acks;

    for topic_request in produce_request.topics {
        let topic = &topic_request.name;
        trace!("handling produce request for topic{}", topic);

        let mut topic_response = TopicProduceResponse {
            name: topic.to_owned(),
            ..Default::default()
        };

        for partition_request in topic_request.partitions {
            let rep_id = ReplicaKey::new(topic.clone(), partition_request.partition_index);

            trace!("handling produce request for replia: {}", rep_id);

            let mut partition_response = PartitionProduceResponse {
                partition_index: rep_id.partition,
                ..Default::default()
            };

            match ctx
                .leaders_state()
                .send_records(&rep_id, partition_request.records, true)
                .await
            {
                Ok(found_flag) => {
                    if found_flag {
                        trace!("records has successfull processed for: {}", rep_id);
                        partition_response.error_code = ErrorCode::None;
                    } else {
                        warn!("no replica found: {}", rep_id);
                        partition_response.error_code = ErrorCode::NotLeaderForPartition;
                    }
                }
                Err(err) => {
                    error!("error: {:#?} writing to replica: {}", err, rep_id);
                    partition_response.error_code = ErrorCode::StorageError;
                }
            }

            topic_response.partitions.push(partition_response);
        }

        response.responses.push(topic_response);
    }

    trace!("produce request completed");

    Ok(RequestMessage::<DefaultProduceRequest>::response_with_header(&header, response))
}
