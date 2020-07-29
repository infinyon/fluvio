use std::io::Error;

use log::warn;
use log::trace;
use log::error;

use kf_protocol::api::ErrorCode;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::KfProduceResponse;
use kf_protocol::message::produce::TopicProduceResponse;
use kf_protocol::message::produce::PartitionProduceResponse;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use flv_metadata_cluster::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;

pub async fn handle_produce_request(
    request: RequestMessage<DefaultKfProduceRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<KfProduceResponse>, Error> {
    let (header, produce_request) = request.get_header_request();
    trace!("handling produce request: {:#?}", produce_request);

    let mut response = KfProduceResponse::default();

    //let ack = produce_request.acks;

    for topic_request in produce_request.topics {
        let topic = &topic_request.name;
        trace!("handling produce request for topic{}", topic);

        let mut topic_response = TopicProduceResponse::default();
        topic_response.name = topic.to_owned();

        for partition_request in topic_request.partitions {
            let rep_id = ReplicaKey::new(topic.clone(), partition_request.partition_index);

            trace!("handling produce request for replia: {}", rep_id);

            let mut partition_response = PartitionProduceResponse::default();
            partition_response.partition_index = rep_id.partition;

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
                    partition_response.error_code = ErrorCode::KafkaStorageError;
                }
            }

            topic_response.partitions.push(partition_response);
        }

        response.responses.push(topic_response);
    }

    trace!("produce request completed");

    Ok(RequestMessage::<DefaultKfProduceRequest>::response_with_header(&header, response))
}
