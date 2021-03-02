use std::io::Error;

use fluvio_storage::StorageError;
use tracing::{ debug, trace,error };

use dataplane::ErrorCode;
use dataplane::produce::{
    DefaultProduceRequest, ProduceResponse, TopicProduceResponse, PartitionProduceResponse,
};
use dataplane::api::RequestMessage;
use dataplane::api::ResponseMessage;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::DefaultSharedGlobalContext;
use crate::InternalServerError;

pub async fn handle_produce_request(
    request: RequestMessage<DefaultProduceRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<ProduceResponse>, Error> {
    let (header, produce_request) = request.get_header_request();
    trace!("handling produce request: {:#?}", produce_request);

    let mut response = ProduceResponse::default();

    //let ack = produce_request.acks;

    for topic_request in produce_request.topics.into_iter() {
        let topic = &topic_request.name;
        trace!("handling produce request for topic{}", topic);

        let mut topic_response = TopicProduceResponse {
            name: topic.to_owned(),
            ..Default::default()
        };

        for partition_request in topic_request.partitions.into_iter() {
            let rep_id = ReplicaKey::new(topic.clone(), partition_request.partition_index);

            trace!("handling produce request for replia: {}", rep_id);

            let mut partition_response = PartitionProduceResponse {
                partition_index: rep_id.partition,
                ..Default::default()
            };

            if let Some(leader_state) = ctx
            .leaders_state()
            .get_mut(&rep_id) {

                match leader_state
                    .write_record_set(partition_request.records)
                    .await
                    {
                        Ok(_) => {
                            partition_response.error_code = ErrorCode::None;
                        }
                        Err(err) => {
                            error!("error: {:#?} writing to replica: {}", err, rep_id);
                            match err {
                                InternalServerError::StorageError(storage_err)
                                    if matches!(storage_err, StorageError::BatchTooBig(_)) =>
                                {
                                    partition_response.error_code = ErrorCode::MessageTooLarge
                                }
                                _ => {
                                    partition_response.error_code = ErrorCode::StorageError;
                                }
                            }
                        }
                    }
            
            } else {
                debug!(%rep_id,"no replica found");
                partition_response.error_code = ErrorCode::NotLeaderForPartition;
            }

            topic_response.partitions.push(partition_response);
        }

        response.responses.push(topic_response);
    }

    trace!("produce request completed");

    Ok(RequestMessage::<DefaultProduceRequest>::response_with_header(&header, response))
}
