use log::{trace, debug};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};

use sc_api::topic::{FlvFetchTopicsRequest, FlvFetchTopicsResponse};
use sc_api::topic::FlvFetchTopicResponse;
use sc_api::topic::FlvPartitionReplica;
use flv_metadata::partition::ReplicaKey;

use crate::core::ShareLocalStores;
use crate::core::topics::TopicLocalStore;
use crate::core::partitions::PartitionLocalStore;

pub async fn handle_fetch_topics_request(
    request: RequestMessage<FlvFetchTopicsRequest>,
    metadata: ShareLocalStores,
) -> Result<ResponseMessage<FlvFetchTopicsResponse>, Error> {
    // is names is provided, return list, otherwise generate all names
    let topic_names = match &request.request.names {
        Some(topic_names) => topic_names.clone(),
        None => metadata.topics().all_keys(),
    };

    // encode topics
    let mut topics = vec![];
    for topic_name in &topic_names {
        let mut topic_response =
            topic_store_metadata_to_topic_response(metadata.topics(), topic_name);

        // lookup partitions, if topic was found
        let partitions = if topic_response.topic.is_some() {
            Some(partition_metadata_to_replica_response(
                metadata.partitions(),
                topic_name,
            ))
        } else {
            None
        };

        topic_response.update_partitions(partitions);

        // push valid and error topics
        topics.push(topic_response);
    }

    // prepare response
    let mut response = FlvFetchTopicsResponse::default();
    response.topics = topics;

    debug!("flv fetch topics resp: {} items", response.topics.len());
    trace!("flv fetch topics resp {:#?}", response);

    Ok(request.new_response(response))
}

/// Encode Topic metadata into a Topic FLV Reponse
pub fn topic_store_metadata_to_topic_response(
    topics: &TopicLocalStore,
    topic_name: &String,
) -> FlvFetchTopicResponse {
    if let Some(topic) = topics.topic(topic_name) {
        FlvFetchTopicResponse::new(
            topic_name.clone(),
            topic.spec.clone(),
            topic.status.clone(),
            None,
        )
    } else {
        FlvFetchTopicResponse::new_not_found(topic_name.clone())
    }
}

/// Encode partitions into a Replica Reponse
pub fn partition_metadata_to_replica_response(
    partitions: &PartitionLocalStore,
    topic: &String,
) -> Vec<FlvPartitionReplica> {
    let mut res: Vec<FlvPartitionReplica> = Vec::default();
    let partition_cnt = partitions.count_topic_partitions(topic);
    for idx in 0..partition_cnt {
        let name = ReplicaKey::new(topic.clone(), idx);
        if let Some(partition) = partitions.value(&name) {
            res.push(FlvPartitionReplica {
                id: idx,
                leader: partition.spec.leader,
                replicas: partition.spec.replicas.clone(),
                live_replicas: partition.status.live_replicas().clone(),
            })
        }
    }
    res
}
