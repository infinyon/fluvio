use log::{trace, debug};
use std::io::Error;

use kf_protocol::api::{RequestMessage, ResponseMessage};
use sc_api::topic::FlvTopicCompositionRequest;
use sc_api::topic::FlvTopicCompositionResponse;
use sc_api::topic::FetchTopicReponse;
use sc_api::topic::FetchPartitionResponse;
use sc_api::topic::FetchSpuReponse;
use kf_protocol::api::FlvErrorCode;

use crate::core::ShareLocalStores;

pub async fn handle_topic_composition_request(
    request: RequestMessage<FlvTopicCompositionRequest>,
    metadata: ShareLocalStores,
) -> Result<ResponseMessage<FlvTopicCompositionResponse>, Error> {
    let mut topic_comp_resp = FlvTopicCompositionResponse::default();
    let mut spu_ids = vec![];

    debug!(
        "topic-composition, encode topics '{:?}'",
        request.request.topic_names
    );

    // encode topics
    let mut topics = vec![];
    for topic_name in &request.request.topic_names {
        let mut topic = FetchTopicReponse::default();
        topic.name = topic_name.clone();

        // if topic is found encode it, otherwise error
        if let Some(topic_metadata) = metadata.topics().topic(topic_name) {
            // check topic resolution, return error if not OK
            let topic_status = topic_metadata.status();
            if !topic_status.is_resolution_provisioned() {
                let error_code = if topic_status.is_resolution_transient() {
                    FlvErrorCode::TopicPendingInitialization
                } else {
                    FlvErrorCode::TopicInvalidConfiguration
                };

                // add error and save
                topic.error_code = error_code;
                topics.push(topic);
            } else {
                // update partitions
                let mut partitions = vec![];
                let partitions_mtd = metadata.partitions().topic_partitions(topic_name);
                for (idx, partition_mtd) in partitions_mtd.iter().enumerate() {
                    let mut partition_response = FetchPartitionResponse::default();
                    partition_response.partition_idx = idx as i32;

                    // partitions pending initializations return error
                    if partition_mtd.spec.leader < 0 {
                        // add pending init error
                        partition_response.error_code =
                            FlvErrorCode::PartitionPendingInitialization;
                    } else {
                        // update partition with metadata
                        partition_response.leader_id = partition_mtd.spec.leader.clone();
                        partition_response.replicas = partition_mtd.spec.replicas.clone();
                        partition_response.live_replicas = partition_mtd.status.live_replicas().clone();
                    }

                    partitions.push(partition_response);
                }

                // collect SPUs ids (if unique)
                spu_ids = append_vals_unique(spu_ids, topic_status.spus_in_replica());

                // add partitions and save
                topic.partitions = partitions;
                topics.push(topic);
            }
        } else {
            // add not found error and save
            topic.error_code = FlvErrorCode::TopicNotFound;
            topics.push(topic);
        }
    }

    debug!("topic-composition, encode spus '{:?}'", spu_ids);

    // encode spus
    let mut spus = vec![];
    for spu_id in &spu_ids {
        let mut spu = FetchSpuReponse::default();
        spu.spu_id = *spu_id;

        // if spu is found encode it, otherwise error
        if let Some(spu_metadata) = metadata.spus().get_by_id(spu_id) {
            // check spu resolution, return error if not OK
            let spu_status = spu_metadata.status();
            if !spu_status.is_online() {
                // add error and save
                spu.error_code = FlvErrorCode::SpuOffline;
                spus.push(spu);
            } else {
                // update spu with metadata and save
                let public_ep = spu_metadata.public_endpoint();
                spu.host = public_ep.host.clone();
                spu.port = public_ep.port;

                spus.push(spu);
            }
        } else {
            // add not found error and save
            spu.error_code = FlvErrorCode::SpuNotFound;
            spus.push(spu);
        }
    }

    // update reponse
    topic_comp_resp.topics = topics;
    topic_comp_resp.spus = spus;

    trace!("topic-composition resp {:#?}", topic_comp_resp);

    Ok(request.new_response(topic_comp_resp))
}

/// append value if unique
fn append_vals_unique<T>(mut existing_list: Vec<T>, new_list: Vec<T>) -> Vec<T>
where
    T: PartialEq + Clone,
{
    for new_id in &new_list {
        if !existing_list.contains(new_id) {
            existing_list.push(new_id.clone());
        }
    }
    existing_list
}

// -----------------------------------
//  Unit Tests
// -----------------------------------
#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_append_vals_unique() {
        let mut existing_spu_ids: Vec<i32> = vec![2, 1];
        let new_spu_ids: Vec<i32> = vec![1, 3];
        let new_spu_ids2: Vec<i32> = vec![2, 3, 4];

        existing_spu_ids = append_vals_unique(existing_spu_ids, new_spu_ids.clone());
        assert_eq!(existing_spu_ids, vec![2, 1, 3]);

        existing_spu_ids = append_vals_unique(existing_spu_ids, new_spu_ids2);
        assert_eq!(existing_spu_ids, vec![2, 1, 3, 4]);

        existing_spu_ids = append_vals_unique(existing_spu_ids, new_spu_ids);
        assert_eq!(existing_spu_ids, vec![2, 1, 3, 4]);

        existing_spu_ids = append_vals_unique(existing_spu_ids, vec![]);
        assert_eq!(existing_spu_ids, vec![2, 1, 3, 4]);
    }
}
