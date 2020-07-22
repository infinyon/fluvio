use log::trace;
use std::io::Error;

use flv_types::Name;

use kf_protocol::message::metadata::{KfMetadataRequest, KfMetadataResponse};
use kf_protocol::message::metadata::MetadataResponseTopic;
use kf_protocol::message::metadata::MetadataResponseBroker;
use kf_protocol::message::metadata::MetadataResponsePartition;

use kf_protocol::api::ErrorCode as KfErrorCode;
use kf_protocol::api::{RequestMessage, ResponseMessage};

use crate::core::SharedContext;
use crate::stores::topic::*;
use crate::stores::spu::*;
use crate::stores::partition::*;

pub async fn handle_kf_metadata_request(
    request: RequestMessage<KfMetadataRequest>,
    metadata: SharedContext,
) -> Result<ResponseMessage<KfMetadataResponse>, Error> {
    // generate broker metadata (from online spus)
    let spus = metadata.spus().online_spus();
    let resp_brokers = flv_online_spus_to_kf_brokers(&spus);

    // generate topics
    let mut resp_topics: Vec<MetadataResponseTopic> = Vec::default();
    if let Some(topics_req) = &request.request.topics {
        // lookup specific topics
        for topic_req in topics_req {
            resp_topics.push(make_kf_topic_response(&topic_req.name, metadata.topics()));
        }
    } else {
        // generate all "ok" topics
        for topic_name in metadata.topics().all_keys() {
            resp_topics.push(make_kf_topic_response(&topic_name, metadata.topics()));
        }
    }

    // generate partitions for all valid topics
    for idx in 0..resp_topics.len() {
        let mut topic = &mut resp_topics[idx];
        if topic.error_code.is_error() {
            continue;
        }

        // append partitions
        topic.partitions = topic_partitions_to_kf_partitions(metadata.partitions(), &topic.name);
    }

    // prepare response
    let mut response = KfMetadataResponse::default();
    response.brokers = resp_brokers;
    response.topics = resp_topics;

    trace!("kf-metadata resp {:#?}", response);

    Ok(request.new_response(response))
}

/// Given a topic name, generate Topic Response
fn make_kf_topic_response(name: &Name, topics: &TopicLocalStore) -> MetadataResponseTopic {
    let mut topic_resp = MetadataResponseTopic::default();
    topic_resp.name = name.clone();

    if let Some(flv_topic) = topics.topic(&name) {
        if !flv_topic.is_provisioned() {
            topic_resp.error_code = KfErrorCode::UnknownTopicOrPartition;
        }
    } else {
        topic_resp.error_code = KfErrorCode::UnknownTopicOrPartition;
    }

    topic_resp
}

/// Convert online SPUs to Kafka Brokers
fn flv_online_spus_to_kf_brokers(online_spus: &Vec<SpuKV>) -> Vec<MetadataResponseBroker> {
    online_spus
        .iter()
        .map(|online_spu| {
            let public_ep = online_spu.public_endpoint();

            MetadataResponseBroker {
                node_id: *online_spu.id(),
                host: public_ep.host_string(),
                port: public_ep.port as i32,
                rack: online_spu.rack_clone(),
            }
        })
        .collect()
}

/// Encode all partitions for a topic in Kf format.
pub fn topic_partitions_to_kf_partitions(
    partitions: &PartitionLocalStore,
    topic: &String,
) -> Vec<MetadataResponsePartition> {
    let mut kf_partitions = vec![];

    for (idx, partition) in partitions.topic_partitions(topic).iter().enumerate() {
        kf_partitions.push(MetadataResponsePartition {
            error_code: KfErrorCode::None,
            partition_index: idx as i32,
            leader_id: partition.spec.leader,
            leader_epoch: 0,
            replica_nodes: partition.spec.replicas.clone(),
            isr_nodes: partition.status.live_replicas().clone(),
            offline_replicas: partition.status.offline_replicas(),
        })
    }

    kf_partitions
}
