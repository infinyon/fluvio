//!
//! # Kafka -- Offset Fetch
//!
//! Communicates with Kafka Controller to Fetch Offsets for topic/partitions
//!
use log::trace;

use kf_protocol::message::offset::{KfOffsetFetchRequest, KfOffsetFetchResponse};
use kf_protocol::message::offset::OffsetFetchRequestTopic;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

use super::query_params::TopicPartitionParam;

// Query Group Coordinator for offsets.
pub async fn kf_offsets_fetch<'a>(
    conn: &'a mut Connection,
    group_id: &'a String,
    topic_name: &'a String,
    tp_param: &'a TopicPartitionParam,
    versions: &'a KfApiVersions,
) -> Result<KfOffsetFetchResponse, CliError> {
    let mut request = KfOffsetFetchRequest::default();
    let version = kf_lookup_version(AllKfApiKey::OffsetFetch, versions);

    // collect partition indexes
    let mut partition_indexes: Vec<i32> = vec![];
    for leader in &tp_param.leaders {
        for partition in &leader.partitions {
            partition_indexes.push(partition.partition_idx);
        }
    }

    // topics
    let topics = vec![OffsetFetchRequestTopic {
        name: topic_name.clone(),
        partition_indexes: partition_indexes,
    }];

    // request
    request.group_id = group_id.clone();
    request.topics = Some(topics);

    trace!("offset-fetch req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("offset-fetch res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
