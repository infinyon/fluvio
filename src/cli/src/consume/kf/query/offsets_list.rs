//!
//! # Kafka -- Offset List
//!
//! Communicates with Kafka Replica Leader to List Offsets for topic/partitions
//!
use log::trace;

use kf_protocol::message::offset::{KfListOffsetRequest, KfListOffsetResponse};
use kf_protocol::message::offset::ListOffsetTopic;
use kf_protocol::message::offset::ListOffsetPartition;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

use super::query_params::LeaderParam;

// Query Replica leader for offsets.
pub async fn kf_list_offsets<'a>(
    conn: &'a mut Connection,
    topic_name: &'a String,
    leader: &'a LeaderParam,
    versions: &'a KfApiVersions,
) -> Result<KfListOffsetResponse, CliError> {
    let mut request = KfListOffsetRequest::default();
    let version = kf_lookup_version(AllKfApiKey::ListOffsets, versions);

    // collect partition index & epoch information from leader
    let mut offset_partitions: Vec<ListOffsetPartition> = vec![];
    for partition in &leader.partitions {
        offset_partitions.push(ListOffsetPartition {
            partition_index: partition.partition_idx,
            current_leader_epoch: partition.epoch,
            timestamp: -1,
        });
    }

    // update request
    request.replica_id = -1;
    request.topics = vec![ListOffsetTopic {
        name: topic_name.clone(),
        partitions: offset_partitions,
    }];

    trace!("list-offsets req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("list-offsets res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
