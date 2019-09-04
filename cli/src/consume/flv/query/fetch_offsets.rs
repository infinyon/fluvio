//!
//! # Fluvio -- Fetch Offsets form SPU Leader
//!
//! Communicates with SPU Replica Leader to Fetch Offsets for topic/partitions
//!
use log::trace;

use spu_api::offsets::{FlvFetchOffsetsRequest, FlvFetchOffsetsResponse};
use spu_api::offsets::FetchOffsetTopic;
use spu_api::offsets::FetchOffsetPartition;
use spu_api::versions::ApiVersions;
use spu_api::SpuApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::spu_lookup_version;

use super::query_params::FlvLeaderParam;

// Query SPU Replica leader for offsets.
pub async fn spu_fetch_offsets<'a>(
    conn: &'a mut Connection,
    topic_name: &'a String,
    leader: &'a FlvLeaderParam,
    versions: &'a ApiVersions,
) -> Result<FlvFetchOffsetsResponse, CliError> {
    let mut request = FlvFetchOffsetsRequest::default();
    let version = spu_lookup_version(SpuApiKey::FlvFetchOffsets, versions);

    // collect partition index & epoch information from leader
    let mut offset_partitions: Vec<FetchOffsetPartition> = vec![];
    for partition in &leader.partitions {
        offset_partitions.push(FetchOffsetPartition {
            partition_index: partition.partition_idx,
        });
    }

    // update request
    request.topics = vec![FetchOffsetTopic {
        name: topic_name.clone(),
        partitions: offset_partitions,
    }];

    trace!("fetch-offsets req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!(
        "fetch-offsets res '{}': {:#?}",
        conn.server_addr(),
        response
    );

    Ok(response)
}
