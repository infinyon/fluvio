//!
//! # Spu Fetch Logs
//!
//! Connects to server and fetches logs
//!

use log::{debug, trace};

use kf_protocol::message::fetch::{DefaultKfFetchRequest, DefaultKfFetchResponse};
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::{KfFetchRequest, FetchableTopic};
use kf_protocol::api::Isolation;

use crate::error::CliError;
use crate::common::Connection;

use super::query_params::FlvFetchLogsParam;

/// Fetch log records from a target server
pub async fn spu_fetch_logs<'a>(
    conn: &'a mut Connection,
    version: Option<i16>,
    fetch_log_param: &'a FlvFetchLogsParam,
) -> Result<DefaultKfFetchResponse, CliError> {
    let mut fetch_partitions = vec![];
    for partition_param in &fetch_log_param.partitions {
        let mut fetch_part = FetchPartition::default();
        fetch_part.partition_index = partition_param.partition_idx;
        fetch_part.current_leader_epoch = -1;
        fetch_part.fetch_offset = partition_param.offset;
        fetch_part.log_start_offset = -1;
        fetch_part.max_bytes = fetch_log_param.max_bytes;

        fetch_partitions.push(fetch_part);
    }

    let mut topic_request = FetchableTopic::default();
    topic_request.name = fetch_log_param.topic.clone();
    topic_request.fetch_partitions = fetch_partitions;

    let mut request: DefaultKfFetchRequest = KfFetchRequest::default();
    request.replica_id = -1;
    request.max_wait = 500;
    request.min_bytes = 1;
    request.max_bytes = fetch_log_param.max_bytes;
    request.isolation_level = Isolation::ReadCommitted;
    request.session_id = 0;
    request.epoch = -1;
    request.topics.push(topic_request);

    debug!(
        "fetch logs '{}' ({}) partition to {}",
        fetch_log_param.topic,
        fetch_log_param.partitions.len(),
        conn.server_addr()
    );
    trace!("fetch logs req {:#?}", request);

    let response = conn.send_request(request, version).await?;

    trace!("fetch logs res: {:#?}", response);
    Ok(response)
}
