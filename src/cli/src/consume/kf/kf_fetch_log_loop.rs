//!
//! # Kafka - Fetch Log (common APIs)
//!
//! Fetch APIs that are shared between
//!  * Fetch Topic All
//!  * Fetch Topic/Partition
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;

use log::debug;

use kf_protocol::message::fetch::DefaultKfFetchResponse;
use kf_protocol::message::offset::KfListOffsetResponse;
use kf_protocol::api::AllKfApiKey;
use kf_protocol::api::ErrorCode as KfErrorCode;

use futures::channel::mpsc;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::select;
use future_helper::sleep;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_get_api_versions;
use crate::common::kf_lookup_version;

use crate::consume::logs_output::ReponseLogParams;
use crate::consume::process_fetch_topic_reponse;

use super::query::FetchLogsParam;
use super::query::LeaderParam;
use super::query::kf_fetch_logs;
use super::query::kf_list_offsets;


// -----------------------------------
// Fetch Loop
// -----------------------------------

/// Fetch log continuously
pub async fn kf_fetch_log_loop(
    topic_name: String,
    max_bytes: i32,
    from_beginning: bool,
    mut leader_param: LeaderParam,
    response_params: ReponseLogParams,
    mut receiver: mpsc::Receiver<bool>,
) -> Result<(), CliError> {
    let mut conn = Connection::new(&leader_param.server_addr).await?;
    let vers = kf_get_api_versions(&mut conn).await?;
    let version = kf_lookup_version(AllKfApiKey::Fetch, &vers);

    // list offsets
    let list_offsets_res =
        kf_list_offsets(&mut conn, &topic_name, &leader_param, &vers).await?;
    let _ = update_leader_partition_offsets(&topic_name, &mut leader_param, &list_offsets_res)?;

    // initialize fetch log parameters
    let mut fetch_param = FetchLogsParam {
        topic: topic_name.clone(),
        max_bytes: max_bytes,
        partitions: leader_param.partitions.clone(),
    };

    // fetch logs - from beginning
    if from_beginning {
        let fetch_logs_res = kf_fetch_logs(&mut conn, version, &fetch_param).await?;
        let _ = update_fetch_log_offsets(&topic_name, &mut fetch_param, &fetch_logs_res, true)?;
    }

    loop {
        select! {
            _ = (sleep(Duration::from_millis(10))).fuse() => {
                // fetch logs
                let fetch_logs_res = kf_fetch_logs(&mut conn, version, &fetch_param).await?;

                // process logs response
                let _ = process_fetch_topic_reponse(&fetch_logs_res, &response_params)?;

                // update offsets
                let _ = update_fetch_log_offsets(&topic_name, &mut fetch_param, &fetch_logs_res, false)?;
            },
           receiver_req = receiver.next() => {
                debug!("<Ctrl-C>... replica {} exiting", leader_param.leader_id);
                println!("");
                return Ok(())
            }
        }
    }
}

// -----------------------------------
//  Conversions & Validations
// -----------------------------------

/// Update leader partition offsets
fn update_leader_partition_offsets(
    topic_name: &String,
    leader_param: &mut LeaderParam,
    list_offsets_res: &KfListOffsetResponse,
) -> Result<(), CliError> {
    for topic_res in &list_offsets_res.topics {
        // ensure valid topic
        if topic_res.name != *topic_name {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("list offsets: unknown topic '{}'", topic_name),
            )));
        }

        for partition_res in &topic_res.partitions {
            let partition_name = format!("{}/{}", topic_res.name, partition_res.partition_index);

            // validate partition response
            if partition_res.error_code != KfErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "partition offset '{}': {}",
                        partition_name,
                        partition_res.error_code.to_sentence()
                    ),
                )));
            }

            // update leader epoch & offsets in partitions
            for partition in &mut leader_param.partitions {
                if partition.partition_idx == partition_res.partition_index {
                    partition.epoch = partition_res.leader_epoch;
                    partition.offset = partition_res.offset;

                    debug!(
                        "list-offsets '{}' updated: epoch: {}, offset {}",
                        partition_name, partition.epoch, partition.offset
                    );
                }
            }
        }
    }

    Ok(())
}

/// Update fetch log offets by copying last_stabe_offset of the reponse to params offset
/// for each partition
fn update_fetch_log_offsets(
    topic_name: &String,
    fetch_logs_param: &mut FetchLogsParam,
    fetch_res: &DefaultKfFetchResponse,
    from_start: bool,
) -> Result<(), CliError> {
    if fetch_res.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("fetch: {}", fetch_res.error_code.to_sentence()),
        )));
    }

    // grab last stable offsets for each partition
    for topic_res in &fetch_res.topics {
        // ensure valid topic
        if topic_res.name != *topic_name {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("fetch: unknown topic '{}'", topic_name),
            )));
        }

        for partition_res in &topic_res.partitions {
            let partition_name = format!("{}/{}", topic_res.name, partition_res.partition_index);

            // validate partition response
            if partition_res.error_code != KfErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "fetch partition '{}': {}",
                        partition_name,
                        partition_res.error_code.to_sentence()
                    ),
                )));
            }

            // update epoch and offsets in partitions
            if from_start {
                for partition in &mut fetch_logs_param.partitions {
                    if partition.partition_idx == partition_res.partition_index
                        && partition.offset != partition_res.log_start_offset
                    {
                        partition.offset = partition_res.log_start_offset;

                        debug!(
                            "partition '{}' - updated offset {} (from start)",
                            partition_name, partition.offset
                        );
                    }
                }
            } else {
                for partition in &mut fetch_logs_param.partitions {
                    if partition.partition_idx == partition_res.partition_index
                        && partition.offset != partition_res.last_stable_offset
                    {
                        partition.offset = partition_res.last_stable_offset;

                        debug!(
                            "partition '{}' - updated offset {}",
                            partition_name, partition.offset
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
