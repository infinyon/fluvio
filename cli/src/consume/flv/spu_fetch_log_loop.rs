//!
//! # SPU - Fetch Log
//!
//! Fetch logs from SPU
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;

use log::debug;
use log::trace;

use spu_api::SpuApiKey;
use spu_api::offsets::FlvFetchOffsetsResponse;

use spu_api::errors::FlvErrorCode;

use kf_protocol::api::ErrorCode as KfErrorCode;
use kf_protocol::message::fetch::DefaultKfFetchResponse;

use futures::channel::mpsc;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::select;
use future_helper::sleep;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::spu_get_api_versions;
use crate::common::spu_lookup_version;

use super::query::FlvFetchLogsParam;
use super::query::FlvLeaderParam;
use super::query::spu_fetch_logs;
use super::query::spu_fetch_offsets;

use crate::consume::logs_output::ReponseLogParams;
use crate::consume::process_fetch_topic_reponse;

// -----------------------------------
// SPU - Fetch Loop
// -----------------------------------

/// Fetch log continuously
pub async fn spu_fetch_log_loop(
    topic_name: String,
    max_bytes: i32,
    from_beginning: bool,
    continous: bool,
    mut leader_param: FlvLeaderParam,
    response_params: ReponseLogParams,
    mut receiver: mpsc::Receiver<bool>,
) -> Result<(), CliError> {
    let mut spu_conn = Connection::new(&leader_param.server_addr).await?;
    let vers = spu_get_api_versions(&mut spu_conn).await?;
    let version = spu_lookup_version(SpuApiKey::KfFetch, &vers);

    debug!("fetch loop version: {:#?}",version);

    // list offsets
    let list_offsets_res =
        spu_fetch_offsets(&mut spu_conn, &topic_name, &leader_param, &vers).await?;

    
    let _ = update_leader_partition_offsets(
        &topic_name,
        &mut leader_param,
        &list_offsets_res,
        from_beginning,
    )?;

    // initialize fetch log parameters
    let mut fetch_param = FlvFetchLogsParam {
        topic: topic_name.clone(),
        max_bytes: max_bytes,
        partitions: leader_param.partitions.clone(),
    };

    trace!("fetch param: {:#?}",fetch_param);

    let mut delay = 0;
    loop {
        select! {
            _ = (sleep(Duration::from_millis(delay))).fuse() => {

                debug!("start fetch loop");
                // fetch logs
                let fetch_logs_res = spu_fetch_logs(&mut spu_conn, version, &fetch_param).await?;

                // process logs response
                let _ = process_fetch_topic_reponse(&fetch_logs_res, &response_params)?;

                // update offsets
                let _ = update_fetch_log_offsets(&topic_name, &mut fetch_param, &fetch_logs_res)?;

                debug!("end fetch loop");

                if continous {
                    delay = 500;
                } else {
                    return Ok(())
                }
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
    leader_param: &mut FlvLeaderParam,
    list_offsets_res: &FlvFetchOffsetsResponse,
    from_start: bool,
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
            if partition_res.error_code != FlvErrorCode::None {
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
                    if from_start {
                        partition.offset = partition_res.start_offset;
                    } else {
                        partition.offset = partition_res.last_stable_offset;
                    }

                    debug!(
                        "list-offsets '{}' offset {}",
                        partition_name, partition.offset
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
    fetch_logs_param: &mut FlvFetchLogsParam,
    fetch_res: &DefaultKfFetchResponse,
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
    Ok(())
}
