//!
//! # Fluvio SPU - Fetch logs from Topic / Partition
//!
//! Connect to Fluvio Streaming Processing Unit, fetch logs (if leader)
//!
//! ## Connect to SPU
//!   * APIVersions
//!   * FetchLocalSPU
//!   * FetchOffsets
//!   * FetchLogs - continuously fetch logs (10 ms)
//!

use std::io::ErrorKind;
use std::io::Error as IoError;
use std::net::SocketAddr;

use ctrlc;
use log::debug;

use spu_api::spus::FlvFetchLocalSpuResponse;
use spu_api::errors::FlvErrorCode;

use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::spu_get_api_versions;

use crate::consume::ConsumeLogConfig;
use crate::consume::ReponseLogParams;

use super::query::FlvTopicPartitionParam;
use super::query::FlvLeaderParam;
use super::query::FlvPartitionParam;
use super::query::spu_fetch_local_spu;

use super::spu_fetch_log_loop::spu_fetch_log_loop;

// -----------------------------------
//  Fluvio SPU - Process Request
// -----------------------------------

// Create execution block to consume log messages
pub fn spu_consume_log_from_topic_partition(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    partition: i32,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    run_block_on(process_log_from_topic_partition(
        server_addr,
        cfg,
        partition,
        response_params,
    ))
}

/// Processing engine to consume logs from one multiple topic & partition.
/// Step 1: Collection system information
///  * Lookup API versions,
///  * Fetch Local SPU
/// Step 2: Create loop for continous log fetch
async fn process_log_from_topic_partition(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    partition: i32,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let mut spu_conn = Connection::new(&server_addr).await?;
    let spu_vers = spu_get_api_versions(&mut spu_conn).await?;
    debug!("consume topic: {}, partition: {}", cfg.topic,partition);

    // query topic composition
    let topic = &cfg.topic;
    let local_spu_res = spu_fetch_local_spu(&mut spu_conn, &spu_vers).await?;
    let tp_params =
        local_spu_to_topic_partition_params(&local_spu_res, &server_addr, topic, partition)?;

    // Generate future for continuous fetch-log
    fetch_log_future(
        cfg.max_bytes,
        cfg.from_beginning,
        cfg.continous,
        tp_params,
        response_params,
    )
    .await?;

    Ok(())
}

/// Generate futures for fetch-log and link with CTRL-C
async fn fetch_log_future(
    max_bytes: i32,
    from_beginning: bool,
    continous: bool,
    tp_param: FlvTopicPartitionParam,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {

    debug!("fetch log future");
    // ensure noly 1 leader
    if tp_param.leaders.len() != 1 {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("expected 1 leader, found {}", tp_param.leaders.len()),
        )));
    }
    let leader = &tp_param.leaders[0];
    let topic_name = tp_param.topic_name.clone();

    // fetch-log channel
    let (sender, receiver) = mpsc::channel::<bool>(5);

    // attach sender to Ctrl-C event handler
    if let Err(err) = ctrlc::set_handler(move || {
        debug!("<Ctrl-C> received");
        send_ctrlc_signal(sender.clone());
    }) {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("CTRL-C handler: {}", err),
        )));
    }

    // spin-off fetch log future
    spu_fetch_log_loop(
        topic_name,
        max_bytes,
        from_beginning,
        continous,
        leader.clone(),
        response_params.clone(),
        receiver,
    )
    .await
}

/// Send CTRL c signal to all channels in send array
fn send_ctrlc_signal(mut sender: Sender<bool>) {
    let _ = run_block_on(async move {
        sender.send(true).await.expect("should not fail");
        Ok(()) as Result<(), ()>
    });
}

// -----------------------------------
//  Conversions & Validations
// -----------------------------------

/// Parse local spu response and generate Topic/Partition parameters
fn local_spu_to_topic_partition_params(
    local_spu_resp: &FlvFetchLocalSpuResponse,
    server_addr: &SocketAddr,
    topic_name: &String,
    partition: i32,
) -> Result<FlvTopicPartitionParam, CliError> {

    debug!("local spu to topic partition name");
    
    // check for errors
    if local_spu_resp.error_code != FlvErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "topic '{}' local-spu error: {}",
                topic_name,
                local_spu_resp.error_code.to_sentence()
            ),
        )));
    }

    // generate topic/partition parameter object
    let tp_param = FlvTopicPartitionParam {
        topic_name: topic_name.clone(),
        leaders: vec![FlvLeaderParam {
            leader_id: local_spu_resp.id,
            server_addr: server_addr.clone(),
            partitions: vec![FlvPartitionParam {
                partition_idx: partition,
                offset: -1,
            }],
        }],
    };

    debug!("topic-partition parameters {:#?}", tp_param);

    Ok(tp_param)
}
