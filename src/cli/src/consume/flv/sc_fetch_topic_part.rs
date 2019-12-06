//!
//! # Fluvio SC - Fetch logs from Topic / Partition
//!
//! Connect to Fluvio Streaming Controller, identify leading SPU and fetch logs.
//!
//! ## Conection 1 - Connect SC:
//!   * APIVersions
//!   * TopicComposition
//!
//! ## Connection 2 - Connect to topic/partition SPU leader
//!   * APIVersions
//!   * FetchOffsets
//!   * FetchLogs - continuously fetch logs (10 ms)
//!

use std::io::ErrorKind;
use std::io::Error as IoError;
use std::net::SocketAddr;

use ctrlc;
use log::debug;
use types::socket_helpers::host_port_to_socket_addr;

use sc_api::topic::FlvTopicCompositionResponse;
use sc_api::errors::FlvErrorCode;

use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_get_topic_composition;

use crate::consume::ConsumeLogConfig;
use crate::consume::ReponseLogParams;

use super::query::FlvTopicPartitionParam;
use super::query::FlvLeaderParam;
use super::query::FlvPartitionParam;

use super::spu_fetch_log_loop::spu_fetch_log_loop;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Create execution block to consume log messages
pub fn sc_consume_log_from_topic_partition(
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

/// Processing engine to consume logs from one topic & partition.
/// Step 1: Collection system information
///  * Lookup API versions,
///  * Request TopicComposition
/// Step 2: Create loop for continous log fetch
async fn process_log_from_topic_partition(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    partition: i32,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let mut sc_conn = Connection::new(&server_addr).await?;
    let sc_vers = sc_get_api_versions(&mut sc_conn).await?;
    debug!("consume topic '{}'", cfg.topic);

    // query topic composition
    let topic = &cfg.topic;
    let topic_comp_res = sc_get_topic_composition(&mut sc_conn, topic.clone(), &sc_vers).await?;
    let tp_params = composition_to_topic_partition_params(&topic_comp_res, topic, partition)?;

    // Generate future for continuous fetch-log
    fetch_log_future(
        cfg.max_bytes,
        cfg.from_beginning,
        cfg.continous,
        tp_params,
        response_params,
    )
    .await
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

/// Parse topic composition and generate Topic/Partition parameters
fn composition_to_topic_partition_params(
    topic_comp_resp: &FlvTopicCompositionResponse,
    topic_name: &String,
    partition: i32,
) -> Result<FlvTopicPartitionParam, CliError> {

    debug!("composition to topic partion param");
    let topics_resp = &topic_comp_resp.topics;

    // there must be one topic in reply
    if topics_resp.len() != 1 {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("expected 1 topic, found {}", topics_resp.len()),
        )));
    }

    // check for errors
    let topic_resp = &topics_resp[0];
    if topic_resp.error_code != FlvErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "topic-composition topic '{}' error: {}",
                topic_name,
                topic_resp.error_code.to_sentence()
            ),
        )));
    }

    // generate topic/partition parameter object
    let mut tp_param = FlvTopicPartitionParam {
        topic_name: topic_name.clone(),
        leaders: vec![],
    };

    // find partition
    for partition_resp in &topic_resp.partitions {
        if partition_resp.partition_idx == partition {
            // ensure valid partition
            if partition_resp.error_code != FlvErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "partition '{}/{}': {}",
                        topic_name,
                        partition,
                        partition_resp.error_code.to_sentence()
                    ),
                )));
            }

            // generate leader
            for spu_resp in &topic_comp_resp.spus {
                if spu_resp.spu_id == partition_resp.leader_id {
                    let host = &spu_resp.host;
                    let port = &spu_resp.port;

                    debug!("resolve {}:{}", spu_resp.host, spu_resp.port);
                    let server_addr = host_port_to_socket_addr(host, *port)
                        .map_err(|err| {
                            CliError::IoError(IoError::new(
                                ErrorKind::InvalidData,
                                format!("cannot resolve '{}:{}': {}", host, port, err),
                            ))
                        })
                        .unwrap();

                    tp_param.leaders.push(FlvLeaderParam {
                        leader_id: spu_resp.spu_id,
                        server_addr: server_addr,
                        partitions: vec![FlvPartitionParam {
                            partition_idx: partition_resp.partition_idx,
                            offset: -1,
                        }],
                    });

                    break;
                }
            }
        }
    }

    // there must be at least one topic generated
    if tp_param.leaders.len() == 0 {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            "error generating topic/partitions",
        )));
    }

    debug!("topic-partition parameters {:#?}", tp_param);

    Ok(tp_param)
}
