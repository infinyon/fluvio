//!
//! # Kafka - Fetch logs from Topic / Partition
//!
//! Connect to any Kafka server, identify leading Brooker and fetch logs.
//!
//! ## Conection 1 - Connect any broker:
//!   * APIVersions
//!   * Metadata
//!
//! ## Connection 2 - Connect to topic/partition leader
//!   * APIVersions
//!   * ListOffsets
//!   * Fetch - continuously fetch logs (10 ms)
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use ctrlc;
use log::debug;

use kf_protocol::message::metadata::KfMetadataResponse;
use kf_protocol::api::ErrorCode as KfErrorCode;

use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_get_api_versions;
use crate::common::query_kf_metadata;

use crate::profile::ProfileConfig;
use crate::consume::ConsumeLogConfig;
use crate::consume::ReponseLogParams;

use super::query::TopicPartitionParam;
use super::query::LeaderParam;
use super::query::PartitionParam;
use super::kf_fetch_log_loop::kf_fetch_log_loop;

// -----------------------------------
//  Kafka - Process Request
// -----------------------------------

// Create execution block to produce log message
pub fn kf_consume_log_from_topic_partition(
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

//// Processing engine to consume logs from one multiple topic & partition.
/// Step 1: Collection system information
///  * Lookup API versions,
///  * Request metadata
/// Step 2: Create loop for continous log fetch
async fn process_log_from_topic_partition(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    partition: i32,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let topic = &cfg.topic;
    let mut brk_conn = Connection::new(&server_addr).await?;
    let bkr_vers = kf_get_api_versions(&mut brk_conn).await?;
    debug!("consume topic '{}'", topic);

    // query metadata for topics
    let query_topics = Some(vec![topic.clone()]);
    let metadata = query_kf_metadata(&mut brk_conn, query_topics, &bkr_vers).await?;
    let tp_params = metadata_to_topic_partition_params(&metadata, topic, partition)?;

    // Generate future for continuous fetch-log
    fetch_log_future(
        cfg.max_bytes,
        cfg.from_beginning,
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
    tp_param: TopicPartitionParam,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
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
    let _ = kf_fetch_log_loop(
        topic_name,
        max_bytes,
        from_beginning,
        leader.clone(),
        response_params.clone(),
        receiver,
    )
    .await;

    Ok(())
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

/// Parse metadata parameters and generate Topic/Partition parameters
fn metadata_to_topic_partition_params(
    metadata_resp: &KfMetadataResponse,
    topic: &String,
    partition: i32,
) -> Result<TopicPartitionParam, CliError> {
    // there must be one topic in reply
    if metadata_resp.topics.len() != 1 {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("expected 1 topic, found {}", metadata_resp.topics.len()),
        )));
    }

    // check for errors
    let topic_resp = &metadata_resp.topics[0];
    if topic_resp.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("metadata response: {}", topic_resp.error_code.to_sentence()),
        )));
    }

    // ensure correct topic
    if topic_resp.name != *topic {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("expected topic '{}', found '{}'", topic, topic_resp.name),
        )));
    }

    // generate topic/partition parameter object
    let mut tp_param = TopicPartitionParam {
        topic_name: topic.clone(),
        leaders: vec![],
    };

    // find partition
    for partition_resp in &topic_resp.partitions {
        if partition_resp.partition_index == partition {
            // ensure valid partition
            if partition_resp.error_code != KfErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "partition '{}/{}': {}",
                        topic_resp.name,
                        partition_resp.partition_index,
                        partition_resp.error_code.to_sentence()
                    ),
                )));
            }

            // generate leader
            for broker_resp in &metadata_resp.brokers {
                if broker_resp.node_id == partition_resp.leader_id {
                    let server_addr = ProfileConfig::host_port_to_socket_addr(&format!(
                        "{}:{}",
                        broker_resp.host, broker_resp.port
                    ))?;

                    tp_param.leaders.push(LeaderParam {
                        leader_id: broker_resp.node_id,
                        server_addr: server_addr,
                        partitions: vec![PartitionParam {
                            partition_idx: partition_resp.partition_index,
                            epoch: partition_resp.leader_epoch,
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
