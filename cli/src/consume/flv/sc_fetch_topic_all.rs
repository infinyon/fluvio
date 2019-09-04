//!
//! # Fluvio SC - Fetch logs from Topic and all Partitions
//!
//! Connect to Fluvio Streaming Controller, look-up SPU leaders for all topic/partitions,
//! connect to all SPU leaders and fetch logs continuously
//!
//! ## Conection 1 - Connect SC:
//!   * APIVersions
//!   * TopicComposition
//!
//! ## Connection 2 - Connect to each topic/partition leader SPU
//!   * APIVersions
//!   * FetchOffsets
//!   * FetchLogs - continuously fetch logs (10 ms)
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use ctrlc;
use log::debug;
use types::socket_helpers::host_port_to_socket_addr;

use sc_api::topic::FlvTopicCompositionResponse;
use sc_api::errors::FlvErrorCode;

use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use futures::future::join_all;
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

/// Generate future to consume logs from topic
pub fn sc_consume_log_from_topic(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    response_paramss: ReponseLogParams,
) -> Result<(), CliError> {
    run_block_on(process_consume_log_from_topic_all(
        server_addr,
        cfg,
        response_paramss,
    ))
}

/// Processing engine to consume logs one topic & and multiple partitions.
/// Step 1: Collection system information
///  * Lookup API versions,
///  * Request TopicComposition
/// Step 2: Create loop for continous log fetch
async fn process_consume_log_from_topic_all(
    sc_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let mut sc_conn = Connection::new(&sc_addr).await?;
    let sc_vers = sc_get_api_versions(&mut sc_conn).await?;
    debug!("consume topic '{}'", cfg.topic);

    // query topic composition
    let topic = &cfg.topic;
    let topic_comp_res = sc_get_topic_composition(&mut sc_conn, topic.clone(), &sc_vers).await?;
    let tp_params = composition_to_all_topic_partition_params(&topic_comp_res)?;

    // Generate futures for group heartbeat and fetch logs
    fetch_log_futures(
        cfg.max_bytes,
        cfg.from_beginning,
        cfg.continous,
        tp_params,
        response_params,
    )
    .await
}

/// Generate futures for to fetch-logs from all SPUs
async fn fetch_log_futures(
    max_bytes: i32,
    from_beginning: bool,
    continous: bool,
    tp_param: FlvTopicPartitionParam,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let mut send_channels = vec![];
    let mut fetch_log_futures = vec![];

    // group fetch channels
    for leader in &tp_param.leaders {
        let (sender, receiver) = mpsc::channel::<bool>(5);
        let topic_name = tp_param.topic_name.clone();
        send_channels.push(sender);
        fetch_log_futures.push(spu_fetch_log_loop(
            topic_name,
            max_bytes,
            from_beginning,
            continous,
            leader.clone(),
            response_params.clone(),
            receiver,
        ));
    }

    // attach all send channels to Ctrl-C event handler
    //  - all futures will exit exit on Ctrl-C event
    if let Err(err) = ctrlc::set_handler(move || {
        debug!("<Ctrl-C> received");
        send_ctrlc_signal(send_channels.clone());
    }) {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("CTRL-C handler: {}", err),
        )));
    }

    // spin-off all futures
    join_all(fetch_log_futures).await;

    Ok(())
}

// -----------------------------------
// Event Processing
// -----------------------------------

/// Send CTRL c signal to all channels in send array
fn send_ctrlc_signal(send_channels: Vec<Sender<bool>>) {
    let _ = run_block_on(async move {
        for mut send_channel in send_channels {
            send_channel.send(true).await.expect("should not fail");
        }
        Ok(()) as Result<(), ()>
    });
}

// -----------------------------------
//  Conversions & Validations
// -----------------------------------

/// Parse topic composition and generate Topic/Partition parameters
fn composition_to_all_topic_partition_params(
    topic_comp_resp: &FlvTopicCompositionResponse,
) -> Result<FlvTopicPartitionParam, CliError> {
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
    let topic_name = &topic_resp.name;
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
        // ensure valid partition
        if partition_resp.error_code != FlvErrorCode::None {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "partition '{}/{}': {}",
                    topic_name,
                    partition_resp.partition_idx,
                    partition_resp.error_code.to_sentence()
                ),
            )));
        }

        // find leader for this partition
        let mut leader: Option<&mut FlvLeaderParam> = None;
        for leader_param in &mut tp_param.leaders {
            if leader_param.leader_id == partition_resp.leader_id {
                leader = Some(leader_param);
                break;
            }
        }

        // generate leader
        if leader.is_none() {
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
                        partitions: vec![],
                    });

                    let len = tp_param.leaders.len();
                    leader = Some(&mut tp_param.leaders[len - 1]);
                    break;
                }
            }
        }

        if let Some(leader) = leader {
            // add partition to leader
            leader.partitions.push(FlvPartitionParam {
                partition_idx: partition_resp.partition_idx,
                offset: -1,
            });
        } else {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                "invalid topic composition",
            )));
        };
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

// ---------------------------------------
// Unit Tests
// ---------------------------------------

#[cfg(test)]
pub mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    use sc_api::topic::FetchTopicReponse;
    use sc_api::topic::FetchPartitionResponse;
    use sc_api::topic::FetchSpuReponse;

    /// create metadata
    fn create_topic_composition_response(leader_ids: Vec<i32>) -> FlvTopicCompositionResponse {
        let mut fetch_partitions: Vec<FetchPartitionResponse> = vec![];
        for (i, leader_id) in leader_ids.iter().enumerate() {
            fetch_partitions.push(FetchPartitionResponse {
                error_code: FlvErrorCode::None,
                partition_idx: i as i32,
                leader_id: *leader_id,
                replicas: vec![2, 3],
                live_replicas: vec![3, 2],
            });
        }

        FlvTopicCompositionResponse {
            spus: vec![
                FetchSpuReponse {
                    error_code: FlvErrorCode::None,
                    spu_id: 2,
                    host: "10.0.0.23".to_owned(),
                    port: 9093,
                },
                FetchSpuReponse {
                    error_code: FlvErrorCode::None,
                    spu_id: 3,
                    host: "10.0.0.23".to_owned(),
                    port: 9094,
                },
                FetchSpuReponse {
                    error_code: FlvErrorCode::None,
                    spu_id: 1,
                    host: "10.0.0.23".to_owned(),
                    port: 9092,
                },
            ],
            topics: vec![FetchTopicReponse {
                error_code: FlvErrorCode::None,
                name: "test2".to_owned(),
                partitions: fetch_partitions,
            }],
        }
    }

    #[test]
    fn test_composition_to_all_topic_partition_params() {
        let topic_comp = create_topic_composition_response(vec![3, 2, 3]);
        let result = composition_to_all_topic_partition_params(&topic_comp);

        let expected_result = FlvTopicPartitionParam {
            topic_name: "test2".to_owned(),
            leaders: vec![
                FlvLeaderParam {
                    leader_id: 3,
                    server_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 23)), 9094),
                    partitions: vec![
                        FlvPartitionParam {
                            partition_idx: 0,
                            offset: -1,
                        },
                        FlvPartitionParam {
                            partition_idx: 2,
                            offset: -1,
                        },
                    ],
                },
                FlvLeaderParam {
                    leader_id: 2,
                    server_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 23)), 9093),
                    partitions: vec![FlvPartitionParam {
                        partition_idx: 1,
                        offset: -1,
                    }],
                },
            ],
        };

        println!("found: {:#?}\nexpected: {:#?}", result, expected_result);
        assert_eq!(result.unwrap(), expected_result);
    }
}
