//!
//! # Kafka - Fetch logs from Topic and all Partitions
//!
//! Kafka creates a group to listen to all partitions.
//!
//! ## Conection 1 - Connect any broker:
//!   * APIVersions
//!   * Metadata
//!   * GroupCoordinator - lookup Broker hosting coordinator
//!
//! ## Connection 2 - Connect to group coordinator:
//!   * APIVersions
//!   * JoinGroup - get server assigned member-id
//!   * JoinGroup - join group membership
//!   * SyncGroup
//!   * OffsetFetch
//!   * HeartBeat - continuously send hearbeat (3 sec)
//!
//! ## Connection 3 - Connect to each topic/partition leader
//!   * APIVersions
//!   * ListOffsets
//!   * Fetch - continuously fetch logs (10 ms)
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

use ctrlc;
use log::debug;
use utils::generators::generate_group_id;

use kf_protocol::message::group::KfFindCoordinatorResponse;
use kf_protocol::message::group::KfJoinGroupResponse;
use kf_protocol::message::group::KfSyncGroupResponse;
use kf_protocol::message::offset::KfOffsetFetchResponse;
use kf_protocol::message::metadata::KfMetadataResponse;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;
use kf_protocol::api::ErrorCode as KfErrorCode;

use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use futures::select;
use futures::future::join_all;
use futures::future::join;
use flv_future_core::run_block_on;
use flv_future_core::sleep;

use crate::error::CliError;
use flv_client::profile::ProfileConfig;

use crate::consume::ConsumeLogConfig;
use crate::consume::ResponseLogParams;

use super::query::TopicPartitionParam;
use super::query::LeaderParam;
use super::query::PartitionParam;
use super::query::kf_group_coordinator;
use super::query::kf_offsets_fetch;
use super::query::kf_join_group;
use super::query::kf_sync_group;
use super::query::kf_leave_group;
use super::query::kf_send_heartbeat;

use super::kf_fetch_log_loop::kf_fetch_log_loop;

// -----------------------------------
//  Kafka - Process Request
// -----------------------------------

/// Lookup group coordinator
pub fn kf_consume_log_from_topic(
    server_addr: SocketAddr,
    cfg: ConsumeLogConfig,
    response_paramss: ResponseLogParams,
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
///  * Request metadata
///  * Fetch group coordinator
/// Step 2: Create loop for Group keep-alives
/// Step 3: Create loop for continuous log fetch
async fn process_consume_log_from_topic_all(
    kf_ctrl_addr: String,
    cfg: ConsumeLogConfig,
    response_params: ResponseLogParams,
) -> Result<(), CliError> {
    let mut brk_conn = Connection::new(kf_ctrl_addr.clone()).await?;
    let bkr_vers = kf_get_api_versions(&mut brk_conn).await?;
    debug!("consume topic '{}'", cfg.topic);

    // query metadata for topics
    let query_topics = Some(vec![cfg.topic.clone()]);
    let metadata = query_kf_metadata(&mut brk_conn, query_topics, &bkr_vers).await?;
    let mut tp_params = metadata_to_all_topic_partition_params(&metadata)?;

    // generate group-id
    let grp_id = generate_group_id();
    debug!("group id: '{}'", grp_id);

    // query group coordinator
    let grp_coordinator = kf_group_coordinator(&mut brk_conn, &grp_id, &bkr_vers).await?;
    let coordinator_addr = group_coordinator_to_socket_addr(&grp_coordinator)?;

    // create connection to the group coordinator
    let mut gc_conn = Connection::new(coordinator_addr.clone()).await?;
    let gc_vers = kf_get_api_versions(&mut gc_conn).await?;

    // join group coordinator (to get member id)
    let empty_id = "".to_owned();
    let join_group_res =
        kf_join_group(&mut gc_conn, &cfg.topic, &grp_id, &empty_id, &gc_vers).await?;
    let mbr_id = join_group_to_member_id(&join_group_res)?;

    // join group
    let join_group_res =
        kf_join_group(&mut gc_conn, &cfg.topic, &grp_id, &mbr_id, &gc_vers).await?;
    let gen_id = join_group_to_generation_id(&join_group_res)?;

    // sync group
    let sync_group_res =
        kf_sync_group(&mut gc_conn, &cfg.topic, &grp_id, &mbr_id, gen_id, &gc_vers).await?;
    sync_group_response_validate(&sync_group_res)?;

    // offsets fetch
    let offsets_fetch_res =
        kf_offsets_fetch(&mut gc_conn, &grp_id, &cfg.topic, &tp_params, &gc_vers).await?;
    let _ = update_topic_partition_params_offsets(&mut tp_params, &offsets_fetch_res);

    // Generate futures for group heartbeat and fetch logs
    group_and_fetch_log_futures(
        gc_conn,
        gc_vers,
        grp_id,
        mbr_id,
        gen_id,
        cfg.max_bytes,
        cfg.from_beginning,
        tp_params,
        response_params,
    )
    .await?;

    Ok(())
}

/// Generate futures for group keep-alive and fetch-logs
async fn group_and_fetch_log_futures(
    conn: Connection,
    vers: KfApiVersions,
    grp_id: String,
    mbr_id: String,
    gen_id: i32,
    max_bytes: i32,
    from_beginning: bool,
    tp_param: TopicPartitionParam,
    response_params: ResponseLogParams,
) -> Result<(), CliError> {
    let mut send_channels = vec![];
    let mut fetch_log_futures = vec![];

    // group heartbeat channel
    let (sender, receiver) = mpsc::channel::<bool>(5);
    send_channels.push(sender);
    for leader in &tp_param.leaders {
        let (sender, receiver) = mpsc::channel::<bool>(5);
        let topic_name = tp_param.topic_name.clone();
        send_channels.push(sender);
        fetch_log_futures.push(kf_fetch_log_loop(
            topic_name,
            max_bytes,
            from_beginning,
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
    let _r = join(
        group_heartbeat_loop(conn, vers, grp_id, mbr_id, gen_id, receiver),
        join_all(fetch_log_futures),
    )
    .await;

    Ok(())
}

// -----------------------------------
// Event Processing
// -----------------------------------

/// Send Group keepalive heartbeat at regilar intervals
/// Leave group on Ctrl-C
async fn group_heartbeat_loop(
    mut conn: Connection,
    vers: KfApiVersions,
    grp_id: String,
    mbr_id: String,
    gen_id: i32,
    mut receiver: mpsc::Receiver<bool>,
) -> Result<(), CliError> {
    let heartbeat_ver = kf_lookup_version(AllKfApiKey::Heartbeat, &vers);

    loop {
        select! {
            _ = (sleep(Duration::from_secs(3))).fuse() => {
                if let Err(err) = kf_send_heartbeat(
                    &mut conn,
                    &grp_id,
                    &mbr_id,
                    gen_id,
                    heartbeat_ver,
                )
                .await {
                    return Err(CliError::IoError(IoError::new(
                        ErrorKind::InvalidData,
                        format!("healthcheck failed: {}", err),
                    )));
                }
            },
            receiver_req = receiver.next() => {
                kf_leave_group(
                    &mut conn,
                    &grp_id,
                    &mbr_id,
                    &vers,
                )
                .await?;
                debug!("<Ctrl-C>... heartbeat exiting");
                return Ok(())
            }
        }
    }
}

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

/// Parse metadata parameters and generate Topic/Partition parameters
fn metadata_to_all_topic_partition_params(
    metadata_resp: &KfMetadataResponse,
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

    // generate topic/partition parameter object
    let mut tp_param = TopicPartitionParam {
        topic_name: topic_resp.name.clone(),
        leaders: vec![],
    };

    // traverse all partitions and look-up leaders
    for partition_resp in &topic_resp.partitions {
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

        // find leader for this partition
        let mut leader: Option<&mut LeaderParam> = None;
        for leader_param in &mut tp_param.leaders {
            if leader_param.leader_id == partition_resp.leader_id {
                leader = Some(leader_param);
                break;
            }
        }

        // create leader for this partition
        if leader.is_none() {
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
            leader.partitions.push(PartitionParam {
                partition_idx: partition_resp.partition_index,
                epoch: partition_resp.leader_epoch,
                offset: -1,
            });
        } else {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                "invalid metadata",
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

/// Parse group coordinator response and generate Server Address
fn group_coordinator_to_socket_addr(
    coordinator_resp: &KfFindCoordinatorResponse,
) -> Result<String, CliError> {
    if coordinator_resp.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "find group coordinator: {}",
                coordinator_resp.error_code.to_sentence()
            ),
        )));
    }

    let server_addr = ProfileConfig::host_port_to_socket_addr(&format!(
        "{}:{}",
        coordinator_resp.host, coordinator_resp.port
    ))?;

    debug!("group coordinator host/port: '{}'", server_addr);

    Ok(server_addr)
}

/// Parse join group response for member-id
fn join_group_to_member_id(join_group_resp: &KfJoinGroupResponse) -> Result<String, CliError> {
    if join_group_resp.error_code != KfErrorCode::None
        && join_group_resp.error_code != KfErrorCode::MemberIdRequired
    {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("join group: {}", join_group_resp.error_code.to_sentence()),
        )));
    }
    debug!("member-id: '{}'", join_group_resp.member_id);

    Ok(join_group_resp.member_id.clone())
}

/// Parse join group response for generation-id
fn join_group_to_generation_id(join_group_resp: &KfJoinGroupResponse) -> Result<i32, CliError> {
    if join_group_resp.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("join group: {}", join_group_resp.error_code.to_sentence()),
        )));
    }
    debug!("generation-id: '{}'", join_group_resp.generation_id);

    Ok(join_group_resp.generation_id)
}

/// Validate sync group response
fn sync_group_response_validate(sync_group_resp: &KfSyncGroupResponse) -> Result<(), CliError> {
    if sync_group_resp.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("join group: {}", sync_group_resp.error_code.to_sentence()),
        )));
    }
    debug!("sync group: ok");

    Ok(())
}

/// Update topic partition offsets from offset fetch response
fn update_topic_partition_params_offsets(
    tp_param: &mut TopicPartitionParam,
    offsets_fetch_res: &KfOffsetFetchResponse,
) -> Result<(), CliError> {
    if offsets_fetch_res.error_code != KfErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "offset fetch: {}",
                offsets_fetch_res.error_code.to_sentence()
            ),
        )));
    }

    for topic_res in &offsets_fetch_res.topics {
        // ensure valid topic
        if topic_res.name != tp_param.topic_name {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("offset fetch: unknown topic '{}'", topic_res.name),
            )));
        }

        for partition_res in &topic_res.partitions {
            let partition_name = format!("{}/{}", topic_res.name, partition_res.partition_index);

            // validate partition response
            if partition_res.error_code != KfErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "partition '{}': {}",
                        partition_name,
                        partition_res.error_code.to_sentence()
                    ),
                )));
            }

            // update epoch & offsets in partitions
            for leader in &mut tp_param.leaders {
                for partition in &mut leader.partitions {
                    if partition.partition_idx == partition_res.partition_index {
                        partition.epoch = partition_res.committed_leader_epoch;
                        partition.offset = partition_res.committed_offset;

                        debug!(
                            "fetch-offsets '{}' updated: epoch: {}, offset {}",
                            partition_name, partition.epoch, partition.offset
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------
// Unit Tests
// ---------------------------------------

#[cfg(test)]
pub mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    use kf_protocol::message::metadata::MetadataResponseBroker;
    use kf_protocol::message::metadata::MetadataResponseTopic;
    use kf_protocol::message::metadata::MetadataResponsePartition;

    /// create metadata
    fn create_metadata_response(leader_ids: Vec<i32>) -> KfMetadataResponse {
        let mut metadata_partitions: Vec<MetadataResponsePartition> = vec![];
        for (i, leader_id) in leader_ids.iter().enumerate() {
            metadata_partitions.push(MetadataResponsePartition {
                error_code: KfErrorCode::None,
                partition_index: i as i32,
                leader_id: *leader_id,
                leader_epoch: 14,
                replica_nodes: vec![2, 3],
                isr_nodes: vec![3, 2],
                offline_replicas: vec![],
            });
        }

        KfMetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 2,
                    host: "10.0.0.23".to_owned(),
                    port: 9093,
                    rack: None,
                },
                MetadataResponseBroker {
                    node_id: 3,
                    host: "10.0.0.23".to_owned(),
                    port: 9094,
                    rack: None,
                },
                MetadataResponseBroker {
                    node_id: 1,
                    host: "10.0.0.23".to_owned(),
                    port: 9092,
                    rack: None,
                },
            ],
            cluster_id: Some("RcFjJ4hKTDK5fhMC3g-AqQ".to_owned()),
            controller_id: 1,
            topics: vec![MetadataResponseTopic {
                error_code: KfErrorCode::None,
                name: "test2".to_owned(),
                is_internal: false,
                partitions: metadata_partitions,
            }],
        }
    }

    #[test]
    fn test_metadata_to_all_topic_partition_params() {
        let metadata = create_metadata_response(vec![3, 2, 3]);
        let result = metadata_to_all_topic_partition_params(&metadata);

        let expected_result = TopicPartitionParam {
            topic_name: "test2".to_owned(),
            leaders: vec![
                LeaderParam {
                    leader_id: 3,
                    server_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 23)), 9094),
                    partitions: vec![
                        PartitionParam {
                            partition_idx: 0,
                            offset: -1,
                            epoch: 14,
                        },
                        PartitionParam {
                            partition_idx: 2,
                            offset: -1,
                            epoch: 14,
                        },
                    ],
                },
                LeaderParam {
                    leader_id: 2,
                    server_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 23)), 9093),
                    partitions: vec![PartitionParam {
                        partition_idx: 1,
                        offset: -1,
                        epoch: 14,
                    }],
                },
            ],
        };

        println!("found: {:#?}\nexpected: {:#?}", result, expected_result);
        assert_eq!(result.unwrap(), expected_result);
    }
}
