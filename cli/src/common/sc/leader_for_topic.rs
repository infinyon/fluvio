//!
//! # Fluvio SC - Leader for Topic
//!
//! Given Fluvio Streaming Controller address, find all SPU replicas for a topic /partition.
//! It is reponsibility of the caller to compute the Leader.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::debug;

use types::socket_helpers::host_port_to_socket_addr;

use sc_api::versions::ApiVersions;
use sc_api::errors::FlvErrorCode;

use crate::common::Connection;
use crate::error::CliError;
use crate::common::sc::sc_get_topic_composition;

/// Find address of the SPU leader for a topic/partition
pub async fn find_spu_leader_for_topic_partition<'a>(
    conn: &'a mut Connection,
    topic: String,
    partition: i32,
    versions: &'a ApiVersions,
) -> Result<SocketAddr, CliError> {
    let topic_comp_resp = sc_get_topic_composition(conn, topic.clone(), versions).await?;
    let topics_resp = &topic_comp_resp.topics;
    let spus_resp = &topic_comp_resp.spus;

    // there must be one topic in reply
    if topics_resp.len() != 1 {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "topic error: expected 1 topic, found {}",
                topics_resp.len()
            ),
        )));
    }

    // check for errors
    let topic_resp = &topics_resp[0];
    if topic_resp.error_code != FlvErrorCode::None {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "topic error: {}",
                topic_resp.error_code.to_sentence()
            ),
        )));
    }
    // lookup leader
    for partition_resp in &topic_resp.partitions {
        if partition_resp.partition_idx == partition {
            // check for errors
            if partition_resp.error_code != FlvErrorCode::None {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "topic-composition partition error: {}",
                        topic_resp.error_code.to_sentence()
                    ),
                )));
            }

            // traverse spus and find leader
            let leader_id = partition_resp.leader_id;
            for spu_resp in spus_resp {
                if spu_resp.spu_id == leader_id {
                    // check for errors
                    if spu_resp.error_code != FlvErrorCode::None {
                        return Err(CliError::IoError(IoError::new(
                            ErrorKind::InvalidData,
                            format!(
                                "topic-composition spu error: {}",
                                topic_resp.error_code.to_sentence()
                            ),
                        )));
                    }

                    let host = &spu_resp.host;
                    let port = &spu_resp.port;

                    debug!("spu {}/{}: is leader", host, port);
                    return host_port_to_socket_addr(host, *port)
                        .map(|addr| {
                            debug!("resolved spu leader: {}",addr);
                            addr
                        })
                        .map_err(|err| err.into())
                }
            }
        }
    }

    Err(CliError::IoError(IoError::new(
        ErrorKind::Other,
        format!(
            "topic-composition '{}/{}': unknown topic or partition",
            topic, partition
        ),
    )))
}
