//!
//! # Kafka - Leader for Topic
//!
//! Given Kafka Controller address, it sends metadata request to retrieve all brokers/partitions.
//! It is reponsibility of the caller to compute the Leader.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;
use types::socket_helpers::host_port_to_socket_addr;

use kf_protocol::message::KfApiVersions;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::query_kf_metadata;

/// Find address of the Broker leader for a topic/partition
pub async fn find_broker_leader_for_topic_partition<'a>(
    conn: &'a mut Connection,
    topic: String,
    partition: i32,
    versions: &'a KfApiVersions
) -> Result<SocketAddr, CliError> {
    let kf_metadata = query_kf_metadata(conn, Some(vec![topic.clone()]), versions).await?;

    let brokers = &kf_metadata.brokers;
    let topics = &kf_metadata.topics;
    for response_topic in topics {
        if response_topic.name == topic {
            for response_partition in &response_topic.partitions {
                if response_partition.partition_index == partition {
                    let leader_id = response_partition.leader_id;

                    // traverse brokers and find leader
                    for broker in brokers {
                        if broker.node_id == leader_id {
                            trace!("broker {}/{} is leader", broker.host, broker.port);
                            return host_port_to_socket_addr(&broker.host, broker.port as u16)
                                .map_err(|err| err.into());
                        }
                    }
                }
            }
        }
    }

    Err(CliError::IoError(IoError::new(
        ErrorKind::Other,
        format!(
            "topic '{}/{}': unknown topic or partition",
            topic, partition
        ),
    )))
}
