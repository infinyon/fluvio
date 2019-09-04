//!
//! # Kafka - Processing
//!
//! Sends Create Topic request to Kafka Controller
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::trace;
use types::defaults::KF_REQUEST_TIMEOUT_MS;

use kf_protocol::message::topic::CreatableTopic;
use kf_protocol::message::topic::{KfCreateTopicsRequest, KfCreateTopicsResponse};
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::handle_kf_response;
use crate::common::{kf_lookup_version, kf_get_api_versions};

use crate::topic::create::{CreateTopicConfig, ReplicaConfig};

// -----------------------------------
//  Kafka - Process Request
// -----------------------------------

// Connect to Kafka Controller and process Create Topic Request
pub fn process_create_topic(
    server_addr: SocketAddr,
    topic_cfg: CreateTopicConfig,
) -> Result<(), CliError> {
    let topic_name = topic_cfg.name.clone();
    let prepend_validation = if topic_cfg.validate_only {
        "(validation-only) "
    } else {
        ""
    };

    // Run command and collect results
    match run_block_on(get_version_and_create_topic(server_addr, topic_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send create topic '{}' to kf: {}", topic_name, err),
        ))),
        Ok(response) => {
            if response.topics.len() > 0 {
                let topic_resp = &response.topics[0];
                let response = handle_kf_response(
                    &topic_resp.name,
                    "topic",
                    "created",
                    prepend_validation,
                    &topic_resp.error_code,
                    &topic_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!(
                        "{}create topic '{}': empty response",
                        prepend_validation, &topic_name
                    ),
                )))
            }
        }
    }
}

// Connect to Kafka server, get version and send request
async fn get_version_and_create_topic(
    server_addr: SocketAddr,
    topic_cfg: CreateTopicConfig,
) -> Result<KfCreateTopicsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let versions = kf_get_api_versions(&mut conn).await?;

    send_request_to_server(&mut conn, topic_cfg, &versions).await
}

/// Send create topic request to Kafka server
async fn send_request_to_server<'a>(
    conn: &'a mut Connection,
    topic_cfg: CreateTopicConfig,
    versions: &'a KfApiVersions,
) -> Result<KfCreateTopicsResponse, CliError> {
    let request = encode_request(&topic_cfg);
    let version = kf_lookup_version(AllKfApiKey::CreateTopics, versions);

    trace!("create topic req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("create topic res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}

/// encode CreateTopicRequest in Kafka format
fn encode_request(topic_cfg: &CreateTopicConfig) -> KfCreateTopicsRequest {
    // create topic request
    let topic_request = match &topic_cfg.replica {
        // Computed Replicas
        ReplicaConfig::Computed(partitions, replicas, _) => CreatableTopic {
            name: topic_cfg.name.clone(),
            num_partitions: *partitions,
            replication_factor: *replicas,
            assignments: vec![],
            configs: vec![],
        },

        // Assigned (user defined)  Replicas
        ReplicaConfig::Assigned(partitions) => CreatableTopic {
            name: topic_cfg.name.clone(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: partitions.kf_encode(),
            configs: vec![],
        },
    };

    // encode topic request
    KfCreateTopicsRequest {
        topics: vec![topic_request],
        timeout_ms: KF_REQUEST_TIMEOUT_MS,
        validate_only: topic_cfg.validate_only,
    }
}
