//!
//! # Fluvio SC - Processing
//!
//! Sends Create Topic request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::topic::{FlvCreateTopicRequest, FlvCreateTopicsRequest, FlvCreateTopicsResponse};
use sc_api::topic::FlvTopicSpecMetadata;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use crate::topic::create::{CreateTopicConfig, ReplicaConfig};

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Create Topic Request
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
    match run_block_on(send_request_to_server(server_addr, topic_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send create topic '{}': {}", topic_name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let topic_resp = &response.results[0];
                let response = handle_sc_response(
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
                        "{}cannot create topic '{}': communication error",
                        prepend_validation, &topic_name
                    ),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send delete request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    topic_cfg: CreateTopicConfig,
) -> Result<FlvCreateTopicsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&topic_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvDeleteTopics, &versions);

    trace!("create topic req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("create topic res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode CreateTopicRequest in Fluvio format
fn encode_request(topic_cfg: &CreateTopicConfig) -> FlvCreateTopicsRequest {
    // generate Topic Create Metadata
    let topic_metadata = match &topic_cfg.replica {
        // Computed Replicas
        ReplicaConfig::Computed(partitions, replicas, ignore_rack) => {
            FlvTopicSpecMetadata::Computed((*partitions, *replicas as i32, *ignore_rack).into())
        }

        // Assigned (user defined)  Replicas
        ReplicaConfig::Assigned(partitions) => {
            FlvTopicSpecMetadata::Assigned(partitions.sc_encode().into())
        }
    };
    // generate topic request
    let create_topic_req = FlvCreateTopicRequest {
        name: topic_cfg.name.clone(),
        topic: topic_metadata,
    };

    // encode & return create topic request
    FlvCreateTopicsRequest {
        topics: vec![create_topic_req],
        validate_only: topic_cfg.validate_only,
    }
}
