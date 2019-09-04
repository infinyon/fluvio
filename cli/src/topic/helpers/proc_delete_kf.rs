//!
//! # Kafka - Delete Topic Processing
//!
//! Sends Delete Topic request to Kafka Controller
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::trace;

use kf_protocol::message::topic::{KfDeleteTopicsRequest, KfDeleteTopicsResponse};
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;
use future_helper::run_block_on;

use types::defaults::KF_REQUEST_TIMEOUT_MS;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::handle_kf_response;
use crate::common::{kf_lookup_version, kf_get_api_versions};

use crate::topic::delete::DeleteTopicConfig;

// -----------------------------------
//  Kafka - Process Request
// -----------------------------------

// Connect to Kafka Controller and process Delete Topic Request
pub fn process_delete_topic<'a>(
    server_addr: SocketAddr,
    topic_cfg: DeleteTopicConfig,
) -> Result<(), CliError> {
    let topic_name = topic_cfg.name.clone();

    // Run command and collect results
    match run_block_on(get_version_and_delete_topic(server_addr, topic_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("sending delete topic '{}': {}", topic_name, err),
        ))),
        Ok(response) => {
            // print errors
            if response.responses.len() > 0 {
                let topic_resp = &response.responses[0];
                let response = handle_kf_response(
                    &topic_resp.name,
                    "topic",
                    "deleted",
                    "",
                    &topic_resp.error_code,
                    &None,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("delete topic '{}': empty response", &topic_name),
                )))
            }
        }
    }
}

// Connect to Kafka server, get version and send request
async fn get_version_and_delete_topic(
    server_addr: SocketAddr,
    topic_cfg: DeleteTopicConfig,
) -> Result<KfDeleteTopicsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let versions = kf_get_api_versions(&mut conn).await?;

    send_request_to_server(&mut conn, topic_cfg, &versions).await
}

/// Send delete topic request to Kafka server
async fn send_request_to_server<'a>(
    conn: &'a mut Connection,
    topic_cfg: DeleteTopicConfig,
    versions: &'a KfApiVersions,
) -> Result<KfDeleteTopicsResponse, CliError> {
    let request = encode_request(&topic_cfg);
    let version = kf_lookup_version(AllKfApiKey::DeleteTopics, versions);

    trace!("delete topic req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("delete topic res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}

/// encode DeleteTopicRequest in Kafka format
fn encode_request(topic_cfg: &DeleteTopicConfig) -> KfDeleteTopicsRequest {
    KfDeleteTopicsRequest {
        topic_names: vec![topic_cfg.name.clone()],
        timeout_ms: KF_REQUEST_TIMEOUT_MS,
    }
}
