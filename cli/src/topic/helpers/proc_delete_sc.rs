//!
//! # Fluvio SC - Delete Topic Processing
//!
//! Sends Delete Topic request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::topic::{FlvDeleteTopicsRequest, FlvDeleteTopicsResponse};

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use crate::topic::delete::DeleteTopicConfig;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Delete Topic Request
pub fn process_delete_topic<'a>(
    server_addr: SocketAddr,
    topic_cfg: DeleteTopicConfig,
) -> Result<(), CliError> {
    let topic_name = topic_cfg.name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, topic_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("sending delete topic '{}': {}", topic_name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let topic_resp = &response.results[0];
                let response = handle_sc_response(
                    &topic_resp.name,
                    "topic",
                    "deleted",
                    "",
                    &topic_resp.error_code,
                    &topic_resp.error_message,
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

/// Connect to server, get version, and send delete request
async fn send_request_to_server(
    server_addr: SocketAddr,
    topic_cfg: DeleteTopicConfig,
) -> Result<FlvDeleteTopicsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&topic_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvDeleteTopics, &versions);

    trace!("delete topic req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("delete topic res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode DeleteTopicRequest in Fluvio format
fn encode_request(topic_cfg: &DeleteTopicConfig) -> FlvDeleteTopicsRequest {
    FlvDeleteTopicsRequest {
        topics: vec![topic_cfg.name.clone()],
    }
}
