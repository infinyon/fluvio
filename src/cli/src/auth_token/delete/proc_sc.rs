//!
//! # Fluvio SC - Delete Auth Token Processing
//!
//! Sends Delete Auth Token request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::auth_tokens::{FlvDeleteAuthTokensRequest, FlvDeleteAuthTokensResponse};

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use super::cli::DeleteAuthTokenConfig;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Delete AuthToken Request
pub fn process_sc_delete_auth_token<'a>(
    server_addr: SocketAddr,
    cfg: DeleteAuthTokenConfig,
) -> Result<(), CliError> {
    let auth_token_name = cfg.auth_token_name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("sending delete auth-token '{}': {}", auth_token_name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let auth_token_resp = &response.results[0];
                let response = handle_sc_response(
                    &auth_token_resp.name,
                    "auth-token",
                    "deleted",
                    "",
                    &auth_token_resp.error_code,
                    &auth_token_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("delete auth-token '{}': empty response", auth_token_name),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send delete request
async fn send_request_to_server(
    server_addr: SocketAddr,
    cfg: DeleteAuthTokenConfig,
) -> Result<FlvDeleteAuthTokensResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvDeleteAuthTokens, &versions);

    trace!("delete auth-token req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("delete auth-token res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode DeleteAuthToken in Fluvio format
fn encode_request(cfg: &DeleteAuthTokenConfig) -> FlvDeleteAuthTokensRequest {
    FlvDeleteAuthTokensRequest {
        auth_tokens: vec![cfg.auth_token_name.clone()],
    }
}
