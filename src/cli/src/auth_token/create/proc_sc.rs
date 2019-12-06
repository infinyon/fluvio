//!
//! # Fluvio SC - Processing
//!
//! Sends Create Auth Token request to Fluvio Streaming Controller
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::auth_tokens::{FlvCreateAuthTokensRequest, FlvCreateAuthTokensResponse};
use sc_api::auth_tokens::{CreateAuthTokenRequest, AuthTokenRequest, FlvTokenType};

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use super::cli::{CreateAuthTokenConfig, CliTokenType};

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Create Auth Token Request
pub fn process_sc_create_auth_token(
    server_addr: SocketAddr,
    auth_token_cfg: CreateAuthTokenConfig,
) -> Result<(), CliError> {
    let name = auth_token_cfg.token_name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, auth_token_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send create auth-token '{}': {}", name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let auth_token_resp = &response.results[0];
                let response = handle_sc_response(
                    &name,
                    "auth-token",
                    "created",
                    "",
                    &auth_token_resp.error_code,
                    &auth_token_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("cannot create auth-token '{}': communication error", name),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send delete request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    auth_token_cfg: CreateAuthTokenConfig,
) -> Result<FlvCreateAuthTokensResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&auth_token_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvCreateAuthTokens, &versions);

    trace!("create auth-token req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("create auth-token res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode CreateAuthTokenRequest in Fluvio format
fn encode_request(auth_token_cfg: &CreateAuthTokenConfig) -> FlvCreateAuthTokensRequest {
    // map token type
    let token_type = match auth_token_cfg.token_type {
        CliTokenType::Any => FlvTokenType::Any,
        CliTokenType::Custom => FlvTokenType::Custom,
        CliTokenType::Managed => FlvTokenType::Managed,
    };

    // generate Create AuthToken request
    let create_auth_token = CreateAuthTokenRequest {
        name: auth_token_cfg.token_name.clone(),
        auth_token: AuthTokenRequest {
            token_secret: auth_token_cfg.token_secret.clone(),
            token_type: token_type,
            min_spu: auth_token_cfg.min_spu,
            max_spu: auth_token_cfg.max_spu,
        },
    };

    // generate request with 1 auth-token
    FlvCreateAuthTokensRequest {
        auth_tokens: vec![create_auth_token],
    }
}
