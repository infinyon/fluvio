//!
//! # Fluvio SC - Process Create Group
//!
//! Sends Create SPU Group request to Fluvio Streaming Controller
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::FlvCreateSpuGroupsResponse;
use sc_api::spu::FlvCreateSpuGroupRequest;
use sc_api::spu::FlvCreateSpuGroupsRequest;


use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;


// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Create Spu Group Request
pub fn process_create_spu_group(
    server_addr: SocketAddr,
    spg_request: FlvCreateSpuGroupRequest,
) -> Result<(), CliError> {

    let name = spg_request.name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, spg_request)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send create spu-group '{}': {}", name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let spu_group_resp = &response.results[0];
                let response = handle_sc_response(
                    &name,
                    "spu-group",
                    "created",
                    "",
                    &spu_group_resp.error_code,
                    &spu_group_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("cannot create spu group '{}': communication error", name),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send create request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    spu_request: FlvCreateSpuGroupRequest,
) -> Result<FlvCreateSpuGroupsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request: FlvCreateSpuGroupsRequest = spu_request.into();
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvCreateSpuGroups, &versions);

    trace!("create spu-group req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("create spu-group res '{}': {:#?}", server_addr, response);

    Ok(response)
}

