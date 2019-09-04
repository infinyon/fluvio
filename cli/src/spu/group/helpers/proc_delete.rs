//!
//! # Fluvio SC - Delete Processing
//!
//! Sends Delete SPU group request to Fluvio Streaming Controller
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::net::SocketAddr;

use log::trace;

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::{FlvDeleteSpuGroupsRequest, FlvDeleteSpuGroupsResponse};

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;
use crate::common::handle_sc_response;

use crate::spu::group::delete::DeleteManagedSpuGroupConfig;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

// Connect to Fluvio Streaming Controller and process Delete Spu Group Request
pub fn process_delete_spu_group(
    server_addr: SocketAddr,
    spu_group_cfg: DeleteManagedSpuGroupConfig,
) -> Result<(), CliError> {
    let name = spu_group_cfg.name.clone();

    // Run command and collect results
    match run_block_on(send_request_to_server(server_addr, spu_group_cfg)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("send delete spu-group '{}': {}", &name, err),
        ))),
        Ok(response) => {
            if response.results.len() > 0 {
                let spu_group_resp = &response.results[0];
                let response = handle_sc_response(
                    &name,
                    "spu-group",
                    "deleted",
                    "",
                    &spu_group_resp.error_code,
                    &spu_group_resp.error_message,
                )?;
                println!("{}", response);

                Ok(())
            } else {
                Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("cannot delete spu-group '{}': communication error", &name),
                )))
            }
        }
    }
}

/// Connect to server, get version, and send delete request
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    spu_group_cfg: DeleteManagedSpuGroupConfig,
) -> Result<FlvDeleteSpuGroupsResponse, CliError> {
    let mut conn = Connection::new(&server_addr).await?;
    let request = encode_request(&spu_group_cfg);
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvDeleteSpuGroups, &versions);

    trace!("delete spu-group req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("delete spu-group res '{}': {:#?}", server_addr, response);

    Ok(response)
}

/// encode DeleteManagedSpuGroupConfig in Fluvio format
fn encode_request(spu_group_cfg: &DeleteManagedSpuGroupConfig) -> FlvDeleteSpuGroupsRequest {
    // generate request with 1 spu group
    FlvDeleteSpuGroupsRequest {
        spu_groups: vec![spu_group_cfg.name.clone()],
    }
}
