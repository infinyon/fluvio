//!
//! # Query SC for SPU metadata
//!
//! Retrieve SPUs from SC
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::trace;
use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::FlvFetchSpusRequest;
use sc_api::spu::FlvFetchSpuResponse;
use sc_api::spu::FlvRequestSpuType;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;

/// Query Fluvio SC server for SPU and convert to SPU Metadata
pub fn query_spu_list_metadata(
    server_addr: SocketAddr,
    only_custom_spu: bool,
) -> Result<Vec<FlvFetchSpuResponse>, CliError> {
    run_block_on(send_request_to_server(server_addr, only_custom_spu)).map_err(|err| {
        CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("cannot retrieve spus: {}", err),
        ))
    })
}

/// Send query to server and retrieve a list of SPUs metadata or errors.
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
    only_custom_spu: bool,
) -> Result<Vec<FlvFetchSpuResponse>, CliError> {
    // look-up version
    let mut conn = Connection::new(&server_addr).await?;
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvFetchSpus, &versions);

    // generate request
    let mut request = FlvFetchSpusRequest::default();
    let req_type = match only_custom_spu {
        true => FlvRequestSpuType::Custom,
        false => FlvRequestSpuType::All,
    };
    request.req_spu_type = req_type;

    trace!("query spu req '{}': {:#?}", server_addr, request);

    let response = conn.send_request(request, version).await?;

    trace!("query spu res '{}': {:#?}", server_addr, response);

    Ok(response.spus)
}
