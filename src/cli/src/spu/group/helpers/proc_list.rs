//!
//! # Query SC for SPU Group metadata
//!
//! Retrieve SPU Groups from SC
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::trace;
use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::spu::FlvFetchSpuGroupsRequest;
use sc_api::spu::FlvFetchSpuGroupsResponse;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;

/// Query Fluvio SC server for SPU Groups and convert to SPU Metadata
pub fn query_spu_group_metadata(
    server_addr: SocketAddr,
) -> Result<FlvFetchSpuGroupsResponse, CliError> {
    run_block_on(send_request_to_server(server_addr)).map_err(|err| {
        CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("cannot retrieve spu groups: {}", err),
        ))
    })
}

/// Send query to server and retrieve a list of SPU Groups or errors.
async fn send_request_to_server<'a>(
    server_addr: SocketAddr,
) -> Result<FlvFetchSpuGroupsResponse, CliError> {
    // look-up version
    let mut conn = Connection::new(&server_addr).await?;
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvFetchSpuGroups, &versions);

    // generate request
    trace!("query spu group req '{}'", server_addr);

    let request = FlvFetchSpuGroupsRequest::default();
    let response = conn.send_request(request, version).await?;

    trace!("query spu group res '{}': {:#?}", server_addr, response);

    Ok(response)
}
