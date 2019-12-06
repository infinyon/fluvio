//!
//! # Request API
//!
//! Defines supported APIs and provides Request fuctionality
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs::read_to_string;
use std::path::Path;

use structopt::clap::arg_enum;
use serde_json::to_string_pretty;
use serde::Serialize;
use serde::de::DeserializeOwned;

use kf_protocol::api::Request;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::connect_and_send_request;

// -----------------------------------
// Request Api
// -----------------------------------

arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum RequestApi {
        ApiVersions,
        ListOffset,
        Metadata,
        LeaderAndIsr,
        FindCoordinator,
        JoinGroup,
        SyncGroup,
        LeaveGroup,
        ListGroups,
        DescribeGroups,
        DeleteGroups,
        Heartbeat,
        OffsetFetch,
    }
}

// -----------------------------------
// Implementation - Generics
// -----------------------------------

/// Parse from file and return Request object
pub fn parse_request_from_file<P, R>(file_path: P) -> Result<R, CliError>
where
    P: AsRef<Path>,
    R: DeserializeOwned,
{
    let file_str: String = read_to_string(file_path)?;
    let list_offset_req: R = serde_json::from_str(&file_str)
        .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
    Ok(list_offset_req)
}

// Connect to Kafka Controller and process request
pub fn send_request_to_server<R>(server_addr: SocketAddr, request: R) -> Result<(), CliError>
where
    R: Request + Send + Sync + 'static,
    R::Response: Send + Sync + Serialize,
{
    // Send request without a version number, max_version will be used
    let response = run_block_on(connect_and_send_request(server_addr, request, None))?;
    let result = to_string_pretty(&response)
        .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
    println!("{}", result);
    Ok(())
}
