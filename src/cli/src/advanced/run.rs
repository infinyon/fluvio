//!
//! # Run API request
//!
//! Send Request to server
//!

use structopt::StructOpt;
use std::path::PathBuf;
use std::io::Error as IoError;
use std::io::ErrorKind;

use kf_protocol::message::offset::KfListOffsetRequest;
use kf_protocol::message::api_versions::KfApiVersionsRequest;
use kf_protocol::message::group::KfListGroupsRequest;
use kf_protocol::message::group::KfJoinGroupRequest;
use kf_protocol::message::group::KfSyncGroupRequest;
use kf_protocol::message::group::KfLeaveGroupRequest;
use kf_protocol::message::group::KfDescribeGroupsRequest;
use kf_protocol::message::group::KfDeleteGroupsRequest;
use kf_protocol::message::group::KfFindCoordinatorRequest;
use kf_protocol::message::group::KfHeartbeatRequest;
use kf_protocol::message::metadata::KfMetadataRequest;
use kf_protocol::message::isr::KfLeaderAndIsrRequest;
use kf_protocol::message::offset::KfOffsetFetchRequest;

use crate::error::CliError;

use crate::profile::{ProfileConfig, TargetServer};
use crate::advanced::RequestApi;
use crate::advanced::send_request_to_server;
use crate::advanced::parse_request_from_file;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct RunRequestOpt {
    /// Request API
    #[structopt(
        short = "r",
        long = "request",
        value_name = "",
        raw(possible_values = "&RequestApi::variants()", case_insensitive = "true")
    )]
    request: RequestApi,

    /// Address of Kafka Controller
    #[structopt(short = "k", long = "kf", value_name = "host:port")]
    kf: Option<String>,

    /// Request details file
    #[structopt(
        short = "j",
        long = "json-file",
        value_name = "file.json",
        parse(from_os_str)
    )]
    details_file: PathBuf,

    ///Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Parse CLI, build server address, run request & display result
pub fn process_run_request(opt: RunRequestOpt) -> Result<(), CliError> {
    let profile_config = ProfileConfig::new(&None, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let server_addr = match target_server {
        TargetServer::Kf(server_addr) => server_addr,
        TargetServer::Sc(server_addr) => server_addr,
        _ => {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("invalid Kafka server {:?}", target_server),
            )))
        }
    };

    // process file based on request type
    match opt.request {
        RequestApi::ApiVersions => {
            let req =
                parse_request_from_file::<PathBuf, KfApiVersionsRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::ListOffset => {
            let req =
                parse_request_from_file::<PathBuf, KfListOffsetRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::Metadata => {
            let req =
                parse_request_from_file::<PathBuf, KfMetadataRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::LeaderAndIsr => {
            let req = parse_request_from_file::<PathBuf, KfLeaderAndIsrRequest>(
                opt.details_file.clone(),
            )?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::FindCoordinator => {
            let req = parse_request_from_file::<PathBuf, KfFindCoordinatorRequest>(
                opt.details_file.clone(),
            )?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::JoinGroup => {
            let req =
                parse_request_from_file::<PathBuf, KfJoinGroupRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::SyncGroup => {
            let req =
                parse_request_from_file::<PathBuf, KfSyncGroupRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::LeaveGroup => {
            let req =
                parse_request_from_file::<PathBuf, KfLeaveGroupRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::DescribeGroups => {
            let req = parse_request_from_file::<PathBuf, KfDescribeGroupsRequest>(
                opt.details_file.clone(),
            )?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::ListGroups => {
            let req =
                parse_request_from_file::<PathBuf, KfListGroupsRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::DeleteGroups => {
            let req = parse_request_from_file::<PathBuf, KfDeleteGroupsRequest>(
                opt.details_file.clone(),
            )?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::Heartbeat => {
            let req =
                parse_request_from_file::<PathBuf, KfHeartbeatRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
        RequestApi::OffsetFetch => {
            let req =
                parse_request_from_file::<PathBuf, KfOffsetFetchRequest>(opt.details_file.clone())?;
            send_request_to_server(server_addr, req)
        }
    }
}
