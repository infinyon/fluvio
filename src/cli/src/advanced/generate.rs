//!
//! # Generate Template
//!
//! Generate template for a Request API
//!
use structopt::StructOpt;
use std::io::Error as IoError;
use std::io::ErrorKind;

use kf_protocol::message::api_versions::KfApiVersionsRequest;
use kf_protocol::message::offset::KfListOffsetRequest;
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

use crate::advanced::RequestApi;
use crate::error::CliError;
use crate::Terminal;
use crate::t_println;

#[derive(Debug, StructOpt)]
pub struct GenerateTemplateOpt {
    /// Request API
    #[structopt(
        short = "r",
        long = "request",
        value_name = "",
        possible_values = &RequestApi::variants(),
        case_insensitive = true
    )]
    request: RequestApi,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Parse request API and generate template
pub fn process_generate_template<O>(out: std::sync::Arc<O>,opt: GenerateTemplateOpt) -> Result<(), CliError> 
    where O: Terminal
{
    let json = match opt.request {
        RequestApi::ApiVersions => serde_json::to_string_pretty(&KfApiVersionsRequest::default()),
        RequestApi::ListOffset => serde_json::to_string_pretty(&KfListOffsetRequest::default()),
        RequestApi::Metadata => serde_json::to_string_pretty(&KfMetadataRequest::default()),
        RequestApi::LeaderAndIsr => serde_json::to_string_pretty(&KfLeaderAndIsrRequest::default()),
        RequestApi::FindCoordinator => {
            serde_json::to_string_pretty(&KfFindCoordinatorRequest::default())
        }
        RequestApi::JoinGroup => serde_json::to_string_pretty(&KfJoinGroupRequest::default()),
        RequestApi::SyncGroup => serde_json::to_string_pretty(&KfSyncGroupRequest::default()),
        RequestApi::LeaveGroup => serde_json::to_string_pretty(&KfLeaveGroupRequest::default()),
        RequestApi::DescribeGroups => {
            serde_json::to_string_pretty(&KfDescribeGroupsRequest::default())
        }
        RequestApi::ListGroups => serde_json::to_string_pretty(&KfListGroupsRequest::default()),
        RequestApi::DeleteGroups => serde_json::to_string_pretty(&KfDeleteGroupsRequest::default()),
        RequestApi::Heartbeat => serde_json::to_string_pretty(&KfHeartbeatRequest::default()),
        RequestApi::OffsetFetch => serde_json::to_string_pretty(&KfOffsetFetchRequest::default()),
    };

    let result = json.map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
    t_println!(out,"{}", result);

    Ok(())
}
