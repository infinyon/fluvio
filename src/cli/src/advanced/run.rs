//!
//! # Run API request
//!
//! Send Request to server
//!

use structopt::StructOpt;
use std::path::PathBuf;

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

use flv_client::profile::KfConfig;
use crate::advanced::RequestApi;
use crate::Terminal;

use super::parse_and_pretty_from_file;

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
        possible_values = &RequestApi::variants(),
        case_insensitive = true
    )]
    request: RequestApi,

    /// Address of Kafka Controller
    #[structopt(short = "k", long = "kf", value_name = "host:port")]
    kf: String,

    /// Request details file
    #[structopt(
        short = "j",
        long = "json-file",
        value_name = "file.json",
        parse(from_os_str)
    )]
    details_file: PathBuf,
}

macro_rules! pretty_send {
    ($out:expr,$r:ident,$client:ident,$file:ident) => {
        parse_and_pretty_from_file::<PathBuf, $r, _>($out, &mut $client, $file)
    };
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Parse CLI, build server address, run request & display result
pub async fn process_run_request<O>(
    out: std::sync::Arc<O>,
    opt: RunRequestOpt,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let kf_config = KfConfig::new(opt.kf);
    let mut kf_server = kf_config.connect().await?;
    let mut client = kf_server.mut_client();
    let file = opt.details_file.to_path_buf();

    match opt.request {
        RequestApi::ApiVersions => pretty_send!(out, KfApiVersionsRequest, client, file).await,
        RequestApi::ListOffset => pretty_send!(out, KfListOffsetRequest, client, file).await,
        RequestApi::Metadata => pretty_send!(out, KfMetadataRequest, client, file).await,
        RequestApi::LeaderAndIsr => pretty_send!(out, KfLeaderAndIsrRequest, client, file).await,
        RequestApi::FindCoordinator => {
            pretty_send!(out, KfFindCoordinatorRequest, client, file).await
        }
        RequestApi::JoinGroup => pretty_send!(out, KfJoinGroupRequest, client, file).await,
        RequestApi::SyncGroup => pretty_send!(out, KfSyncGroupRequest, client, file).await,
        RequestApi::LeaveGroup => pretty_send!(out, KfLeaveGroupRequest, client, file).await,
        RequestApi::DescribeGroups => {
            pretty_send!(out, KfDescribeGroupsRequest, client, file).await
        }
        RequestApi::ListGroups => pretty_send!(out, KfListGroupsRequest, client, file).await,
        RequestApi::DeleteGroups => pretty_send!(out, KfDeleteGroupsRequest, client, file).await,
        RequestApi::Heartbeat => pretty_send!(out, KfHeartbeatRequest, client, file).await,
        RequestApi::OffsetFetch => pretty_send!(out, KfOffsetFetchRequest, client, file).await,
    }
}
