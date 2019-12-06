//!
//! # Kafka -- Leave Group
//!
//! Communicates with Kafka Group Coordinator and request to leave the group
//!
use log::trace;

use kf_protocol::message::group::KfLeaveGroupRequest;
use kf_protocol::message::group::KfLeaveGroupResponse;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

// Ask Group Coordinator to leave the group
pub async fn kf_leave_group<'a>(
    conn: &'a mut Connection,
    group_id: &'a String,
    member_id: &'a String,
    versions: &'a KfApiVersions,
) -> Result<KfLeaveGroupResponse, CliError> {
    let mut request = KfLeaveGroupRequest::default();
    let version = kf_lookup_version(AllKfApiKey::LeaveGroup, versions);

    // request with protocol
    request.group_id = group_id.clone();
    request.member_id = member_id.clone();

    trace!("leave-group req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("leave-group  res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
