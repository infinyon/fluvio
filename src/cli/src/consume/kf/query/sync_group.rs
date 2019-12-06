//!
//! # Kafka -- Sync Group
//!
//! Communicates with Kafka Group Coordinator and request to sync the group
//!
use log::trace;

use kf_protocol::message::group::KfSyncGroupRequest;
use kf_protocol::message::group::KfSyncGroupResponse;
use kf_protocol::message::group::SyncGroupRequestAssignment;
use kf_protocol::api::{GroupAssignment, Assignment};
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

// Query Kafka server for Group Coordinator
pub async fn kf_sync_group<'a>(
    conn: &'a mut Connection,
    topic_name: &'a String,
    group_id: &'a String,
    member_id: &'a String,
    generation_id: i32,
    versions: &'a KfApiVersions,
) -> Result<KfSyncGroupResponse, CliError> {
    let mut request = KfSyncGroupRequest::default();
    let version = kf_lookup_version(AllKfApiKey::SyncGroup, versions);

    // assignment
    let mut assignment = Assignment::default();
    assignment.topics = vec![topic_name.clone()];
    assignment.reserved_i32 = 1;

    // group assignment
    let mut group_assignment = GroupAssignment::default();
    group_assignment.content = Some(assignment);

    // sync group assignment reqeust
    let sync_group_assignment = SyncGroupRequestAssignment {
        member_id: member_id.clone(),
        assignment: group_assignment,
    };

    // sync group request
    request.group_id = group_id.clone();
    request.generation_id = generation_id;
    request.member_id = member_id.clone();
    request.assignments = vec![sync_group_assignment];

    trace!("sync-group req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("sunc-group  res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
