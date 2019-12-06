//!
//! # Kafka -- Join Group
//!
//! Communicates with Kafka Group Coordinator and request to join the group
//!
use log::trace;

use kf_protocol::message::group::KfJoinGroupRequest;
use kf_protocol::message::group::KfJoinGroupResponse;
use kf_protocol::message::group::JoinGroupRequestProtocol;
use kf_protocol::api::{ProtocolMetadata, Metadata};
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

// Ask Group Coordinator to join the group
pub async fn kf_join_group<'a>(
    conn: &'a mut Connection,
    topic_name: &'a String,
    group_id: &'a String,
    member_id: &'a String,
    versions: &'a KfApiVersions,
) -> Result<KfJoinGroupResponse, CliError> {
    let mut request = KfJoinGroupRequest::default();
    let version = kf_lookup_version(AllKfApiKey::JoinGroup, versions);

    // metadata
    let mut metadata = Metadata::default();
    metadata.topics = vec![topic_name.clone()];

    // protocol metadata
    let mut protocol_metadata = ProtocolMetadata::default();
    protocol_metadata.content = Some(metadata);

    // join group protocol
    let join_group_protocol = JoinGroupRequestProtocol {
        name: "range".to_owned(),
        metadata: protocol_metadata,
    };

    // request with protocol
    request.group_id = group_id.clone();
    request.session_timeout_ms = 10000;
    request.rebalance_timeout_ms = 300000;
    request.member_id = member_id.clone();
    request.protocol_type = "consumer".to_owned();
    request.protocols = vec![join_group_protocol];

    trace!("join-group req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("join-group  res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
