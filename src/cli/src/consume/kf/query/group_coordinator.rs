//!
//! # Kafka -- Find Group Coordinator
//!
//! Communicates with Kafka Controller to retrieve Group Coordinator for a consumer
//!
use log::trace;

use kf_protocol::message::group::KfFindCoordinatorRequest;
use kf_protocol::message::group::KfFindCoordinatorResponse;
use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::kf_lookup_version;

// Query Kafka server for Group Coordinator
pub async fn kf_group_coordinator<'a>(
    conn: &'a mut Connection,
    group_id: &'a String,
    versions: &'a KfApiVersions,
) -> Result<KfFindCoordinatorResponse, CliError> {
    let mut request = KfFindCoordinatorRequest::default();
    let version = kf_lookup_version(AllKfApiKey::FindCoordinator, versions);
    request.key = group_id.clone();

    trace!(
        "find group-coordinator req '{}': {:#?}",
        conn.server_addr(),
        request
    );

    let response = conn.send_request(request, version).await?;

    trace!(
        "find group-coordinator  res '{}': {:#?}",
        conn.server_addr(),
        response
    );

    Ok(response)
}
