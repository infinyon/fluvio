//!
//! # Kafka -- Send Heartbeat
//!
//! Communicates with Kafka Group Coordinator and sends heartbeat
//!
use log::trace;

use kf_protocol::message::group::KfHeartbeatRequest;
use kf_protocol::message::group::KfHeartbeatResponse;

use crate::error::CliError;
use crate::common::Connection;

// Send Heartbeat to group coordinator
pub async fn kf_send_heartbeat<'a>(
    conn: &'a mut Connection,
    group_id: &'a String,
    member_id: &'a String,
    generation_id: i32,
    version: Option<i16>,
) -> Result<KfHeartbeatResponse, CliError> {
    let mut request = KfHeartbeatRequest::default();

    // request with protocol
    request.group_id = group_id.clone();
    request.member_id = member_id.clone();
    request.generationid = generation_id;

    trace!("heartbeat req '{}': {:#?}", conn.server_addr(), request);

    let response = conn.send_request(request, version).await?;

    trace!("heartbeat  res '{}': {:#?}", conn.server_addr(), response);

    Ok(response)
}
