#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;
use fluvio_controlplane_metadata::message::ReplicaMsg;
use fluvio_controlplane_metadata::partition::Replica;
use crate::InternalSpuApi;

/// Changes to Replica Specs
#[derive(Decoder, Encoder, Debug, Default)]
pub struct UpdateReplicaRequest {
    pub epoch: i64,
    pub changes: Vec<ReplicaMsg>,
    pub all: Vec<Replica>,
}

impl Request for UpdateReplicaRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateReplica as u16;
    type Response = UpdateReplicaResponse;
}

impl UpdateReplicaRequest {
    pub fn with_changes(epoch: i64, changes: Vec<ReplicaMsg>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<Replica>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateReplicaResponse {}
