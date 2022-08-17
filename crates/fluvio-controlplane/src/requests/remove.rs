#![allow(clippy::assign_op_pattern)]

use std::fmt;

use fluvio_protocol::api::Request;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::InternalScKey;

/// Confirmation of Replica replica
#[derive(Decoder, Encoder, Debug, Default, Clone)]
pub struct ReplicaRemovedRequest {
    pub id: ReplicaKey,
    pub confirm: bool, // replica remove confirmed
}

impl ReplicaRemovedRequest {
    pub fn new(id: ReplicaKey, confirm: bool) -> Self {
        Self { id, confirm }
    }
}

impl fmt::Display for ReplicaRemovedRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "replica delete {}, confirm: {}", self.id, self.confirm)
    }
}

impl Request for ReplicaRemovedRequest {
    const API_KEY: u16 = InternalScKey::ReplicaRemoved as u16;
    type Response = ReplicaRemovedResponse;
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct ReplicaRemovedResponse {}
