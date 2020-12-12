#![allow(clippy::assign_op_pattern)]

use std::fmt;

use dataplane::api::Request;
use dataplane::derive::Decode;
use dataplane::derive::Encode;
use fluvio_controlplane_metadata::partition::ReplicaKey;


use crate::InternalScKey;

/// Confirmation of Replica replica
#[derive(Decode, Encode, Debug, Default, Clone)]
pub struct ReplicaRemovedRequest {
    pub id: ReplicaKey,
    pub confirm: bool       // replica remove confirmed
}

impl ReplicaRemovedRequest {
    pub fn new(id: ReplicaKey,confirm: bool) -> Self {
        Self { 
            id,
            confirm
         }
    }

}

impl fmt::Display for ReplicaRemovedRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "replica delete {}, confirm: {}", self.id,self.confirm)
    }
}

impl Request for ReplicaRemovedRequest {
    const API_KEY: u16 = InternalScKey::ReplicaRemoved as u16;
    type Response = ReplicaRemovedResponse;
}

#[derive(Decode, Encode, Default, Debug)]
pub struct ReplicaRemovedResponse {}
