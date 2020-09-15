//!
//! # register replica to listen
//!
//!
use std::fmt::Debug;

use fluvio_protocol::api::Request;
use fluvio_protocol::derive::Decode;
use fluvio_protocol::derive::Encode;
use dataplane_protocol::ReplicaKey;

use super::SpuServerApiKey;

#[derive(Decode, Encode, Default, Debug)]
pub struct RegisterSyncReplicaRequest {
    pub leader_replicas: Vec<ReplicaKey>,
}

impl Request for RegisterSyncReplicaRequest {
    const API_KEY: u16 = SpuServerApiKey::RegisterSyncReplicaRequest as u16;
    const DEFAULT_API_VERSION: i16 = 10;
    type Response = RegisterSyncReplicaResponse;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct RegisterSyncReplicaResponse {}
