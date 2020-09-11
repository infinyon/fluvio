//!
//! # register replica to listen
//!
//!
use std::fmt::Debug;

use kf_protocol::api::Request;
use kf_protocol::api::ReplicaKey;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;

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
