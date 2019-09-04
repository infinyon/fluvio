//!
//! # Create SPU Groups
//!
//! Public API to request the SC to create managed spu groups
//!
//!
use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use crate::FlvResponseMessage;
use crate::ScApiKey;

// -----------------------------------
// FlvCreateSpuGroupsRequest
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateSpuGroupsRequest {
    /// A list of one or more spu groups to be created.
    pub spu_groups: Vec<FlvCreateSpuGroupRequest>,
}

// quick way to convert a single group into groups requests
impl From<FlvCreateSpuGroupRequest> for FlvCreateSpuGroupsRequest {
    fn from(group: FlvCreateSpuGroupRequest) -> Self {
        let mut groups = Self::default();
        groups.spu_groups.push(group);
        groups
    }
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateSpuGroupRequest {
    /// The name of the managed spu group
    pub name: String,

    /// The number of replicas for the spu group
    pub replicas: u16,

    /// The base spu id that the spu group uses to increment the spu ids
    /// Note: Spu id is a globally unique resource and it cannot be shared
    pub min_id: Option<i32>,

    /// Configuration elements to be applied to each SPUs in the group
    pub config: FlvGroupConfig,

    /// The rack to be used for all SPUs in the group. Racks are used by
    /// replication assignment algorithm
    pub rack: Option<String>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvGroupConfig {
    pub storage: Option<FlvStorageConfig>,
    pub replication: Option<FlvReplicationConfig>,
    pub env: Vec<FlvEnvVar>
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvStorageConfig {
    pub log_dir: Option<String>,
    pub size: Option<String>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvReplicationConfig {
    pub in_sync_replica_min: Option<u16>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvEnvVar {
    pub name: String,
    pub value: String,
}

// -----------------------------------
// FlvCreateSpuGroupsResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvCreateSpuGroupsResponse {
    /// The spu group creation result messages.
    pub results: Vec<FlvResponseMessage>,
}

// -----------------------------------
// Implementation - FlvCreateSpuGroupsRequest
// -----------------------------------

impl Request for FlvCreateSpuGroupsRequest {
    const API_KEY: u16 = ScApiKey::FlvCreateSpuGroups as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvCreateSpuGroupsResponse;
}
