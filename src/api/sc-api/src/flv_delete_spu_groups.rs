//!
//! # Delete Spu Groups
//!
//! Public API to request the SC to delete one or more managed spu groups.
//!
//!

use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use crate::FlvResponseMessage;
use crate::ScApiKey;

// -----------------------------------
// FlvDeleteSpuGroupsRequest
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteSpuGroupsRequest {
    /// Each spu group in the delete request.
    pub spu_groups: Vec<String>,
}

// -----------------------------------
// FlvDeleteSpuGroupsResponse
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteSpuGroupsResponse {
    /// A response message for each delete spu group request
    pub results: Vec<FlvResponseMessage>,
}

// -----------------------------------
// Implementation - FlvDeleteSpuGroupsRequest
// -----------------------------------

impl Request for FlvDeleteSpuGroupsRequest {
    const API_KEY: u16 = ScApiKey::FlvDeleteSpuGroups as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvDeleteSpuGroupsResponse;
}
