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
use crate::ApiError;

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteSpuGroupsRequest {
    /// Each spu group in the delete request.
    pub spu_groups: Vec<String>,
}

impl Request for FlvDeleteSpuGroupsRequest {
    const API_KEY: u16 = ScApiKey::FlvDeleteSpuGroups as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvDeleteSpuGroupsResponse;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvDeleteSpuGroupsResponse {
    /// A response message for each delete spu group request
    pub results: Vec<FlvResponseMessage>,
}

impl FlvDeleteSpuGroupsResponse {
    /// validate and extract a single response
    pub fn validate(self) -> Result<(), ApiError> {
        if let Some(item) = self.results.into_iter().find(|_| true) {
            item.as_result()
        } else {
            Err(ApiError::NoResourceFounded("spu group".to_owned()))
        }
    }
}
