//!
//! # Delete Custon SPUs
//!
//! Public API to request the SC to delete one or more custom spus.
//!
//!
use kf_protocol::api::Request;
use kf_protocol::derive::{Decode, Encode};

use super::ApiError;
use super::FlvResponseMessage;
use super::ScServerApiKey;
use super::common::flv_spus::FlvCustomSpu;

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvUnregisterCustomSpusRequest {
    /// Each spu name or id to be deleted.
    pub custom_spus: Vec<FlvCustomSpu>,
}

impl Request for FlvUnregisterCustomSpusRequest {
    const API_KEY: u16 = ScServerApiKey::FlvUnregisterCustomSpus as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvUnregisterCustomSpusResponse;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvUnregisterCustomSpusResponse {
    /// A response message for each delete request
    pub results: Vec<FlvResponseMessage>,
}

impl FlvUnregisterCustomSpusResponse {
    /// validate and extract a single response
    pub fn validate(self) -> Result<(), ApiError> {
        // ? what is name, so just find first item
        if let Some(item) = self.results.into_iter().find(|_| true) {
            item.as_result()
        } else {
            Err(ApiError::NoResourceFounded("custom spu".to_owned()))
        }
    }
}
